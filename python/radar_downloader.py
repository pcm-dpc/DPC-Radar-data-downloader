 
#!/usr/bin/env python3
"""
RADAR-DPC continuous downloader (STOMP over WebSocket → REST → S3 presigned URL)

Features
- Subscribe to /topic/product on wss://websocket.geosdi.org/wide-websocket (STOMP 1.2)
- Filter by a configurable allowlist of product types (e.g., VMI,SRI,TEMP)
- For matching messages, POST to the downloadProduct API to obtain a presigned URL
- Stream-download the file to disk, preserving the S3 "key" path under OUTPUT_DIR
- Auto-reconnect with backoff, deduplication window, graceful shutdown (Ctrl+C)
- Minimal dependencies: websocket-client, requests

Usage
    python radar_downloader.py --products VMI,SRI,TEMP --output ./downloads

Environment variables (optional, override defaults)
    RADAR_WS_URL
    RADAR_WS_TOPIC
    RADAR_API_ENDPOINT
    RADAR_PRODUCTS
    RADAR_OUTPUT_DIR
"""
import argparse
import json
import logging
import os
import queue
import signal
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Set

import requests
from websocket import WebSocketApp

# ---------------------- Defaults / Config ----------------------
WS_URL_DEFAULT = os.getenv("RADAR_WS_URL", "wss://websocket.geosdi.org/wide-websocket")
WS_TOPIC_DEFAULT = os.getenv("RADAR_WS_TOPIC", "/topic/product")
API_ENDPOINT_DEFAULT = os.getenv(
    "RADAR_API_ENDPOINT",
    "https://wagiqofvnk.execute-api.eu-south-1.amazonaws.com/prod/downloadProduct"
)
PRODUCTS_DEFAULT = os.getenv("RADAR_PRODUCTS", "VMI,SRI,TEMP")
OUTPUT_DIR_DEFAULT = os.getenv("RADAR_OUTPUT_DIR", "./downloads")

BACKOFF_MIN = 1.0
BACKOFF_MAX = 30.0
DOWNLOAD_TIMEOUT = (15, 120)  # (connect, read) seconds

def print_start_banner():
    try:
        import shutil
        width = max(60, min(100, shutil.get_terminal_size((100, 20)).columns))
    except Exception:
        width = 80

    title = "Radar DPC Data Downloader"
    subtitle = "Dipartimento della Protezione Civile Nazionale"
    byline = "Sviluppato da Laboratorio GEOSDI"

    def center(s): 
        return s.center(width)

    line = "═" * width
    top = "╔" + line + "╗"
    bottom = "╚" + line + "╝"
    print()
    print(top)
    print(center(""))
    print(center(title))
    print(center(subtitle))
    print(center(byline))
    print(center(""))
    print(bottom)
    print()

POST_TIMEOUT = 15

# ---------------------- Helpers ----------------------
def ensure_relative_path(key: str) -> Path:
    """
    Sanitize S3 'key' to ensure a safe relative path (prevent path traversal).
    """
    # Normalize and drop any leading slashes
    p = Path(key.lstrip("/")).as_posix()
    # Block any attempts to go upward
    parts = [seg for seg in p.split("/") if seg not in ("", ".", "..")]
    return Path(*parts)


def safe_join(root: Path, rel: Path) -> Path:
    """
    Join root and rel ensuring the result stays under root.
    """
    candidate = (root / rel).resolve()
    root = root.resolve()
    if not str(candidate).startswith(str(root)):
        raise ValueError("Unsafe path join detected")
    return candidate


def chunked_download(url: str, dest: Path, chunk_size: int = 1_048_576) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
    tmp.replace(dest)


# ---------------------- Downloader worker ----------------------
class Downloader(threading.Thread):
    def __init__(self, jobs: "queue.Queue[dict]", output_dir: Path, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.jobs = jobs
        self.output_dir = output_dir
        self.stop_event = stop_event
        self.session = requests.Session()

    def run(self):
        while not self.stop_event.is_set():
            try:
                job = self.jobs.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.process_job(job)
            except Exception as e:
                logging.exception("Download job failed: %s", e)
            finally:
                self.jobs.task_done()

    def process_job(self, job: dict):
        product_type = job["productType"]
        product_date = job["productDate"]  # epoch ms

        # 1) Ask API for presigned URL
        payload = {"productType": product_type, "productDate": product_date}
        logging.debug("POST %s payload=%s", API_ENDPOINT_DEFAULT, payload)
        r = self.session.post(API_ENDPOINT_DEFAULT, json=payload, timeout=POST_TIMEOUT)
        r.raise_for_status()
        info = r.json()

        key = info.get("key")
        url = info.get("url")
        if not key or not url:
            raise ValueError(f"Malformed response from API: {info}")

        # 2) Download file
        rel = ensure_relative_path(key)
        dest = safe_join(self.output_dir, rel)

        # Avoid re-downloading if already exists with size > 0
        if dest.exists() and dest.stat().st_size > 0:
            logging.info("Skip existing file: %s", dest)
            return

        logging.info("⬇️  Downloading → %s", dest)
        chunked_download(url, dest)
        logging.info("✅ Done → %s", dest)


# ---------------------- STOMP over WebSocket ----------------------
class StompClient:
    def __init__(self, ws_url: str, topic: str, on_product, stop_event: threading.Event):
        self.ws_url = ws_url
        self.topic = topic
        self.on_product = on_product
        self.stop_event = stop_event
        self.ws: Optional[WebSocketApp] = None
        self.connected = False
        self.subscribed = False
        self._recv_buf = ""
        self._hb_send_interval = None  # seconds
        self._hb_last_recv = time.time()
        self._hb_thread = None


    # ---- STOMP framing helpers ----
    @staticmethod
    def _frame(command: str, headers: Dict[str, str], body: str = "") -> str:
        lines = [command]
        for k, v in headers.items():
            lines.append(f"{k}:{v}")
        # blank line, then body, then \x00
        return "\n".join(lines) + "\n\n" + body + "\x00"

    def _send_connect(self):
        headers = {
            "accept-version": "1.2",
            "host": "websocket.geosdi.org",
            "heart-beat": "10000,10000",
        }
        frame = self._frame("CONNECT", headers)
        self.ws.send(frame)

    def _send_subscribe(self):
        headers = {
            "id": "sub-0",
            "destination": self.topic,
            "ack": "auto",
        }
        frame = self._frame("SUBSCRIBE", headers)
        self.ws.send(frame)
        self.subscribed = True

    
    def _hb_loop(self):
        # Send "\n" at negotiated interval; monitor liveness
        while not self.stop_event.is_set():
            interval = self._hb_send_interval or 10.0
            time.sleep(interval * 0.9)
            try:
                if self.ws and self.connected:
                    self.ws.send("\n")  # STOMP heart-beat
            except Exception as e:
                logging.debug("Heart-beat send failed: %s", e)
            # Optional: detect stalled connection (no recv for 3x interval)
            if time.time() - self._hb_last_recv > max(30.0, 3 * interval):
                logging.warning("No STOMP heart-beat received for %.1fs; server might have stalled", time.time() - self._hb_last_recv)

    # ---- WebSocket callbacks ----
    def _on_open(self, ws):
        logging.info("WebSocket opened")
        self.connected = False
        self.subscribed = False
        self._send_connect()

    def _on_message(self, ws, message):
        # STOMP frames may be delivered one-by-one by websocket-client.
        # Each frame ends with \x00. There could be multiple frames in one message.
        self._recv_buf += message
        # Handle pure heart-beat newlines (no null terminator)
        if self._recv_buf == "\n":
            self._hb_last_recv = time.time()
            self._recv_buf = ""
            return
        while "\x00" in self._recv_buf:
            frame, self._recv_buf = self._recv_buf.split("\x00", 1)
            frame = frame.replace("\r\n", "\n")
            header_part, _, body = frame.partition("\n\n")
            lines = [ln for ln in header_part.split("\n") if ln.strip()]
            if not lines:
                continue
            command = lines[0].strip()
            headers = {}
            for ln in lines[1:]:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    headers[k.strip()] = v.strip()

            # refresh last-recv on any parsed frame
            self._hb_last_recv = time.time()
            if command == "CONNECTED":
                self.connected = True
                # (deduped) CONNECTED log handled below
                self.connected = True
                logging.info("STOMP CONNECTED: version=%s", headers.get("version"))
                # Negotiate heart-beats
                # client: 10s/10s (CONNECT); server responds "server,client" in ms
                srv_hb = headers.get("heart-beat", "0,0")
                try:
                    sx, sy = [int(x) for x in srv_hb.split(",")]
                except Exception:
                    sx, sy = 0, 0
                # We advertised 10000,10000 in CONNECT; choose max(client_send, server_accept)
                client_send_ms = 10000
                server_accept_ms = sy
                interval_ms = max(client_send_ms, server_accept_ms) if (client_send_ms and server_accept_ms) else client_send_ms or server_accept_ms
                self._hb_send_interval = (interval_ms or 10000) / 1000.0
                self._hb_last_recv = time.time()
                if not self._hb_thread or not self._hb_thread.is_alive():
                    self._hb_thread = threading.Thread(target=self._hb_loop, daemon=True)
                    self._hb_thread.start()

                if not self.subscribed:
                    self._send_subscribe()
            elif command == "MESSAGE":
                self._hb_last_recv = time.time()
                self._handle_message(headers, body)
            elif command == "ERROR":
                logging.error("STOMP ERROR: %s | %s", headers, body)
            # Other frames ignored for brevity

    def _handle_message(self, headers: Dict[str, str], body: str):
        try:
            data = json.loads(body.strip())
        except Exception:
            logging.warning("Invalid JSON body: %r", body[:200])
            return

        product_type = data.get("productType")
        epoch_ms = data.get("time")
        if product_type and isinstance(epoch_ms, (int, float)):
            self.on_product(product_type, int(epoch_ms))

    def _on_close(self, ws, code, reason):
        logging.warning("WebSocket closed: code=%s reason=%s", code, reason)
        self.connected = False
        self.subscribed = False

    def _on_error(self, ws, error):
        logging.error("WebSocket error: %s", error)

    def run_forever(self):
        backoff = BACKOFF_MIN
        while not self.stop_event.is_set():
            self.ws = WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_close=self._on_close,
                on_error=self._on_error,
                subprotocols=["v12.stomp"],
                header=["Origin: https://websocket.geosdi.org"],
            )
            try:
                self.ws.run_forever(ping_interval=0)  # disable ws-level pings; we use STOMP heart-beats
            except Exception as e:
                logging.error("run_forever exception: %s", e)

            if self.stop_event.is_set():
                break

            # Exponential backoff before reconnect
            logging.info("Reconnecting in %.1fs ...", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)


# ---------------------- Main app ----------------------
class App:
    def __init__(self, products: Set[str], output_dir: Path, workers: int = 3):
        self.products = {p.strip().upper() for p in products if p.strip()}
        self.output_dir = output_dir
        self.jobs: "queue.Queue[dict]" = queue.Queue(maxsize=1000)
        self.stop_event = threading.Event()
        self.dedup: Dict[str, float] = {}  # key: f"{ptype}:{ms}" -> timestamp seen
        self.lock = threading.Lock()

        # Start workers
        self.workers = [Downloader(self.jobs, self.output_dir, self.stop_event) for _ in range(workers)]
        for w in self.workers:
            w.start()

        # STOMP client
        self.stomp = StompClient(WS_URL_DEFAULT, WS_TOPIC_DEFAULT, self.on_product, self.stop_event)

        # Start GC thread to cleanup dedup map
        self.gc_thread = threading.Thread(target=self._gc_loop, daemon=True)
        self.gc_thread.start()

    def _gc_loop(self):
        while not self.stop_event.is_set():
            now = time.time()
            with self.lock:
                # keep at most 3 hours
                cutoff = now - 3 * 3600
                to_del = [k for k, ts in self.dedup.items() if ts < cutoff]
                for k in to_del:
                    del self.dedup[k]
            time.sleep(300)

    def on_product(self, product_type: str, epoch_ms: int):
        p = product_type.upper()
        if p not in self.products:
            logging.debug("Ignoring product %s", p)
            return

        key = f"{p}:{epoch_ms}"
        with self.lock:
            if key in self.dedup:
                logging.debug("Duplicate %s (ignored)", key)
                return
            self.dedup[key] = time.time()

        # Enqueue download job
        job = {"productType": p, "productDate": epoch_ms}
        try:
            self.jobs.put_nowait(job)
            ts = datetime.utcfromtimestamp(epoch_ms / 1000.0)
            logging.debug("Enqueued %s at %sZ", p, ts.strftime("%Y-%m-%d %H:%M"))
        except queue.Full:
            logging.warning("Job queue full; dropping %s", job)

    def start(self):
        # Run STOMP client in main thread; block until stop
        def handle_sig(sig, frame):
            logging.info("Signal %s received, shutting down...", sig)
            self.stop_event.set()

        signal.signal(signal.SIGINT, handle_sig)
        signal.signal(signal.SIGTERM, handle_sig)

        logging.info("Listening products: %s", ",".join(sorted(self.products)))
        logging.info("Writing files under: %s", self.output_dir.resolve())

        self.stomp.run_forever()

        # Graceful stop
        self.stop_event.set()
        logging.info("Waiting for pending downloads to finish...")
        self.jobs.join()
        logging.info("Bye.")

# ---------------------- CLI ----------------------
def main():
    print_start_banner()
    parser = argparse.ArgumentParser(description="RADAR-DPC continuous downloader")
    parser.add_argument("--products", type=str, default=PRODUCTS_DEFAULT,
                        help="Comma-separated list of product types to download (default: VMI,SRI,TEMP)")
    parser.add_argument("--output", type=str, default=OUTPUT_DIR_DEFAULT,
                        help="Output directory to store files (default: ./downloads)")
    parser.add_argument("--workers", type=int, default=3, help="Parallel download workers (default: 3)")
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level (default: INFO)")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    products = [p.strip() for p in args.products.split(",") if p.strip()]
    output_dir = Path(args.output)

    app = App(set(products), output_dir, workers=args.workers)
    app.start()


if __name__ == "__main__":
    main()
