#!/usr/bin/env python3
"""
RADAR-DPC continuous downloader (Pure WebSocket → REST → S3 presigned URL)

- Connect to pure WebSocket: wss://radar-wss.protezionecivile.it
- Filter by allowlist product types
- Call downloadProduct API -> presigned URL -> download to disk
- Auto-reconnect, dedup, graceful shutdown
"""

import argparse
import json
import logging
import os
import queue
import signal
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Union, List

import requests
from websocket import WebSocketApp

# ---------------------- Defaults / Config ----------------------
WS_URL_DEFAULT = os.getenv("RADAR_WS_URL", "wss://radar-wss.protezionecivile.it")
WS_SUBSCRIBE_DEFAULT = os.getenv("RADAR_WS_SUBSCRIBE", "")  # optional payload to send after connect

# Application-level heartbeat (message routed to $default)
WS_HEARTBEAT_INTERVAL = int(os.getenv("RADAR_WS_HEARTBEAT_INTERVAL", "30"))  # seconds
WS_HEARTBEAT_ENABLED = os.getenv("RADAR_WS_HEARTBEAT_ENABLED", "1").lower() not in ("0", "false", "no")

# IMPORTANT: CloudFront/WAF often blocks non-browser UA; set a "browser-like" default UA
WS_USER_AGENT_DEFAULT = os.getenv(
    "RADAR_WS_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
WS_ORIGIN_DEFAULT = os.getenv("RADAR_WS_ORIGIN", "https://radar.protezionecivile.it")

API_ENDPOINT_DEFAULT = os.getenv(
    "RADAR_API_ENDPOINT",
    "https://radar-api.protezionecivile.it/downloadProduct"
)
PRODUCTS_DEFAULT = os.getenv("RADAR_PRODUCTS", "VMI,SRI,TEMP")
OUTPUT_DIR_DEFAULT = os.getenv("RADAR_OUTPUT_DIR", "./downloads")

BACKOFF_MIN = 1.0
BACKOFF_MAX = 30.0

# WebSocket keepalive (ping/pong)
WS_PING_INTERVAL = 20  # seconds
WS_PING_TIMEOUT = 10   # seconds

DOWNLOAD_TIMEOUT = (15, 120)  # (connect, read) seconds
POST_TIMEOUT = 15


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


# ---------------------- Helpers ----------------------
def ensure_relative_path(key: str) -> Path:
    p = Path(key.lstrip("/")).as_posix()
    parts = [seg for seg in p.split("/") if seg not in ("", ".", "..")]
    return Path(*parts)


def safe_join(root: Path, rel: Path) -> Path:
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
    def __init__(self, jobs: "queue.Queue[dict]", output_dir: Path, api_endpoint: str, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.jobs = jobs
        self.output_dir = output_dir
        self.api_endpoint = api_endpoint
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

        payload = {"productType": product_type, "productDate": product_date}
        r = self.session.post(self.api_endpoint, json=payload, timeout=POST_TIMEOUT)
        r.raise_for_status()
        info = r.json()

        key = info.get("key")
        url = info.get("url")
        if not key or not url:
            raise ValueError(f"Malformed response from API: {info}")

        rel = ensure_relative_path(key)
        dest = safe_join(self.output_dir, rel)

        if dest.exists() and dest.stat().st_size > 0:
            logging.info("Skip existing file: %s", dest)
            return

        logging.info("⬇️  Downloading → %s", dest)
        chunked_download(url, dest)
        logging.info("✅ Done → %s", dest)


# ---------------------- Pure WebSocket client ----------------------
JsonLike = Union[dict, list, str, int, float, bool, None]


class PureWsClient:
    def __init__(
        self,
        ws_url: str,
        subscribe_payload: str,
        on_event,
        stop_event: threading.Event,
        headers: List[str],
    ):
        self.ws_url = ws_url
        self.subscribe_payload = subscribe_payload or ""
        self.on_event = on_event
        self.stop_event = stop_event
        self.headers = headers

        self.ws: Optional[WebSocketApp] = None

        # heartbeat thread control (per-connection)
        self._hb_stop = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None

    @staticmethod
    def _extract_events(obj: JsonLike) -> Iterable[dict]:
        if isinstance(obj, dict):
            if "data" in obj and isinstance(obj["data"], dict) and ("productType" in obj["data"] or "time" in obj["data"]):
                yield obj["data"]
                return
            yield obj
            return
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    yield item

    def _stop_heartbeat(self):
        try:
            self._hb_stop.set()
            if self._hb_thread and self._hb_thread.is_alive():
                self._hb_thread.join(timeout=1.0)
        except Exception:
            pass
        finally:
            self._hb_thread = None
            self._hb_stop = threading.Event()

    def _start_heartbeat(self, ws: WebSocketApp):
        # stop any previous heartbeat (on reconnect)
        self._stop_heartbeat()

        if not WS_HEARTBEAT_ENABLED or WS_HEARTBEAT_INTERVAL <= 0:
            return

        def loop():
            # manda heartbeat finché la connessione è viva e non stiamo spegnendo
            while not self.stop_event.is_set() and not self._hb_stop.is_set():
                time.sleep(WS_HEARTBEAT_INTERVAL)
                if self.stop_event.is_set() or self._hb_stop.is_set():
                    break

                # controlla che la socket sia ancora connessa
                try:
                    if not (ws.sock and ws.sock.connected):
                        break
                except Exception:
                    break

                # Payload per $default: la Lambda server-side deve riconoscerlo e non trattarlo come product event
                hb = {"type": "heartbeat", "ts": int(time.time() * 1000)}
                try:
                    ws.send(json.dumps(hb, separators=(",", ":")))
                    logging.debug("→ heartbeat sent")
                except Exception as e:
                    logging.debug("heartbeat send failed: %s", e)
                    break

        self._hb_thread = threading.Thread(target=loop, daemon=True)
        self._hb_thread.start()

    def _on_open(self, ws):
        logging.info("WebSocket opened: %s", self.ws_url)

        # start app-level heartbeat (so $default can refresh TTL)
        self._start_heartbeat(ws)

        # optional subscribe payload
        if self.subscribe_payload.strip():
            try:
                ws.send(self.subscribe_payload)
                logging.info("Sent subscribe payload on open")
            except Exception as e:
                logging.warning("Failed to send subscribe payload: %s", e)

    def _on_message(self, ws, message):
        if isinstance(message, (bytes, bytearray)):
            try:
                message = message.decode("utf-8", "ignore")
            except Exception:
                return

        msg = (message or "").strip()
        if not msg:
            return

        chunks = [msg]
        if "\n" in msg and not msg.lstrip().startswith("["):
            chunks = [ln.strip() for ln in msg.splitlines() if ln.strip()]

        for chunk in chunks:
            try:
                obj = json.loads(chunk)
            except Exception:
                logging.debug("Non-JSON message ignored (first 200 chars): %r", chunk[:200])
                continue

            # se il server risponde ai heartbeat, qui verranno ignorati perché non hanno productType/time
            for ev in self._extract_events(obj):
                self.on_event(ev)

    def _on_close(self, ws, code, reason):
        self._stop_heartbeat()
        logging.warning("WebSocket closed: code=%s reason=%s", code, reason)

    def _on_error(self, ws, error):
        # spesso _on_error precede la close su alcune condizioni
        self._stop_heartbeat()
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
                header=self.headers if self.headers else None,
            )
            try:
                self.ws.run_forever(
                    ping_interval=WS_PING_INTERVAL,
                    ping_timeout=WS_PING_TIMEOUT,
                )
            except Exception as e:
                logging.error("run_forever exception: %s", e)

            self._stop_heartbeat()

            if self.stop_event.is_set():
                break

            logging.info("Reconnecting in %.1fs ...", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)



# ---------------------- Main app ----------------------
class App:
    def __init__(
        self,
        products: Set[str],
        output_dir: Path,
        api_endpoint: str,
        ws_url: str,
        ws_subscribe: str,
        ws_headers: List[str],
        workers: int = 3,
    ):
        self.products = {p.strip().upper() for p in products if p.strip()}
        self.output_dir = output_dir
        self.api_endpoint = api_endpoint

        self.jobs: "queue.Queue[dict]" = queue.Queue(maxsize=1000)
        self.stop_event = threading.Event()

        self.dedup: Dict[str, float] = {}
        self.lock = threading.Lock()

        self.workers = [Downloader(self.jobs, self.output_dir, self.api_endpoint, self.stop_event) for _ in range(workers)]
        for w in self.workers:
            w.start()

        self.ws = PureWsClient(ws_url, ws_subscribe, self.on_ws_event, self.stop_event, headers=ws_headers)

        self.gc_thread = threading.Thread(target=self._gc_loop, daemon=True)
        self.gc_thread.start()

    def _gc_loop(self):
        while not self.stop_event.is_set():
            now = time.time()
            with self.lock:
                cutoff = now - 3 * 3600
                for k in [k for k, ts in self.dedup.items() if ts < cutoff]:
                    del self.dedup[k]
            time.sleep(300)

    def on_ws_event(self, ev: dict):
        product_type = (ev.get("productType") or ev.get("type") or "").upper().strip()
        epoch_ms = ev.get("time")
        if epoch_ms is None:
            epoch_ms = ev.get("productDate") or ev.get("timestamp")

        if not product_type or not isinstance(epoch_ms, (int, float)):
            logging.debug("Ignoring event (missing productType/time): %s", ev)
            return

        if product_type not in self.products:
            logging.debug("Ignoring product %s", product_type)
            return

        epoch_ms = int(epoch_ms)
        key = f"{product_type}:{epoch_ms}"

        with self.lock:
            if key in self.dedup:
                return
            self.dedup[key] = time.time()

        job = {"productType": product_type, "productDate": epoch_ms}
        try:
            self.jobs.put_nowait(job)
            ts = datetime.utcfromtimestamp(epoch_ms / 1000.0)
            logging.debug("Enqueued %s at %sZ", product_type, ts.strftime("%Y-%m-%d %H:%M"))
        except queue.Full:
            logging.warning("Job queue full; dropping %s", job)

    def start(self):
        def handle_sig(sig, _frame):
            logging.info("Signal %s received, shutting down...", sig)
            self.stop_event.set()

        signal.signal(signal.SIGINT, handle_sig)
        signal.signal(signal.SIGTERM, handle_sig)

        logging.info("Listening products: %s", ",".join(sorted(self.products)))
        logging.info("Writing files under: %s", self.output_dir.resolve())

        self.ws.run_forever()

        self.stop_event.set()
        logging.info("Waiting for pending downloads to finish...")
        self.jobs.join()
        logging.info("Bye.")


def main():
    print_start_banner()
    parser = argparse.ArgumentParser(description="RADAR-DPC continuous downloader (pure WebSocket)")
    parser.add_argument("--products", type=str, default=PRODUCTS_DEFAULT)
    parser.add_argument("--output", type=str, default=OUTPUT_DIR_DEFAULT)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--log-level", type=str, default="INFO")

    parser.add_argument("--ws-url", type=str, default=WS_URL_DEFAULT)
    parser.add_argument("--ws-subscribe", type=str, default=WS_SUBSCRIBE_DEFAULT)
    parser.add_argument("--ws-user-agent", type=str, default=WS_USER_AGENT_DEFAULT)
    parser.add_argument("--ws-origin", type=str, default=WS_ORIGIN_DEFAULT)
    parser.add_argument(
        "--ws-header",
        action="append",
        default=[],
        help='Extra WS header (repeatable), e.g. --ws-header "X-Foo: bar"',
    )

    parser.add_argument("--api-endpoint", type=str, default=API_ENDPOINT_DEFAULT)
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    products = [p.strip() for p in args.products.split(",") if p.strip()]
    output_dir = Path(args.output)

    # Build headers (this is the key fix for your 403)
    ws_headers = [
        f"User-Agent: {args.ws_user_agent}",
        f"Origin: {args.ws_origin}",
    ] + (args.ws_header or [])

    logging.info("WS headers: %s", ws_headers)

    app = App(
        products=set(products),
        output_dir=output_dir,
        api_endpoint=args.api_endpoint,
        ws_url=args.ws_url,
        ws_subscribe=args.ws_subscribe,
        ws_headers=ws_headers,
        workers=args.workers,
    )
    app.start()


if __name__ == "__main__":
    main()
