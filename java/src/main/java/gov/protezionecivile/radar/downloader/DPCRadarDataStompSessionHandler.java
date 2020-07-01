/**
 *
 *    Automatic download DPC Radar Data
 *    Spring Boot Application
 *    http://www.protezionecivile.gov.it/radar-dpc/
 *   ====================================================================
 *
 *   Copyright (C) 2020-2020 geoSDI Group (CNR IMAA - Potenza - ITALY).
 *
 *   This program is free software: you can redistribute it and/or modify it
 *   under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version. This program is distributed in the
 *   hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR
 *   A PARTICULAR PURPOSE. See the GNU General Public License
 *   for more details. You should have received a copy of the GNU General
 *   Public License along with this program. If not, see http://www.gnu.org/licenses/
 *
 *   ====================================================================
 *
 *   Linking this library statically or dynamically with other modules is
 *   making a combined work based on this library. Thus, the terms and
 *   conditions of the GNU General Public License cover the whole combination.
 *
 *   As a special exception, the copyright holders of this library give you permission
 *   to link this library with independent modules to produce an executable, regardless
 *   of the license terms of these independent modules, and to copy and distribute
 *   the resulting executable under terms of your choice, provided that you also meet,
 *   for each linked independent module, the terms and conditions of the license of
 *   that module. An independent module is a module which is not derived from or
 *   based on this library. If you modify this library, you may extend this exception
 *   to your version of the library, but you are not obligated to do so. If you do not
 *   wish to do so, delete this exception statement from your version.
 */

package gov.protezionecivile.radar.downloader;

import com.google.common.base.Preconditions;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.stereotype.Component;

import java.io.*;
import java.lang.reflect.Type;

/**
 * @author Francesco Izzi @ CNR IMAA geoSDI
 */

@Component(value = "dpcSessionHandler")
public class DPCRadarDataStompSessionHandler extends StompSessionHandlerAdapter implements InitializingBean {

    @Value("${productToDownload}")
    public String productToDownload;

    @Value("${defaultSavePath}")
    public String defaultSavePath;

    private final String DOWNLOAD_PRODUCT_URL = "http://www.protezionecivile.gov.it/wide-api/wide/product/downloadProduct";

    private Logger logger = LogManager.getLogger(DPCRadarDataStompSessionHandler.class);


    @Override
    public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
        logger.info("New session established : " + session.getSessionId());
        session.subscribe("/topic/product", this);
        logger.info("Subscribed to /topic/product");
    }

    @Override
    public void handleException(StompSession session, StompCommand command, StompHeaders headers, byte[] payload, Throwable exception) {
        logger.error("Got an exception", exception);
    }

    @Override
    public Type getPayloadType(StompHeaders headers) {
        return DPCWebsocketMessage.class;
    }

    @Override
    public void handleFrame(StompHeaders headers, Object payload) {
        DPCWebsocketMessage msg = (DPCWebsocketMessage) payload;
        logger.info("Web socket message received processing ... : " + msg);

        if(this.productToDownload.contains(msg.getProductType())) {

            CloseableHttpClient client = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(DOWNLOAD_PRODUCT_URL);

            StringEntity requestEntity = new StringEntity(
                    msg.toJsonString(),
                    ContentType.APPLICATION_JSON);

            httpPost.setEntity(requestEntity);
            InputStream input = null;
            OutputStream output = null;
            byte[] buffer = new byte[1024];
            try {

                CloseableHttpResponse response = client.execute(httpPost);
                String filename = "noname";

                String dispositionValue = response.getFirstHeader("Content-Disposition").getValue();
                int index = dispositionValue.indexOf("filename=");
                if (index > 0) {
                    filename = dispositionValue.substring(index + 10, dispositionValue.length() - 1);
                    filename = filename.substring(0, filename.indexOf("\""));
                }

                logger.info("Downloading " + msg.getProductType() +" file: " + filename);

                input = response.getEntity().getContent();

                File directory = new File(String.valueOf(defaultSavePath + msg.getProductType() + "/"));

                if (!directory.exists()) {

                    directory.mkdir();
                }

                output = new FileOutputStream(defaultSavePath + msg.getProductType() + "/" + filename);
                for (int length; (length = input.read(buffer)) > 0; ) {
                    output.write(buffer, 0, length);
                }
                logger.info("File successfully downloaded!");

            } catch (IOException e) {
                logger.error("Error downloading file ...",e);
            } finally {
                if (output != null)
                    try {
                        output.close();
                    } catch (IOException logOrIgnore) {
                    }
                if (input != null)
                    try {
                        input.close();
                    } catch (IOException logOrIgnore) {
                    }
            }

            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            logger.info("Nothing to do ... passing");
        }
    }

    public String getProductToDownload() {
        return productToDownload;
    }

    public String getDefaultSavePath() {
        return defaultSavePath;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Preconditions.checkArgument(this.productToDownload != null && !this.productToDownload.trim().isEmpty(), "The paramenter productToDownload not present");
        Preconditions.checkArgument(this.defaultSavePath != null && !this.defaultSavePath.trim().isEmpty(), "The paramenter defaultSavePath not present");
        logger.info("Configured files to Download : {} ",productToDownload);
        logger.info("Directory to download DPC-Radar data : {} ", defaultSavePath);
    }
}
