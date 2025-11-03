package org.apache.nifi.processor.StreamLoad;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StarRocksStreamLoad {

   // private static final Logger LOG = LoggerFactory.getLogger(StreamLoadProcessor.class);
    private String STARROCKS_HOST ;//= "120.79.54.230";
    private   String STARROCKS_DB ;//= "test";

    private   String STARROCKS_TABLE ;//= "stream_test";
    private final static String STARROCKS_USER = "root";
    private final static String STARROCKS_PASSWORD = "poly2023";


    private String TABLETYPE ;

    private int STARROCKS_HTTP_PORT;// = 8030;

    public StarRocksStreamLoad() {

    }
    public StarRocksStreamLoad(String starrocks_db, String starrocks_table) {
        STARROCKS_DB = starrocks_db;
        STARROCKS_TABLE = starrocks_table;

    }

    public void setTABLETYPE(String TABLETYPE) {
        this.TABLETYPE = TABLETYPE;
    }

    public void setSTARROCKS_HTTP_PORT(int STARROCKS_HTTP_PORT) {
        this.STARROCKS_HTTP_PORT = STARROCKS_HTTP_PORT;
    }

    public void setSTARROCKS_HOST(String STARROCKS_HOST) {
        this.STARROCKS_HOST = STARROCKS_HOST;
    }


    public void SetSTARROCKS_DB(String starDB) {
         STARROCKS_DB=starDB;
    }

    public void SetSTARROCKS_TABLE(String starTable) {
         STARROCKS_TABLE=starTable;
    }
    public String sendData(String content,String label,String updateFeildlist) throws Exception {
        final String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
                STARROCKS_HOST,
                STARROCKS_HTTP_PORT,
                STARROCKS_DB,
                STARROCKS_TABLE);

        final HttpClientBuilder httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });

        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(loadUrl);
            StringEntity entity = new StringEntity(content, "UTF-8");
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(STARROCKS_USER, STARROCKS_PASSWORD));
            // the label header is optional, not necessary
            // use label header can ensure at most once semantics
            put.setHeader("label", label); //"39c25a5c-7000-496e-a98e-348a264c81dg"
            if(TABLETYPE.equalsIgnoreCase("primary")) {
                put.setHeader("partial_update", String.valueOf(true));
            }
            put.setHeader("columns",updateFeildlist);
            put.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }
                final int statusCode = response.getStatusLine().getStatusCode();
                // statusCode 200 just indicates that starrocks be service is ok, not stream load
                // you should see the output content to find whether stream load is success
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
                }

                System.out.println(loadResult);
                return loadResult;

            }
        }
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }
}