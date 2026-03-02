package com.bhcode.flare.flink.runtime.control;

import com.bhcode.flare.common.lineage.LineageManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RuntimeControlRestServerTest {

    private RuntimeControlRestServer server;
    private RecordingService service;

    @Before
    public void setUp() throws Exception {
        this.service = new RecordingService();
        RuntimeControlRestServer.Config config = new RuntimeControlRestServer.Config("127.0.0.1", 0, "");
        this.server = new RuntimeControlRestServer(config, service);
        this.server.start();
    }

    @After
    public void tearDown() {
        if (this.server != null) {
            this.server.close();
        }
    }

    @Test
    public void shouldHandleKillEndpoint() throws Exception {
        HttpResult res = request(this.server.baseUrl() + "/system/kill", "POST", "{}", Map.of());
        Assert.assertEquals(200, res.code);
        Assert.assertTrue(res.body.contains("\"success\":true"));
        Assert.assertTrue(this.service.killed);
    }

    @Test
    public void shouldHandleSetConfEndpoint() throws Exception {
        String body = "{\"a\":\"1\",\"b\":\"2\"}";
        HttpResult res = request(this.server.baseUrl() + "/system/setConf", "POST", body, Map.of());
        Assert.assertEquals(200, res.code);
        Assert.assertTrue(res.body.contains("\"success\":true"));
        Assert.assertEquals("1", this.service.lastConf.get("a"));
        Assert.assertEquals("2", this.service.lastConf.get("b"));
    }

    @Test
    public void shouldHandleCheckpointEndpoint() throws Exception {
        String body = "{\"interval\":1000,\"timeout\":2000,\"minPauseBetween\":3000}";
        HttpResult res = request(this.server.baseUrl() + "/system/checkpoint", "POST", body, Map.of());
        Assert.assertEquals(200, res.code);
        Assert.assertTrue(res.body.contains("\"success\":true"));
        Assert.assertNotNull(this.service.lastCheckpointRequest);
        Assert.assertEquals(Long.valueOf(1000), this.service.lastCheckpointRequest.interval());
        Assert.assertEquals(Long.valueOf(2000), this.service.lastCheckpointRequest.timeout());
        Assert.assertEquals(Long.valueOf(3000), this.service.lastCheckpointRequest.minPauseBetween());
    }

    @Test
    public void shouldRejectUnauthorizedWhenTokenConfigured() throws Exception {
        RuntimeControlRestServer secured = new RuntimeControlRestServer(
                new RuntimeControlRestServer.Config("127.0.0.1", 0, "secret"),
                this.service
        );
        secured.start();
        try {
            HttpResult unauthorized = request(secured.baseUrl() + "/system/kill", "POST", "{}", Map.of());
            Assert.assertEquals(401, unauthorized.code);

            HttpResult authorized = request(
                    secured.baseUrl() + "/system/kill",
                    "POST",
                    "{}",
                    Map.of("Authorization", "secret")
            );
            Assert.assertEquals(200, authorized.code);
        } finally {
            secured.close();
        }
    }

    @Test
    public void shouldRejectUnsupportedMethod() throws Exception {
        HttpResult res = request(this.server.baseUrl() + "/system/setConf", "GET", null, Map.of());
        Assert.assertEquals(405, res.code);
    }

    @Test
    public void shouldHandleLineageEndpoint() throws Exception {
        HttpResult res = request(this.server.baseUrl() + "/system/lineage", "GET", null, Map.of());
        Assert.assertEquals(200, res.code);
        Assert.assertTrue(res.body.contains("\"success\":true"));
        Assert.assertTrue(res.body.contains("\"lineage\""));
    }

    @Test
    public void shouldHandleConfigEndpoint() throws Exception {
        HttpResult res = request(this.server.baseUrl() + "/system/config", "GET", null, Map.of());
        Assert.assertEquals(200, res.code);
        Assert.assertTrue(res.body.contains("\"success\":true"));
        Assert.assertTrue(res.body.contains("\"config\""));
        Assert.assertTrue(res.body.contains("\"runtime.test.key\""));
    }

    @Test
    public void shouldHandleMetricsEndpoint() throws Exception {
        HttpResult res = request(this.server.baseUrl() + "/system/metrics", "GET", null, Map.of());
        Assert.assertEquals(200, res.code);
        Assert.assertTrue(res.body.contains("\"success\":true"));
        Assert.assertTrue(res.body.contains("\"metrics\""));
        Assert.assertTrue(res.body.contains("\"runtime.test.metric\""));
    }

    @Test
    public void shouldHandleDatasourceEndpoint() throws Exception {
        HttpResult res = request(this.server.baseUrl() + "/system/datasource", "GET", null, Map.of());
        Assert.assertEquals(200, res.code);
        Assert.assertTrue(res.body.contains("\"success\":true"));
        Assert.assertTrue(res.body.contains("\"datasource\""));
        Assert.assertTrue(res.body.contains("Kafka:test"));
    }

    @Test
    public void shouldHandleExceptionEndpoint() throws Exception {
        HttpResult res = request(this.server.baseUrl() + "/system/exception?clear=true&limit=5", "GET", null, Map.of());
        Assert.assertEquals(200, res.code);
        Assert.assertTrue(res.body.contains("\"success\":true"));
        Assert.assertTrue(res.body.contains("\"exceptions\""));
        Assert.assertTrue(res.body.contains("RuntimeException"));
    }

    @Test
    public void shouldHandleDistributeSyncEndpoint() throws Exception {
        HttpResult res = request(this.server.baseUrl() + "/system/distributeSync", "GET", null, Map.of());
        Assert.assertEquals(200, res.code);
        Assert.assertTrue(res.body.contains("\"success\":true"));
        Assert.assertTrue(res.body.contains("\"sync\""));
        Assert.assertTrue(res.body.contains("\"module\":\"conf\""));
    }

    @Test
    public void shouldHandleCollectLineageEndpoint() throws Exception {
        String body = "[{\"source\":\"Kafka:orders\",\"target\":\"Flink\",\"operation\":\"SOURCE\",\"count\":3,\"lastUpdatedTime\":1000}]";
        HttpResult res = request(this.server.baseUrl() + "/system/collectLineage", "POST", body, Map.of());
        Assert.assertEquals(200, res.code);
        Assert.assertTrue(res.body.contains("\"success\":true"));
        Assert.assertTrue(res.body.contains("\"merged\":1"));
        Assert.assertEquals(1, this.service.collectedLineage.size());
        Assert.assertEquals("Kafka:orders", this.service.collectedLineage.get(0).source());
        Assert.assertEquals(3L, this.service.collectedLineage.get(0).count());
    }

    @Test
    public void shouldRejectInvalidCollectLineageBody() throws Exception {
        HttpResult res = request(this.server.baseUrl() + "/system/collectLineage", "POST", "{invalid", Map.of());
        Assert.assertEquals(400, res.code);
        Assert.assertTrue(res.body.contains("\"success\":false"));
    }

    private HttpResult request(String url, String method, String body, Map<String, String> headers) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        conn.setRequestProperty("Content-Type", "application/json");
        if (headers != null) {
            for (Map.Entry<String, String> e : headers.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }
        }
        if (body != null) {
            conn.setDoOutput(true);
            byte[] data = body.getBytes(StandardCharsets.UTF_8);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(data);
            }
        }
        int code = conn.getResponseCode();
        InputStream is = code >= 400 ? conn.getErrorStream() : conn.getInputStream();
        String text = "";
        if (is != null) {
            text = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            is.close();
        }
        conn.disconnect();
        return new HttpResult(code, text);
    }

    private static class HttpResult {
        private final int code;
        private final String body;

        private HttpResult(int code, String body) {
            this.code = code;
            this.body = body;
        }
    }

    private static class RecordingService implements RuntimeControlService {
        private boolean killed;
        private Map<String, String> lastConf = new HashMap<>();
        private CheckpointControlRequest lastCheckpointRequest;
        private final RuntimeDistributePayload syncPayload =
                new RuntimeDistributePayload("conf", "{\"runtime.test.key\":\"runtime-test-value\"}", 1L, 123L);
        private List<LineageManager.LineageEdge> collectedLineage = new ArrayList<>();

        @Override
        public void kill() {
            this.killed = true;
        }

        @Override
        public int setConf(Map<String, String> conf) {
            this.lastConf = conf;
            return conf == null ? 0 : conf.size();
        }

        @Override
        public CheckpointApplyResult updateCheckpoint(CheckpointControlRequest request) {
            this.lastCheckpointRequest = request;
            return new CheckpointApplyResult(3, false);
        }

        @Override
        public List<LineageManager.LineageEdge> lineage() {
            return List.of(new LineageManager.LineageEdge("Kafka:test", "Flink", "SOURCE", 1L, 123L));
        }

        @Override
        public Map<String, String> config() {
            return Map.of("runtime.test.key", "runtime-test-value");
        }

        @Override
        public Map<String, Long> metrics() {
            return Map.of("runtime.test.metric", 5L);
        }

        @Override
        public List<LineageManager.LineageEdge> datasource() {
            return List.of(new LineageManager.LineageEdge("Kafka:test", "Flink", "SOURCE", 1L, 123L));
        }

        @Override
        public List<RuntimeExceptionSnapshot> exceptions(boolean clear, int limit) {
            return List.of(new RuntimeExceptionSnapshot("RuntimeException", "boom", "stack", System.currentTimeMillis()));
        }

        @Override
        public RuntimeDistributePayload distributeSync() {
            return this.syncPayload;
        }

        @Override
        public int collectLineage(List<LineageManager.LineageEdge> edges) {
            this.collectedLineage = edges == null ? List.of() : edges;
            return this.collectedLineage.size();
        }
    }
}
