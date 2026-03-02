package com.bhcode.flare.flink.runtime.control;

import com.bhcode.flare.common.util.JSONUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Slf4j
public class RuntimeControlRestServer implements AutoCloseable {

    private final Config config;
    private final RuntimeControlService service;

    private HttpServer server;
    private ExecutorService executor;
    private String baseUrl;

    public RuntimeControlRestServer(Config config, RuntimeControlService service) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.service = Objects.requireNonNull(service, "service must not be null");
    }

    public synchronized void start() throws IOException {
        if (this.server != null) {
            return;
        }
        String host = StringUtils.defaultIfBlank(this.config.host(), "0.0.0.0");
        int port = this.config.port() == null ? 0 : Math.max(this.config.port(), 0);
        this.server = HttpServer.create(new InetSocketAddress(host, port), 0);
        this.executor = Executors.newFixedThreadPool(4, daemonFactory("flare-rest-"));
        this.server.setExecutor(this.executor);

        this.server.createContext("/system/kill", this::handleKill);
        this.server.createContext("/system/setConf", this::handleSetConf);
        this.server.createContext("/system/checkpoint", this::handleCheckpoint);
        this.server.createContext("/system/lineage", this::handleLineage);
        this.server.createContext("/system/config", this::handleConfig);
        this.server.createContext("/system/metrics", this::handleMetrics);
        this.server.createContext("/system/datasource", this::handleDatasource);
        this.server.createContext("/system/exception", this::handleException);
        this.server.createContext("/system/distributeSync", this::handleDistributeSync);
        this.server.createContext("/system/collectLineage", this::handleCollectLineage);
        this.server.start();

        int actualPort = this.server.getAddress().getPort();
        String publishHost = "0.0.0.0".equals(host) ? "127.0.0.1" : host;
        this.baseUrl = "http://" + publishHost + ":" + actualPort;
        log.info("Runtime control REST server started at {}", this.baseUrl);
    }

    public synchronized String baseUrl() {
        return this.baseUrl;
    }

    @Override
    public synchronized void close() {
        if (this.server != null) {
            this.server.stop(0);
            this.server = null;
        }
        if (this.executor != null) {
            this.executor.shutdownNow();
            this.executor = null;
        }
        this.baseUrl = null;
    }

    private void handleKill(HttpExchange exchange) throws IOException {
        if (!checkMethod(exchange, "POST") || !checkAuth(exchange)) {
            return;
        }
        try {
            this.service.kill();
            writeJson(exchange, 200, success("Kill signal accepted"));
        } catch (Exception e) {
            log.error("Failed to handle /system/kill", e);
            writeJson(exchange, 500, error("Failed to process kill request"));
        }
    }

    private void handleSetConf(HttpExchange exchange) throws IOException {
        if (!checkMethod(exchange, "POST") || !checkAuth(exchange)) {
            return;
        }
        try {
            String body = readBody(exchange);
            Map<String, String> confMap = parseStringMap(body);
            if (confMap == null) {
                writeJson(exchange, 400, error("Invalid JSON body for setConf"));
                return;
            }
            int applied = this.service.setConf(confMap);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("applied", applied);
            writeJson(exchange, 200, success("Configuration updated", data));
        } catch (Exception e) {
            log.error("Failed to handle /system/setConf", e);
            writeJson(exchange, 500, error("Failed to process setConf request"));
        }
    }

    private void handleCheckpoint(HttpExchange exchange) throws IOException {
        if (!checkMethod(exchange, "POST") || !checkAuth(exchange)) {
            return;
        }
        try {
            String body = readBody(exchange);
            CheckpointControlRequest request = JSONUtils.parseObject(body, CheckpointControlRequest.class);
            if (request == null) {
                writeJson(exchange, 400, error("Invalid JSON body for checkpoint"));
                return;
            }
            CheckpointApplyResult result = this.service.updateCheckpoint(request);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("updatedKeys", result.updatedKeys());
            data.put("runtimeApplied", result.runtimeApplied());
            writeJson(exchange, 200, success("Checkpoint config updated", data));
        } catch (Exception e) {
            log.error("Failed to handle /system/checkpoint", e);
            writeJson(exchange, 500, error("Failed to process checkpoint request"));
        }
    }

    private void handleLineage(HttpExchange exchange) throws IOException {
        if (!checkMethod(exchange, "GET") || !checkAuth(exchange)) {
            return;
        }
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("lineage", this.service.lineage());
            writeJson(exchange, 200, success("Lineage snapshot", data));
        } catch (Exception e) {
            log.error("Failed to handle /system/lineage", e);
            writeJson(exchange, 500, error("Failed to process lineage request"));
        }
    }

    private void handleConfig(HttpExchange exchange) throws IOException {
        if (!checkMethod(exchange, "GET") || !checkAuth(exchange)) {
            return;
        }
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("config", this.service.config());
            writeJson(exchange, 200, success("Config snapshot", data));
        } catch (Exception e) {
            log.error("Failed to handle /system/config", e);
            writeJson(exchange, 500, error("Failed to process config request"));
        }
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        if (!checkMethod(exchange, "GET") || !checkAuth(exchange)) {
            return;
        }
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("metrics", this.service.metrics());
            writeJson(exchange, 200, success("Metrics snapshot", data));
        } catch (Exception e) {
            log.error("Failed to handle /system/metrics", e);
            writeJson(exchange, 500, error("Failed to process metrics request"));
        }
    }

    private void handleDatasource(HttpExchange exchange) throws IOException {
        if (!checkMethod(exchange, "GET") || !checkAuth(exchange)) {
            return;
        }
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("datasource", this.service.datasource());
            writeJson(exchange, 200, success("Datasource snapshot", data));
        } catch (Exception e) {
            log.error("Failed to handle /system/datasource", e);
            writeJson(exchange, 500, error("Failed to process datasource request"));
        }
    }

    private void handleException(HttpExchange exchange) throws IOException {
        if (!checkMethod(exchange, "GET") || !checkAuth(exchange)) {
            return;
        }
        try {
            Map<String, String> params = parseQuery(exchange.getRequestURI());
            boolean clear = Boolean.parseBoolean(params.getOrDefault("clear", "true"));
            int limit = parseInt(params.getOrDefault("limit", "100"), 100);

            Map<String, Object> data = new LinkedHashMap<>();
            data.put("exceptions", this.service.exceptions(clear, limit));
            writeJson(exchange, 200, success("Exception snapshot", data));
        } catch (Exception e) {
            log.error("Failed to handle /system/exception", e);
            writeJson(exchange, 500, error("Failed to process exception request"));
        }
    }

    private void handleDistributeSync(HttpExchange exchange) throws IOException {
        if (!checkMethod(exchange, "GET") || !checkAuth(exchange)) {
            return;
        }
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("sync", this.service.distributeSync());
            writeJson(exchange, 200, success("Distribute sync snapshot", data));
        } catch (Exception e) {
            log.error("Failed to handle /system/distributeSync", e);
            writeJson(exchange, 500, error("Failed to process distributeSync request"));
        }
    }

    private void handleCollectLineage(HttpExchange exchange) throws IOException {
        if (!checkMethod(exchange, "POST") || !checkAuth(exchange)) {
            return;
        }
        try {
            String body = readBody(exchange);
            List<LineageEdgeDTO> edgeDTOs = parseLineageEdgeList(body);
            if (edgeDTOs == null) {
                writeJson(exchange, 400, error("Invalid JSON body for collectLineage"));
                return;
            }
            int merged = this.service.collectLineage(toLineageEdges(edgeDTOs));
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("merged", merged);
            writeJson(exchange, 200, success("Lineage collected", data));
        } catch (Exception e) {
            log.error("Failed to handle /system/collectLineage", e);
            writeJson(exchange, 500, error("Failed to process collectLineage request"));
        }
    }

    private boolean checkMethod(HttpExchange exchange, String expected) throws IOException {
        if (!expected.equalsIgnoreCase(exchange.getRequestMethod())) {
            writeJson(exchange, 405, error("Method not allowed"));
            return false;
        }
        return true;
    }

    private boolean checkAuth(HttpExchange exchange) throws IOException {
        String required = this.config.token();
        if (StringUtils.isBlank(required)) {
            return true;
        }
        String provided = exchange.getRequestHeaders().getFirst("Authorization");
        if (!required.equals(provided)) {
            writeJson(exchange, 401, error("Unauthorized"));
            return false;
        }
        return true;
    }

    private String readBody(HttpExchange exchange) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        return new String(body, StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseStringMap(String json) {
        Map<Object, Object> raw = JSONUtils.parseObject(json, Map.class);
        if (raw == null) {
            return null;
        }
        Map<String, String> confMap = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : raw.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            confMap.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        return confMap;
    }

    private Map<String, String> parseQuery(URI uri) {
        Map<String, String> query = new LinkedHashMap<>();
        if (uri == null || StringUtils.isBlank(uri.getRawQuery())) {
            return query;
        }
        String[] pairs = uri.getRawQuery().split("&");
        for (String pair : pairs) {
            if (StringUtils.isBlank(pair)) {
                continue;
            }
            String[] kv = pair.split("=", 2);
            if (kv.length == 2 && StringUtils.isNotBlank(kv[0])) {
                query.put(kv[0], kv[1]);
            } else if (kv.length == 1 && StringUtils.isNotBlank(kv[0])) {
                query.put(kv[0], "");
            }
        }
        return query;
    }

    private int parseInt(String value, int defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @SuppressWarnings("unchecked")
    private List<LineageEdgeDTO> parseLineageEdgeList(String json) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        List<Object> raw = JSONUtils.parseObject(json, List.class);
        if (raw == null) {
            return null;
        }
        if (raw.isEmpty()) {
            return List.of();
        }
        List<LineageEdgeDTO> edges = new ArrayList<>(raw.size());
        for (Object item : raw) {
            if (!(item instanceof Map<?, ?> map)) {
                continue;
            }
            edges.add(new LineageEdgeDTO(
                    toStringValue(map.get("source")),
                    toStringValue(map.get("target")),
                    toStringValue(map.get("operation")),
                    toLongValue(map.get("count"), 1L),
                    toLongValue(map.get("lastUpdatedTime"), System.currentTimeMillis())
            ));
        }
        return edges;
    }

    private List<com.bhcode.flare.common.lineage.LineageManager.LineageEdge> toLineageEdges(List<LineageEdgeDTO> dtos) {
        if (dtos == null || dtos.isEmpty()) {
            return List.of();
        }
        List<com.bhcode.flare.common.lineage.LineageManager.LineageEdge> edges = new ArrayList<>(dtos.size());
        for (LineageEdgeDTO dto : dtos) {
            if (dto == null) {
                continue;
            }
            edges.add(new com.bhcode.flare.common.lineage.LineageManager.LineageEdge(
                    dto.source(),
                    dto.target(),
                    dto.operation(),
                    dto.count(),
                    dto.lastUpdatedTime()
            ));
        }
        return edges;
    }

    private String toStringValue(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private long toLongValue(Object value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private void writeJson(HttpExchange exchange, int status, Map<String, Object> payload) throws IOException {
        byte[] bytes = Objects.requireNonNullElse(JSONUtils.toJSONString(payload), "{}")
                .getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private Map<String, Object> success(String message) {
        return success(message, null);
    }

    private Map<String, Object> success(String message, Map<String, Object> data) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("message", message);
        if (data != null && !data.isEmpty()) {
            payload.put("data", data);
        }
        return payload;
    }

    private Map<String, Object> error(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", false);
        payload.put("message", message);
        return payload;
    }

    private ThreadFactory daemonFactory(String namePrefix) {
        return runnable -> {
            Thread t = new Thread(runnable);
            t.setDaemon(true);
            t.setName(namePrefix + t.getId());
            return t;
        };
    }

    public record Config(
            String host,
            Integer port,
            String token
    ) {
    }

    private record LineageEdgeDTO(
            String source,
            String target,
            String operation,
            long count,
            long lastUpdatedTime
    ) {
    }
}
