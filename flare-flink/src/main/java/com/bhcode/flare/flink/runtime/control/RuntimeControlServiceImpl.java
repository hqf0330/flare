package com.bhcode.flare.flink.runtime.control;

import com.bhcode.flare.common.lineage.LineageManager;
import com.bhcode.flare.common.util.ExceptionBus;
import com.bhcode.flare.common.util.JSONUtils;
import com.bhcode.flare.common.util.PropUtils;
import com.bhcode.flare.flink.conf.FlareFlinkConf;
import com.bhcode.flare.flink.util.FlinkSingletonFactory;
import org.apache.commons.lang3.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RuntimeControlServiceImpl implements RuntimeControlService {

    private final Runnable killAction;
    private final RuntimeCheckpointUpdater checkpointUpdater;
    private final AtomicLong syncVersion = new AtomicLong(0L);
    private final AtomicReference<RuntimeDistributePayload> lastSync = new AtomicReference<>(
            new RuntimeDistributePayload("none", "", 0L, 0L)
    );

    public RuntimeControlServiceImpl(Runnable killAction, RuntimeCheckpointUpdater checkpointUpdater) {
        this.killAction = Objects.requireNonNull(killAction, "killAction must not be null");
        this.checkpointUpdater = Objects.requireNonNull(checkpointUpdater, "checkpointUpdater must not be null");
    }

    @Override
    public void kill() {
        this.killAction.run();
    }

    @Override
    public int setConf(Map<String, String> conf) {
        if (conf == null || conf.isEmpty()) {
            return 0;
        }
        int count = 0;
        Map<String, String> applied = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isBlank(key) || value == null) {
                continue;
            }
            String finalKey = key.trim();
            String finalValue = value.trim();
            PropUtils.setProperty(finalKey, finalValue);
            applied.put(finalKey, finalValue);
            count++;
        }
        if (count > 0) {
            publishSync("conf", JSONUtils.toJSONString(applied));
        }
        return count;
    }

    @Override
    public CheckpointApplyResult updateCheckpoint(CheckpointControlRequest request) {
        if (request == null) {
            return new CheckpointApplyResult(0, false);
        }
        int updated = 0;
        if (request.interval() != null && request.interval() > 0) {
            PropUtils.setProperty(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_INTERVAL, String.valueOf(request.interval()));
            updated++;
        }
        if (request.timeout() != null && request.timeout() > 0) {
            PropUtils.setProperty(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_TIMEOUT, String.valueOf(request.timeout()));
            updated++;
        }
        if (request.minPauseBetween() != null && request.minPauseBetween() > 0) {
            PropUtils.setProperty(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN,
                    String.valueOf(request.minPauseBetween()));
            updated++;
        }

        boolean runtimeApplied = updated > 0 && this.checkpointUpdater.apply(request);
        return new CheckpointApplyResult(updated, runtimeApplied);
    }

    @Override
    public List<LineageManager.LineageEdge> lineage() {
        return LineageManager.snapshot();
    }

    @Override
    public Map<String, String> config() {
        return new TreeMap<>(PropUtils.getSettings());
    }

    @Override
    public Map<String, Long> metrics() {
        return new LinkedHashMap<>(FlinkSingletonFactory.getInstance().getMetrics());
    }

    @Override
    public List<LineageManager.LineageEdge> datasource() {
        return LineageManager.snapshot();
    }

    @Override
    public List<RuntimeExceptionSnapshot> exceptions(boolean clear, int limit) {
        List<Throwable> throwables = clear ? ExceptionBus.drain(limit) : ExceptionBus.snapshot(limit);
        return throwables.stream()
                .filter(Objects::nonNull)
                .map(this::toSnapshot)
                .toList();
    }

    @Override
    public RuntimeDistributePayload distributeSync() {
        return this.lastSync.get();
    }

    @Override
    public int collectLineage(List<LineageManager.LineageEdge> edges) {
        if (edges == null || edges.isEmpty()) {
            return 0;
        }
        int merged = 0;
        for (LineageManager.LineageEdge edge : edges) {
            if (edge == null) {
                continue;
            }
            LineageManager.mergeLineage(
                    edge.source(),
                    edge.target(),
                    edge.operation(),
                    edge.count(),
                    edge.lastUpdatedTime()
            );
            merged++;
        }
        return merged;
    }

    private RuntimeExceptionSnapshot toSnapshot(Throwable throwable) {
        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        return new RuntimeExceptionSnapshot(
                throwable.getClass().getName(),
                throwable.getMessage(),
                sw.toString(),
                System.currentTimeMillis()
        );
    }

    private void publishSync(String module, String payload) {
        long version = this.syncVersion.incrementAndGet();
        this.lastSync.set(new RuntimeDistributePayload(
                StringUtils.defaultIfBlank(module, "conf"),
                StringUtils.defaultString(payload),
                version,
                System.currentTimeMillis()
        ));
    }
}
