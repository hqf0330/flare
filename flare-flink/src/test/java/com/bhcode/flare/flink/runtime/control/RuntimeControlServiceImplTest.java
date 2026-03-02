package com.bhcode.flare.flink.runtime.control;

import com.bhcode.flare.common.lineage.LineageManager;
import com.bhcode.flare.common.util.ExceptionBus;
import com.bhcode.flare.common.util.PropUtils;
import com.bhcode.flare.flink.conf.FlareFlinkConf;
import com.bhcode.flare.flink.util.FlinkSingletonFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class RuntimeControlServiceImplTest {

    private String intervalBackup;
    private String timeoutBackup;
    private String pauseBackup;
    private String configBackup;

    @Before
    public void setUp() {
        this.intervalBackup = PropUtils.getString(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_INTERVAL);
        this.timeoutBackup = PropUtils.getString(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_TIMEOUT);
        this.pauseBackup = PropUtils.getString(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN);
        this.configBackup = PropUtils.getString("runtime.test.key");
        FlinkSingletonFactory.getInstance().getMetrics().clear();
        LineageManager.clear();
        ExceptionBus.drain(1000);
    }

    @After
    public void tearDown() {
        PropUtils.setProperty(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_INTERVAL, this.intervalBackup == null ? "" : this.intervalBackup);
        PropUtils.setProperty(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_TIMEOUT, this.timeoutBackup == null ? "" : this.timeoutBackup);
        PropUtils.setProperty(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN, this.pauseBackup == null ? "" : this.pauseBackup);
        PropUtils.setProperty("runtime.test.key", this.configBackup == null ? "" : this.configBackup);
        FlinkSingletonFactory.getInstance().getMetrics().clear();
        LineageManager.clear();
        ExceptionBus.drain(1000);
    }

    @Test
    public void shouldTriggerKillAction() {
        AtomicBoolean killed = new AtomicBoolean(false);
        RuntimeControlServiceImpl service = new RuntimeControlServiceImpl(() -> killed.set(true), req -> false);

        service.kill();

        Assert.assertTrue(killed.get());
    }

    @Test
    public void shouldUpdateCheckpointConfigAndDelegateRuntimeApply() {
        RuntimeControlServiceImpl service = new RuntimeControlServiceImpl(() -> {}, req -> true);

        CheckpointApplyResult result = service.updateCheckpoint(new CheckpointControlRequest(1000L, 2000L, 3000L));

        Assert.assertEquals(3, result.updatedKeys());
        Assert.assertTrue(result.runtimeApplied());
        Assert.assertEquals("1000", PropUtils.getString(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_INTERVAL));
        Assert.assertEquals("2000", PropUtils.getString(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_TIMEOUT));
        Assert.assertEquals("3000", PropUtils.getString(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN));
    }

    @Test
    public void shouldReturnLineageSnapshot() {
        RuntimeControlServiceImpl service = new RuntimeControlServiceImpl(() -> {}, req -> false);

        List<?> lineages = service.lineage();

        Assert.assertNotNull(lineages);
    }

    @Test
    public void shouldReturnConfigSnapshot() {
        PropUtils.setProperty("runtime.test.key", "runtime-test-value");
        RuntimeControlServiceImpl service = new RuntimeControlServiceImpl(() -> {}, req -> false);

        Map<String, String> config = service.config();

        Assert.assertEquals("runtime-test-value", config.get("runtime.test.key"));
    }

    @Test
    public void shouldReturnMetricSnapshot() {
        FlinkSingletonFactory.getInstance().updateMetric("runtime.test.metric", 3L);
        FlinkSingletonFactory.getInstance().updateMetric("runtime.test.metric", 2L);
        RuntimeControlServiceImpl service = new RuntimeControlServiceImpl(() -> {}, req -> false);

        Map<String, Long> metrics = service.metrics();

        Assert.assertEquals(Long.valueOf(5L), metrics.get("runtime.test.metric"));
    }

    @Test
    public void shouldReturnDatasourceSnapshot() {
        LineageManager.addLineage("Kafka:a", "Flink", "SOURCE");
        RuntimeControlServiceImpl service = new RuntimeControlServiceImpl(() -> {}, req -> false);

        List<LineageManager.LineageEdge> datasources = service.datasource();

        Assert.assertEquals(1, datasources.size());
        Assert.assertEquals("Kafka:a", datasources.get(0).source());
    }

    @Test
    public void shouldReturnAndClearExceptions() {
        ExceptionBus.post(new IllegalStateException("boom-1"));
        ExceptionBus.post(new RuntimeException("boom-2"));
        RuntimeControlServiceImpl service = new RuntimeControlServiceImpl(() -> {}, req -> false);

        List<RuntimeExceptionSnapshot> first = service.exceptions(true, 10);
        List<RuntimeExceptionSnapshot> second = service.exceptions(true, 10);

        Assert.assertEquals(2, first.size());
        Assert.assertEquals(0, second.size());
    }

    @Test
    public void shouldPublishSyncPayloadAfterSetConf() {
        RuntimeControlServiceImpl service = new RuntimeControlServiceImpl(() -> {}, req -> false);

        int applied = service.setConf(Map.of("runtime.sync.key", "v1"));
        RuntimeDistributePayload payload = service.distributeSync();

        Assert.assertEquals(1, applied);
        Assert.assertEquals("conf", payload.module());
        Assert.assertTrue(payload.payload().contains("\"runtime.sync.key\""));
        Assert.assertTrue(payload.payload().contains("\"v1\""));
        Assert.assertEquals(1L, payload.version());
        Assert.assertTrue(payload.timestamp() > 0);
    }

    @Test
    public void shouldCollectAndMergeLineageEdges() {
        RuntimeControlServiceImpl service = new RuntimeControlServiceImpl(() -> {}, req -> false);

        int merged = service.collectLineage(List.of(
                new LineageManager.LineageEdge("Kafka:orders", "Flink", "SOURCE", 2L, 1000L),
                new LineageManager.LineageEdge("Kafka:orders", "Flink", "SOURCE", 3L, 2000L)
        ));

        Assert.assertEquals(2, merged);
        List<LineageManager.LineageEdge> snapshot = service.lineage();
        Assert.assertEquals(1, snapshot.size());
        Assert.assertEquals(5L, snapshot.get(0).count());
        Assert.assertEquals(2000L, snapshot.get(0).lastUpdatedTime());
    }
}
