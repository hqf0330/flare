package com.bhcode.flare.examples;

import com.bhcode.flare.common.anno.Config;
import com.bhcode.flare.common.util.PropUtils;
import com.bhcode.flare.core.anno.connector.After;
import com.bhcode.flare.core.anno.connector.Before;
import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.core.anno.connector.Process;
import com.bhcode.flare.core.anno.lifecycle.Step1;
import com.bhcode.flare.core.anno.lifecycle.Step2;
import com.bhcode.flare.core.anno.lifecycle.Step3;
import com.bhcode.flare.flink.FlinkJobLauncher;
import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.anno.Checkpoint;
import com.bhcode.flare.flink.anno.State;
import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.util.FlinkUtils;
import com.bhcode.flare.flink.util.MetricUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 复杂示例：覆盖 Flare 当前主要能力（注解、配置、生命周期、连接器、执行配置）
 */
@Config(
        files = {"flink", "flink-streaming", "flare"},
        props = {
                "flink.appName=complex-flink-task-from-config",
                "flink.default.parallelism=3",
                "example.use.kafka=false"
        },
        value = "example.tag=config-default"
)
@Slf4j
@Kafka(
        keyNum = 1,
        brokers = "localhost:9092",
        topics = "flare-topic",
        groupId = "flare-example",
        startingOffset = "latest",
        startFromTimestamp = 1736870400000L, // 演示：按时间戳启动 (2025-01-15)
        config = {"max.poll.records=500", "fetch.min.bytes=1024"}, // 演示：自定义参数
        watermarkStrategy = "bounded",
        watermarkMaxOutOfOrderness = 3
)
@Jdbc(
        keyNum = 1,
        url = "jdbc:mysql://localhost:3306/flare",
        username = "root",
        password = "root",
        driver = "com.mysql.cj.jdbc.Driver",
        sql = "INSERT INTO flare_sink(id, value, tag) VALUES (?, ?, ?)",
        upsertMode = "mysql", // 演示：自动生成 ON DUPLICATE KEY UPDATE
        keyColumns = "id",    // 演示：主键列不参与更新
        batchSize = 100,
        batchIntervalMs = 1000
)
@Streaming(
        interval = 15,
        parallelism = 2,
        disableOperatorChaining = false,
        unaligned = true,
        autoStart = true
)
@Checkpoint(
        timeout = 60,
        concurrent = 1,
        failureNumber = 3
)
@State(
        backend = "rocksdb",
        checkpointDir = "hdfs:///flink/checkpoints",
        incremental = true,
        ttl = 7
)
public class ComplexFlinkTask extends FlinkStreaming {

    /**
     * Java 17 record: 极其适合作为 Flink 的 DTO
     */
    public record UserAction(String user, String action, Long ts) {}

    @Before
    public void beforeInit() {
        log.info("Before init. Flink version: {}", FlinkUtils.getVersion());
        log.info("Deploy mode: {}", FlinkUtils.getDeployMode());
        log.info("ResourceId: {}", FlinkUtils.getResourceId());
    }

    @Override
    public void process() {
        log.info("Complex task started. appName={}", this.appName);
        
        // 演示：使用内建指标监控
        this.counter("task_start_count");

        // 演示一：使用 Java 17 record 自动解析 Kafka JSON
        DataStream<UserAction> actions = this.kafkaSourceFromConf(UserAction.class, 1);
        this.uname(actions, "source_user_action", "Kafka Source (UserAction)");

        actions.map(new RichMapFunction<UserAction, String>() {
            @Override
            public String map(UserAction action) {
                // 演示：分布式环境下的指标统计（真正的分布式累加器）
                MetricUtils.counter(getRuntimeContext(), "action_" + action.action());
                return "User: " + action.user() + " performed " + action.action();
            }
        }).print("action-log");

        // 演示二：原有 String 类型的 Source 处理
        DataStream<String> source = buildSource();
        DataStream<String> normalized = source
                .filter(Objects::nonNull)
                .map(value -> value.trim())
                .filter(value -> !value.isEmpty())
                .map(value -> value.toUpperCase());
        
        this.uname(normalized, "op_normalize", "Normalize String Stream");

        if (PropUtils.getBoolean("example.use.jdbc", false)) {
            this.jdbcSinkFromConf(normalized, this::bindJdbc, 1);
        } else {
            normalized.print("stdout");
        }

        if (PropUtils.getBoolean("example.use.table", false)) {
            runTableExample(normalized);
        }
    }

    private DataStream<String> buildSource() {
        boolean useKafka = PropUtils.getBoolean("example.use.kafka", false);
        if (useKafka) {
            return this.kafkaSourceFromConf(1);
        }
        StreamExecutionEnvironment env = this.getEnv();
        List<String> data = Arrays.asList("flare", "complex", "task", "demo");
        return env.fromCollection(data).name("collection-source");
    }

    private void bindJdbc(PreparedStatement ps, String value) {
        try {
            ps.setInt(1, value.hashCode()); // 演示：设置 ID
            ps.setString(2, value);
            ps.setString(3, PropUtils.getString("example.tag", "default"));
        } catch (SQLException e) {
            throw new RuntimeException("JDBC bind failed", e);
        }
    }

    private void runTableExample(DataStream<String> stream) {
        StreamTableEnvironment tableEnv = getTableEnv();
        if (tableEnv == null) {
            log.warn("TableEnv disabled. Set flink.table.env.enable=true to enable.");
            return;
        }
        tableEnv.createTemporaryView("t_input", stream);
        Table table = tableEnv.sqlQuery("SELECT UPPER(f0) AS value FROM t_input");
        log.info("Table schema: {}", table.getResolvedSchema());
    }

    @Process("post-process hook")
    public void postProcess() {
        log.info("@Process executed. SQL log enable={}", PropUtils.getBoolean("flink.sql.log.enable", false));
    }

    @Step1("validate config")
    public void stepValidateConfig() {
        log.info("Step1: example.use.kafka={}, example.use.jdbc={}",
                PropUtils.getBoolean("example.use.kafka", false),
                PropUtils.getBoolean("example.use.jdbc", false));
    }

    @Step2("emit diagnostics")
    public void stepDiagnostics() {
        log.info("Step2: engineUp={}, jobManager={}", FlinkUtils.isEngineUp(), FlinkUtils.isJobManager());
    }

    @Step3("cleanup hint")
    public void stepCleanupHint() {
        log.info("Step3: if needed, set fire.env.local=false for cluster mode");
    }

    @After
    public void afterFinish() {
        log.info("After finish. elapsedMs={}", this.elapsed());
    }

    public static void main(String[] args) {
        FlinkJobLauncher.run(ComplexFlinkTask.class, args);
    }
}
