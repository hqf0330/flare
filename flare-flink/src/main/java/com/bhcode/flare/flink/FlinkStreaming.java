package com.bhcode.flare.flink;

import com.bhcode.flare.common.enums.JobType;
import com.bhcode.flare.common.util.PropUtils;
import com.bhcode.flare.connector.FlinkConnectors;
import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.conf.FlareFlinkConf;
import com.bhcode.flare.flink.util.FlinkSingletonFactory;
import com.bhcode.flare.flink.util.FlinkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.PreparedStatement;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;

@Slf4j
public abstract class FlinkStreaming extends BaseFlink {
    
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tableEnv;
    
    // 用于存放延期的数据
    // TODO: 如果需要使用，可以创建：new OutputTag<Type>("later_data")

    /**
     * 构建或合并 Configuration
     *
     * @param conf 在 conf 基础上构建
     * @return 合并后的 Configuration 对象
     */
    @Override
    public Configuration buildConf(Configuration conf) {
        Configuration finalConf = conf != null ? conf : new Configuration();
        
        // 仅本地模式启用 Web UI
        if (com.bhcode.flare.common.util.FlareUtils.isLocalRunMode()) {
            finalConf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        }
        
        // 从配置中加载其他 Flink 参数
        PropUtils.getSettings().forEach((k, v) -> finalConf.setString(k, v));
        
        this.configuration = finalConf;
        return finalConf;
    }

    /**
     * 创建计算引擎运行时环境
     *
     * @param conf 配置信息
     */
    @Override
    protected void createContext(Object conf) {
        // TODO: 启动 REST 服务（如果启用）
        // if (FlinkUtils.isYarnApplicationMode() || FlareUtils.isLocalRunMode()) {
        //     this.restfulRegister.startRestServer();
        // }
        
        Configuration finalConf = this.buildConf((Configuration) conf);
        
        // 创建 StreamExecutionEnvironment
        // 判断是否是本地模式
        if (com.bhcode.flare.common.util.FlareUtils.isLocalRunMode()) {
            this.env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(finalConf);
            log.info("Running in local mode with Web UI enabled");
        } else {
            this.env = StreamExecutionEnvironment.getExecutionEnvironment();
            log.info("Running in cluster mode");
        }
        
        // 解析并应用 @Streaming 注解配置
        this.applyStreamingAnnotation();
        
        // 获取运行时模式（优先从注解，其次从配置文件，最后默认值）
        RuntimeExecutionMode runtimeMode = RuntimeExecutionMode.STREAMING;
        Streaming streaming = this.getClass().getAnnotation(Streaming.class);
        if (streaming != null) {
            runtimeMode = streaming.executionMode();
        } else {
            // 从配置文件读取运行时模式
            String runtimeModeStr = FlareFlinkConf.getFlinkRuntimeMode();
            if (StringUtils.isNotBlank(runtimeModeStr)) {
                try {
                    runtimeMode = RuntimeExecutionMode.valueOf(runtimeModeStr.trim().toUpperCase());
                } catch (IllegalArgumentException e) {
                    log.warn("Invalid runtime mode from config: {}, using default STREAMING", runtimeModeStr);
                }
            }
        }
        this.env.setRuntimeMode(runtimeMode);
        
        // 设置全局参数
        this.env.getConfig().setGlobalJobParameters(org.apache.flink.api.java.utils.ParameterTool.fromMap(finalConf.toMap()));
        
        // 从配置文件读取 operatorChainingEnable
        if (!FlareFlinkConf.isOperatorChainingEnable()) {
            this.env.disableOperatorChaining();
            log.debug("Operator chaining disabled from config");
        }
        
        // 配置解析 and 应用（解析配置文件中的 Flink 参数）
        this.configParse(this.env);
        
        // 创建 TableEnvironment（可选，默认为关闭，避免引入 Table Planner 依赖）
        if (FlareFlinkConf.isTableEnvEnable()) {
            EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();
            if (runtimeMode == RuntimeExecutionMode.BATCH) {
                this.tableEnv = StreamTableEnvironment.create(
                    this.env,
                    builder.inBatchMode().build()
                );
            } else {
                this.tableEnv = StreamTableEnvironment.create(
                    this.env,
                    builder.inStreamingMode().build()
                );
            }
        } else {
            log.info("TableEnvironment is disabled by config: {}", FlareFlinkConf.FLINK_TABLE_ENV_ENABLE);
            this.tableEnv = null;
        }
        
        // 保存到单例工厂
        com.bhcode.flare.flink.util.FlinkSingletonFactory.getInstance()
                .setStreamEnv(this.env)
                .setTableEnv(this.tableEnv)
                .setAppName(this.appName);
        
        log.info("Flink StreamExecutionEnvironment initialized");
    }

    /**
     * 加载 SQL set statement 参数
     */
    @Override
    protected void loadSqlConf() {
        if (this.tableEnv == null) {
            log.warn("TableEnvironment is not initialized, skip loading SQL configuration");
            return;
        }
        
        // 从配置中加载 SQL 配置
        // 查找所有以 "sql.set." 开头的配置项
        Map<String, String> sqlConfigs = PropUtils.sliceKeys("sql.set.");
        if (!sqlConfigs.isEmpty()) {
            sqlConfigs.forEach((k, v) -> {
                log.info("Execute SQL set: {}={}", k, v);
                this.tableEnv.getConfig().getConfiguration().setString(k, v);
            });
        }
        
        // 也支持 "flink.sql.set." 前缀
        Map<String, String> flinkSqlConfigs = PropUtils.sliceKeys("flink.sql.set.");
        if (!flinkSqlConfigs.isEmpty()) {
            flinkSqlConfigs.forEach((k, v) -> {
                log.info("Execute SQL set: {}={}", k, v);
                this.tableEnv.getConfig().getConfiguration().setString(k, v);
            });
        }
    }
    
    /**
     * 在加载任务配置文件前将被加载
     */
    @Override
    protected void loadConf() {
        // 加载 Flink Streaming 配置文件
        PropUtils.load("flink-streaming");
        log.debug("Flink Streaming configuration loaded");
    }

    /**
     * 初始化运行信息
     * <p>
     * 链路：init -> processAll -> start
     *
     * @param conf 配置信息
     * @param args main方法参数
     */
    @Override
    public void init(Object conf, String[] args) {
        // 应用连接器注解配置（Kafka/JDBC）
        FlinkConnectors.applyConnectorAnnotations(this.getClass());
        super.init(conf, args);
        this.processAll();
        
        // 自动启动任务（可通过配置关闭）
        if (FlareFlinkConf.isJobAutoStart()) {
            this.start();
        } else {
            log.info("Auto start is disabled by config: {}", FlareFlinkConf.FLINK_JOB_AUTO_START);
        }
    }
    
    /**
     * 获取任务类型
     *
     * @return 任务类型
     */
    @Override
    public JobType getJobType() {
        return JobType.FLINK_STREAMING;
    }

    /**
     * 执行 SQL 语句
     *
     * @param sql SQL 语句
     */
    public void sql(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            log.warn("SQL statement is empty");
            return;
        }
        if (this.tableEnv == null) {
            throw new IllegalStateException("TableEnvironment is not initialized. " +
                    "Enable it by setting '" + FlareFlinkConf.FLINK_TABLE_ENV_ENABLE + "=true'.");
        }
        try {
            this.tableEnv.executeSql(sql);
            log.debug("Execute SQL: {}", sql);
        } catch (Exception e) {
            log.error("Failed to execute SQL: {}", sql, e);
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    /**
     * 执行 SQL 查询并返回结果
     *
     * @param sql SQL 查询语句
     * @return Table 对象
     */
    public Table sqlQuery(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL query statement cannot be empty");
        }
        if (this.tableEnv == null) {
            throw new IllegalStateException("TableEnvironment is not initialized. " +
                    "Enable it by setting '" + FlareFlinkConf.FLINK_TABLE_ENV_ENABLE + "=true'.");
        }
        try {
            return this.tableEnv.sqlQuery(sql);
        } catch (Exception e) {
            log.error("Failed to execute SQL query: {}", sql, e);
            throw new RuntimeException("Failed to execute SQL query: " + sql, e);
        }
    }

    /**
     * 为指定的 DataStream 设定 uid 与 name（对标 fire 的 uname）
     *
     * @param stream DataStream 实例
     * @param uid    唯一标识（同时也作为默认名称）
     * @param <T>    数据类型
     * @return 原始 stream
     */
    public <T> DataStream<T> uname(
            DataStream<T> stream, String uid) {
        return uname(stream, uid, uid);
    }

    /**
     * 为指定的 DataStream 设定 uid 与 name
     *
     * @param stream DataStream 实例
     * @param uid    唯一标识
     * @param name   算子名称
     * @param <T>    数据类型
     * @return 原始 stream
     */
    public <T> DataStream<T> uname(
            DataStream<T> stream, String uid, String name) {
        return FlinkUtils.uname(stream, uid, name);
    }

    /**
     * 设置并行度（对标 fire 的 repartition）
     */
    public <T> DataStream<T> repartition(
            DataStream<T> stream, int parallelism) {
        if (stream instanceof SingleOutputStreamOperator && parallelism > 0) {
            ((SingleOutputStreamOperator<T>) stream).setParallelism(parallelism);
        }
        return stream;
    }

    /**
     * 启动 Flink 任务
     */
    public void start() {
        start(this.resolveJobName());
    }

    /**
     * 启动 Flink 任务并指定 jobName
     *
     * @param jobName 任务名称
     */
    public void start(String jobName) {
        String finalJobName = (jobName == null || jobName.trim().isEmpty())
                ? this.resolveJobName()
                : jobName.trim();
        try {
            if (this.env == null) {
                throw new IllegalStateException("StreamExecutionEnvironment is not initialized");
            }
            this.env.execute(finalJobName);
        } catch (Exception e) {
            log.error("Failed to start Flink job: {}", finalJobName, e);
            throw new RuntimeException("Failed to start Flink job", e);
        }
    }

    /**
     * 解析最终的 jobName
     * 优先级：args(appName/jobName) -> flink.appName -> appName字段 -> driverClass
     */
    private String resolveJobName() {
        if (this.appName != null && !this.appName.trim().isEmpty()) {
            return this.appName.trim();
        }
        String singletonAppName = FlinkSingletonFactory.getInstance().getAppName();
        if (singletonAppName != null && !singletonAppName.trim().isEmpty()) {
            return singletonAppName.trim();
        }
        return this.driverClass;
    }

    public <T> DataStream<T> kafkaSourceFromConf(Class<T> clazz) {
        return kafkaSourceFromConf(clazz, null, 1);
    }

    public <T> DataStream<T> kafkaSourceFromConf(Class<T> clazz, String topicOverride) {
        return kafkaSourceFromConf(clazz, topicOverride, 1);
    }

    public <T> DataStream<T> kafkaSourceFromConf(Class<T> clazz, int keyNum) {
        return kafkaSourceFromConf(clazz, null, keyNum);
    }

    public <T> DataStream<T> kafkaSourceFromConf(Class<T> clazz, String topicOverride, int keyNum) {
        return FlinkConnectors.kafkaSourceFromConf(this.env, clazz, topicOverride, keyNum);
    }

    public DataStream<String> kafkaSourceFromConf() {
        return kafkaSourceFromConf(String.class, null, 1);
    }

    public DataStream<String> kafkaSourceFromConf(String topicOverride) {
        return kafkaSourceFromConf(String.class, topicOverride, 1);
    }

    public DataStream<String> kafkaSourceFromConf(int keyNum) {
        return kafkaSourceFromConf(String.class, null, keyNum);
    }

    public DataStream<String> kafkaSourceFromConf(String topicOverride, int keyNum) {
        return kafkaSourceFromConf(String.class, topicOverride, keyNum);
    }

    public <T> void jdbcSinkFromConf(
            DataStream<T> stream,
            BiConsumer<PreparedStatement, T> binder) {
        jdbcSinkFromConf(stream, binder, 1);
    }

    public <T> void jdbcSinkFromConf(
            DataStream<T> stream,
            BiConsumer<PreparedStatement, T> binder,
            int keyNum) {
        FlinkConnectors.jdbcSinkFromConf(stream, binder, keyNum);
    }

    /**
     * 获取 Flink StreamExecutionEnvironment
     *
     * @return StreamExecutionEnvironment
     */
    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    /**
     * 获取 Flink TableEnvironment
     *
     * @return StreamTableEnvironment
     */
    public StreamTableEnvironment getTableEnv() {
        return tableEnv;
    }
    
    /**
     * 配置解析和应用
     * <p>
     * 解析配置文件中的 Flink 参数并应用到 Flink 环境
     * 与 Fire 框架的 configParse 方法保持一致
     * </p>
     *
     * @param env StreamExecutionEnvironment
     */
    protected void configParse(StreamExecutionEnvironment env) {
        if (env == null) {
            log.warn("StreamExecutionEnvironment is null, skip config parsing");
            return;
        }

        // flink.max.parallelism
        int maxParallelism = FlareFlinkConf.getMaxParallelism();
        if (maxParallelism != -1) {
            env.setMaxParallelism(maxParallelism);
            log.debug("Set max parallelism from config: {}", maxParallelism);
        }

        // flink.default.parallelism
        int defaultParallelism = FlareFlinkConf.getDefaultParallelism();
        if (defaultParallelism != -1) {
            env.setParallelism(defaultParallelism);
            log.debug("Set default parallelism from config: {}", defaultParallelism);
        }

        // flink.stream.buffer.timeout.millis
        long bufferTimeout = FlareFlinkConf.getStreamBufferTimeoutMillis();
        if (bufferTimeout != -1) {
            env.setBufferTimeout(bufferTimeout);
            log.debug("Set buffer timeout from config: {} ms", bufferTimeout);
        }

        // flink.stream.number.execution.retries
        int retries = FlareFlinkConf.getStreamNumberExecutionRetries();
        if (retries != -1) {
            env.setNumberOfExecutionRetries(retries);
            log.debug("Set number of execution retries from config: {}", retries);
        }

        // Flink 1.12+ no longer uses TimeCharacteristic; keep config key but skip applying.

        // Checkpoint 相关参数
        CheckpointConfig ckConfig = env.getCheckpointConfig();
        long checkpointInterval = FlareFlinkConf.getStreamCheckpointInterval();
        if (ckConfig != null && checkpointInterval > 0) {
            // flink.stream.checkpoint.interval 单位：毫秒
            env.enableCheckpointing(checkpointInterval);
            log.debug("Enabled checkpointing with interval: {} ms", checkpointInterval);

            // flink.stream.checkpoint.mode
            String checkpointMode = FlareFlinkConf.getStreamCheckpointMode();
            if (StringUtils.isNotBlank(checkpointMode)) {
                try {
                    CheckpointingMode mode = CheckpointingMode.valueOf(checkpointMode.trim().toUpperCase());
                    ckConfig.setCheckpointingMode(mode);
                    log.debug("Set checkpoint mode from config: {}", mode);
                } catch (IllegalArgumentException e) {
                    log.warn("Invalid checkpoint mode: {}, using default", checkpointMode);
                }
            }

            // flink.stream.checkpoint.timeout 单位：毫秒
            long checkpointTimeout = FlareFlinkConf.getStreamCheckpointTimeout();
            if (checkpointTimeout > 0) {
                ckConfig.setCheckpointTimeout(checkpointTimeout);
                log.debug("Set checkpoint timeout from config: {} ms", checkpointTimeout);
            }

            // flink.stream.checkpoint.max.concurrent
            int maxConcurrent = FlareFlinkConf.getStreamCheckpointMaxConcurrent();
            if (maxConcurrent > 0) {
                ckConfig.setMaxConcurrentCheckpoints(maxConcurrent);
                log.debug("Set max concurrent checkpoints from config: {}", maxConcurrent);
            }

            // flink.stream.checkpoint.min.pause.between
            int minPauseBetween = FlareFlinkConf.getStreamCheckpointMinPauseBetween();
            if (minPauseBetween >= 0) {
                ckConfig.setMinPauseBetweenCheckpoints(minPauseBetween);
                log.debug("Set min pause between checkpoints from config: {} ms", minPauseBetween);
            } else {
                // 如果未设置，默认使用 checkpoint 间隔
                ckConfig.setMinPauseBetweenCheckpoints(checkpointInterval);
            }

            // flink.stream.checkpoint.tolerable.failure.number
            int tolerableFailure = FlareFlinkConf.getStreamCheckpointTolerableFailureNumber();
            if (tolerableFailure >= 0) {
                ckConfig.setTolerableCheckpointFailureNumber(tolerableFailure);
                log.debug("Set tolerable checkpoint failure number from config: {}", tolerableFailure);
            }

            // flink.stream.checkpoint.externalized
            String externalized = FlareFlinkConf.getStreamCheckpointExternalized();
            if (StringUtils.isNotBlank(externalized)) {
                try {
                    ExternalizedCheckpointCleanup cleanup = 
                        ExternalizedCheckpointCleanup.valueOf(externalized.trim());
                    ckConfig.enableExternalizedCheckpoints(cleanup);
                    log.debug("Set externalized checkpoint cleanup from config: {}", cleanup);
                } catch (IllegalArgumentException e) {
                    log.warn("Invalid externalized checkpoint cleanup: {}, using default", externalized);
                }
            }

            // flink.stream.checkpoint.unaligned.enable
            boolean unaligned = FlareFlinkConf.isUnalignedCheckpointEnable();
            ckConfig.enableUnalignedCheckpoints(unaligned);
            log.debug("Set unaligned checkpoint from config: {}", unaligned);
        }

        // Apply ExecutionConfig settings from FlareFlinkConf (aligned with Fire)
        FlinkUtils.parseConf(env.getConfig());

        log.debug("Flink configuration parsed and applied from config file");
    }

    /**
     * 应用 @Streaming 注解配置到 Flink 环境
     */
    private void applyStreamingAnnotation() {
        Streaming streaming = this.getClass().getAnnotation(Streaming.class);
        if (streaming == null) {
            log.debug("No @Streaming annotation found, using default configuration");
            return;
        }

        log.info("Applying @Streaming annotation configuration");

        // 设置运行时模式
        RuntimeExecutionMode runtimeMode = streaming.executionMode();
        this.env.setRuntimeMode(runtimeMode);

        // 设置并行度
        if (streaming.parallelism() > 0) {
            this.env.setParallelism(streaming.parallelism());
            log.debug("Set parallelism: {}", streaming.parallelism());
        }

        // 设置是否禁用 OperatorChaining
        if (streaming.disableOperatorChaining()) {
            this.env.disableOperatorChaining();
            log.debug("Operator chaining disabled");
        }

        // 配置 Checkpoint
        int checkpointInterval = streaming.value() > 0 ? streaming.value() : streaming.interval();
        if (checkpointInterval > 0) {
            // 转换为毫秒
            long intervalMs = checkpointInterval * 1000L;
            this.env.enableCheckpointing(intervalMs);
            
            CheckpointConfig checkpointConfig = this.env.getCheckpointConfig();
            
            // 设置 checkpoint 模式
            checkpointConfig.setCheckpointingMode(streaming.mode());
            
            // 设置 checkpoint 超时时间
            if (streaming.timeout() > 0) {
                checkpointConfig.setCheckpointTimeout(streaming.timeout() * 1000L);
            }
            
            // 设置最大并发 checkpoint
            if (streaming.concurrent() > 0) {
                checkpointConfig.setMaxConcurrentCheckpoints(streaming.concurrent());
            }
            
            // 设置两次 checkpoint 之间的最小间隔
            if (streaming.pauseBetween() > 0) {
                checkpointConfig.setMinPauseBetweenCheckpoints(streaming.pauseBetween() * 1000L);
            } else {
                // 如果没有设置，默认使用 checkpoint 间隔
                checkpointConfig.setMinPauseBetweenCheckpoints(intervalMs);
            }
            
            // 设置可容忍的 checkpoint 失败次数
            if (streaming.failureNumber() >= 0) {
                checkpointConfig.setTolerableCheckpointFailureNumber(streaming.failureNumber());
            }
            
            // 设置外部化 checkpoint
            checkpointConfig.setExternalizedCheckpointCleanup(streaming.cleanup());
            
            // 设置非对齐 checkpoint
            checkpointConfig.enableUnalignedCheckpoints(streaming.unaligned());
            
            log.info("Checkpoint configured: interval={}s, mode={}, timeout={}s", 
                    checkpointInterval, streaming.mode(), streaming.timeout());
        }

        // 设置 watermark 间隔
        if (streaming.watermarkInterval() > 0) {
            // TODO: 设置 watermark 间隔
            log.debug("Watermark interval: {}ms", streaming.watermarkInterval());
        }

        log.info("@Streaming annotation configuration applied successfully");
    }

}
