package com.bhcode.flare.flink.conf;

import com.bhcode.flare.common.util.PropUtils;
import org.apache.commons.lang3.StringUtils;

public class FlareFlinkConf {

    // ========== 基础配置 ==========
    public static final String FLINK_AUTO_GENERATE_UID_ENABLE = "flink.auto.generate.uid.enable";
    public static final String FLINK_AUTO_TYPE_REGISTRATION_ENABLE = "flink.auto.type.registration.enable";
    public static final String FLINK_FORCE_AVRO_ENABLE = "flink.force.avro.enable";
    public static final String FLINK_FORCE_KRYO_ENABLE = "flink.force.kryo.enable";
    public static final String FLINK_GENERIC_TYPES_ENABLE = "flink.generic.types.enable";
    public static final String FLINK_OBJECT_REUSE_ENABLE = "flink.object.reuse.enable";
    public static final String FLINK_AUTO_WATERMARK_INTERVAL = "flink.auto.watermark.interval";
    public static final String FLINK_CLOSURE_CLEANER_LEVEL = "flink.closure.cleaner.level";
    public static final String FLINK_DEFAULT_INPUT_DEPENDENCY_CONSTRAINT = "flink.default.input.dependency.constraint";
    public static final String FLINK_EXECUTION_MODE = "flink.execution.mode";
    public static final String FLINK_RUNTIME_MODE = "flink.runtime.mode";
    public static final String FLINK_LATENCY_TRACKING_INTERVAL = "flink.latency.tracking.interval";
    public static final String FLINK_MAX_PARALLELISM = "flink.max.parallelism";
    public static final String FLINK_DEFAULT_PARALLELISM = "flink.default.parallelism";
    public static final String FLINK_TASK_CANCELLATION_INTERVAL = "flink.task.cancellation.interval";
    public static final String FLINK_TASK_CANCELLATION_TIMEOUT_MILLIS = "flink.task.cancellation.timeout.millis";
    public static final String FLINK_USE_SNAPSHOT_COMPRESSION = "flink.use.snapshot.compression";
    public static final String FLINK_STREAM_BUFFER_TIMEOUT_MILLIS = "flink.stream.buffer.timeout.millis";
    public static final String FLINK_STREAM_NUMBER_EXECUTION_RETRIES = "flink.stream.number.execution.retries";
    public static final String FLINK_STREAM_TIME_CHARACTERISTIC = "flink.stream.time.characteristic";
    public static final String FLINK_DRIVER_CLASS_NAME = "flink.driver.class.name";
    public static final String FLINK_CLIENT_SIMPLE_CLASS_NAME = "flink.client.simple.class.name";
    public static final String FLINK_APP_NAME = "flink.appName";
    public static final String FLINK_SQL_CONF_UDF_JARS = "flink.sql.conf.pipeline.jars";
    public static final String FLINK_SQL_LOG_ENABLE = "flink.sql.log.enable";
    public static final String FLINK_SQL_DEFAULT_CATALOG_NAME = "flink.sql.default.catalog.name";
    public static final String FLINK_STATE_TTL_DAYS = "flink.state.ttl.days";
    public static final String DISTRIBUTE_SYNC_ENABLE = "fire.distribute.sync.enable";
    public static final String OPERATOR_CHAINING_ENABLE = "flink.env.operatorChaining.enable";
    public static final String FLINK_TABLE_ENV_ENABLE = "flink.table.env.enable";
    public static final String FLINK_DIRTY_DATA_PRINT_ENABLE = "flare.dirty.data.print.enable";

    // ========== Checkpoint 相关配置 ==========
    public static final String FLINK_STREAM_CHECKPOINT_INTERVAL = "flink.stream.checkpoint.interval";
    public static final String FLINK_STREAM_CHECKPOINT_MODE = "flink.stream.checkpoint.mode";
    public static final String FLINK_STREAM_CHECKPOINT_TIMEOUT = "flink.stream.checkpoint.timeout";
    public static final String FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT = "flink.stream.checkpoint.max.concurrent";
    public static final String FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN = "flink.stream.checkpoint.min.pause.between";
    public static final String FLINK_STREAM_CHECKPOINT_PREFER_RECOVERY = "flink.stream.checkpoint.prefer.recovery";
    public static final String FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER = "flink.stream.checkpoint.tolerable.failure.number";
    public static final String FLINK_STREAM_CHECKPOINT_EXTERNALIZED = "flink.stream.checkpoint.externalized";
    public static final String FLINK_STREAM_CHECKPOINT_UNALIGNED = "flink.stream.checkpoint.unaligned.enable";
    public static final String FLINK_SQL_WITH_REPLACE_MODE_ENABLE = "flink.sql_with.replaceMode.enable";
    public static final String FLINK_STATE_CLEAN_HDFS_URL = "flink.state.clean.hdfs.url";

    // ========== UDF 相关配置 ==========
    public static final String FLINK_SQL_UDF_CONF_PREFIX = "flink.sql.udf.conf.";
    public static final String FLINK_SQL_UDF_ENABLE = "flink.sql.udf.fireUdf.enable";
    public static final String FLINK_SQL_WITH_PREFIX = "flink.sql.with.";
    public static final String FLINK_SQL_USE_STATEMENT_SET = "flink.sql.useStatementSet";
    public static final String FLINK_JOB_AUTO_START = "flink.job.auto.start";

    // ========== 配置值读取方法 ==========

    public static int getMaxParallelism() {
        return PropUtils.getInt(FLINK_MAX_PARALLELISM, 1024);
    }

    public static boolean isAutoGenerateUidEnable() {
        return PropUtils.getBoolean(FLINK_AUTO_GENERATE_UID_ENABLE, true);
    }

    public static boolean isAutoTypeRegistrationEnable() {
        return PropUtils.getBoolean(FLINK_AUTO_TYPE_REGISTRATION_ENABLE, true);
    }

    public static boolean isForceAvroEnable() {
        return PropUtils.getBoolean(FLINK_FORCE_AVRO_ENABLE, false);
    }

    public static boolean isForceKryoEnable() {
        return PropUtils.getBoolean(FLINK_FORCE_KRYO_ENABLE, false);
    }

    public static boolean isGenericTypesEnable() {
        return PropUtils.getBoolean(FLINK_GENERIC_TYPES_ENABLE, true);
    }

    public static boolean isObjectReuseEnable() {
        return PropUtils.getBoolean(FLINK_OBJECT_REUSE_ENABLE, false);
    }

    public static long getAutoWatermarkInterval() {
        return PropUtils.getLong(FLINK_AUTO_WATERMARK_INTERVAL, -1);
    }

    public static String getClosureCleanerLevel() {
        return PropUtils.getString(FLINK_CLOSURE_CLEANER_LEVEL, "");
    }

    public static String getDefaultInputDependencyConstraint() {
        return PropUtils.getString(FLINK_DEFAULT_INPUT_DEPENDENCY_CONSTRAINT, "");
    }

    public static String getExecutionMode() {
        return PropUtils.getString(FLINK_EXECUTION_MODE, "");
    }

    public static long getLatencyTrackingInterval() {
        return PropUtils.getLong(FLINK_LATENCY_TRACKING_INTERVAL, -1);
    }

    public static int getDefaultParallelism() {
        return PropUtils.getInt(FLINK_DEFAULT_PARALLELISM, -1);
    }

    public static long getStreamBufferTimeoutMillis() {
        return PropUtils.getLong(FLINK_STREAM_BUFFER_TIMEOUT_MILLIS, -1);
    }

    public static int getStreamNumberExecutionRetries() {
        return PropUtils.getInt(FLINK_STREAM_NUMBER_EXECUTION_RETRIES, -1);
    }

    public static long getTaskCancellationInterval() {
        return PropUtils.getLong(FLINK_TASK_CANCELLATION_INTERVAL, -1);
    }

    public static long getTaskCancellationTimeoutMillis() {
        return PropUtils.getLong(FLINK_TASK_CANCELLATION_TIMEOUT_MILLIS, -1);
    }

    public static boolean isUseSnapshotCompression() {
        return PropUtils.getBoolean(FLINK_USE_SNAPSHOT_COMPRESSION, false);
    }

    public static String getStreamTimeCharacteristic() {
        return PropUtils.getString(FLINK_STREAM_TIME_CHARACTERISTIC, "");
    }

    public static String getFlinkAppName() {
        return PropUtils.getString(FLINK_APP_NAME, "");
    }

    public static boolean isSqlLogEnable() {
        return PropUtils.getBoolean(FLINK_SQL_LOG_ENABLE, false);
    }

    public static long getStreamCheckpointInterval() {
        return PropUtils.getLong(FLINK_STREAM_CHECKPOINT_INTERVAL, -1);
    }

    public static String getStreamCheckpointMode() {
        return PropUtils.getString(FLINK_STREAM_CHECKPOINT_MODE, "EXACTLY_ONCE");
    }

    public static long getStreamCheckpointTimeout() {
        return PropUtils.getLong(FLINK_STREAM_CHECKPOINT_TIMEOUT, 600000L);
    }

    public static int getStreamCheckpointMaxConcurrent() {
        return PropUtils.getInt(FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT, 1);
    }

    public static int getStreamCheckpointMinPauseBetween() {
        return PropUtils.getInt(FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN, -1);
    }

    public static int getStreamCheckpointTolerableFailureNumber() {
        return PropUtils.getInt(FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER, 0);
    }

    public static String getStreamCheckpointExternalized() {
        return PropUtils.getString(FLINK_STREAM_CHECKPOINT_EXTERNALIZED, "RETAIN_ON_CANCELLATION");
    }

    public static boolean isUnalignedCheckpointEnable() {
        return PropUtils.getBoolean(FLINK_STREAM_CHECKPOINT_UNALIGNED, true);
    }

    public static String getFlinkRuntimeMode() {
        return PropUtils.getString(FLINK_RUNTIME_MODE, 
                PropUtils.getString("execution.runtime-mode", "STREAMING"));
    }

    public static boolean isOperatorChainingEnable() {
        return PropUtils.getBoolean(OPERATOR_CHAINING_ENABLE, true);
    }

    public static boolean isTableEnvEnable() {
        return PropUtils.getBoolean(FLINK_TABLE_ENV_ENABLE, false);
    }

    public static boolean isFlinkUdfEnable() {
        return PropUtils.getBoolean(FLINK_SQL_UDF_ENABLE, true);
    }

    public static java.util.Map<String, String> getFlinkUdfList() {
        return PropUtils.sliceKeys(FLINK_SQL_UDF_CONF_PREFIX);
    }

    public static boolean isDistributeSyncEnabled() {
        return PropUtils.getBoolean(DISTRIBUTE_SYNC_ENABLE, true);
    }

    public static int getFlinkStateTtlDays() {
        return PropUtils.getInt(FLINK_STATE_TTL_DAYS, 31);
    }

    public static boolean isAutoAddStatementSet() {
        return PropUtils.getBoolean(FLINK_SQL_USE_STATEMENT_SET, true);
    }

    public static java.util.Map<String, String> getFlinkSqlWithOptions() {
        return PropUtils.sliceKeys(FLINK_SQL_WITH_PREFIX);
    }

    public static String getStateHdfsUrl() {
        return PropUtils.getString(FLINK_STATE_CLEAN_HDFS_URL, "");
    }

    public static boolean isJobAutoStart() {
        return PropUtils.getBoolean(FLINK_JOB_AUTO_START, true);
    }

    public static boolean isDirtyDataPrintEnable() {
        return PropUtils.getBoolean(FLINK_DIRTY_DATA_PRINT_ENABLE, false);
    }

    private FlareFlinkConf() {
        // Private constructor to prevent instantiation
    }
}
