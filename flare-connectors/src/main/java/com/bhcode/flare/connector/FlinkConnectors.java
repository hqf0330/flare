package com.bhcode.flare.connector;

import com.bhcode.flare.common.util.PropUtils;
import com.bhcode.flare.connector.hbase.HBaseConnector;
import com.bhcode.flare.connector.jdbc.JdbcConnector;
import com.bhcode.flare.connector.kafka.KafkaConnector;
import com.bhcode.flare.connector.redis.RedisConnector;
import com.bhcode.flare.core.anno.connector.HBase;
import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.core.anno.connector.Redis;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.util.function.BiConsumer;

/**
 * Flink connectors facade. Concrete connectors live in subpackages.
 */
@NoArgsConstructor
public final class FlinkConnectors {

    public static void applyConnectorAnnotations(Class<?> targetClass) {
        Kafka[] kafkaAnnos = targetClass.getAnnotationsByType(Kafka.class);
        for (Kafka kafka : kafkaAnnos) {
            String prefix = KafkaConnector.kafkaPrefix(kafka.keyNum());
            setIfAbsent(prefix + "bootstrap.servers", kafka.brokers());
            setIfAbsent(prefix + "topic", kafka.topics());
            setIfAbsent(prefix + "group.id", kafka.groupId());
            setIfAbsent(prefix + "starting.offsets", kafka.startingOffset());
            if (kafka.startFromTimestamp() > 0) {
                setIfAbsent(prefix + "startFromTimestamp", String.valueOf(kafka.startFromTimestamp()));
            }
            if (kafka.config() != null) {
                for (String conf : kafka.config()) {
                    if (conf != null && conf.contains("=")) {
                        String[] kv = conf.split("=", 2);
                        setIfAbsent(prefix + "props." + kv[0].trim(), kv[1].trim());
                    }
                }
            }
            setIfAbsent(prefix + "watermark.strategy", kafka.watermarkStrategy());
            setIfAbsent(prefix + "watermark.maxOutOfOrderness", String.valueOf(kafka.watermarkMaxOutOfOrderness()));
            setIfAbsent(prefix + "watermark.timestampField", kafka.watermarkTimestampField());
        }

        Jdbc[] jdbcAnnos = targetClass.getAnnotationsByType(Jdbc.class);
        for (Jdbc jdbc : jdbcAnnos) {
            String prefix = JdbcConnector.jdbcPrefix(jdbc.keyNum());
            setIfAbsent(prefix + "url", jdbc.url());
            setIfAbsent(prefix + "user", jdbc.username());
            setIfAbsent(prefix + "password", jdbc.password());
            setIfAbsent(prefix + "driver", jdbc.driver());
            setIfAbsent(prefix + "sql", jdbc.sql());
            setIfAbsent(prefix + "batch.size", String.valueOf(jdbc.batchSize()));
            setIfAbsent(prefix + "batch.interval.ms", String.valueOf(jdbc.batchIntervalMs()));
            setIfAbsent(prefix + "max.retries", String.valueOf(jdbc.maxRetries()));
            setIfAbsent(prefix + "upsert.mode", jdbc.upsertMode());
            setIfAbsent(prefix + "key.columns", jdbc.keyColumns());
        }

        HBase[] hbaseAnnos = targetClass.getAnnotationsByType(HBase.class);
        for (HBase hbase : hbaseAnnos) {
            String prefix = HBaseConnector.hbasePrefix(hbase.keyNum());
            setIfAbsent(prefix + "zk.quorum", hbase.zkQuorum());
            setIfAbsent(prefix + "zk.port", hbase.zkPort());
            setIfAbsent(prefix + "znode.parent", hbase.znodeParent());
            setIfAbsent(prefix + "table.name", hbase.tableName());
        }

        Redis[] redisAnnos = targetClass.getAnnotationsByType(Redis.class);
        for (Redis redis : redisAnnos) {
            String prefix = RedisConnector.redisPrefix(redis.keyNum());
            setIfAbsent(prefix + "host", redis.host());
            setIfAbsent(prefix + "port", String.valueOf(redis.port()));
            setIfAbsent(prefix + "password", redis.password());
            setIfAbsent(prefix + "database", String.valueOf(redis.database()));
            setIfAbsent(prefix + "timeout", String.valueOf(redis.timeout()));
            setIfAbsent(prefix + "maxTotal", String.valueOf(redis.maxTotal()));
            setIfAbsent(prefix + "maxIdle", String.valueOf(redis.maxIdle()));
            setIfAbsent(prefix + "minIdle", String.valueOf(redis.minIdle()));
        }
    }

    public static <T> DataStream<T> kafkaSourceFromConf(
            StreamExecutionEnvironment env,
            Class<T> clazz,
            String topicOverride,
            int keyNum
    ) {
        return KafkaConnector.kafkaSourceFromConf(env, clazz, topicOverride, keyNum);
    }

    public static DataStream<String> kafkaSourceFromConf(
            StreamExecutionEnvironment env,
            String topicOverride,
            int keyNum
    ) {
        return KafkaConnector.kafkaSourceFromConf(env, topicOverride, keyNum);
    }

    public static <T> void jdbcSinkFromConf(
            DataStream<T> stream,
            BiConsumer<PreparedStatement, T> binder,
            int keyNum
    ) {
        JdbcConnector.jdbcSinkFromConf(stream, binder, keyNum);
    }

    private static void setIfAbsent(String key, String value) {
        if (key == null || key.trim().isEmpty()) {
            return;
        }
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        // 关键：如果配置中已存在，则不覆盖（实现配置覆盖注解）
        if (PropUtils.getString(key) == null) {
            PropUtils.setProperty(key, value.trim());
        }
    }

    private static void setIfNotBlank(String key, String value) {
        if (key == null || key.trim().isEmpty()) {
            return;
        }
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        PropUtils.setProperty(key, value.trim());
    }
}
