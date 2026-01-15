package com.bhcode.flare.connector.kafka;

import com.bhcode.flare.common.lineage.LineageManager;
import com.bhcode.flare.common.util.PropUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public final class KafkaConnector {

    private KafkaConnector() {
        // Utility class
    }

    public static String kafkaPrefix(int keyNum) {
        validateKeyNum(keyNum);
        return keyNum == 1 ? "kafka." : "kafka" + keyNum + ".";
    }

    /**
     * Create Kafka source with generic type support.
     */
    public static <T> DataStream<T> kafkaSourceFromConf(
            StreamExecutionEnvironment env,
            Class<T> clazz,
            String topicOverride,
            int keyNum
    ) {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment is null");
        }
        String prefix = kafkaPrefix(keyNum);
        String bootstrapServers = PropUtils.getString(prefix + "bootstrap.servers");
        String topic = topicOverride != null && !topicOverride.trim().isEmpty()
                ? topicOverride.trim()
                : PropUtils.getString(prefix + "topic");
        String groupId = PropUtils.getString(prefix + "group.id", "flare");
        String startOffsets = PropUtils.getString(prefix + "starting.offsets", "latest");
        long startFromTimestamp = PropUtils.getLong(prefix + "startFromTimestamp", -1);

        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalStateException(prefix + "bootstrap.servers is required");
        }
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalStateException(prefix + "topic is required");
        }

        OffsetsInitializer offsetsInitializer;
        if (startFromTimestamp > 0) {
            offsetsInitializer = OffsetsInitializer.timestamp(startFromTimestamp);
            log.info("Kafka source {} starting from timestamp: {}", prefix, startFromTimestamp);
        } else {
            offsetsInitializer = "earliest".equalsIgnoreCase(startOffsets)
                    ? OffsetsInitializer.earliest()
                    : OffsetsInitializer.latest();
        }

        Properties extraProps = new Properties();
        Map<String, String> extra = PropUtils.sliceKeys(prefix + "props.");
        extra.forEach(extraProps::setProperty);

        List<String> topics = parseTopics(topic);
        if (topics.isEmpty()) {
            throw new IllegalStateException(prefix + "topic is required");
        }

        DeserializationSchema<T> deserializer;
        if (clazz == String.class) {
            deserializer = (DeserializationSchema<T>) new SimpleStringSchema();
        } else {
            deserializer = new FlareJsonDeserializationSchema<>(clazz);
        }

        KafkaSource<T> source = KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(deserializer)
                .setProperties(extraProps)
                .build();

        LineageManager.addLineage("Kafka:" + bootstrapServers + "/" + topic, "Flink", "SOURCE");

        return env.fromSource(source, resolveWatermarkStrategy(prefix), "kafka-source-" + keyNum);
    }

    /**
     * Backwards compatibility for String source.
     */
    public static DataStream<String> kafkaSourceFromConf(
            StreamExecutionEnvironment env,
            String topicOverride,
            int keyNum
    ) {
        return kafkaSourceFromConf(env, String.class, topicOverride, keyNum);
    }

    /**
     * Resolve WatermarkStrategy from configuration.
     */
    private static <T> WatermarkStrategy<T> resolveWatermarkStrategy(String prefix) {
        String strategyType = PropUtils.getString(prefix + "watermark.strategy", "no");
        WatermarkStrategy<T> strategy;

        switch (strategyType.toLowerCase()) {
            case "bounded":
                int maxOutOfOrderness = PropUtils.getInt(prefix + "watermark.maxOutOfOrderness", 5);
                strategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderness));
                break;
            case "monotonic":
                strategy = WatermarkStrategy.forMonotonousTimestamps();
                break;
            default:
                return WatermarkStrategy.noWatermarks();
        }

        // TODO: Resolve timestamp field if provided
        // String timestampField = PropUtils.getString(prefix + "watermark.timestampField");
        // if (timestampField != null && !timestampField.isEmpty()) {
        //     strategy = strategy.withTimestampAssigner(...);
        // }

        return strategy;
    }

    private static void validateKeyNum(int keyNum) {
        if (keyNum <= 0) {
            throw new IllegalArgumentException("keyNum must be >= 1");
        }
    }

    private static List<String> parseTopics(String topics) {
        String[] parts = topics.split(",");
        List<String> result = new ArrayList<>();
        for (String part : parts) {
            if (part != null) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    result.add(trimmed);
                }
            }
        }
        return result;
    }
}
