package com.bhcode.flare.core.anno.connector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Kafka connector annotation (Flink)
 * <p>
 * Supports multi-config via keyNum.
 * </p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Kafkas.class)
public @interface Kafka {

    /**
     * Sequence number for multi Kafka configs.
     */
    int keyNum() default 1;

    /**
     * Kafka bootstrap servers.
     */
    String brokers();

    /**
     * Kafka topic.
     */
    String topics();

    /**
     * Kafka groupId.
     */
    String groupId() default "flare";

    /**
     * Starting offsets: earliest / latest.
     */
    String startingOffset() default "latest";

    /**
     * Start consuming from a specific timestamp (milliseconds).
     */
    long startFromTimestamp() default -1;

    /**
     * Custom Kafka properties in key=value format.
     */
    String[] config() default {};

    /**
     * Watermark strategy: no / bounded / monotonic.
     */
    String watermarkStrategy() default "no";

    /**
     * Max out-of-orderness for bounded strategy (in seconds).
     */
    int watermarkMaxOutOfOrderness() default 5;

    /**
     * Timestamp field for event-time watermark.
     */
    String watermarkTimestampField() default "";
}
