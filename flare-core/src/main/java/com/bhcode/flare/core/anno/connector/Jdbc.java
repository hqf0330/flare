package com.bhcode.flare.core.anno.connector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * JDBC connector annotation (Flink)
 * <p>
 * Supports multi-config via keyNum.
 * </p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Jdbcs.class)
public @interface Jdbc {

    /**
     * Sequence number for multi JDBC configs.
     */
    int keyNum() default 1;

    String url();

    String username() default "";

    String password() default "";

    String driver() default "";

    /**
     * SQL template used by sink (optional, can be provided via properties).
     */
    String sql() default "";

    /**
     * Batch size for JDBC sink.
     */
    int batchSize() default 500;

    /**
     * Batch interval in milliseconds for JDBC sink.
     */
    long batchIntervalMs() default 0;

    /**
     * Max retries for JDBC sink.
     */
    int maxRetries() default 3;

    /**
     * Upsert mode: none / mysql / postgresql.
     */
    String upsertMode() default "none";

    /**
     * Key columns for upsert (comma separated).
     */
    String keyColumns() default "";
}
