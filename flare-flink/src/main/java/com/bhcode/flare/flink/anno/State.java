package com.bhcode.flare.flink.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * State backend configuration for Flink.
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface State {

    /**
     * State backend type: hashmap / rocksdb.
     */
    String backend() default "hashmap";

    /**
     * Checkpoint storage directory (e.g., hdfs://...).
     */
    String checkpointDir() default "";

    /**
     * Savepoint storage directory (e.g., hdfs://...).
     */
    String savepointDir() default "";

    /**
     * Whether to enable incremental checkpoints (for RocksDB).
     */
    boolean incremental() default true;

    /**
     * TTL for keyed state in days.
     */
    int ttl() default 30;
}
