package com.bhcode.flare.core.anno.connector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Annotation for asynchronous lookup (Async I/O).
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AsyncLookup {

    /**
     * Cache maximum size.
     */
    long cacheSize() default 10000;

    /**
     * Cache expire time.
     */
    long cacheExpire() default 60;

    /**
     * Cache expire time unit.
     */
    TimeUnit cacheUnit() default TimeUnit.SECONDS;

    /**
     * Async I/O timeout in seconds.
     */
    long timeout() default 30;

    /**
     * Async I/O capacity (max concurrent requests).
     */
    int capacity() default 100;
}
