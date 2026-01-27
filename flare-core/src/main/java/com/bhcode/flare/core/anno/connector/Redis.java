package com.bhcode.flare.core.anno.connector;

import java.lang.annotation.*;

/**
 * Annotation for Redis connection configuration.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Redises.class)
public @interface Redis {
    int keyNum() default 1;
    String host() default "localhost";
    int port() default 6379;
    String password() default "";
    int database() default 0;
    int timeout() default 2000;
    int maxTotal() default 8;
    int maxIdle() default 8;
    int minIdle() default 0;
}
