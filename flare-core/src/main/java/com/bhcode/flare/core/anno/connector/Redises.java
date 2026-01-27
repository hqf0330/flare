package com.bhcode.flare.core.anno.connector;

import java.lang.annotation.*;

/**
 * Container for repeatable Redis annotation.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Redises {
    Redis[] value();
}
