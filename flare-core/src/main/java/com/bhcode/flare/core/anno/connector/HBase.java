package com.bhcode.flare.core.anno.connector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * HBase connector annotation.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(HBases.class)
public @interface HBase {

    /**
     * Sequence number for multi HBase configs.
     */
    int keyNum() default 1;

    /**
     * Zookeeper quorum.
     */
    String zkQuorum() default "";

    /**
     * Zookeeper client port.
     */
    String zkPort() default "2181";

    /**
     * Zookeeper znode parent.
     */
    String znodeParent() default "/hbase";

    /**
     * HBase table name.
     */
    String tableName() default "";
}
