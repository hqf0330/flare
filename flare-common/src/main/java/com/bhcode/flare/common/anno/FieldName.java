package com.bhcode.flare.common.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author binghu
 * @description: TODO
 * @date 2026/1/14 22:32
 */

@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface FieldName {

    /**
     * fieldName，映射到hbase中作为qualifier名称
     */
    String value() default "";

    /**
     * 列族名称
     */
    String family() default "info";

    /**
     * 不使用该字段，默认为使用
     */
    boolean disuse() default false;

    /**
     * 是否可以为空
     */
    boolean nullable() default true;

    /**
     * 是否为主键字段
     *
     * @return
     */
    boolean id() default false;

    /**
     * HBase表的命名空间
     */
    String namespace() default "default";

    /**
     * 字段注释
     */
    String comment() default "";
}
