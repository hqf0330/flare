package com.bhcode.flare.common.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Config {
    /**
     * 配置文件名称列表
     */
    String[] files() default "";

    /**
     * 配置项列表，key=value的字符串形式
     */
    String[] props() default "";

    /**
     * 配置的字符串
     */
    String value() default "";
}
