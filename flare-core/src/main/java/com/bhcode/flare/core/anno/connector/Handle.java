package com.bhcode.flare.core.anno.connector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author binghu
 * @description: 用于标注业务逻辑代码入口方法，用法同@Process
 * @date 2026/1/14 23:29
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Handle {

    /**
     * 业务代码逻辑描述
     */
    String value() default "";

    /**
     * 当发生异常时，是否跳过异常执行下一步
     */
    boolean skipError() default false;
}
