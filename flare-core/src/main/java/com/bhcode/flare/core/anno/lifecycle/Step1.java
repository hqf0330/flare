package com.bhcode.flare.core.anno.lifecycle;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记注解：用于标注业务逻辑代码执行步骤（第1步）
 * <p>
 * 使用 @Step1, @Step2, @Step3 等注解可以按顺序组织业务逻辑
 * 适合 SQL 开发场景，将建表、插入等操作分步骤执行
 * </p>
 *
 * @author Flare Team
 * @since 1.0.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Step1 {
    /**
     * 业务代码逻辑描述
     */
    String value() default "";

    /**
     * 当发生异常时，是否跳过异常执行下一步
     */
    boolean skipError() default false;
}
