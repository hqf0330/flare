package com.bhcode.flare.core.anno.lifecycle;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记注解：用于标注业务逻辑代码执行步骤（第6步）
 *
 * @author Flare Team
 * @since 1.0.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Step6 {
    String value() default "";
    boolean skipError() default false;
}
