package com.bhcode.flare.common.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 轻量级定时任务注解。
 * 仅支持固定间隔调度（fixed delay），适用于运行时治理类的非业务任务。
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Scheduled {

    /**
     * 调度间隔（毫秒），必须大于 0。
     */
    long fixedInterval();

    /**
     * 首次执行前延迟（毫秒），默认立即执行。
     */
    long initialDelay() default 0L;

    /**
     * 执行次数，默认 -1 表示无限循环。
     */
    long repeatCount() default -1L;

    /**
     * 是否启用该任务。
     */
    boolean enabled() default true;
}
