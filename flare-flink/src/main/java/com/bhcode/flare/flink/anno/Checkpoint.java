package com.bhcode.flare.flink.anno;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author binghu
 * @description: TODO
 * @date 2026/1/15 10:12
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Checkpoint {
    /**
     * checkpoint周期（s）
     */
    int value() default -1;

    /**
     * checkpoint周期（s），同value
     */
    int interval() default -1;

    /**
     * checkpoint超时时间（s）
     */
    int timeout() default -1;

    /**
     * 是否开启非对齐的checkpoint
     */
    boolean unaligned() default true;

    /**
     * checkpoint的并发度
     */
    int concurrent() default -1;

    /**
     * 两次checkpoint的最短时间间隔
     */
    int pauseBetween() default -1;

    /**
     * 运行checkpoint失败的总次数
     */
    int failureNumber() default -1;

    /**
     * checkpoint的模式
     */
    CheckpointingMode mode() default CheckpointingMode.EXACTLY_ONCE;

    /**
     * 当任务停止时checkpoint的保持策略
     */
    CheckpointConfig.ExternalizedCheckpointCleanup cleanup() default CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
}
