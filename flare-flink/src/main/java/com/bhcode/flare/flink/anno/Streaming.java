package com.bhcode.flare.flink.anno;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;

import java.lang.annotation.*;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;

@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Streaming {

    /**
     * checkpoint 周期（秒）
     */
    int value() default -1;

    /**
     * checkpoint 周期（秒），同 value
     */
    int interval() default -1;

    /**
     * checkpoint 超时时间（秒）
     */
    int timeout() default -1;

    /**
     * 是否开启非对齐的 checkpoint
     */
    boolean unaligned() default true;

    /**
     * checkpoint 的并发度
     */
    int concurrent() default -1;

    /**
     * 两次 checkpoint 的最短时间间隔（秒）
     */
    int pauseBetween() default -1;

    /**
     * 运行 checkpoint 失败的总次数
     */
    int failureNumber() default -1;

    /**
     * checkpoint 的模式
     */
    CheckpointingMode mode() default CheckpointingMode.EXACTLY_ONCE;

    /**
     * 当任务停止时 checkpoint 的保持策略
     */
    ExternalizedCheckpointCleanup cleanup() default ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

    /**
     * 是否自动提交 job：call env.execute()
     */
    boolean autoStart() default true;

    /**
     * 任务的并行度
     */
    int parallelism() default -1;

    /**
     * 是否禁用 OperatorChaining
     */
    boolean disableOperatorChaining() default false;

    /**
     * 状态的 TTL 时间（天）
     */
    int stateTTL() default 31;

    /**
     * 是否使用 statementSet
     */
    boolean useStatementSet() default true;

    /**
     * Flink 任务运行模式
     */
    RuntimeExecutionMode executionMode() default RuntimeExecutionMode.STREAMING;

    /**
     * watermark 生成周期（毫秒）
     */
    int watermarkInterval() default -1;
}
