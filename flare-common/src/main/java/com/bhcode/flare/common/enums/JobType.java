package com.bhcode.flare.common.enums;

/**
 * Flare 任务类型
 * <p>
 * 仅支持 Flink 引擎的任务类型
 * </p>
 *
 * @author Flare Team
 * @since 1.0.0
 */
public enum JobType {
    /**
     * Flink 流处理任务
     */
    FLINK_STREAMING("flink_streaming"),

    /**
     * Flink 批处理任务
     */
    FLINK_BATCH("flink_batch"),

    /**
     * 未定义的任务类型
     */
    UNDEFINED("undefined");

    /**
     * 任务类型描述
     */
    private final String jobTypeDesc;

    JobType(String jobTypeDesc) {
        this.jobTypeDesc = jobTypeDesc;
    }

    /**
     * 获取当前任务的类型描述
     *
     * @return 任务类型描述
     */
    public String getJobTypeDesc() {
        return this.jobTypeDesc;
    }

    /**
     * 用于判断当前任务是否为 Flink 任务
     *
     * @return true: Flink任务  false：非Flink任务
     */
    public boolean isFlink() {
        return this.jobTypeDesc.contains("flink");
    }

    /**
     * 用于判断当前任务是否为流处理任务
     *
     * @return true: 流处理任务  false：批处理任务
     */
    public boolean isStreaming() {
        return this == FLINK_STREAMING;
    }

    /**
     * 用于判断当前任务是否为批处理任务
     *
     * @return true: 批处理任务  false：流处理任务
     */
    public boolean isBatch() {
        return this == FLINK_BATCH;
    }
}
