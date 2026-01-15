package com.bhcode.flare.common.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author binghu
 * @description: 任务信息
 * @date 2026/1/14 22:51
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlareTask {

    /**
     * 触发异常的执行引擎：spark/flink
     */
    protected String engine;

    /**
     * 引擎版本
     */
    protected String engineVersion;

    /**
     * flare框架版本
     */
    protected String fireVersion;

    /**
     * 异常所在jvm进程发送的主机ip
     */
    protected String ip;

    /**
     * 异常所属jvm进程所在的主机名称
     */
    protected String hostname;

    /**
     * 进程的pid
     */
    protected String pid;

    /**
     * 任务的主类名：package+类名
     */
    protected String mainClass;

    /**
     * 异常发生的时间戳
     */
    protected String timestamp;

    /**
     * 任务启动时间
     */
    protected String launchTime;

    /**
     * 任务运行时间
     */
    protected Long uptime;

    /**
     * 运行时的appId
     */
    protected String appId;

    /**
     * 任务提交模式
     */
    protected String deployMode;

    /**
     * spark：streaming、structured streaming、core
     * flink：streaming、batch
     */
    protected String jobType;

    /**
     * 任务在实时平台中的id标识
     */
    protected String platformAppId;

}
