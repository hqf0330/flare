package com.bhcode.flare.common.util;

import com.bhcode.flare.common.enums.JobType;
import lombok.extern.slf4j.Slf4j;

/**
 * @description: 脚手架工具
 * @author binghu
 * @date 2026/1/14 22:53
 */
@Slf4j
public class FlareUtils {

    private static volatile boolean isSplash = false;
    private static volatile JobType jobType = JobType.UNDEFINED;
    private static final long LAUNCH_TIME = System.currentTimeMillis();

    /**
     * 获取任务启动时间
     *
     * @return 启动时间戳（毫秒）
     */
    public static long launchTime() {
        return LAUNCH_TIME;
    }

    /**
     * 任务运行时间
     *
     * @return 运行时间（毫秒）
     */
    public static long uptime() {
        return System.currentTimeMillis() - LAUNCH_TIME;
    }

    /**
     * 设置当前任务的类型
     *
     * @param type 任务类型
     */
    public static void setJobType(JobType type) {
        if (type != null) {
            jobType = type;
        }
    }

    /**
     * 用于判断任务类型是否为流式计算任务
     *
     * @return true: 流处理任务  false: 批处理任务
     */
    public static boolean isStreamingJob() {
        return jobType == JobType.FLINK_STREAMING;
    }

    /**
     * 用于判断任务类型是否为批处理任务
     *
     * @return true: 批处理任务  false: 流处理任务
     */
    public static boolean isBatchJob() {
        return jobType == JobType.FLINK_BATCH;
    }

    /**
     * 用于在 Flare 框架启动时展示信息
     */
    public static void splash() {
        if (!isSplash) {
            String version = "1.0-SNAPSHOT"; // TODO: 从配置或 manifest 中读取版本号
            String info = String.format(
                    """
                            
                            ╔═══════════════════════════════════════════════════════════╗
                            ║                                                           ║
                            ║                    ╦═╗╔═╗╔═╗╦ ╦╔═╗                       ║
                            ║                    ╠╦╝║╣ ╠═╣║║║╚═╗                       ║
                            ║                    ╩╚═╚═╝╩ ╩╚╩╝╚═╝                       ║
                            ║                                                           ║
                            ║              A lightweight Java framework                 ║
                            ║              for Flink stream processing                  ║
                            ║                                                           ║
                            ║                    Version: %-30s ║
                            ║                                                           ║
                            ╚═══════════════════════════════════════════════════════════╝
                            """, version);
            log.info("\n{}", info);
            isSplash = true;
        }
    }

    /**
     * 判断是否为本地运行模式
     * <p>
     * 判断逻辑：
     * 1. 检查系统属性 "fire.env.local" 或配置 "fire.env.local"
     * 2. 检查是否有 master 参数且为 "local" 或 "local[*]"
     * 3. 检查是否在 IDE 中运行（通过检查类路径）
     * </p>
     *
     * @return true: 本地模式, false: 集群模式
     */
    public static boolean isLocalRunMode() {
        // 1. 检查系统属性
        String localEnv = System.getProperty("fire.env.local");
        if (localEnv != null) {
            return Boolean.parseBoolean(localEnv);
        }

        // 2. 检查配置
        String configLocal = PropUtils.getString("fire.env.local");
        if (configLocal != null) {
            return Boolean.parseBoolean(configLocal);
        }

        // 3. 检查 master 参数
        String master = System.getProperty("flink.master");
        if (master != null && (master.startsWith("local") || master.equals("local"))) {
            return true;
        }

        // 4. 检查是否在 IDE 中运行（简化判断：检查是否有 IDE 相关的系统属性）
        String javaClassPath = System.getProperty("java.class.path");
        if (javaClassPath != null) {
            // 如果在 IDE 中运行，classpath 通常包含 IDE 相关的路径
            // 这是一个启发式判断，不是绝对准确
            if (javaClassPath.contains("idea") || javaClassPath.contains("eclipse") 
                    || javaClassPath.contains("IntelliJ") || javaClassPath.contains(".gradle")) {
                return true;
            }
        }

        // 5. 检查是否有 Flink 集群配置（如果有集群配置，则不是本地模式）
        String jobManagerAddress = System.getProperty("jobmanager.rpc.address");
        if (jobManagerAddress != null && !jobManagerAddress.isEmpty() && !jobManagerAddress.equals("localhost")) {
            return false;
        }

        // 默认返回 false（集群模式）
        return false;
    }
}
