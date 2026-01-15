package com.bhcode.flare.flink;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class FlinkJobLauncher {

    private FlinkJobLauncher() {
        // Utility class
    }

    /**
     * 启动指定的 FlinkStreaming 任务
     *
     * @param clazz 任务类
     * @param args  main 参数
     */
    public static void run(Class<? extends FlinkStreaming> clazz, String[] args) {
        try {
            FlinkStreaming task = clazz.getDeclaredConstructor().newInstance();
            run(task, args);
        } catch (Exception e) {
            log.error("Failed to create task instance: {}", clazz.getName(), e);
            System.exit(1);
        }
    }

    /**
     * 启动指定的 FlinkStreaming 实例
     *
     * @param task 任务实例
     * @param args main 参数
     */
    public static void run(FlinkStreaming task, String[] args) {
        try {
            task.init(null, args);
        } catch (Exception e) {
            log.error("Task execution failed: {}", task.getClass().getName(), e);
            System.exit(1);
        }
    }
}
