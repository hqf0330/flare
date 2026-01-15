package com.bhcode.flare.examples;

import com.bhcode.flare.common.anno.Config;
import com.bhcode.flare.core.anno.connector.Process;
import com.bhcode.flare.flink.FlinkJobLauncher;
import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.anno.Streaming;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 简单的 Flink 流处理任务示例
 * <p>
 * 演示如何使用 Flare 框架开发 Flink 任务
 * </p>
 *
 * @author Flare Team
 * @since 1.0.0
 */
@Slf4j
@Config("")
@Streaming(
        interval = 10,              // checkpoint 间隔：10秒
        parallelism = 2,            // 并行度：2
        unaligned = true,           // 启用非对齐 checkpoint
        mode = CheckpointingMode.EXACTLY_ONCE
)
public class SimpleFlinkTask extends FlinkStreaming {

    /**
     * 业务逻辑入口方法
     * <p>
     * 方式一：所有逻辑写在 process() 方法中（适合简单任务）
     * </p>
     * <p>
     * 方式二：在 process() 中调用其他方法（推荐，适合复杂任务）
     * </p>
     */
    @Override
    public void process() {
        log.info("=== Simple Flink Task Started ===");
        
        // 使用 env 资源
        StreamExecutionEnvironment env = this.getEnv();
        
        // 创建一个简单的数据流
        DataStreamSource<String> source = env.fromCollection(
                Arrays.asList("Hello", "World", "Flink", "Flare")
        );
        
        // 简单的转换操作
        DataStream<String> result = source
                .map(s -> s.toUpperCase())
                .filter(s -> s.length() > 4);
        
        // 打印结果
        result.print();
        
        log.info("=== Simple Flink Task Process Completed ===");
        
        // 也可以调用其他方法，将逻辑拆分：
        // this.setupSource();
        // this.transformData();
        // this.outputResult();
    }

    /**
     * 使用 @Process 注解的方法（可选）
     * 这个方法会在 process() 方法之后被自动调用
     */
    @Process("Additional processing step")
    public void additionalProcess() {
        log.info("Additional processing step executed");
    }

    /**
     * Main 方法
     * <p>
     * 注意：Java 中必须写 main 方法（这是 Java 语言限制，与 Fire 框架的 Scala 不同）
     * <p>
     * 与 Fire 框架保持一致：调用 init 后会自动启动任务
     * </p>
     * <p>
     * 标准模板（可直接复制使用）：
     * <pre>{@code
     * public static void main(String[] args) {
     *     FlinkJobLauncher.run(YourTask.class, args);
     * }
     * }</pre>
     * </p>
     */
    public static void main(String[] args) {
        FlinkJobLauncher.run(SimpleFlinkTask.class, args);
    }
}
