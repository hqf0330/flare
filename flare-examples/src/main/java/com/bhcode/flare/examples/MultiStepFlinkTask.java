package com.bhcode.flare.examples;

import com.bhcode.flare.common.anno.Config;
import com.bhcode.flare.core.anno.connector.Process;
import com.bhcode.flare.core.anno.lifecycle.Step1;
import com.bhcode.flare.core.anno.lifecycle.Step2;
import com.bhcode.flare.core.anno.lifecycle.Step3;
import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.anno.Streaming;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

@Slf4j
@Config("")
@Streaming(
        interval = 10,
        parallelism = 2
)
public class MultiStepFlinkTask extends FlinkStreaming {

    /**
     * 方式一：在 process() 中调用其他方法（推荐用于复杂逻辑）
     */
    @Override
    public void process() {
        log.info("=== Main Process Started ===");
        
        // 可以调用其他方法，将逻辑拆分
        this.setupSource();
        this.transformData();
        this.outputResult();
        
        log.info("=== Main Process Completed ===");
    }

    /**
     * 方式二：使用 @Process 注解的方法（会在 process() 之后自动调用）
     */
    @Process("Additional processing")
    public void additionalProcess() {
        log.info("This method will be called after process()");
    }

    /**
     * 方式三：使用 @Step 注解按顺序执行（适合 SQL 开发场景）
     */
    @Step1("创建数据源")
    public void createSource() {
        log.info("Step1: Creating data source");
        StreamExecutionEnvironment env = this.getEnv();
        DataStreamSource<String> source = env.fromCollection(
                Arrays.asList("Step1", "Step2", "Step3")
        );
        source.print("Step1-Output");
    }

    @Step2("数据转换")
    public void transformStep() {
        log.info("Step2: Transforming data");
        // Step 方法中可以访问 env 资源
        StreamExecutionEnvironment env = this.getEnv();
        DataStreamSource<String> source = env.fromCollection(
                Arrays.asList("Transform1", "Transform2")
        );
        DataStream<String> transformed = source.map(s -> "Transformed: " + s);
        transformed.print("Step2-Output");
    }

    @Step3("输出结果")
    public void outputStep() {
        log.info("Step3: Outputting results");
        // 可以继续处理数据
    }

    // ========== 辅助方法（在 process() 中调用） ==========

    private void setupSource() {
        log.info("Setting up data source");
    }

    private void transformData() {
        log.info("Transforming data");
    }

    private void outputResult() {
        log.info("Outputting results");
    }

    /**
     * Main 方法
     */
    public static void main(String[] args) {
        MultiStepFlinkTask task = new MultiStepFlinkTask();
        try {
            task.init(null, args);
        } catch (Exception e) {
            log.error("Task execution failed", e);
            System.exit(1);
        }
    }
}
