package com.bhcode.flare.examples;

import com.bhcode.flare.flink.FlinkJobLauncher;
import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.functions.FlareRichProcessFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 演示：标准化侧输出流（脏数据分流）
 */
@Slf4j
@Streaming(parallelism = 1)
public class SideOutputTask extends FlinkStreaming {

    @Override
    public void process() {
        // 1. 创建模拟数据源（包含一些“脏”数据）
        DataStream<String> source = this.getEnv().fromCollection(
                Arrays.asList("data-1", "dirty-2", "data-3", "error-4")
        );

        // 2. 使用 FlareRichProcessFunction 进行分流处理
        SingleOutputStreamOperator<String> mainStream = source.process(new FlareRichProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) {
                if (value.contains("dirty") || value.contains("error")) {
                    // 发现脏数据，打入侧输出流
                    collectDirtyData(ctx, "Invalid data found: " + value);
                } else {
                    // 正常数据，进入主流
                    out.collect(value.toUpperCase());
                }
            }
        });

        // 3. 打印主流结果
        mainStream.print("main-stream");

        // 4. 使用框架提供的便捷方法处理脏数据（打印或落地）
        this.printDirtyData(mainStream);
    }

    public static void main(String[] args) {
        FlinkJobLauncher.run(SideOutputTask.class, args);
    }
}
