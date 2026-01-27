package com.bhcode.flare.examples;

import com.bhcode.flare.core.anno.connector.HBase;
import com.bhcode.flare.flink.FlinkJobLauncher;
import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.functions.FlareHBaseSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * 演示：HBase Sink 高性能写入
 */
@Slf4j
@Streaming(parallelism = 1)
@HBase(
    keyNum = 1,
    zkQuorum = "localhost",
    zkPort = "2181",
    tableName = "flare_test_table"
)
public class HBaseTask extends FlinkStreaming {

    public record User(String id, String name, int age) {}

    @Override
    public void process() {
        // 1. 模拟数据源
        DataStream<User> source = this.getEnv().fromCollection(
                Arrays.asList(
                        new User("1", "Alice", 25),
                        new User("2", "Bob", 30)
                )
        );

        // 2. 写入 HBase
        source.addSink(new FlareHBaseSink<User>(1) {
            @Override
            protected Mutation convert(User value) {
                Put put = new Put(Bytes.toBytes(value.id()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(value.name()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(value.age())));
                return put;
            }
        }).name("hbase-sink");
    }

    public static void main(String[] args) {
        FlinkJobLauncher.run(HBaseTask.class, args);
    }
}
