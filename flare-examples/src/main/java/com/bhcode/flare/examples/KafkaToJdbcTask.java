package com.bhcode.flare.examples;

import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.flink.FlinkJobLauncher;
import com.bhcode.flare.flink.FlinkStreaming;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@Slf4j
@Kafka(keyNum = 1, brokers = "localhost:9092", topics = "test-topic", groupId = "flare")
@Jdbc(keyNum = 1, url = "jdbc:mysql://localhost:3306/demo", username = "root",
        password = "123456", driver = "com.mysql.cj.jdbc.Driver",
        sql = "INSERT INTO t_demo(value) VALUES (?)")
public class KafkaToJdbcTask extends FlinkStreaming {

    @Override
    public void process() {
        DataStream<String> source = this.kafkaSourceFromConf(1);

        this.jdbcSinkFromConf(source, (PreparedStatement ps, String value) -> {
            try {
                ps.setString(1, value);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, 1);
    }

    public static void main(String[] args) {
        FlinkJobLauncher.run(KafkaToJdbcTask.class, args);
    }
}
