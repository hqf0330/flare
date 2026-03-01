package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.core.anno.connector.Redis;
import com.bhcode.flare.flink.doctor.DoctorReport;
import com.bhcode.flare.flink.doctor.DoctorRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RequiredConfigRulesTest {

    @Kafka(brokers = "", topics = "t1", groupId = "g1")
    static class MissingKafkaBrokersJob {
    }

    @Kafka(brokers = "127.0.0.1:9092", topics = "", groupId = "g1")
    static class MissingKafkaTopicJob {
    }

    @Jdbc(url = "", username = "u", password = "p")
    static class MissingJdbcUrlJob {
    }

    @Jdbc(url = "jdbc:mysql://localhost:3306/test", username = "", password = "p")
    static class MissingJdbcUserJob {
    }

    @Redis(host = "")
    static class MissingRedisHostJob {
    }

    @Test
    public void shouldReportMissingKafkaBrokers() {
        DoctorRunner runner = new DoctorRunner(List.of(new KafkaRequiredConfigRule()));
        DoctorReport report = runner.run(MissingKafkaBrokersJob.class);
        Assert.assertTrue(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-101"));
    }

    @Test
    public void shouldReportMissingKafkaTopic() {
        DoctorRunner runner = new DoctorRunner(List.of(new KafkaRequiredConfigRule()));
        DoctorReport report = runner.run(MissingKafkaTopicJob.class);
        Assert.assertTrue(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-102"));
    }

    @Test
    public void shouldReportMissingJdbcUrl() {
        DoctorRunner runner = new DoctorRunner(List.of(new JdbcRequiredConfigRule()));
        DoctorReport report = runner.run(MissingJdbcUrlJob.class);
        Assert.assertTrue(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-201"));
    }

    @Test
    public void shouldReportMissingJdbcUser() {
        DoctorRunner runner = new DoctorRunner(List.of(new JdbcRequiredConfigRule()));
        DoctorReport report = runner.run(MissingJdbcUserJob.class);
        Assert.assertTrue(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-202"));
    }

    @Test
    public void shouldReportMissingRedisHost() {
        DoctorRunner runner = new DoctorRunner(List.of(new RedisRequiredConfigRule()));
        DoctorReport report = runner.run(MissingRedisHostJob.class);
        Assert.assertTrue(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-221"));
    }
}
