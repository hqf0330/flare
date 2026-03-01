package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.flink.doctor.DoctorReport;
import com.bhcode.flare.flink.doctor.DoctorRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ConnectorAnnotationKeyRuleTest {

    @Jdbc(keyNum = 1, url = "jdbc:mysql://localhost:3306/a", username = "u", password = "p")
    @Jdbc(keyNum = 1, url = "jdbc:mysql://localhost:3306/b", username = "u", password = "p")
    static class DuplicateJdbcKeyJob {
    }

    @Jdbc(keyNum = 1, url = "jdbc:mysql://localhost:3306/a", username = "u", password = "p")
    @Jdbc(keyNum = 2, url = "jdbc:mysql://localhost:3306/b", username = "u", password = "p")
    static class DistinctJdbcKeyJob {
    }

    @Test
    public void shouldFailWhenDuplicateConnectorKeyNum() {
        DoctorRunner runner = new DoctorRunner(List.of(new ConnectorAnnotationKeyRule()));
        DoctorReport report = runner.run(DuplicateJdbcKeyJob.class);
        Assert.assertTrue(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-011"));
    }

    @Test
    public void shouldPassWhenConnectorKeyNumDistinct() {
        DoctorRunner runner = new DoctorRunner(List.of(new ConnectorAnnotationKeyRule()));
        DoctorReport report = runner.run(DistinctJdbcKeyJob.class);
        Assert.assertFalse(report.hasErrors());
    }
}
