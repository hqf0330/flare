package com.bhcode.flare.flink.doctor;

import org.junit.Assert;
import org.junit.Test;

public class DoctorReportTest {

    @Test
    public void shouldRenderJsonAndDetectBlockingErrors() {
        DoctorReport report = new DoctorReport();
        report.add(new Diagnostic(
                DiagnosticSeverity.ERROR,
                "DR-001",
                "missing @Streaming",
                "add @Streaming"
        ));
        report.add(new Diagnostic(
                DiagnosticSeverity.WARN,
                "DR-901",
                "parallelism not set",
                "set parallelism"
        ));

        Assert.assertTrue(report.hasErrors());
        String json = report.toJson();
        Assert.assertTrue(json.contains("DR-001"));
        Assert.assertTrue(json.contains("ERROR"));
        Assert.assertTrue(json.contains("DR-901"));
    }
}
