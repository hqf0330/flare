package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.doctor.DoctorReport;
import com.bhcode.flare.flink.doctor.DoctorRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StreamingAnnotationRuleTest {

    static class NoStreamingJob {
    }

    @Streaming(parallelism = 1)
    static class WithStreamingJob {
    }

    @Test
    public void shouldFailWhenStreamingAnnotationMissing() {
        DoctorRunner runner = new DoctorRunner(List.of(new StreamingAnnotationRule()));
        DoctorReport report = runner.run(NoStreamingJob.class);
        Assert.assertTrue(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-001"));
    }

    @Test
    public void shouldPassWhenStreamingAnnotationExists() {
        DoctorRunner runner = new DoctorRunner(List.of(new StreamingAnnotationRule()));
        DoctorReport report = runner.run(WithStreamingJob.class);
        Assert.assertFalse(report.hasErrors());
    }
}
