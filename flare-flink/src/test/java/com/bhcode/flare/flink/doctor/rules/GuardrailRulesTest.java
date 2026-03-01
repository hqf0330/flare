package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.doctor.DoctorReport;
import com.bhcode.flare.flink.doctor.DoctorRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class GuardrailRulesTest {

    @Streaming(parallelism = 1, interval = -1)
    static class NoCheckpointJob {
    }

    @Streaming(parallelism = -1, interval = 10)
    static class InvalidParallelismJob {
    }

    @Streaming(parallelism = 1, interval = 10, autoStart = false)
    static class AutoStartDisabledJob {
    }

    @Test
    public void shouldWarnWhenCheckpointDisabled() {
        DoctorRunner runner = new DoctorRunner(List.of(new CheckpointGuardrailRule()));
        DoctorReport report = runner.run(NoCheckpointJob.class);
        Assert.assertFalse(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-301"));
    }

    @Test
    public void shouldWarnWhenParallelismMissing() {
        DoctorRunner runner = new DoctorRunner(List.of(new ParallelismGuardrailRule()));
        DoctorReport report = runner.run(InvalidParallelismJob.class);
        Assert.assertFalse(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-311"));
    }

    @Test
    public void shouldWarnWhenAutoStartDisabled() {
        DoctorRunner runner = new DoctorRunner(List.of(new AutoStartGuardrailRule()));
        DoctorReport report = runner.run(AutoStartDisabledJob.class);
        Assert.assertFalse(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-321"));
    }
}
