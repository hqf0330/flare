package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.doctor.DoctorReport;
import com.bhcode.flare.flink.doctor.DoctorRunner;
import com.bhcode.flare.flink.FlinkStreaming;
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

    @Streaming(parallelism = 1, interval = 10)
    static class NotFlinkStreamingJob {
    }

    @Streaming(parallelism = 1, interval = 10)
    abstract static class AbstractFlinkStreamingJob extends FlinkStreaming {
    }

    @Streaming(parallelism = 1, interval = 10)
    public static class ValidFlinkStreamingJob extends FlinkStreaming {
        @Override
        public void process() {
            // no-op
        }
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

    @Test
    public void shouldErrorWhenJobDoesNotExtendFlinkStreaming() {
        DoctorRunner runner = new DoctorRunner(List.of(new JobClassGuardrailRule()));
        DoctorReport report = runner.run(NotFlinkStreamingJob.class);
        Assert.assertTrue(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-041"));
    }

    @Test
    public void shouldErrorWhenFlinkStreamingJobIsAbstract() {
        DoctorRunner runner = new DoctorRunner(List.of(new JobClassGuardrailRule()));
        DoctorReport report = runner.run(AbstractFlinkStreamingJob.class);
        Assert.assertTrue(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-042"));
    }

    @Test
    public void shouldPassWhenJobIsConcreteFlinkStreamingClass() {
        DoctorRunner runner = new DoctorRunner(List.of(new JobClassGuardrailRule()));
        DoctorReport report = runner.run(ValidFlinkStreamingJob.class);
        Assert.assertFalse(report.hasErrors());
        Assert.assertFalse(report.toJson().contains("DR-041"));
        Assert.assertFalse(report.toJson().contains("DR-042"));
    }
}
