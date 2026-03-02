package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.common.anno.Config;
import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.doctor.DoctorReport;
import com.bhcode.flare.flink.doctor.DoctorRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RuntimeGovernanceGuardrailRuleTest {

    @Streaming(parallelism = 1, interval = 10)
    @Config(props = {"flink.job.autoStart=false"})
    static class LegacyAutoStartKeyJob {
    }

    @Streaming(parallelism = 1, interval = 10)
    @Config(props = {"flare.runtime.rest.enable=true"})
    static class RestWithoutTokenJob {
    }

    @Streaming(parallelism = 1, interval = 10)
    @Config(props = {
            "flare.runtime.schedule.enable=true",
            "flare.runtime.schedule.pool.size=0"
    })
    static class InvalidSchedulePoolSizeJob {
    }

    @Test
    public void shouldWarnWhenUsingDeprecatedAutoStartKey() {
        DoctorRunner runner = new DoctorRunner(List.of(new RuntimeGovernanceGuardrailRule()));

        DoctorReport report = runner.run(LegacyAutoStartKeyJob.class);

        Assert.assertFalse(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-331"));
    }

    @Test
    public void shouldWarnWhenRuntimeRestEnabledWithoutToken() {
        DoctorRunner runner = new DoctorRunner(List.of(new RuntimeGovernanceGuardrailRule()));

        DoctorReport report = runner.run(RestWithoutTokenJob.class);

        Assert.assertFalse(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-332"));
    }

    @Test
    public void shouldWarnWhenRuntimeSchedulePoolSizeInvalid() {
        DoctorRunner runner = new DoctorRunner(List.of(new RuntimeGovernanceGuardrailRule()));

        DoctorReport report = runner.run(InvalidSchedulePoolSizeJob.class);

        Assert.assertFalse(report.hasErrors());
        Assert.assertTrue(report.toJson().contains("DR-333"));
    }
}
