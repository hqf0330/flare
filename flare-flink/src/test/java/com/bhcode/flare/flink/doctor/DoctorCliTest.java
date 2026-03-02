package com.bhcode.flare.flink.doctor;

import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.FlinkStreaming;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class DoctorCliTest {

    static class InvalidJob {
    }

    @Streaming(parallelism = 1, interval = 10)
    public static class ValidJob extends FlinkStreaming {
        @Override
        public void process() {
            // no-op
        }
    }

    @Streaming(parallelism = 1, interval = 10)
    static class NonFlinkStreamingAnnotatedJob {
    }

    @Test
    public void shouldReturnNonZeroWhenErrorExists() {
        int code = DoctorCli.run(new String[]{
                "--job", InvalidJob.class.getName()
        });
        Assert.assertNotEquals(0, code);
    }

    @Test
    public void shouldReturnZeroWhenNoBlockingErrors() {
        int code = DoctorCli.run(new String[]{
                "--job", ValidJob.class.getName()
        });
        Assert.assertEquals(0, code);
    }

    @Test
    public void shouldReturnNonZeroWhenJobNotFlinkStreaming() {
        int code = DoctorCli.run(new String[]{
                "--job", NonFlinkStreamingAnnotatedJob.class.getName()
        });
        Assert.assertNotEquals(0, code);
    }

    @Test
    public void shouldWriteJsonReportWhenJsonOutProvided() throws Exception {
        Path tmp = Files.createTempFile("flare-doctor", ".json");
        int code = DoctorCli.run(new String[]{
                "--job", InvalidJob.class.getName(),
                "--json-out", tmp.toString()
        });
        Assert.assertNotEquals(0, code);
        Assert.assertTrue(Files.exists(tmp));
        String content = Files.readString(tmp);
        Assert.assertTrue(content.contains("DR-001"));
    }

    @Test
    public void shouldReturnArgumentErrorOnUnknownOption() {
        int code = DoctorCli.run(new String[]{
                "--unknown"
        });
        Assert.assertEquals(2, code);
    }
}
