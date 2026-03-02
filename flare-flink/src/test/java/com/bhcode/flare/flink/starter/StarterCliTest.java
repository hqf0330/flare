package com.bhcode.flare.flink.starter;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class StarterCliTest {

    @Test
    public void shouldGenerateJobAndProperties() throws Exception {
        Path out = Files.createTempDirectory("starter-test");
        int code = StarterCli.run(new String[]{
                "--template", "kafka-process-print",
                "--job", "DemoJob",
                "--out", out.toString()
        });
        Assert.assertEquals(0, code);
        Assert.assertTrue(Files.exists(out.resolve("src/main/java/com/example/DemoJob.java")));
        Assert.assertTrue(Files.exists(out.resolve("src/main/resources/flink-streaming.properties")));
        Assert.assertTrue(Files.exists(out.resolve("pom.xml")));
        Assert.assertTrue(Files.exists(out.resolve("run-local.sh")));
        Assert.assertTrue(Files.exists(out.resolve("submit.sh")));
        Assert.assertTrue(Files.exists(out.resolve("README-run.md")));
        String conf = Files.readString(out.resolve("src/main/resources/flink-streaming.properties"));
        String pom = Files.readString(out.resolve("pom.xml"));
        String runScript = Files.readString(out.resolve("run-local.sh"));
        String submitScript = Files.readString(out.resolve("submit.sh"));
        String readme = Files.readString(out.resolve("README-run.md"));
        Assert.assertTrue(conf.contains("flink.job.auto.start=true"));
        Assert.assertFalse(conf.contains("flink.job.autoStart=true"));
        Assert.assertTrue(pom.contains("<artifactId>demo-job</artifactId>"));
        Assert.assertTrue(pom.contains("<artifactId>flare-flink</artifactId>"));
        Assert.assertTrue(runScript.contains("com.example.DemoJob"));
        Assert.assertTrue(runScript.contains("target/demo-job-*-all.jar"));
        Assert.assertTrue(submitScript.contains("flink run -c com.example.DemoJob"));
        Assert.assertTrue(readme.contains("mvn -DskipTests clean package"));
        Assert.assertTrue(readme.contains("./run-local.sh"));
        Assert.assertTrue(readme.contains("./submit.sh"));
    }

    @Test
    public void shouldGenerateSecondGoldenTemplate() throws Exception {
        Path out = Files.createTempDirectory("starter-test-2");
        int code = StarterCli.run(new String[]{
                "--template", "kafka-asyncJdbc-jdbcSink",
                "--job", "EnrichmentJob",
                "--out", out.toString()
        });
        Assert.assertEquals(0, code);
        Path jobFile = out.resolve("src/main/java/com/example/EnrichmentJob.java");
        Assert.assertTrue(Files.exists(jobFile));
        Assert.assertTrue(Files.readString(jobFile).contains("class EnrichmentJob"));
    }

    @Test
    public void shouldReturnErrorWhenTemplateUnknown() {
        int code = StarterCli.run(new String[]{
                "--template", "unknown-template",
                "--job", "DemoJob",
                "--out", "/tmp/not-used"
        });
        Assert.assertNotEquals(0, code);
    }

    @Test
    public void shouldUseCustomArtifactIdWhenProvided() throws Exception {
        Path out = Files.createTempDirectory("starter-test-custom-artifact");
        int code = StarterCli.run(new String[]{
                "--template", "kafka-process-print",
                "--job", "DemoJob",
                "--artifact", "orders-stream-job",
                "--out", out.toString()
        });
        Assert.assertEquals(0, code);
        String pom = Files.readString(out.resolve("pom.xml"));
        Assert.assertTrue(pom.contains("<artifactId>orders-stream-job</artifactId>"));
    }
}
