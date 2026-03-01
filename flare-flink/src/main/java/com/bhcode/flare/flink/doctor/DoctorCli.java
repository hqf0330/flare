package com.bhcode.flare.flink.doctor;

import com.bhcode.flare.flink.doctor.rules.AutoStartGuardrailRule;
import com.bhcode.flare.flink.doctor.rules.CheckpointGuardrailRule;
import com.bhcode.flare.flink.doctor.rules.ConnectorAnnotationKeyRule;
import com.bhcode.flare.flink.doctor.rules.DoctorRule;
import com.bhcode.flare.flink.doctor.rules.JdbcRequiredConfigRule;
import com.bhcode.flare.flink.doctor.rules.KafkaRequiredConfigRule;
import com.bhcode.flare.flink.doctor.rules.ParallelismGuardrailRule;
import com.bhcode.flare.flink.doctor.rules.RedisRequiredConfigRule;
import com.bhcode.flare.flink.doctor.rules.StreamingAnnotationRule;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class DoctorCli {

    private DoctorCli() {
    }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    public static int run(String[] args) {
        return run(args, System.out, System.err);
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        try {
            CliArgs cliArgs = CliArgs.parse(args);
            if (cliArgs.help) {
                out.println(usage());
                return 0;
            }
            if (cliArgs.jobClassName == null || cliArgs.jobClassName.trim().isEmpty()) {
                err.println("Missing required argument: --job <fully-qualified-job-class>");
                err.println(usage());
                return 2;
            }

            Class<?> jobClass = Class.forName(cliArgs.jobClassName);
            DoctorRunner runner = new DoctorRunner(defaultRules());
            DoctorReport report = runner.run(jobClass);
            String json = report.toJson();
            out.println(json);

            if (cliArgs.jsonOut != null && !cliArgs.jsonOut.trim().isEmpty()) {
                Path outPath = Path.of(cliArgs.jsonOut);
                Path parent = outPath.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.writeString(outPath, json, StandardCharsets.UTF_8);
            }
            return report.hasErrors() ? 1 : 0;
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            err.println(usage());
            return 2;
        } catch (Exception e) {
            err.println("Failed to run doctor: " + e.getMessage());
            return 2;
        }
    }

    private static String usage() {
        return "Usage: flare-doctor --job <fully-qualified-job-class> [--json-out <path>]";
    }

    private static List<DoctorRule> defaultRules() {
        return List.of(
                new StreamingAnnotationRule(),
                new ConnectorAnnotationKeyRule(),
                new KafkaRequiredConfigRule(),
                new JdbcRequiredConfigRule(),
                new RedisRequiredConfigRule(),
                new CheckpointGuardrailRule(),
                new ParallelismGuardrailRule(),
                new AutoStartGuardrailRule()
        );
    }

    static final class CliArgs {
        private final String jobClassName;
        private final String jsonOut;
        private final boolean help;

        private CliArgs(String jobClassName, String jsonOut, boolean help) {
            this.jobClassName = jobClassName;
            this.jsonOut = jsonOut;
            this.help = help;
        }

        static CliArgs parse(String[] args) {
            if (args == null || args.length == 0) {
                return new CliArgs(null, null, false);
            }
            String job = null;
            String jsonOut = null;
            boolean help = false;
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if ("--help".equals(arg) || "-h".equals(arg)) {
                    help = true;
                } else if ("--job".equals(arg)) {
                    job = nextValue("--job", args, i);
                    i++;
                } else if ("--json-out".equals(arg)) {
                    jsonOut = nextValue("--json-out", args, i);
                    i++;
                } else {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            return new CliArgs(job, jsonOut, help);
        }

        private static String nextValue(String option, String[] args, int index) {
            int valueIndex = index + 1;
            if (valueIndex >= args.length) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            return args[valueIndex];
        }
    }
}
