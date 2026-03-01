package com.bhcode.flare.flink.starter;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class StarterCli {

    private static final String TEMPLATE_PROCESS_PRINT = "kafka-process-print";
    private static final String TEMPLATE_ASYNC_JDBC = "kafka-asyncJdbc-jdbcSink";

    private StarterCli() {
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
            if (isBlank(cliArgs.template)) {
                err.println("Missing required argument: --template");
                err.println(usage());
                return 2;
            }
            if (isBlank(cliArgs.jobName)) {
                err.println("Missing required argument: --job");
                err.println(usage());
                return 2;
            }
            if (isBlank(cliArgs.outDir)) {
                err.println("Missing required argument: --out");
                err.println(usage());
                return 2;
            }
            if (!isValidJavaIdentifier(cliArgs.jobName)) {
                err.println("Invalid job name: " + cliArgs.jobName);
                return 2;
            }
            if (!isSupportedTemplate(cliArgs.template)) {
                err.println("Unsupported template: " + cliArgs.template);
                return 2;
            }

            Path outRoot = Path.of(cliArgs.outDir);
            String packageName = cliArgs.packageName;
            String packagePath = packageName.replace('.', '/');
            Map<String, String> placeholders = Map.of(
                    "PACKAGE", packageName,
                    "JOB_NAME", cliArgs.jobName
            );

            TemplateRenderer renderer = new TemplateRenderer();
            writeJobFile(outRoot, cliArgs.template, cliArgs.jobName, packagePath, placeholders, renderer);
            writePropertiesFile(outRoot, placeholders, renderer);
            writeReadme(outRoot, cliArgs.template, cliArgs.jobName, packageName);
            out.println("Starter files generated at: " + outRoot.toAbsolutePath());
            return 0;
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            err.println(usage());
            return 2;
        } catch (Exception e) {
            err.println("Failed to generate starter: " + e.getMessage());
            return 2;
        }
    }

    private static void writeJobFile(Path outRoot,
                                     String template,
                                     String jobName,
                                     String packagePath,
                                     Map<String, String> placeholders,
                                     TemplateRenderer renderer) throws Exception {
        String templatePath = "/starter/templates/" + template + "/Job.java.tpl";
        String content = renderer.renderClasspathTemplate(templatePath, placeholders);
        Path jobFile = outRoot.resolve("src/main/java").resolve(packagePath).resolve(jobName + ".java");
        Files.createDirectories(jobFile.getParent());
        Files.writeString(jobFile, content, StandardCharsets.UTF_8);
    }

    private static void writePropertiesFile(Path outRoot,
                                            Map<String, String> placeholders,
                                            TemplateRenderer renderer) throws Exception {
        String templatePath = "/starter/templates/common/flink-streaming.properties.tpl";
        String content = renderer.renderClasspathTemplate(templatePath, placeholders);
        Path confFile = outRoot.resolve("src/main/resources/flink-streaming.properties");
        Files.createDirectories(confFile.getParent());
        Files.writeString(confFile, content, StandardCharsets.UTF_8);
    }

    private static void writeReadme(Path outRoot,
                                    String template,
                                    String jobName,
                                    String packageName) throws Exception {
        String readme = """
                # Flare Starter Output
                
                Template: %s
                Job Class: %s.%s
                
                ## Quick Run
                1. Put this directory into your business module.
                2. Keep `flink-streaming.properties` aligned with your env.
                3. Start with:
                   `java -cp <your-jar> %s.%s`
                """.formatted(template, packageName, jobName, packageName, jobName);
        Path readmeFile = outRoot.resolve("README-run.md");
        Files.createDirectories(readmeFile.getParent());
        Files.writeString(readmeFile, readme, StandardCharsets.UTF_8);
    }

    private static boolean isSupportedTemplate(String template) {
        return TEMPLATE_PROCESS_PRINT.equals(template) || TEMPLATE_ASYNC_JDBC.equals(template);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static boolean isValidJavaIdentifier(String name) {
        if (isBlank(name)) {
            return false;
        }
        if (!Character.isJavaIdentifierStart(name.charAt(0))) {
            return false;
        }
        for (int i = 1; i < name.length(); i++) {
            if (!Character.isJavaIdentifierPart(name.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static String usage() {
        return "Usage: flare-starter --template <name> --job <ClassName> --out <dir> [--package <java.package>]";
    }

    static final class CliArgs {
        private final String template;
        private final String jobName;
        private final String outDir;
        private final String packageName;
        private final boolean help;

        private CliArgs(String template, String jobName, String outDir, String packageName, boolean help) {
            this.template = template;
            this.jobName = jobName;
            this.outDir = outDir;
            this.packageName = packageName;
            this.help = help;
        }

        static CliArgs parse(String[] args) {
            if (args == null || args.length == 0) {
                return new CliArgs(null, null, null, "com.example", false);
            }
            String template = null;
            String job = null;
            String out = null;
            String packageName = "com.example";
            boolean help = false;
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if ("--help".equals(arg) || "-h".equals(arg)) {
                    help = true;
                } else if ("--template".equals(arg)) {
                    template = nextValue("--template", args, i);
                    i++;
                } else if ("--job".equals(arg)) {
                    job = nextValue("--job", args, i);
                    i++;
                } else if ("--out".equals(arg)) {
                    out = nextValue("--out", args, i);
                    i++;
                } else if ("--package".equals(arg)) {
                    packageName = nextValue("--package", args, i);
                    i++;
                } else {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            return new CliArgs(template, job, out, packageName, help);
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
