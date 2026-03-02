package com.bhcode.flare.flink.starter;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
            if (!isValidArtifactId(cliArgs.artifactId)) {
                err.println("Invalid artifact id: " + cliArgs.artifactId);
                return 2;
            }

            Path outRoot = Path.of(cliArgs.outDir);
            String packageName = cliArgs.packageName;
            String packagePath = packageName.replace('.', '/');
            String artifactId = isBlank(cliArgs.artifactId) ? toKebabCase(cliArgs.jobName) : cliArgs.artifactId.trim();
            String flareVersion = isBlank(cliArgs.flareVersion) ? detectFlareVersion() : cliArgs.flareVersion.trim();

            Map<String, String> placeholders = new LinkedHashMap<>();
            placeholders.put("PACKAGE", packageName);
            placeholders.put("JOB_NAME", cliArgs.jobName);
            placeholders.put("GROUP_ID", packageName);
            placeholders.put("ARTIFACT_ID", artifactId);
            placeholders.put("FLARE_VERSION", flareVersion);

            TemplateRenderer renderer = new TemplateRenderer();
            writeJobFile(outRoot, cliArgs.template, cliArgs.jobName, packagePath, placeholders, renderer);
            writePropertiesFile(outRoot, placeholders, renderer);
            writePomFile(outRoot, placeholders, renderer);
            writeReadme(outRoot, cliArgs.template, cliArgs.jobName, packageName, artifactId);
            writeScriptFiles(outRoot, placeholders, renderer);
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

    private static void writePomFile(Path outRoot,
                                     Map<String, String> placeholders,
                                     TemplateRenderer renderer) throws Exception {
        String templatePath = "/starter/templates/common/pom.xml.tpl";
        String content = renderer.renderClasspathTemplate(templatePath, placeholders);
        Path pomFile = outRoot.resolve("pom.xml");
        Files.createDirectories(pomFile.getParent());
        Files.writeString(pomFile, content, StandardCharsets.UTF_8);
    }

    private static void writeReadme(Path outRoot,
                                    String template,
                                    String jobName,
                                    String packageName,
                                    String artifactId) throws Exception {
        String readme = """
                # Flare Starter Output
                
                Template: %s
                Job Class: %s.%s
                
                ## Quick Run
                1. Keep `flink-streaming.properties` aligned with your env.
                2. Package:
                   `mvn -DskipTests clean package`
                3. Local run:
                   `./run-local.sh`
                4. Submit:
                   `flink run -c %s.%s target/%s-*-all.jar`
                   or `./submit.sh`
                """.formatted(template, packageName, jobName, packageName, jobName, artifactId);
        Path readmeFile = outRoot.resolve("README-run.md");
        Files.createDirectories(readmeFile.getParent());
        Files.writeString(readmeFile, readme, StandardCharsets.UTF_8);
    }

    private static void writeScriptFiles(Path outRoot,
                                         Map<String, String> placeholders,
                                         TemplateRenderer renderer) throws Exception {
        writeScript(outRoot, "run-local.sh", "/starter/templates/common/run-local.sh.tpl", placeholders, renderer);
        writeScript(outRoot, "submit.sh", "/starter/templates/common/submit.sh.tpl", placeholders, renderer);
    }

    private static void writeScript(Path outRoot,
                                    String filename,
                                    String templatePath,
                                    Map<String, String> placeholders,
                                    TemplateRenderer renderer) throws Exception {
        String content = renderer.renderClasspathTemplate(templatePath, placeholders);
        Path scriptFile = outRoot.resolve(filename);
        Files.createDirectories(scriptFile.getParent());
        Files.writeString(scriptFile, content, StandardCharsets.UTF_8);
        setExecutableIfSupported(scriptFile);
    }

    private static void setExecutableIfSupported(Path path) {
        try {
            Set<PosixFilePermission> permissions = new HashSet<>();
            permissions.add(PosixFilePermission.OWNER_READ);
            permissions.add(PosixFilePermission.OWNER_WRITE);
            permissions.add(PosixFilePermission.OWNER_EXECUTE);
            permissions.add(PosixFilePermission.GROUP_READ);
            permissions.add(PosixFilePermission.GROUP_EXECUTE);
            permissions.add(PosixFilePermission.OTHERS_READ);
            permissions.add(PosixFilePermission.OTHERS_EXECUTE);
            Files.setPosixFilePermissions(path, permissions);
        } catch (Exception ignored) {
            // Non-POSIX filesystem (e.g. Windows) can ignore executable permission setup.
        }
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

    private static boolean isValidArtifactId(String artifactId) {
        if (isBlank(artifactId)) {
            return true;
        }
        String value = artifactId.trim();
        if (value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9')
                    || c == '-'
                    || c == '.';
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    private static String toKebabCase(String value) {
        if (isBlank(value)) {
            return "flare-job";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    sb.append('-');
                }
                sb.append(Character.toLowerCase(c));
            } else if (Character.isLetterOrDigit(c)) {
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append('-');
            }
        }
        String result = sb.toString();
        while (result.contains("--")) {
            result = result.replace("--", "-");
        }
        return result.replaceAll("^-|-$", "");
    }

    private static String detectFlareVersion() {
        Package pkg = StarterCli.class.getPackage();
        if (pkg != null && !isBlank(pkg.getImplementationVersion())) {
            return pkg.getImplementationVersion();
        }
        return "1.0-SNAPSHOT";
    }

    private static String usage() {
        return "Usage: flare-starter --template <name> --job <ClassName> --out <dir> [--package <java.package>] [--artifact <maven-artifact-id>] [--flare-version <version>]";
    }

    static final class CliArgs {
        private final String template;
        private final String jobName;
        private final String outDir;
        private final String packageName;
        private final String artifactId;
        private final String flareVersion;
        private final boolean help;

        private CliArgs(String template,
                        String jobName,
                        String outDir,
                        String packageName,
                        String artifactId,
                        String flareVersion,
                        boolean help) {
            this.template = template;
            this.jobName = jobName;
            this.outDir = outDir;
            this.packageName = packageName;
            this.artifactId = artifactId;
            this.flareVersion = flareVersion;
            this.help = help;
        }

        static CliArgs parse(String[] args) {
            if (args == null || args.length == 0) {
                return new CliArgs(null, null, null, "com.example", null, null, false);
            }
            String template = null;
            String job = null;
            String out = null;
            String packageName = "com.example";
            String artifactId = null;
            String flareVersion = null;
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
                } else if ("--artifact".equals(arg)) {
                    artifactId = nextValue("--artifact", args, i);
                    i++;
                } else if ("--flare-version".equals(arg)) {
                    flareVersion = nextValue("--flare-version", args, i);
                    i++;
                } else {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            return new CliArgs(template, job, out, packageName, artifactId, flareVersion, help);
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
