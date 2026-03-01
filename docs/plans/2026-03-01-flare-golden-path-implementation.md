# Flare Golden Path (Starter + Doctor) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Deliver a practical golden path that reduces non-business time for new Flink DataStream jobs from scaffolding to stable run-through.

**Architecture:** Add two tooling capabilities on top of existing Flare runtime: `Starter` (job scaffold generator) and `Doctor` (preflight validator with blocking diagnostics). Keep runtime unchanged except optional diagnostic hooks and docs, and explicitly exclude UI/config-center work.

**Tech Stack:** Java 17, Maven, JUnit4, existing Flare modules (`flare-flink`, `flare-connectors`, `flare-common`), shell scripts for local command wrappers.

---

### Task 1: Build Doctor Core (diagnostic model + report)

**Files:**
- Modify: `flare-flink/pom.xml`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/DiagnosticSeverity.java`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/Diagnostic.java`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/DoctorReport.java`
- Test: `flare-flink/src/test/java/com/bhcode/flare/flink/doctor/DoctorReportTest.java`

**Step 1: Write the failing test**

```java
@Test
public void shouldRenderJsonAndDetectBlockingErrors() {
    DoctorReport report = new DoctorReport();
    report.add(new Diagnostic(DiagnosticSeverity.ERROR, "DR-001", "missing @Streaming", "add @Streaming"));
    report.add(new Diagnostic(DiagnosticSeverity.WARN, "DR-901", "parallelism not set", "set parallelism"));

    Assert.assertTrue(report.hasErrors());
    String json = report.toJson();
    Assert.assertTrue(json.contains("DR-001"));
    Assert.assertTrue(json.contains("ERROR"));
}
```

**Step 2: Run test to verify it fails**

Run: `mvn -pl flare-flink -Dtest=DoctorReportTest test`
Expected: FAIL with class not found / symbol not found for doctor classes.

**Step 3: Write minimal implementation**

```java
public enum DiagnosticSeverity { ERROR, WARN, INFO }
```

```java
public record Diagnostic(DiagnosticSeverity severity, String code, String message, String suggestion) {}
```

```java
public class DoctorReport {
  private final List<Diagnostic> diagnostics = new ArrayList<>();
  public void add(Diagnostic d) { diagnostics.add(d); }
  public boolean hasErrors() { return diagnostics.stream().anyMatch(d -> d.severity() == DiagnosticSeverity.ERROR); }
  public String toJson() { return diagnostics.stream().map(d -> String.format("{\"severity\":\"%s\",\"code\":\"%s\",\"message\":\"%s\",\"suggestion\":\"%s\"}", d.severity(), d.code(), d.message(), d.suggestion())).collect(Collectors.joining(",", "[", "]")); }
}
```

**Step 4: Run test to verify it passes**

Run: `mvn -pl flare-flink -Dtest=DoctorReportTest test`
Expected: PASS.

**Step 5: Commit**

```bash
git add flare-flink/pom.xml \
  flare-flink/src/main/java/com/bhcode/flare/flink/doctor/DiagnosticSeverity.java \
  flare-flink/src/main/java/com/bhcode/flare/flink/doctor/Diagnostic.java \
  flare-flink/src/main/java/com/bhcode/flare/flink/doctor/DoctorReport.java \
  flare-flink/src/test/java/com/bhcode/flare/flink/doctor/DoctorReportTest.java
git commit -m "feat(doctor): add diagnostic model and report core"
```

### Task 2: Add Doctor Rule Engine and Annotation Rules

**Files:**
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules/DoctorRule.java`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules/StreamingAnnotationRule.java`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules/ConnectorAnnotationKeyRule.java`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/DoctorRunner.java`
- Test: `flare-flink/src/test/java/com/bhcode/flare/flink/doctor/rules/StreamingAnnotationRuleTest.java`
- Test: `flare-flink/src/test/java/com/bhcode/flare/flink/doctor/rules/ConnectorAnnotationKeyRuleTest.java`

**Step 1: Write the failing tests**

```java
@Test
public void shouldFailWhenStreamingAnnotationMissing() {
    DoctorReport r = new DoctorRunner(List.of(new StreamingAnnotationRule())).run(NoStreamingJob.class);
    Assert.assertTrue(r.hasErrors());
}
```

```java
@Test
public void shouldFailWhenDuplicateConnectorKeyNum() {
    DoctorReport r = new DoctorRunner(List.of(new ConnectorAnnotationKeyRule())).run(DuplicateJdbcKeyJob.class);
    Assert.assertTrue(r.toJson().contains("DR-011"));
}
```

**Step 2: Run tests to verify failures**

Run: `mvn -pl flare-flink -Dtest=StreamingAnnotationRuleTest,ConnectorAnnotationKeyRuleTest test`
Expected: FAIL due missing rule classes/runner.

**Step 3: Write minimal implementation**

```java
public interface DoctorRule { void check(Class<?> jobClass, DoctorReport report); }
```

```java
if (!jobClass.isAnnotationPresent(Streaming.class)) {
    report.add(new Diagnostic(ERROR, "DR-001", "Missing @Streaming", "Add @Streaming to job class"));
}
```

```java
// For Jdbc/Kafka/Redis annotations: detect duplicated keyNum in same annotation family
```

**Step 4: Run tests to verify pass**

Run: `mvn -pl flare-flink -Dtest=StreamingAnnotationRuleTest,ConnectorAnnotationKeyRuleTest test`
Expected: PASS.

**Step 5: Commit**

```bash
git add flare-flink/src/main/java/com/bhcode/flare/flink/doctor \
  flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules \
  flare-flink/src/test/java/com/bhcode/flare/flink/doctor/rules
git commit -m "feat(doctor): add rule engine and annotation validation rules"
```

### Task 3: Add Config Completeness Rules (Kafka/JDBC/Redis)

**Files:**
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules/KafkaRequiredConfigRule.java`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules/JdbcRequiredConfigRule.java`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules/RedisRequiredConfigRule.java`
- Test: `flare-flink/src/test/java/com/bhcode/flare/flink/doctor/rules/RequiredConfigRulesTest.java`

**Step 1: Write failing tests**

```java
@Test
public void shouldReportMissingKafkaBrokers() {
    DoctorReport r = runner.run(MissingKafkaBrokersJob.class);
    Assert.assertTrue(r.toJson().contains("DR-101"));
}
```

```java
@Test
public void shouldReportMissingJdbcUrl() {
    DoctorReport r = runner.run(MissingJdbcUrlJob.class);
    Assert.assertTrue(r.toJson().contains("DR-201"));
}
```

**Step 2: Run to see failures**

Run: `mvn -pl flare-flink -Dtest=RequiredConfigRulesTest test`
Expected: FAIL because rules not implemented.

**Step 3: Implement minimal rules**

```java
// Kafka: brokers/topic/groupId must exist on @Kafka or equivalent property mapping
report.add(new Diagnostic(ERROR, "DR-101", "Kafka brokers missing", "Set @Kafka(brokers=...)"));
```

```java
// JDBC: url/username/password required when @Jdbc present
```

```java
// Redis: host/port required when @Redis present
```

**Step 4: Re-run tests**

Run: `mvn -pl flare-flink -Dtest=RequiredConfigRulesTest test`
Expected: PASS.

**Step 5: Commit**

```bash
git add flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules \
  flare-flink/src/test/java/com/bhcode/flare/flink/doctor/rules/RequiredConfigRulesTest.java
git commit -m "feat(doctor): add required config rules for kafka/jdbc/redis"
```

### Task 4: Add Runtime Stability Guardrail Rules

**Files:**
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules/CheckpointGuardrailRule.java`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules/ParallelismGuardrailRule.java`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules/AutoStartGuardrailRule.java`
- Test: `flare-flink/src/test/java/com/bhcode/flare/flink/doctor/rules/GuardrailRulesTest.java`

**Step 1: Write failing tests**

```java
@Test
public void shouldWarnWhenCheckpointDisabled() {
    DoctorReport r = runner.run(NoCheckpointJob.class);
    Assert.assertTrue(r.toJson().contains("DR-301"));
}
```

**Step 2: Verify failure**

Run: `mvn -pl flare-flink -Dtest=GuardrailRulesTest test`
Expected: FAIL.

**Step 3: Implement minimal guardrails**

```java
// WARN if @Streaming interval <= 0
// WARN if parallelism <= 0
// WARN if autoStart=false and no explicit deployment guidance
```

**Step 4: Verify pass**

Run: `mvn -pl flare-flink -Dtest=GuardrailRulesTest test`
Expected: PASS.

**Step 5: Commit**

```bash
git add flare-flink/src/main/java/com/bhcode/flare/flink/doctor/rules \
  flare-flink/src/test/java/com/bhcode/flare/flink/doctor/rules/GuardrailRulesTest.java
git commit -m "feat(doctor): add runtime stability guardrail rules"
```

### Task 5: Add Doctor CLI and Build Integration

**Files:**
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/doctor/DoctorCli.java`
- Create: `scripts/flare-doctor.sh`
- Modify: `flare-flink/pom.xml`
- Test: `flare-flink/src/test/java/com/bhcode/flare/flink/doctor/DoctorCliTest.java`

**Step 1: Write failing CLI test**

```java
@Test
public void shouldReturnNonZeroWhenErrorExists() {
    int code = DoctorCli.run(new String[]{"--job", "com.bhcode.flare.examples.InvalidJob"});
    Assert.assertNotEquals(0, code);
}
```

**Step 2: Verify fails**

Run: `mvn -pl flare-flink -Dtest=DoctorCliTest test`
Expected: FAIL.

**Step 3: Implement CLI**

```java
// args: --job <fully-qualified-job-class> [--json-out path]
// load class by reflection
// execute DoctorRunner with default rules
// print report
// return 1 if report.hasErrors() else 0
```

**Step 4: Verify pass**

Run: `mvn -pl flare-flink -Dtest=DoctorCliTest test`
Expected: PASS.

**Step 5: Commit**

```bash
git add flare-flink/src/main/java/com/bhcode/flare/flink/doctor/DoctorCli.java \
  flare-flink/src/test/java/com/bhcode/flare/flink/doctor/DoctorCliTest.java \
  flare-flink/pom.xml scripts/flare-doctor.sh
git commit -m "feat(doctor): add cli entry and local command wrapper"
```

### Task 6: Add Starter Generator (Two Golden Templates)

**Files:**
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/starter/StarterCli.java`
- Create: `flare-flink/src/main/java/com/bhcode/flare/flink/starter/TemplateRenderer.java`
- Create: `flare-flink/src/main/resources/starter/templates/kafka-process-print/Job.java.tpl`
- Create: `flare-flink/src/main/resources/starter/templates/kafka-asyncJdbc-jdbcSink/Job.java.tpl`
- Create: `flare-flink/src/main/resources/starter/templates/common/flink-streaming.properties.tpl`
- Create: `scripts/flare-starter.sh`
- Test: `flare-flink/src/test/java/com/bhcode/flare/flink/starter/StarterCliTest.java`

**Step 1: Write failing starter test**

```java
@Test
public void shouldGenerateJobAndProperties() throws Exception {
    Path out = Files.createTempDirectory("starter-test");
    int code = StarterCli.run(new String[]{"--template", "kafka-process-print", "--job", "DemoJob", "--out", out.toString()});
    Assert.assertEquals(0, code);
    Assert.assertTrue(Files.exists(out.resolve("src/main/java/com/example/DemoJob.java")));
    Assert.assertTrue(Files.exists(out.resolve("src/main/resources/flink-streaming.properties")));
}
```

**Step 2: Verify fail**

Run: `mvn -pl flare-flink -Dtest=StarterCliTest test`
Expected: FAIL.

**Step 3: Implement minimal starter**

```java
// Render simple placeholders: ${JOB_NAME}, ${PACKAGE}
// Generate job class + properties + README-run.md
```

**Step 4: Verify pass**

Run: `mvn -pl flare-flink -Dtest=StarterCliTest test`
Expected: PASS.

**Step 5: Commit**

```bash
git add flare-flink/src/main/java/com/bhcode/flare/flink/starter \
  flare-flink/src/main/resources/starter/templates \
  flare-flink/src/test/java/com/bhcode/flare/flink/starter/StarterCliTest.java \
  scripts/flare-starter.sh
git commit -m "feat(starter): add golden path job scaffold generator"
```

### Task 7: Documentation and Pipeline Contract

**Files:**
- Modify: `README.md`
- Modify: `docs/iteration_plan.md`
- Create: `docs/plans/doctor-rule-catalog.md`
- Create: `scripts/verify-golden-path.sh`

**Step 1: Write failing doc contract test (smoke script first)**

```bash
#!/usr/bin/env bash
set -euo pipefail
./scripts/flare-starter.sh --template kafka-process-print --job SmokeJob --out /tmp/flare-smoke
./scripts/flare-doctor.sh --job com.example.SmokeJob
```

**Step 2: Run smoke to verify it fails before docs/commands are aligned**

Run: `bash scripts/verify-golden-path.sh`
Expected: FAIL until command paths/docs are corrected.

**Step 3: Implement docs and verification script**

```markdown
# Doctor Rule Catalog
- DR-001 Missing @Streaming (ERROR)
- DR-101 Missing kafka brokers (ERROR)
...
```

**Step 4: Re-run smoke and module tests**

Run: `bash scripts/verify-golden-path.sh`
Expected: PASS.

Run: `mvn test`
Expected: PASS.

**Step 5: Commit**

```bash
git add README.md docs/iteration_plan.md docs/plans/doctor-rule-catalog.md scripts/verify-golden-path.sh
git commit -m "docs(golden-path): add usage, rule catalog, and verification script"
```

---

## Final Verification Checklist

- Run: `mvn -pl flare-flink test`
- Run: `mvn test`
- Run: `bash scripts/verify-golden-path.sh`
- Confirm doctor returns non-zero on an intentionally invalid job.
- Confirm starter generates runnable skeleton for both golden templates.

## Notes

- Keep SQL/UI/config-center work out of scope for this implementation.
- Keep only high-confidence ERROR rules in MVP; move uncertain checks to WARN.
- Prefer deterministic messages and stable error codes for CI and support playbooks.
