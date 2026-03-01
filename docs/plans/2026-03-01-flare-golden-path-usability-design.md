# Flare Golden Path Usability Design

**Date:** 2026-03-01
**Owner:** Flare Core Team

## 1. Background

Flare is positioned for DataStream JAR development with annotation-driven configuration.
The primary usability goal is:

- Minimize total time from "new job from zero" to "stable running in target environment".
- Specifically reduce non-business effort (project scaffolding, config trial-and-error, packaging, pre-launch verification).

## 2. Goal and KPI

### Goal

Build a constrained "golden path" that optimizes the development-to-stability workflow.

### Target KPI (initial baseline)

- P50: stable run-through within 4 hours.
- P90: stable run-through within 1 working day.
- Pre-submit config issue interception rate >= 80%.
- Online failures caused by basic config mistakes reduce >= 50% (after rollout).

## 3. Scope

### In Scope

- DataStream JAR development path.
- Annotation-driven configuration validation.
- Local/CI preflight checks.
- Runtime startup diagnostics for fast troubleshooting.

### Out of Scope (explicitly deferred)

- UI platform.
- Configuration center.
- Flink SQL job orchestration and SQL platform capabilities.

## 4. Architecture

Golden path has three layers:

1. Starter (bootstrap layer)
- Generate runnable business job skeletons.
- Provide stable default config and test skeleton.

2. Doctor (preflight gate)
- Validate annotation/config/runtime-critical options before package/release.
- Produce standardized diagnostics with severity and error codes.

3. Runtime report (execution layer enhancement)
- Emit startup report showing effective settings and config sources.
- Improve observability and reduce troubleshooting round-trips.

## 5. Component Design

### 5.1 Starter

Input:

- job name
- template type (initially two):
  - `kafka-process-print`
  - `kafka-asyncJdbc-jdbcSink`

Output:

- `<JobName>.java` (extends `FlinkStreaming`)
- `flink-streaming.properties`
- `README-run.md` (run/package/deploy commands)
- `<JobName>Test.java` skeleton

Constraints:

- Keep generated code minimal and business-editable.
- Defaults should be stable-first (not extreme performance-first).

### 5.2 Doctor

Execution modes:

- local CLI
- CI pre-package stage

Severity model:

- `ERROR`: blocks package/release (non-zero exit)
- `WARN`: non-blocking recommendation

Initial check categories (MVP):

1. Annotation completeness
- Missing `@Streaming`
- Connector annotation/key mismatch

2. Required config completeness
- Kafka required fields (brokers/topic/group)
- JDBC required fields (url/user/password)
- Redis required fields (host/port when enabled)

3. Runtime stability guardrails
- checkpoint critical settings
- parallelism sanity
- auto-start consistency
- dirty-data handling switch recommendation

Output:

- human-readable console report
- machine-readable JSON report

### 5.3 Runtime Report

Enhance startup logs to include:

- effective values of key annotations/configs
- config source trace (annotation vs file vs args)
- connector validation summary

Expected outcome:

- on-call or developer can identify misconfiguration quickly.

## 6. Standard Error Model

Use unified diagnostic codes:

- `DR-001`: missing `@Streaming`
- `DR-101`: missing Kafka bootstrap servers
- `DR-102`: missing Kafka topic
- `DR-201`: missing JDBC url
- `DR-202`: missing JDBC user
- `DR-301`: checkpoint interval invalid

Principles:

- one issue => one stable code
- doctor and runtime should share code vocabulary

## 7. End-to-End Workflow

1. Generate skeleton via Starter
2. Implement business logic in `process()` only
3. Run Doctor locally
4. Package JAR
5. Submit to Flink cluster
6. Use Runtime Report for quick confirmation/troubleshooting

## 8. Delivery Plan

### Phase 1 (2 weeks, MVP)

- Starter: 2 templates
- Doctor: 10-15 high-value rules
- Console + JSON report
- CI integration before package

### Phase 2 (4-6 weeks)

- Expand rules to 30+
- Add safe autofix for selected defaults (`doctor --fix`)
- Improve source-trace diagnostics and team metrics

## 9. Risks and Mitigation

Risk 1: too many strict checks increase friction.

- Mitigation: start with high-confidence blocking rules only.

Risk 2: too many templates increase cognitive load.

- Mitigation: only keep two golden templates in MVP.

Risk 3: legacy jobs adoption is slow.

- Mitigation: pilot rollout first, then incremental migration.

## 10. Acceptance Criteria

- New job can be scaffolded and locally runnable in <= 30 minutes.
- Doctor must block clearly invalid setup before package.
- CI can consume doctor JSON output.
- Pilot teams confirm reduced non-business troubleshooting time.
