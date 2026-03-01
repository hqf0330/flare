#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/flare-golden-path.XXXXXX")"
trap 'rm -rf "${WORK_DIR}"' EXIT

STARTER_OUT="${WORK_DIR}/starter-out"

echo "[verify] build flare-flink and dependencies"
mvn -q -pl flare-flink -am -DskipTests install

echo "[verify] generate starter project"
FLARE_SKIP_BUILD=1 ./scripts/flare-starter.sh \
  --template kafka-process-print \
  --job SmokeJob \
  --out "${STARTER_OUT}"

test -f "${STARTER_OUT}/src/main/java/com/example/SmokeJob.java"
test -f "${STARTER_OUT}/src/main/resources/flink-streaming.properties"
test -f "${STARTER_OUT}/README-run.md"

echo "[verify] doctor should pass for valid smoke job"
FLARE_SKIP_BUILD=1 ./scripts/flare-doctor.sh --job com.bhcode.flare.flink.doctor.samples.DoctorSmokeValidJob >/dev/null

echo "[verify] doctor should block invalid smoke job"
set +e
FLARE_SKIP_BUILD=1 ./scripts/flare-doctor.sh --job com.bhcode.flare.flink.doctor.samples.DoctorSmokeInvalidJob >/dev/null
invalid_code=$?
set -e
if [ "${invalid_code}" -eq 0 ]; then
  echo "expected non-zero exit code for invalid smoke job"
  exit 1
fi

echo "[verify] golden path verification passed"
