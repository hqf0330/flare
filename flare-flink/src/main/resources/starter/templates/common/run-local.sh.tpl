#!/usr/bin/env bash
set -euo pipefail

if [[ "${FLARE_SKIP_BUILD:-0}" != "1" ]]; then
  mvn -DskipTests clean package
fi

JAR="$(ls -1 target/${ARTIFACT_ID}-*-all.jar 2>/dev/null | head -n 1)"
if [[ -z "${JAR}" ]]; then
  echo "Cannot find shaded jar: target/${ARTIFACT_ID}-*-all.jar"
  exit 1
fi

echo "[run-local] using jar: ${JAR}"
java -cp "${JAR}" ${PACKAGE}.${JOB_NAME} "$@"
