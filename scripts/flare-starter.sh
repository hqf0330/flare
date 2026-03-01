#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ "${FLARE_SKIP_BUILD:-0}" != "1" ]]; then
  mvn -q -pl flare-flink -am -DskipTests install
fi

mvn -q -f flare-flink/pom.xml -am -DskipTests \
  -Dexec.mainClass=com.bhcode.flare.flink.starter.StarterCli \
  -Dexec.args="$*" \
  org.codehaus.mojo:exec-maven-plugin:3.6.0:java
