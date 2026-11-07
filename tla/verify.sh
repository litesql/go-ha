#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TLA2TOOLS_JAR="${TLA2TOOLS_JAR:-${SCRIPT_DIR}/.cache/tla2tools.jar}"
TLA2TOOLS_URL="https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar"

cleanup() {
    rm -rf "${SCRIPT_DIR}/states"
}
trap cleanup EXIT

if [[ ! -f "${TLA2TOOLS_JAR}" ]]; then
    echo "Downloading tla2tools.jar..."
    mkdir -p "$(dirname "${TLA2TOOLS_JAR}")"
    curl -sSL -o "${TLA2TOOLS_JAR}" "${TLA2TOOLS_URL}"
fi

run_tlc() {
    local spec="$1"
    local cfg="$2"
    echo "=================================================================="
    echo "Running TLC on ${spec} (config: ${cfg})"
    echo "=================================================================="
    java -XX:-UseContainerSupport -XX:+UseParallelGC \
        -cp "${TLA2TOOLS_JAR}" tlc2.TLC \
        -workers auto -cleanup -nowarning \
        -config "${SCRIPT_DIR}/${cfg}" \
        "${SCRIPT_DIR}/${spec}"
    echo ""
}

if [[ $# -gt 0 ]]; then
    for target in "$@"; do
        base="${target%.tla}"
        run_tlc "${base}.tla" "${base}.cfg"
    done
else
    run_tlc "TwoPhaseCommit.tla" "TwoPhaseCommit.cfg"
    run_tlc "LeaderBasedReplication.tla" "LeaderBasedReplication.cfg"
    run_tlc "LeaderlessReplication.tla" "LeaderlessReplication.cfg"
    run_tlc "RowIdentifyStrategies.tla" "RowIdentifyStrategies.cfg"
fi

echo "All go-ha TLA+ strategy specifications verified successfully!"
