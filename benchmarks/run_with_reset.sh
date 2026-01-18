#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

"${SCRIPT_DIR}/reset_cluster.sh"

if [ "$#" -eq 0 ]; then
    echo "Usage: $(basename "$0") <baseline|scaling|load|failure> [--config path] [--output-dir path]"
    exit 1
fi

java -jar "${ROOT_DIR}/benchmarks/target/benchmarks.jar" "$@"
