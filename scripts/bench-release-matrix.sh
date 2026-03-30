#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 <packed-dataset-dir> <artifact-root-dir> [sample-count]" >&2
  exit 1
fi

DATASET_DIR=$1
ARTIFACT_ROOT=$2
SAMPLE_COUNT=${3:-10}
BIN=target/release/wax-bench-cli

mkdir -p "$ARTIFACT_ROOT"

cargo build --release -p wax-bench-cli

for WORKLOAD in container_open ttfq_text ttfq_vector; do
  RUN_DIR="$ARTIFACT_ROOT/$WORKLOAD"
  rm -rf "$RUN_DIR"
  "$BIN" run \
    --dataset "$DATASET_DIR" \
    --workload "$WORKLOAD" \
    --sample-count "$SAMPLE_COUNT" \
    --artifact-dir "$RUN_DIR"
  "$BIN" reduce --input "$RUN_DIR"
done
