#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 <packed-dataset-dir> <artifact-root-dir> [sample-count]" >&2
  exit 1
fi

DATASET_DIR=$1
ARTIFACT_ROOT=$2
SAMPLE_COUNT=${3:-30}
BIN=target/release/wax-bench-cli

mkdir -p "$ARTIFACT_ROOT"

cargo build --release -p wax-bench-cli

for VECTOR_MODE in exact_flat hnsw; do
  for WORKLOAD in materialize_vector ttfq_vector warm_vector warm_hybrid; do
    RUN_DIR="$ARTIFACT_ROOT/$VECTOR_MODE/$WORKLOAD"
    rm -rf "$RUN_DIR"
    "$BIN" run \
      --dataset "$DATASET_DIR" \
      --workload "$WORKLOAD" \
      --sample-count "$SAMPLE_COUNT" \
      --vector-mode "$VECTOR_MODE" \
      --artifact-dir "$RUN_DIR"
    "$BIN" reduce --input "$RUN_DIR"
  done
  "$BIN" matrix-report \
    --input "$ARTIFACT_ROOT/$VECTOR_MODE" \
    --output "$ARTIFACT_ROOT/$VECTOR_MODE/vector-lane-summary.md"
done

"$BIN" mode-compare-report \
  --input "$ARTIFACT_ROOT" \
  --output "$ARTIFACT_ROOT/vector-mode-compare.md"
