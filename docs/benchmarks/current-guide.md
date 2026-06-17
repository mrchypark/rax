# Current Benchmark Guide

This guide describes the current legacy-named `rax-bench-cli` benchmark harness.
It is a runbook for producing local `rax` artifacts and reports, not a claim
that a particular machine's numbers are canonical.

For the latest small smoke report generated while refreshing these docs, see
[results/2026-05-29-current.md](results/2026-05-29-current.md).

## Command Matrix

| Command | Purpose |
| --- | --- |
| `pack` | Build a dataset pack from a fixture source directory. |
| `pack-adhoc` | Build a small synthetic pack from a `docs.ndjson` file. |
| `run` | Execute one workload against a packed dataset and write artifacts. |
| `query` | Query a packed dataset directly and print previewed hits. |
| `query-batch` | Run a query-set JSONL file and print ranked results. |
| `profile-vector-query` | Profile the first vector query under a selected vector mode. |
| `search-bench` | Build a temporary product `.rax` store from a pack and measure runtime search per-query latency/QPS. |
| `quality-report` | Compare ranked results to qrels and print quality metrics. |
| `reduce` | Reduce one run directory into summary JSON and Markdown. |
| `matrix-report` | Render a workload matrix from a release-matrix artifact root. |
| `mode-compare-report` | Compare `exact_flat` and `hnsw` artifact roots. |
| `replay` | Print the `cargo run` command stored in `run-manifest.json`. |

Use `cargo run -p rax-bench-cli -- <command> --help` for exact options.

## Dataset Inputs

Fixture source directories live under `fixtures/bench/source/`.

Pack the minimal fixture:

```bash
cargo run -p rax-bench-cli -- pack \
  --source fixtures/bench/source/minimal \
  --out /tmp/rax-pack \
  --tier small \
  --variant clean
```

Pack ad hoc documents:

```bash
cargo run -p rax-bench-cli -- pack-adhoc \
  --docs /tmp/docs.ndjson \
  --out /tmp/rax-adhoc-pack \
  --tier small
```

The benchmark harness expects packed datasets to contain `manifest.json` plus
the sidecars referenced by the manifest, such as documents, query sets, qrels,
text artifacts, vector payloads, and optional HNSW sidecars.

## Workloads

Current workload ids:

| Workload | Measures |
| --- | --- |
| `container_open` | Mount/open path without a query. |
| `materialize_vector` | First vector-lane materialization timing. |
| `ttfq_text` | Time to first text query. |
| `ttfq_vector` | Time to first vector query. |
| `warm_text` | Warm text search latency after warmup. |
| `warm_vector` | Warm vector search latency after warmup. |
| `warm_hybrid` | Warm hybrid search latency after warmup. |
| `warm_hybrid_with_previews` | Warm hybrid search latency with preview hydration. |

Run one workload:

```bash
cargo run -p rax-bench-cli -- run \
  --dataset /tmp/rax-pack \
  --workload ttfq_text \
  --sample-count 10 \
  --artifact-dir /tmp/rax-artifacts/ttfq_text
```

If `--artifact-dir` is omitted, `RAX_BENCH_ARTIFACT_DIR` is used. If neither is
set, artifacts go to `artifacts/latest`.

Set `RAX_BENCH_TEST_MODE=1` for deterministic test measurement plumbing. Do not
use that mode for performance claims.

## Runtime Search Benchmark

Use `search-bench` when you need rax's product search path as the benchmark
target. It reads a packed dataset, builds a temporary product `.rax` store from
the pack's documents and vectors, opens that store through `RuntimeStore`, and
measures each query in the query set with `RuntimeStore::search`.

```bash
cargo run --release -p rax-bench-cli -- search-bench \
  --dataset /tmp/rax-pack \
  --query-set /tmp/rax-pack/queries/core.jsonl \
  --sample-count 30 \
  --concurrency 1 \
  --vector-mode auto \
  --scale-label small-clean \
  --output /tmp/rax-search-bench.json \
  --artifact-dir /tmp/rax-search-bench-artifacts
```

The JSON summary keeps store preparation separate from timed search work:
`store_build_ms` is setup-only, while `total_elapsed_ms` and `qps_end_to_end`
cover request building, store opens, cold/warm sample passes, and other harness
overhead after setup. `request_build_ms` records query vector/request
construction outside `RuntimeStore::search`. `total_search_only_ms` and
`qps_search_only` use only the sum of timed `RuntimeStore::search` calls.
`qps` is retained as an alias for `qps_end_to_end`.

The summary includes `concurrency` and `scale_label`; if `--scale-label` is not
provided, `scale_label` defaults to the dataset id. It also includes
`rss_kb_before`, `rss_kb_after`, and `rss_kb_delta`. RSS readings are std-only
best effort: Linux reads `/proc/self/status`, and other Unix-like platforms use
`ps`.

For `cold_query`, each measured query opens its own fresh read-only
`RuntimeStore` before timing `RuntimeStore::search`, so lazy lane
materialization is not shared across cold samples. For `warm_steady`, each
sample opens one read-only store, runs an unmeasured warmup pass over all
prebuilt requests, then measures the same requests on the warmed store. The
summary reports overall p50/p95/p99 query latency for compatibility, plus
separate cold, warm, and warm-concurrent p50/p95/p99 latency fields.

`--concurrency` defaults to `1` and rejects `0`. With `--concurrency 1`,
`search-bench` records only the existing `cold_query` and `warm_steady` phases.
With `--concurrency N` for `N > 1`, it additionally records a measured
`warm_concurrent` phase: each worker thread opens its own read-only
`RuntimeStore`, warms it with all prepared requests outside measurement, then
runs `sample_count` measured iterations over every prepared request. Samples in
that phase include `worker_index`; cold and warm samples serialize
`worker_index` as `null`. `qps_warm_concurrent` is the wall-clock QPS for that
concurrent phase only; `qps_end_to_end` still includes serial cold and warm
passes.

If `--artifact-dir` is provided, `search-bench` writes:

| File | Contents |
| --- | --- |
| `summary.json` | The same summary printed to stdout and written by `--output`. |
| `samples.ndjson` | One per-query latency sample per line, including `phase`. |
| `ranked-results.json` | Warm-pass sample 0 ranked results as `{query_id, hits:[{doc_id}]}`. |
| `quality.json` | Quality reducer output, when the dataset manifest declares qrels for the query set. |

Runtime search currently supports only `--vector-mode auto`; use the packed
harness commands for vector-mode sidecar comparisons. The runtime request type
does not yet carry metadata filters, so `search-bench` reports
`supports_metadata_filters: false` and `filter_query_count`, and rejects query
sets containing any non-empty `filter_spec`.

## Vector Modes

Current vector modes:

| Mode | Meaning |
| --- | --- |
| `auto` | Let the vector lane choose the mode for the dataset and sidecars. |
| `exact_flat` | Force exact flat vector search. |
| `hnsw` | Request HNSW; missing sidecars fall back where the lane supports it. |
| `preview_q8` | Use quantized preview vectors. |

Example:

```bash
cargo run -p rax-bench-cli -- run \
  --dataset /tmp/rax-pack \
  --workload warm_vector \
  --vector-mode exact_flat \
  --sample-count 30 \
  --artifact-dir /tmp/rax-artifacts/exact_flat/warm_vector
```

## Standard Runbooks

Release matrix over the standard workload set:

```bash
scripts/bench-release-matrix.sh /tmp/rax-pack /tmp/rax-release-matrix 10
```

The script runs:

```text
container_open
materialize_vector
ttfq_text
ttfq_vector
warm_text
warm_vector
warm_hybrid
warm_hybrid_with_previews
```

It reduces each run and writes:

```text
/tmp/rax-release-matrix/vector-lane-summary.md
```

Vector mode comparison:

```bash
scripts/bench-vector-mode-compare.sh /tmp/rax-pack /tmp/rax-vector-compare 30
```

The script runs vector-heavy workloads for `exact_flat` and `hnsw`, writes a
matrix per mode, and produces:

```text
/tmp/rax-vector-compare/vector-mode-compare.md
```

## Artifact Layout

A `run` directory contains:

```text
sample-000.json
sample-001.json
...
summary.json
summary.md
run-manifest.json
```

After `reduce`, it also contains:

```text
reduced-summary.json
```

`run-manifest.json` stores file digests and replay configuration. Use it with:

```bash
cargo run -p rax-bench-cli -- replay --input /tmp/rax-artifacts/ttfq_text
```

## Metrics

The reducer summarizes available metric samples as p50/p95/p99 values.

| Metric | Meaning |
| --- | --- |
| `container_open_ms` | Mount/open duration. |
| `vector_materialization_ms` | Vector lane materialization duration when measured. |
| `total_ttfq_ms` | Total time to first query for cold workloads. |
| `search_latency_ms` | Measured warm search latency. |

Some metrics are intentionally unavailable for a workload. p99 is also
unavailable when the sample count is too small.

## Quality Reports

Run ranked results:

```bash
cargo run -p rax-bench-cli -- query-batch \
  --dataset /tmp/rax-pack \
  --query-set /tmp/rax-pack/queries/core.jsonl \
  --output /tmp/rax-results.json
```

Compare against qrels:

```bash
cargo run -p rax-bench-cli -- quality-report \
  --query-set /tmp/rax-pack/queries/core.jsonl \
  --qrels /tmp/rax-pack/queries/core-qrels.jsonl \
  --results /tmp/rax-results.json \
  --output /tmp/rax-quality.json
```

Quality summaries include ranked metrics such as NDCG, recall, and MRR where the
qrels file provides enough judgment data.

## Baseline Comparison

`reduce` accepts an optional baseline run directory:

```bash
cargo run -p rax-bench-cli -- reduce \
  --input /tmp/rax-candidate/ttfq_vector \
  --baseline /tmp/rax-baseline/ttfq_vector
```

The reducer rejects fairness mismatches, including incompatible dataset
fingerprints.

## Reporting Template

Use dated reports under `docs/benchmarks/results/` for measured outputs. Follow
this structure:

- Date, platform, commit, dataset, sample count
- Exact commands
- Summary tables
- Evidence paths
- Notes and caveats

Keep release-grade claims separate from smoke reports. Always include enough
metadata for another run to judge whether a comparison is fair.

## Upstream Wax Comparison Caveats

The upstream Wax reports are useful as a documentation pattern, especially their
commit/platform/command/evidence discipline. Do not copy their numbers or compare
directly without matching:

- language/runtime implementation
- platform and hardware acceleration
- dataset and query set
- workload definition
- sample count
- vector mode and embedding policy
- artifact and reducer semantics
