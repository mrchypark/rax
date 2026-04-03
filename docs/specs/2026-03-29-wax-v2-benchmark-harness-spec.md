# Wax v2 Benchmark Harness Spec

Status: Draft  
Date: 2026-03-29  
Scope: executable benchmark harness requirements for Wax v2 on iOS and macOS

## 1. Purpose

This document defines the benchmark harness that will execute the Wax v2 benchmark plans.

Its job is to turn the benchmark specs into a repeatable system:

- dataset preparation
- benchmark execution
- metric collection
- fairness labeling
- artifact output

This is the implementation-facing companion to:

- [2026-03-29-wax-v2-benchmark-plan.md](./2026-03-29-wax-v2-benchmark-plan.md)
- [2026-03-29-wax-v2-ttfq-benchmark-plan.md](./2026-03-29-wax-v2-ttfq-benchmark-plan.md)
- [2026-03-29-wax-upstream-benchmark-analysis.md](./2026-03-29-wax-upstream-benchmark-analysis.md)

The first implementation should bias toward a simpler deliverable:

- dataset packer
- runner
- metric collector
- artifact writer

The report reducer can begin as a thin compare/export layer and grow later.

## 2. Non-Goals

This harness spec does not define:

- final Rust crate names
- final iOS app packaging
- dashboard UX for benchmark reports
- long-term perf infra hosting

It defines the minimal stable harness contract.

## 3. Harness Design Goals

### 3.1 Primary Goals

- reproducible benchmark execution
- like-for-like comparison across revisions
- explicit distinction between `container_open` and `TTFQ`
- artifact-rich output for regression analysis

### 3.2 Secondary Goals

- low harness overhead
- ability to run on developer machines and CI
- easy extension for new workloads or dataset tiers

### 3.3 Non-Goals

- zero-cost instrumentation in debug mode
- perfect cross-platform metric parity
- one binary that works identically on every host

## 4. High-Level Architecture

The harness should be split into five units:

1. `dataset packer`
   - prepares named benchmark corpora
2. `benchmark runner`
   - executes workloads against a Wax v2 build
3. `metric collector`
   - records latency, memory, and fairness state
4. `artifact writer`
   - writes machine-readable and human-readable output
5. `report reducer`
   - aggregates samples into summary tables

These units should be loosely coupled.

## 5. Benchmark Unit Model

Every executable benchmark case should be defined as a tuple:

- dataset id
- workload id
- environment id
- run mode
- fairness labels

This is the canonical benchmark identity.

### 5.1 Required Fields

Each benchmark case must include:

- `benchmark_id`
- `dataset_id`
- `device_id`
- `platform`
- `build_profile`
- `workload_id`
- `cache_state`
- `preview_mode`
- `query_embedding_mode`
- `sample_index`

## 6. Dataset Packer Contract

The dataset packer is responsible for producing deterministic input corpora.

### 6.1 Required Outputs

For each dataset pack, the packer must emit:

- manifest JSON
- document payload files or a compact packed representation
- metadata payload
- query set definitions
- expected embedding dimensions
- dirty/clean variant labels

### 6.2 Dataset Manifest

Each dataset manifest must include:

- `dataset_id`
- `dataset_version`
- `doc_count`
- `total_text_bytes`
- `vector_count`
- `embedding_dimensions`
- `avg_doc_length`
- `median_doc_length`
- `tombstone_ratio`
- `segment_expectations` if prebuilt stores are used
- `query_set_ids`

### 6.3 Clean And Dirty Variants

The packer must support:

- clean stores
- dirty stores with meaningful tombstone accumulation

This should not be left to ad-hoc manual setup.

## 7. Workload Definition Contract

Each workload should be declarative.

### 7.1 Required Workload Types

The harness must support at least:

- `container_open`
- `ttfq_text`
- `ttfq_vector`
- `ttfq_hybrid`
- `warm_text`
- `warm_vector`
- `warm_hybrid`
- `commit_latency`
- `ingest_latency`

### 7.2 Workload Definition Fields

Each workload definition must include:

- `workload_id`
- `query_id` or mutation recipe
- `mode`
- `top_k`
- `preview_mode`
- `query_embedding_mode`
- `expected_lane_mix`
- `timeout_policy`
- `sample_count`

### 7.3 TTFQ Query Modes

For TTFQ workloads, `query_embedding_mode` must be one of:

- `none`
- `precomputed`
- `runtime_generic`
- `runtime_ane_cold`
- `runtime_ane_warm`

## 8. Runner Contract

The benchmark runner executes benchmark units.

### 8.1 Required Runner Modes

The runner must support:

- single benchmark case
- full dataset tier sweep
- selected workload group
- repeated sample run

### 8.2 Required Runner Lifecycle

Each run should follow this lifecycle:

1. environment capture
2. dataset materialization or store restore
3. cache-state preparation
4. optional thermal cooldown
5. benchmark execution
6. metric flush
7. artifact write

The harness must not mix these phases implicitly.

## 9. Cache-State Preparation

Cache state is a first-class part of fairness.

### 9.1 Required Cache Labels

The harness must label runs as:

- `warm_process`
- `cold_process`
- `cold_process_warm_fs_cache`
- `cold_process_cold_fs_cache`

If the platform cannot truly provide one of these, the label must be omitted rather than faked.

### 9.2 Required Preparation Hooks

The runner should support hooks for:

- process restart
- store handle disposal
- cache-cold preparation when available
- ANE warmup
- ANE cold-start isolation

### 9.3 iOS Cold-Start Reality

On physical iOS devices, a true cold-start benchmark cannot be implemented purely inside the app process.

The harness should therefore define a host-driven execution path using one of:

- `XCTest` launch metrics
- `xcrun devicectl`
- equivalent external launch/kill tooling

If the host-driven path is not used, the run must not be labeled as true cold start.

## 10. Metric Collector Contract

The metric collector must record both top-level and slice-level numbers.

### 10.1 Required Latency Metrics

For all workloads:

- `total_ms`
- `p50_ms`
- `p95_ms`
- `p99_ms`
- `min_ms`
- `max_ms`

For TTFQ workloads:

- `container_open_ms`
- `metadata_readiness_ms`
- `lane_materialize_ms`
- `search_core_ms`
- `fusion_ms`
- `rerank_ms`
- `preview_ms`

### 10.2 Required Memory Metrics

On iOS-class devices, the collector must record:

- `phys_footprint_before`
- `phys_footprint_after_open`
- `phys_footprint_after_query`
- `phys_footprint_peak`

Supporting metrics when available:

- resident memory
- major faults
- minor faults

### 10.3 Required Fairness Metrics

The collector must also record:

- cache state
- whether previews were enabled
- whether query embeddings were precomputed or runtime
- whether ANE was cold or warm
- whether the dataset was clean or dirty

### 10.4 Guest-To-Host Metric Transport

For iOS device runs, the harness must define how metrics leave the app process and reach the host runner.

Allowed transport patterns:

- shared container or exported file pulled by host tooling
- signpost/trace capture collected by host tooling
- explicit local transport channel if reliable in the target environment

This transport must be defined early. Otherwise iOS-device automation will stall.

## 11. Instrumentation Boundaries

Instrumentation must not distort the hot path more than necessary.

### 11.1 Required Timing API

The harness should use:

- monotonic clocks for latency
- explicit span boundaries
- feature-gated detailed slice instrumentation

### 11.2 Two Output Tiers

The harness should support:

- `summary tier`
  - minimal overhead, top-level metrics
- `trace tier`
  - full breakdown and debugging fields

This avoids paying maximum instrumentation cost on every routine run.

The first implementation may keep the trace tier narrower than the full eventual vision, as long as summary-tier metrics and fairness labels remain stable.

## 12. Artifact Writer Contract

Every benchmark run must leave artifacts on disk.

### 12.1 Required Artifact Types

For each benchmark case, write:

- raw JSON result
- condensed JSON summary
- plain-text log
- environment manifest

Optional when available:

- OS signpost trace
- page-fault dump
- thermal or power state sample

### 12.2 Artifact Directory Layout

Recommended layout:

```text
artifacts/benchmarks/
  <run_id>/
    run_manifest.json
    summary.json
    environment.json
    logs/
    raw/
    traces/
```

### 12.3 Stable Filenames

Artifact filenames should be deterministic so reducers can find them without guessing.

## 13. Report Reducer Contract

The report reducer turns raw samples into decision-ready summaries.

### 13.1 Required Reducer Outputs

The reducer must produce:

- per-case summary table
- per-dataset summary
- per-device summary
- regression comparison against baseline run

For the first implementation, a lightweight reducer that emits Markdown, CSV, or compact JSON is sufficient.

### 13.2 Required Comparison Fields

Every comparison should include:

- absolute delta
- percent delta
- baseline run id
- current run id
- fairness mismatch warning if labels differ

## 14. Environment Capture

Every run must record enough information to make the result meaningful later.

### 14.1 Required Environment Fields

The environment manifest must include:

- git commit
- dirty worktree flag
- Rust toolchain version
- target triple
- build profile
- device model
- OS version
- battery or charging state if available
- thermal state if available
- benchmark harness version
- binary size if available
- executable text-segment size if available

### 14.2 iOS-Specific Fields

When running on iOS-class devices, also record:

- `phys_footprint` collection mode
- whether ANE was used
- whether ANE was prewarmed
- whether the app was foreground-only
- thermal cooldown policy
- host-driven or in-app launch mode

## 15. Baseline Comparison Rules

The harness must support comparison against two baseline classes:

- upstream Wax published reference
- previous Wax v2 run

### 15.1 Upstream Comparison Rule

When comparing to upstream Wax:

- compare only like-for-like metrics
- never compare upstream `cold open` to v2 `ttfq_hybrid`
- always annotate when upstream data is published-only and not locally reproduced

### 15.2 Regression Rule

A regression report must not be emitted as clean unless:

- dataset id matches
- workload id matches
- preview mode matches
- embedding mode matches
- cache-state label matches

## 16. CI And Local Execution

The harness should support both developer runs and CI runs.

### 16.1 Local Mode

Local mode should optimize for:

- quick selection of a few workloads
- easy artifact browsing
- optional trace tier

### 16.2 CI Mode

CI mode should optimize for:

- deterministic configs
- fixed sample counts
- machine-readable pass/fail output
- artifact retention

CI should not pretend to provide iPhone realism if it is running only on macOS.

## 17. Pass/Fail Contract

The harness must be able to mark runs as:

- `pass`
- `warn`
- `fail`

### 17.1 Hard Fail Cases

Examples:

- missing required metrics
- fairness labels missing for comparison mode
- `phys_footprint` unavailable on iOS benchmark runs where it is required
- TTFQ variant mislabeled as cold when cache state is unknown

### 17.2 Performance Gate Failures

Examples:

- first query exceeds device-class gate
- first hybrid query causes unacceptable memory spike
- vector first query requires graph rebuild above tolerated threshold

## 18. Recommended CLI Shape

The exact implementation can vary, but the harness should expose a CLI equivalent to:

```text
wax-bench dataset build --dataset medium_clean
wax-bench run --device ios_a17 --workload ttfq_hybrid_with_preview --dataset medium_clean
wax-bench sweep --plan ttfq_core
wax-bench reduce --run-id <id>
wax-bench compare --baseline <id> --current <id>
```

This keeps the system scriptable and explicit.

## 19. Rewrite Triggers For The Harness Itself

The harness should be replaced or reworked if:

- instrumentation overhead materially changes top-level latency
- artifacts are too incomplete to explain regressions
- fairness labels are routinely wrong or missing
- developers cannot reproduce CI summaries locally
- iOS device runs require too much manual setup to remain credible

## 20. Summary

The benchmark harness should lock down six things:

1. deterministic dataset and workload identities
2. explicit cache-state and fairness labeling
3. split metrics for container open, metadata readiness, and TTFQ
4. iOS-first memory collection using `phys_footprint`
5. durable artifacts and comparable summaries
6. clear pass/fail rules for both performance and benchmark validity

If those six things hold, Wax v2 will have a benchmark system that is good enough to compare fairly against upstream Wax, catch deferred-initialization traps, and justify backend rewrites when the numbers demand them.
