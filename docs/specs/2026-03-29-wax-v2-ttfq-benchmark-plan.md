# Wax v2 TTFQ Benchmark Plan

Status: Draft  
Date: 2026-03-29  
Scope: benchmark rules for separating container-open latency from time-to-first-query in Wax v2

## 1. Purpose

This document defines how Wax v2 should measure `TTFQ`:

- `time to first query`

It exists because a single "cold open" number is not enough for a search system with independent text and vector lanes.

The main goal is to prevent benchmark confusion between:

- container open
- lane materialization
- first useful query

This document is a companion to:

- [2026-03-29-wax-v2-benchmark-plan.md](/Users/cypark/Documents/project/rax/docs/specs/2026-03-29-wax-v2-benchmark-plan.md)
- [2026-03-29-wax-upstream-benchmark-analysis.md](/Users/cypark/Documents/project/rax/docs/specs/2026-03-29-wax-upstream-benchmark-analysis.md)

## 2. Why This Needs Its Own Plan

Upstream Wax publishes strong `cold open` numbers, but upstream analysis shows those numbers mostly reflect cheap container open, not necessarily full first-query readiness across all lanes.

Wax v2 must therefore measure three different things separately:

1. how fast the file opens
2. how much work the first lane query triggers
3. how much memory and page-fault cost the first lane query causes

If these are collapsed into one benchmark, the rewrite can appear better or worse than it really is.

## 3. Decision Role

TTFQ benchmarks answer:

- is Wax v2 actually ready for user-perceived search after open?
- which lane is hiding deferred initialization cost?
- is open cheap because the work was merely delayed?
- does first-query cost on iPhone-class devices make the architecture unacceptable even if warm p95 is good?

These are architecture questions, not only performance-report questions.

## 4. Definitions

The benchmark suite must use these terms consistently.

### 4.1 Container Open

`container_open`

Time from process-controlled open start to the point where the Wax store handle is returned and metadata needed for routing is available.

This must exclude:

- first text-engine materialization
- first vector-engine materialization
- first preview generation
- first query embedding generation

### 4.1.1 Metadata Readiness

`metadata_readiness`

This is the point where Wax knows:

- which lane families are present
- their versions
- their offsets and lengths
- enough routing metadata to decide the first query plan

but has not yet faulted lane payload pages or built lane-private runtime structures.

`container_open` reporting should make clear whether `metadata_readiness` is included, because that boundary is part of fair comparison.

### 4.2 First Text Query

`ttfq_text`

Time from issuing the first eligible text query after `container_open` to receiving the first completed text result set.

This includes any deferred text-lane work such as:

- text-engine open
- mmap faults
- backend blob validation
- doc filter setup
- preview assembly if the benchmark variant includes previews

### 4.3 First Vector Query

`ttfq_vector`

Time from issuing the first eligible vector query after `container_open` to receiving the first completed vector result set.

This includes any deferred vector-lane work such as:

- graph open or graph rebuild
- vector payload mapping
- GPU buffer upload
- exact rerank payload access
- query-vector validation

Query embedding generation must be measured separately unless the benchmark explicitly says it is included.

For a performance-first Wax v2, a large CPU-bound graph rebuild on first query should be treated as a benchmark failure signal, not a normal steady architecture cost.

### 4.4 First Hybrid Query

`ttfq_hybrid`

Time from issuing the first eligible hybrid query after `container_open` to receiving the first completed hybrid result set.

This includes:

- planner work
- lane opens
- fusion
- bounded rerank
- preview assembly if enabled

### 4.5 Warm Query

`warm_query`

A query run after the same lane or lane combination has already been exercised and the benchmark variant has reached steady state.

Warm queries must not be mixed into TTFQ numbers.

## 5. Required TTFQ Variants

Every benchmark matrix must include all of these:

- `container_open`
- `ttfq_text_no_preview`
- `ttfq_text_with_preview`
- `ttfq_vector_no_embedder`
- `ttfq_hybrid_no_preview`
- `ttfq_hybrid_with_preview`

If the product includes on-device query embedding in the default path, also include:

- `ttfq_vector_with_query_embedding`
- `ttfq_hybrid_with_query_embedding`

If Apple-platform on-device embedding is part of product evaluation, also include:

- `ttfq_vector_with_query_embedding_ane_cold`
- `ttfq_hybrid_with_query_embedding_ane_cold`
- `ttfq_vector_with_query_embedding_ane_warm`
- `ttfq_hybrid_with_query_embedding_ane_warm`

This keeps storage/search cost separate from embedder cost.

## 6. Fairness Rules

These rules are mandatory for comparison against upstream Wax or future Wax v2 variants.

### 6.1 Never Compare Container Open To Hybrid TTFQ

Do not compare:

- Wax upstream `cold open`

against:

- Wax v2 `first hybrid query`

That is not a fair comparison.

Allowed comparisons:

- container open vs container open
- first text query vs first text query
- first vector query vs first vector query
- first hybrid query vs first hybrid query

### 6.2 Separate Search From Embedding

If query embedding is involved, benchmark both:

- with precomputed query vectors
- with real query embedding generation

Otherwise the benchmark cannot tell whether the cost belongs to search or to the embedder.

### 6.3 Preview Is Its Own Cost Center

Preview-enabled and preview-disabled benchmarks must be reported separately.

Do not hide preview work inside one generic search number.

### 6.4 Page Cache Fairness

Process restart alone is not enough to claim a cold measurement.

The benchmark harness must record whether the run is:

- process-cold only
- filesystem-cache-cold
- warm in-process

When true cache-cold runs are possible, they should be preferred for TTFQ reporting.

### 6.5 Resident Memory Is Coequal With Latency

A TTFQ result is not acceptable if it hits a good latency number by causing unacceptable iOS memory pressure.

Every TTFQ report must include memory observations alongside latency.

On iOS-class devices, the primary memory gate should be:

- `phys_footprint`

If additional memory counters are collected, they should be reported as supporting metrics, not substitutes for `phys_footprint`.

## 7. Measurement Slices

Each TTFQ benchmark should produce a breakdown, not only one final number.

### 7.1 Minimum Breakdown

At minimum, record:

- `container_open_ms`
- `metadata_readiness_ms`
- `lane_open_or_materialize_ms`
- `search_core_ms`
- `fusion_ms`
- `rerank_ms`
- `preview_ms`
- `total_ttfq_ms`

If a slice is not used, record it as zero or not-applicable explicitly.

### 7.2 Why Breakdown Matters

Without a breakdown, the team cannot answer:

- whether text open dominates
- whether vector graph build dominates
- whether preview dominates
- whether fusion is unexpectedly expensive

## 8. Memory And Fault Metrics

For each TTFQ variant, record:

- `phys_footprint` before open
- `phys_footprint` after `container_open`
- `phys_footprint` after first query completion
- peak `phys_footprint` during first query
- resident memory before open if available
- resident memory after `container_open` if available
- resident memory after first query completion if available
- peak resident memory during first query if available
- major page faults if available
- minor page faults if available

On iOS-class devices, these should be treated as first-class gates, not supporting notes.

## 9. Device Matrix

TTFQ must be measured on:

- older supported iPhone-class device
- current mainstream iPhone-class device
- high-memory iPad or equivalent iOS-class device
- Apple Silicon macOS for upstream-style comparability

Linux can remain in the broader benchmark plan, but it is not the primary environment for TTFQ product decisions.

## 10. Dataset Matrix

TTFQ must be run on at least three dataset tiers:

### 10.1 Small

- `1K-5K` docs
- useful for fixed overhead

### 10.2 Medium

- `50K-100K` docs
- useful for realistic startup behavior

### 10.3 Large

- `500K+` docs
- useful for exposing deferred index materialization, mmap, and memory cliffs

### 10.4 Required Dataset Metadata

Every TTFQ result must record:

- doc count
- total text bytes
- vector count
- embedding dimensions
- average and median doc length
- tombstone ratio
- active segment counts by family

TTFQ runs should include both:

- clean datasets with low tombstone ratio
- dirty datasets with meaningful tombstone accumulation

## 11. Query Matrix

TTFQ must not be benchmarked with one toy query.

### 11.1 Text Queries

Include:

- exact keyword query
- multi-term topical query
- no-hit query
- metadata-filtered query
- high-recall query

### 11.2 Vector Queries

Include:

- precomputed semantic query vector
- broad semantic query
- selective-filter semantic query
- high-recall semantic query

### 11.3 Hybrid Queries

Include:

- balanced hybrid query
- lexical-heavy hybrid query
- semantic-heavy hybrid query
- selective-filter hybrid query
- high-recall hybrid query

The selective-filter variants are especially important because they expose planner mistakes and oversample waste.

## 12. TTFQ Pass/Fail Gates

TTFQ should be used as a hard gate in addition to warm p95.

### 12.1 Required Pass Conditions

Wax v2 should pass only if:

- `ttfq_text` remains close enough to `container_open` that the lexical lane does not feel hidden-expensive
- `ttfq_vector` does not reveal unacceptable deferred graph or buffer setup
- `ttfq_hybrid` remains product-acceptable on iPhone-class devices
- resident memory after first hybrid query remains within safe operating bounds for the target device class

For persisted ANN designs, the preferred pass condition is:

- zero-rebuild or near-zero-rebuild first-query behavior from persisted vector state

### 12.2 Failure Interpretation

If `container_open` is excellent but `ttfq_vector` is bad:

- the vector lane is still effectively cold
- or persisted vector state is not actually query-ready

If `ttfq_text` is bad but warm text p95 is good:

- text engine materialization is the hidden bottleneck

If `ttfq_hybrid` is much worse than both lane-local TTFQ values:

- planner, fusion, preview, or rerank is the problem

## 13. Reporting Format

Every TTFQ report should contain three views.

### 13.1 Executive View

- container open
- metadata readiness
- first text query
- first vector query
- first hybrid query
- peak `phys_footprint` after first hybrid query

### 13.2 Breakdown View

- slice timings
- memory deltas
- page-fault counters if available

### 13.3 Fairness View

- cache state
- whether query embedding was included
- whether previews were included
- whether filesystem cache was cold

Without the fairness view, cross-run comparison will be misleading.

## 14. Rewrite Triggers

TTFQ results should trigger subsystem rewrites or backend changes if any of these remain true after tuning:

- text lane materialization dominates `ttfq_text`
- vector first query requires large rebuild or upload cost
- first hybrid query causes unacceptable iOS memory spikes
- cache-cold first query is much worse than process-cold first query
- preview or rerank dominates first-query latency

These are not minor optimizations. They indicate architecture shape problems.

## 15. Summary

Wax v2 should treat `TTFQ` as a top-level product metric, not a supporting metric.

The stable rules are:

1. container open and first query must be measured separately
2. text, vector, and hybrid first-query paths need separate TTFQ numbers
3. preview and query embedding must be split out explicitly
4. iOS resident memory during first query is a hard gate
5. fair comparison requires cache-state labeling and like-for-like benchmark matching

If these rules are followed, the team can compare Wax v2 fairly against upstream Wax and avoid being misled by container-open numbers that hide expensive deferred initialization.
