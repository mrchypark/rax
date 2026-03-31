# Wax v2 Benchmark Plan

Status: Draft  
Date: 2026-03-29  
Scope: benchmark gates for validating the Wax v2 architecture on iOS and other platforms

## 1. Purpose

This document defines how Wax v2 will be evaluated.

Its job is not just to collect numbers. Its job is to answer architectural questions:

- is the current v2 design good enough to keep?
- which subsystem is the real bottleneck?
- when should a backend be replaced rather than tuned?

The benchmark plan is therefore a decision tool, not just a performance report template.

## 2. Decision Role

Benchmarks must decide whether to:

- continue with the current architecture
- optimize within the current architecture
- rewrite a subsystem
- introduce an optional specialized backend later

This benchmark plan is tied directly to the v2 architecture goals:

- cold open
- search p95

## 3. Primary Success Criteria

The first release candidate for Wax v2 is judged primarily on:

1. cold open latency
2. first-query latency after open
3. warm search p95
4. resident memory behavior on iOS

Secondary metrics:

- ingest throughput
- commit latency
- compaction overhead

These matter, but they are not the first go/no-go gates.

## 4. Benchmark Questions

Every benchmark run should help answer one of these questions.

### 4.1 Open Path

- can the store open cheaply without rebuild?
- does cold open scale with segment count or corpus size?
- is first query close to open cost, or is there hidden deferred initialization?

### 4.2 Search Path

- is text-only search stable at p95?
- is vector-only search stable at p95?
- is hybrid search p95 acceptable once both lanes are active?
- does metadata filtering introduce a latency cliff?

### 4.3 Memory Behavior

- does open create excessive resident memory?
- does first query cause an mmap/page-fault spike?
- does repeated querying steadily inflate memory?

### 4.4 Write Path

- are commits cheap enough that they do not force a future redesign?
- does compaction hurt read latency too much?

## 5. Minimum Device Matrix

The first benchmark matrix must include real iOS devices.

### 5.1 Required iOS Classes

At minimum:

- older-but-supported iPhone class
  - example target class: A15 generation
- current mainstream iPhone class
  - example target class: A17 or equivalent
- iPad or high-memory iOS-class device
  - to separate CPU limits from memory-pressure limits

If only one device class is tested, the results are not sufficient to drive architecture decisions.

### 5.2 Required Non-iOS Classes

At minimum:

- Apple Silicon macOS
- one Linux desktop/server-class machine

Reason:

- cross-platform drift is easier to catch early
- some regressions show up as portability problems before they show up as absolute slowdowns

## 6. Dataset Matrix

The benchmark matrix needs multiple corpus sizes.

### 6.1 Small Dataset

Purpose:

- detect fixed overheads
- test cold open and first query on tiny stores

Suggested shape:

- 1,000 to 5,000 docs
- short and medium text mixture

### 6.2 Medium Dataset

Purpose:

- likely first realistic app-sized store
- should surface index startup and p95 trends

Suggested shape:

- 50,000 to 100,000 docs
- mixed lengths
- realistic metadata

### 6.3 Large Dataset

Purpose:

- stress search p95 and mmap behavior
- expose open-time scaling mistakes

Suggested shape:

- 500,000+ docs
- mixed lengths
- larger vector corpus

### 6.4 Dataset Composition Rules

Every benchmark dataset should track:

- doc count
- total UTF-8 bytes
- average doc length
- median doc length
- metadata cardinality
- vector count
- embedding dimension
- tombstone ratio if deletes exist

Without these, benchmark comparisons become hard to interpret.

## 7. Workload Matrix

### 7.1 Open Workloads

Required:

- cold open from app/process start
- reopen after recent close
- first query after cold open

Measure separately:

- open only
- open + first text query
- open + first vector query
- open + first hybrid query

### 7.2 Search Workloads

Required query classes:

- exact term / keyword query
- multi-term topical query
- vector semantic query
- hybrid query
- metadata-filtered query
- no-hit query
- broad/high-recall query

These query classes must exist in every dataset size tier.

### 7.3 Query Distribution

Each query set should include:

- easy queries
- medium queries
- hard queries

The p95 should not be computed from only one query shape.

### 7.4 Repetition Model

For each query class:

- single-run first query
- repeated warm queries
- mixed interleaving of text and vector queries

This catches hidden caches and warmup artifacts.

## 8. Measurement Protocol

### 8.1 Latency Metrics

For every measured operation, record:

- p50
- p95
- p99
- min
- max

Do not ship decisions based on average latency only.

### 8.2 Memory Metrics

Record:

- resident memory after open
- resident memory after first query
- resident memory after steady-state warm workload
- peak resident memory during compaction

### 8.3 Storage Metrics

Record:

- total file size
- segment count by family
- manifest size
- average object size
- compaction delta

These numbers help explain open-path regressions.

### 8.4 Environment Recording

Every run must record:

- device model
- OS version
- app/build mode
- Rust toolchain version
- benchmark git commit
- dataset version
- whether the run was charging or on battery when relevant

## 9. Core Benchmark Gates

These are the first architecture gates.

### 9.1 Gate A: Cold Open

Pass if:

- cold open remains stable across repeated runs
- cold open does not scale catastrophically with corpus growth
- first query after open does not reveal a hidden rebuild stage

Fail if:

- open cost scales like full index load or rebuild
- first query cost is dramatically larger than warm query because of missing open-time guarantees

### 9.2 Gate B: Search p95

Pass if:

- text, vector, and hybrid search p95 stay within the targeted budget envelope on real iOS devices
- no one lane dominates p95 so badly that it forces architecture changes

Fail if:

- one lane repeatedly drives p95 outside acceptable bounds
- hybrid p95 is unstable because fusion or preview fetch dominates

### 9.3 Gate C: Memory Behavior

Pass if:

- resident memory remains bounded after open and repeated queries
- the vector path does not require full heap hydration
- repeated manifest generations do not steadily inflate memory

Fail if:

- memory use trends upward over repeated query cycles
- first query triggers a large mmap/page-fault spike that makes UX unacceptable

## 10. Rewrite Triggers

Benchmarks should trigger architectural change only under clear conditions.

### 10.1 Text Lane Rewrite Trigger

Consider replacing or redesigning the text backend if:

- text-only search is the dominant p95 contributor
- text open path introduces large startup overhead
- the backend forces container compromises or segment bloat

### 10.2 Vector Lane Rewrite Trigger

Consider replacing or redesigning the vector backend if:

- cold open requires heavy vector hydration
- vector-only or hybrid p95 remains the dominant bottleneck after tuning
- memory pressure on iOS is primarily due to graph layout

### 10.3 Format Rewrite Trigger

Format changes should be rare.

Consider rewriting the v2 format only if:

- manifest loading itself becomes the bottleneck
- publication protocol causes correctness or recovery complexity
- segment-family independence turns out to be insufficient

Do not rewrite the format just because one backend is slow.

## 11. Benchmark Phases

### Phase 1. Architecture Validation

Purpose:

- prove the format and read path are viable

Must include:

- small/medium datasets
- cold open
- first query
- warm p95
- resident memory

### Phase 2. Scale Validation

Purpose:

- find scaling cliffs

Must include:

- large dataset
- mixed workloads
- segment-count sensitivity
- compaction interaction

### Phase 3. Replacement Decision

Purpose:

- decide whether to keep the current backend choices

Must include:

- per-lane breakdown
- bottleneck attribution
- decision memo: tune vs replace

## 12. Common Benchmark Mistakes

These mistakes will produce misleading results, especially on iOS.

### 12.1 Measuring Only Warm Runs

This hides the exact problem v2 is trying to solve.

### 12.2 Using Only One Device

This confuses one hardware profile with a general architecture result.

### 12.3 Ignoring First Query

A fast open followed by a terrible first query still fails the product goal.

### 12.4 Benchmarking on Debug Builds

This distorts both CPU and memory behavior.

### 12.5 Benchmarking With Unrealistic Datasets

A corpus with uniform short documents will hide real query and paging behavior.

### 12.6 Ignoring Thermal and Memory Pressure

iOS can look fast in short runs and then degrade or get killed under sustained load.

### 12.7 Mixing Multiple Changes at Once

If the format, backend, and query planner all change at the same time, the data stops being diagnostic.

## 13. Required Benchmark Outputs

Every benchmark cycle should produce:

- summary table by device and dataset
- latency percentiles by workload class
- resident-memory table
- file/segment size table
- bottleneck notes
- recommendation:
  - keep architecture
  - tune subsystem
  - replace subsystem

## 14. Initial Pass/Fail Framing

At this stage, exact numeric budgets are intentionally not frozen in this document.

Reason:

- the current architecture needs first implementation data
- the first useful milestone is comparability and bottleneck attribution

What is frozen now:

- cold open and search p95 are top-level gates
- iOS real-device testing is mandatory
- memory behavior is a co-equal gate, not a side metric
- rewrite decisions must be tied to repeated benchmark evidence, not intuition

## 15. Recommended Next Docs

- `wax-v2-benchmark-harness.md`
- `wax-v2-dataset-spec.md`
- `wax-v2-device-matrix.md`
- `wax-v2-pass-fail-thresholds.md`
