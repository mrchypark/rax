# Wax v2 Benchmark Harness Plan

Status: Draft  
Date: 2026-03-29  
Scope: phased execution plan for delivering the Wax v2 benchmark harness

## 1. Purpose

This document turns the harness requirements into an execution plan.

It answers:

- what should be built first
- which pieces can remain thin in the first iteration
- which validation gates must pass before the next phase begins

This is the implementation-order companion to:

- [2026-03-29-wax-v2-benchmark-harness-spec.md](./2026-03-29-wax-v2-benchmark-harness-spec.md)
- [2026-03-29-wax-v2-benchmark-plan.md](./2026-03-29-wax-v2-benchmark-plan.md)
- [2026-03-29-wax-v2-ttfq-benchmark-plan.md](./2026-03-29-wax-v2-ttfq-benchmark-plan.md)

## 2. Planning Assumptions

This plan assumes:

- Wax v2 remains performance-first
- the first benchmark target is architectural decision support, not polished reporting UX
- iOS physical-device execution is required before the harness is considered complete
- `container_open`, `TTFQ`, and warm-search measurements remain separate throughout the stack

## 3. Delivery Principles

The first implementation should optimize for correctness of measurement boundaries rather than tooling completeness.

This means:

- collect fewer metrics correctly rather than many metrics ambiguously
- prefer host-readable artifacts over live dashboards
- use explicit lifecycle steps instead of hidden helper behavior
- treat fairness labeling as required data, not optional metadata

## 4. Output Of The First Usable Harness

The first usable harness must be able to:

- materialize a named dataset pack
- execute one named benchmark case on macOS
- execute one named benchmark case on iOS
- emit machine-readable artifacts for each sample
- summarize latency and memory slices for `container_open`, `ttfq_text`, `ttfq_vector`, and `ttfq_hybrid`

The first usable harness does not need:

- a web dashboard
- a large historical result store
- automatic benchmark bisect
- distributed execution

## 5. Phased Build Plan

### 5.1 Phase 0: Measurement Contract Freeze

Goal:

- freeze benchmark case identity and artifact schema before code grows around unstable names

Required outputs:

- stable benchmark case schema
- stable artifact directory layout
- stable slice names for open and TTFQ metrics
- stable cold-state labels for each platform
- stable materialization-mode labels for each lane

Exit criteria:

- the team can describe one benchmark sample unambiguously from artifact metadata alone
- `container_open` and `metadata_readiness` have distinct recorded fields
- iOS runs cannot claim one generic `cold` state without saying whether they are `restart_cold`, `pressure_cold`, or `reboot_cold`

### 5.2 Phase 1: Dataset Packer MVP

Goal:

- produce deterministic benchmark input packs from source corpora

Required work:

- define dataset manifest JSON
- support `small`, `medium`, and `large` tiers
- support `clean` and `dirty` variants
- emit query sets and metadata payload alongside document payloads

Exit criteria:

- repeated packer runs produce byte-stable manifests for the same source corpus and config
- pack output contains enough information for the runner to avoid ad-hoc dataset logic

### 5.3 Phase 2: Local Runner MVP

Goal:

- execute benchmark cases on macOS or Linux with deterministic lifecycle control

Required work:

- implement benchmark case loader
- implement run lifecycle orchestration
- implement benchmark case labels and fairness labels
- emit per-sample artifact bundles
- include minimal timing hooks required to prove lifecycle boundaries are being honored
- support explicit lane materialization policies for audit runs

Initial supported workloads:

- `container_open`
- `ttfq_text`
- `ttfq_vector`
- `ttfq_hybrid`
- `warm_text`
- `warm_vector`
- `warm_hybrid`

Exit criteria:

- one command can run one named case end to end
- rerunning the same warm case produces bounded variance on a stable host
- artifact bundles are sufficient to debug an outlier sample without rerunning immediately
- the harness can run an audit variant that forces lane materialization before the first query to expose lazy-init skew

### 5.4 Phase 3: Metric Collector MVP

Goal:

- capture the minimum trustworthy latency and memory slices

Required work:

- wall-clock slice timing
- memory snapshots before open, after open, and after first query
- fairness-state recording
- environment capture
- thermal-state capture
- compiler optimization capture

Required first metrics:

- `container_open_ms`
- `metadata_readiness_ms`
- `lane_open_or_materialize_ms`
- `search_core_ms`
- `fusion_ms`
- `rerank_ms`
- `preview_ms`
- `total_ttfq_ms`
- `binary_size_bytes` when available
- `text_segment_size_bytes` when available

Exit criteria:

- the harness can explain whether a regression belongs to open, lane materialization, or search core
- a missing metric is explicit rather than silently omitted
- thermal state and compiler optimization level are visible in each sample artifact

### 5.5 Phase 4: Host-Driven iOS Path

Goal:

- support true or clearly labeled pseudo-cold execution on physical iOS devices

Required work:

- define host-side launcher flow
- define guest-to-host metric transport
- capture device identity and power state
- separate process-cold from filesystem-cache-cold labels when possible
- define iOS cold-state labels and enforcement rules
- add thermal cooldown enforcement between sensitive runs

Preferred execution mechanisms:

- `XCTest` host-driven launch metrics
- `xcrun devicectl`
- equivalent reproducible host-driven tooling

Exit criteria:

- the harness can produce a physical-device artifact set for at least one TTFQ case
- runs that are not true cold start are labeled accordingly
- the harness never emits a generic iOS `cold` label without a more specific cold-state qualifier

### 5.6 Phase 5: Reducer And Comparison Layer

Goal:

- turn raw sample bundles into comparable benchmark reports

Required work:

- summary table generation
- p50, p95, and p99 reduction
- baseline-to-candidate comparison
- regression flag generation

The reducer can remain thin in the first cut.

It only needs to:

- read artifact bundles
- aggregate sample groups
- emit markdown and machine-readable summaries

Exit criteria:

- one baseline and one candidate run can be compared without manual spreadsheet work
- the reducer makes fairness mismatches obvious

### 5.7 Phase 6: Reliability Hardening

Goal:

- make the harness safe enough for repeated developer and CI use

Required work:

- retry policy for flaky device communication
- artifact corruption detection
- thermal cooldown controls
- benchmark timeout and cancellation handling
- seed and config recording for full replay

Exit criteria:

- repeated full-suite runs do not require manual log forensics for normal failures
- partial failure still leaves usable artifacts

## 6. Recommended Build Order Inside Each Phase

Within each phase, implement components in this order:

1. schema and artifact contract
2. deterministic local execution with minimal boundary instrumentation
3. full metric capture
4. iOS host integration
5. reduction and comparison
6. hardening

This order keeps the harness debuggable.

If host-driven iOS is started before the local path is stable, measurement bugs become harder to isolate.

## 7. Validation Gates

Each phase should stop at a review gate before more scope is added.

### 7.1 Gate A: Dataset Reproducibility

Pass conditions:

- manifest identity is stable
- query-set ids are stable
- dirty variant ratios are reproducible from config

### 7.2 Gate B: Local Measurement Boundary Integrity

Pass conditions:

- `container_open` excludes first-lane work
- `ttfq_*` includes first-lane work
- preview and embedding variants are reported separately
- audit runs that force lane materialization can be compared against default runs to detect lazy-init skew

### 7.3 Gate C: Artifact Sufficiency

Pass conditions:

- every failed or suspicious sample can be inspected offline
- environment drift is visible in the artifact metadata

### 7.4 Gate D: iOS Label Integrity

Pass conditions:

- true cold and pseudo-cold are never conflated
- `phys_footprint` is captured or explicitly marked unavailable
- thermal state is captured before sensitive runs
- cold-state labels distinguish `restart_cold`, `pressure_cold`, and `reboot_cold` when applicable

### 7.5 Gate E: Decision Readiness

Pass conditions:

- the harness can compare one candidate build against one baseline build
- the output is sufficient to decide keep, tune, or replace for a subsystem

## 8. Suggested Ownership Boundaries

The implementation should be split by responsibility rather than platform alone.

Suggested ownership units:

- dataset packer
- benchmark runner
- metric collection and instrumentation
- iOS host integration
- reducer and report generation

This reduces coupling between measurement semantics and device-control code.

## 9. Key Risks

### 9.1 Hidden Lifecycle Coupling

Risk:

- helpers silently perform open, warmup, or cache preparation inside measurement windows

Mitigation:

- keep lifecycle phases explicit in artifacts and logs

### 9.2 Artifact Under-Specification

Risk:

- results are numerically correct but not diagnosable later

Mitigation:

- require raw per-sample bundles before optimizing summary output

### 9.3 iOS False-Cold Runs

Risk:

- app-internal restart loops get mislabeled as true cold start

Mitigation:

- only allow true-cold labels from host-driven execution paths

### 9.5 Thermal Contamination

Risk:

- repeated iOS runs look like valid samples but are actually throttled or heat-skewed

Mitigation:

- treat thermal state as required sample metadata and enforce cooldown before sensitive phases

### 9.6 Lazy Materialization Skew

Risk:

- `container_open` appears cheap only because work was silently deferred to the first query

Mitigation:

- keep materialization policy explicit and require audit runs that force lane materialization ahead of first query

### 9.4 Dataset Drift

Risk:

- benchmark results shift because corpus shape changed, not because the engine changed

Mitigation:

- version and checksum dataset packs as first-class artifacts

## 10. Deferred Work

The following are intentionally deferred until the harness is already useful:

- dashboard visualization
- large historical benchmark storage
- automatic PR commenting
- distributed benchmark scheduling
- benchmark auto-bisect

## 11. Immediate Next Plan After This Document

After this plan, the next concrete design dependency is:

- [2026-03-29-wax-v2-dataset-spec.md](./2026-03-29-wax-v2-dataset-spec.md)

The dataset spec should be written before implementation begins, because the runner contract depends on stable dataset identity and workload packaging rules.
