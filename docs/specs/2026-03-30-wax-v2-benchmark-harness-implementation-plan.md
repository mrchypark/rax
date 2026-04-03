# Wax v2 Benchmark Harness Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first usable Wax v2 benchmark harness that can pack datasets, run named benchmark cases, collect trustworthy latency and memory slices, and emit artifacts on macOS first and iOS next.

**Architecture:** The harness is split into small Rust crates around stable contracts: dataset packer, benchmark case model, runner lifecycle, metric collection, artifact writing, and report reduction. iOS host-driven execution is kept as a separate adapter layer so measurement semantics stay owned by the core harness instead of being buried inside platform glue.

**Tech Stack:** Rust workspace, `serde`, `serde_json`, `clap`, `tokio` only if async is required, optional Swift/XCTest host shim for physical iOS launch control, JSON artifact output, markdown reducer output.

---

## 1. Current Repo Context

The repository currently contains only design documents under `docs/specs`.

There is no Rust workspace yet.

This plan therefore starts with workspace scaffolding and contract tests before any benchmark engine logic.

Related specs:

- [2026-03-29-wax-v2-benchmark-harness-spec.md](./2026-03-29-wax-v2-benchmark-harness-spec.md)
- [2026-03-29-wax-v2-benchmark-harness-plan.md](./2026-03-29-wax-v2-benchmark-harness-plan.md)
- [2026-03-29-wax-v2-dataset-spec.md](./2026-03-29-wax-v2-dataset-spec.md)
- [2026-03-29-wax-v2-ttfq-benchmark-plan.md](./2026-03-29-wax-v2-ttfq-benchmark-plan.md)
- [2026-03-30-wax-v2-dataset-pack-manifest-schema.md](./2026-03-30-wax-v2-dataset-pack-manifest-schema.md)

## 2. Proposed File Structure

Create a Rust workspace with these units:

- `Cargo.toml`
  - workspace root
- `crates/wax-bench-model/src/lib.rs`
  - benchmark case identity, fairness labels, workload enums, manifest types
- `crates/wax-bench-packer/src/lib.rs`
  - dataset pack generation and validation
- `crates/wax-bench-runner/src/lib.rs`
  - lifecycle orchestration for `container_open`, `ttfq_*`, and warm workloads
- `crates/wax-bench-metrics/src/lib.rs`
  - timing slices, memory sampling, thermal/compiler metadata capture
- `crates/wax-bench-artifacts/src/lib.rs`
  - artifact schema and read/write helpers
- `crates/wax-bench-reducer/src/lib.rs`
  - p50/p95/p99 summarization and baseline comparison
- `crates/wax-bench-cli/src/main.rs`
  - CLI entrypoint for pack, run, and reduce commands
- `apps/ios-harness-host/`
  - optional Swift/XCTest or host-driven control adapter for physical iOS execution
- `tests/contracts/`
  - cross-crate contract fixtures and golden manifests
- `fixtures/bench/`
  - tiny synthetic corpora and expected artifacts for tests

## 3. Execution Strategy

Build in three layers:

1. contracts first
2. local macOS execution second
3. iOS host-driven execution third

Do not start the iOS adapter before the local runner can already produce trustworthy artifacts.

## Chunk 1: Workspace And Contract Skeleton

### Task 1: Create Rust workspace shell

**Files:**
- Create: `/Users/cypark/Documents/project/rax/Cargo.toml`
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-model/Cargo.toml`
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-model/src/lib.rs`
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-cli/Cargo.toml`
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-cli/src/main.rs`

- [ ] **Step 1: Write the failing workspace smoke test**

Create:

`/Users/cypark/Documents/project/rax/tests/contracts/workspace_smoke.rs`

```rust
#[test]
fn workspace_smoke_loads_model_crate() {
    let id = wax_bench_model::BenchmarkId::new_for_test("dataset", "workload", 0);
    assert_eq!(id.dataset_id, "dataset");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test --test workspace_smoke
```

Expected:

- fail because the workspace and crate do not exist yet

- [ ] **Step 3: Write minimal workspace and model shell**

Implement:

- workspace members
- minimal `BenchmarkId`
- minimal CLI binary that exits successfully with `--help`

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test workspace_smoke
cargo run -p wax-bench-cli -- --help
```

Expected:

- test passes
- CLI prints usage

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/wax-bench-model crates/wax-bench-cli tests/contracts/workspace_smoke.rs
git commit -m "feat: scaffold benchmark workspace"
```

### Task 2: Define the engine control trait before runner work

**Files:**
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-model/src/lib.rs`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/engine_trait_contract.rs`

- [ ] **Step 1: Write the failing engine trait contract test**

Cover:

- engine can expose `mount()`
- engine can expose `open()`
- engine can expose `search()`
- engine can expose `get_stats()`
- runner-facing trait does not leak backend-specific lifecycle behavior

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test engine_trait_contract
```

- [ ] **Step 3: Implement minimal engine control API**

Include:

- trait definitions
- request and response types
- explicit lifecycle boundary comments around what `mount()` and `open()` are allowed to do

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test engine_trait_contract
```

- [ ] **Step 5: Commit**

```bash
git add crates/wax-bench-model/src/lib.rs tests/contracts/engine_trait_contract.rs
git commit -m "feat: define benchmark engine control trait"
```

### Task 3: Lock benchmark identity and fairness enums

**Files:**
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-model/src/lib.rs`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/benchmark_identity_contract.rs`

- [ ] **Step 1: Write the failing contract tests**

Cover:

- `cache_state`
- `cold_state`
- `materialization_mode`
- `preview_mode`
- `query_embedding_mode`

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test benchmark_identity_contract
```

Expected:

- fail because enums and serde round-trip are incomplete

- [ ] **Step 3: Implement minimal typed model**

Include:

- serde support
- stable string forms
- parse failures for unknown enum variants

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test benchmark_identity_contract
```

- [ ] **Step 5: Commit**

```bash
git add crates/wax-bench-model/src/lib.rs tests/contracts/benchmark_identity_contract.rs
git commit -m "feat: add benchmark identity contracts"
```

## Chunk 2: Dataset Pack Contract

### Task 4: Implement manifest schema types and validation

**Files:**
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-model/src/lib.rs`
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-packer/Cargo.toml`
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-packer/src/lib.rs`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/dataset_manifest_roundtrip.rs`
- Create: `/Users/cypark/Documents/project/rax/fixtures/bench/minimal-dataset-pack/manifest.json`

- [ ] **Step 1: Write the failing manifest round-trip and validation tests**

Cover:

- parse valid manifest
- reject duplicate `query_id`
- reject inconsistent embedding dimensions
- reject missing dirty profile fields

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test dataset_manifest_roundtrip
```

- [ ] **Step 3: Implement schema structs and validator**

Required behavior:

- serde read and write
- semantic validation separate from parse
- stable ordering for rewritten manifests

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test dataset_manifest_roundtrip
```

- [ ] **Step 5: Commit**

```bash
git add crates/wax-bench-model crates/wax-bench-packer tests/contracts/dataset_manifest_roundtrip.rs fixtures/bench/minimal-dataset-pack/manifest.json
git commit -m "feat: add dataset manifest schema validation"
```

### Task 5: Build the dataset packer MVP

**Files:**
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-packer/src/lib.rs`
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-cli/src/main.rs`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/dataset_packer_reproducibility.rs`
- Create: `/Users/cypark/Documents/project/rax/fixtures/bench/source/`

- [ ] **Step 1: Write failing reproducibility tests**

Cover:

- same source corpus and config produce byte-stable manifest
- `clean` and `dirty_light` differ in expected fields
- query set ids remain stable

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test dataset_packer_reproducibility
```

- [ ] **Step 3: Implement minimal packer**

CLI examples:

```bash
cargo run -p wax-bench-cli -- pack \
  --source fixtures/bench/source \
  --out fixtures/bench/out/minimal-pack \
  --tier small \
  --variant clean
```

Implementation focus:

- deterministic manifest generation
- deterministic query-set emission
- checksum generation
- dirty profile metadata emission

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test dataset_packer_reproducibility
```

- [ ] **Step 5: Commit**

```bash
git add crates/wax-bench-packer crates/wax-bench-cli tests/contracts/dataset_packer_reproducibility.rs fixtures/bench/source
git commit -m "feat: add deterministic dataset packer"
```

## Chunk 3: Runner And Artifact Path

### Task 6: Add artifact schema and writer

**Files:**
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-artifacts/Cargo.toml`
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-artifacts/src/lib.rs`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/artifact_bundle_contract.rs`
- Create: `/Users/cypark/Documents/project/rax/fixtures/bench/expected-artifacts/sample-summary.json`

- [ ] **Step 1: Write failing artifact bundle tests**

Cover:

- sample artifact contains benchmark identity
- metric slices and availability markers serialize correctly
- missing metric is explicit

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test artifact_bundle_contract
```

- [ ] **Step 3: Implement artifact bundle types**

Include:

- one per-sample JSON
- one run summary JSON
- one markdown summary template

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test artifact_bundle_contract
```

- [ ] **Step 5: Commit**

```bash
git add crates/wax-bench-artifacts tests/contracts/artifact_bundle_contract.rs fixtures/bench/expected-artifacts/sample-summary.json
git commit -m "feat: add benchmark artifact bundle schema"
```

### Task 7: Implement local runner lifecycle shell

**Files:**
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-runner/Cargo.toml`
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-runner/src/lib.rs`
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-cli/src/main.rs`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/runner_lifecycle_contract.rs`

- [ ] **Step 1: Write failing lifecycle tests**

Cover:

- lifecycle phases appear in order
- `container_open` excludes lane materialization
- audit mode can force lane materialization before first query

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test runner_lifecycle_contract
```

- [ ] **Step 3: Implement local runner shell**

CLI example:

```bash
cargo run -p wax-bench-cli -- run \
  --dataset fixtures/bench/out/minimal-pack \
  --workload ttfq_text \
  --sample-count 3
```

Implementation focus:

- declarative lifecycle
- no hidden warmup
- pluggable engine adapter bound to the previously defined `WaxEngine` trait

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test runner_lifecycle_contract
```

- [ ] **Step 5: Commit**

```bash
git add crates/wax-bench-runner crates/wax-bench-cli tests/contracts/runner_lifecycle_contract.rs
git commit -m "feat: add benchmark runner lifecycle shell"
```

## Chunk 4: Metrics And Local End-To-End Runs

### Task 8: Implement metric collector MVP

**Files:**
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-metrics/Cargo.toml`
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-metrics/src/lib.rs`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/metric_slice_contract.rs`
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-runner/src/lib.rs`

- [ ] **Step 1: Write failing metric tests**

Cover:

- slice timing records `container_open_ms`, `metadata_readiness_ms`, and `total_ttfq_ms`
- explicit unavailable memory field is preserved
- compiler optimization and thermal metadata are optional but typed

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test metric_slice_contract
```

- [ ] **Step 3: Implement metric collector**

Focus:

- monotonic timers
- memory sampler abstraction
- mock clock and mock memory sampler for deterministic tests
- thermal and build metadata capture hooks
- no platform-specific logic in the core trait surface

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test metric_slice_contract
```

- [ ] **Step 5: Commit**

```bash
git add crates/wax-bench-metrics crates/wax-bench-runner tests/contracts/metric_slice_contract.rs
git commit -m "feat: add benchmark metric collector"
```

### Task 9: Wire local end-to-end benchmark command

**Files:**
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-cli/src/main.rs`
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-runner/src/lib.rs`
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-artifacts/src/lib.rs`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/local_e2e_smoke.rs`

- [ ] **Step 1: Write failing end-to-end smoke test**

Cover:

- run one tiny dataset benchmark
- emit sample artifact
- emit summary artifact

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test local_e2e_smoke
```

- [ ] **Step 3: Implement minimal integration path**

Use:

- fixture dataset pack
- stub engine adapter
- deterministic metrics in test mode

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test local_e2e_smoke
cargo run -p wax-bench-cli -- run --dataset fixtures/bench/out/minimal-pack --workload container_open --sample-count 1
```

- [ ] **Step 5: Commit**

```bash
git add crates/wax-bench-cli crates/wax-bench-runner crates/wax-bench-artifacts tests/contracts/local_e2e_smoke.rs
git commit -m "feat: wire local benchmark e2e path"
```

## Chunk 5: Reducer And Comparison

### Task 10: Add reducer MVP

**Files:**
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-reducer/Cargo.toml`
- Create: `/Users/cypark/Documents/project/rax/crates/wax-bench-reducer/src/lib.rs`
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-cli/src/main.rs`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/reducer_contract.rs`

- [ ] **Step 1: Write failing reducer tests**

Cover:

- compute p50, p95, p99 from sample bundle set
- detect fairness mismatch between baseline and candidate
- reject incompatible fairness fingerprints before reduction
- emit markdown summary

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test reducer_contract
```

- [ ] **Step 3: Implement reducer**

CLI example:

```bash
cargo run -p wax-bench-cli -- reduce \
  --input artifacts/run-001 \
  --baseline artifacts/run-000
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test reducer_contract
```

- [ ] **Step 5: Commit**

```bash
git add crates/wax-bench-reducer crates/wax-bench-cli tests/contracts/reducer_contract.rs
git commit -m "feat: add benchmark reducer"
```

## Chunk 6: Physical iOS Path

### Task 11: Add host-driven iOS launch adapter

**Files:**
- Create: `/Users/cypark/Documents/project/rax/apps/ios-harness-host/README.md`
- Create: `/Users/cypark/Documents/project/rax/apps/ios-harness-host/Package.swift`
- Create: `/Users/cypark/Documents/project/rax/apps/ios-harness-host/Sources/HostMain/main.swift`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/ios_cold_label_contract.md`

- [ ] **Step 1: Write the failing contract note and shell integration test**

Cover:

- generic iOS `cold` label is rejected
- host adapter emits one of `restart_cold`, `pressure_cold`, or `reboot_cold`
- system-cold aspiration is never inferred from plain relaunch alone

- [ ] **Step 2: Run the shell verification to show the adapter is missing**

Run:

```bash
test -f apps/ios-harness-host/Sources/HostMain/main.swift
```

Expected:

- fail before implementation

- [ ] **Step 3: Implement host-driven shell**

Focus:

- launch target app or test host
- collect guest artifact payload
- stamp cold-state and thermal metadata
- expose separate hooks for process relaunch and memory-pressure triggering

- [ ] **Step 4: Run the shell verification to show files exist**

Run:

```bash
test -f apps/ios-harness-host/Sources/HostMain/main.swift
```

- [ ] **Step 5: Commit**

```bash
git add apps/ios-harness-host tests/contracts/ios_cold_label_contract.md
git commit -m "feat: add ios benchmark host adapter shell"
```

## Chunk 7: Hardening

### Task 12: Add corruption and replay guarantees

**Files:**
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-artifacts/src/lib.rs`
- Modify: `/Users/cypark/Documents/project/rax/crates/wax-bench-cli/src/main.rs`
- Create: `/Users/cypark/Documents/project/rax/tests/contracts/artifact_replay_contract.rs`

- [ ] **Step 1: Write failing replay and corruption tests**

Cover:

- artifact checksum mismatch is detected
- incomplete run is still readable as partial
- run config can be replayed exactly

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test artifact_replay_contract
```

- [ ] **Step 3: Implement replay and corruption handling**

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --test artifact_replay_contract
```

- [ ] **Step 5: Commit**

```bash
git add crates/wax-bench-artifacts crates/wax-bench-cli tests/contracts/artifact_replay_contract.rs
git commit -m "feat: add artifact replay and corruption checks"
```

## 4. Cross-Cutting Rules

- Keep all benchmark identity and manifest types in `wax-bench-model`.
- Keep semantic validation outside parsing so invalid input can be diagnosed cleanly.
- Do not let the runner infer unlabeled warmup or cold-state behavior.
- Treat missing metrics as explicit values, not omitted keys.
- Keep the engine execution surface behind a small trait so the harness can begin with a stub store and later bind to the real Wax engine.
- Make fairness fingerprint calculation a shared model concern, not reducer-local logic.

## 5. Verification Commands

Run after each major chunk:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Run before claiming the harness MVP is complete:

```bash
cargo run -p wax-bench-cli -- pack --source fixtures/bench/source --out /tmp/wax-pack --tier small --variant clean
cargo run -p wax-bench-cli -- run --dataset /tmp/wax-pack --workload ttfq_text --sample-count 3
cargo run -p wax-bench-cli -- reduce --input artifacts/latest
```

Expected:

- pack succeeds
- run emits sample artifacts and summary
- reduce emits markdown and machine-readable comparison output

## 6. Deferrals

Do not include in the first implementation:

- dashboard UI
- distributed execution
- auto-bisect
- cloud artifact ingestion
- backend-specific optimization logic inside the harness core

## 7. Handoff

This plan assumes the manifest schema is frozen first.

Implementation should start with:

1. Chunk 1
2. Chunk 2
3. Chunk 3

and only move to the iOS host path after local artifact integrity is proven.
