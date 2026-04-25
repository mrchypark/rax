# Rax Vector Lane Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `wax-v2-vector` as the next real lane crate and move the current ANN and exact-search boundary behind it without changing benchmark runner or CLI call sites.

**Architecture:** Treat this as a compatibility extraction, not the final persisted vector-segment format. `wax-v2-vector` should own the current document-id list, vector payload, preview payload, and HNSW sidecar loading contract while exposing one search boundary that can select exact, HNSW, or preview-backed modes. `wax-bench-text-engine` becomes its caller while preserving the current benchmark harness behavior.

**Tech Stack:** Rust workspace crates, `bytemuck`, `hnsw_rs`, `serde`, `wax-bench-model`, cargo tests

---

## Chunk 1: Add The Crate

### Task 1: Introduce `wax-v2-vector`

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/wax-v2-vector/Cargo.toml`
- Create: `crates/wax-v2-vector/src/lib.rs`

- [x] Step 1: Write failing tests for loading vector-lane compatibility inputs and searching through them.
- [x] Step 2: Implement a minimal `VectorLane` boundary in the new crate.
- [x] Step 3: Run `cargo test -p wax-v2-vector`.

## Chunk 2: Move Benchmark Callers Behind The Boundary

### Task 2: Make `wax-bench-text-engine` call the new crate

**Files:**
- Modify: `crates/wax-bench-text-engine/Cargo.toml`
- Modify: `crates/wax-bench-text-engine/src/lib.rs`
- Modify: `crates/wax-bench-text-engine/src/query_support.rs`

- [x] Step 1: Remove direct ANN and exact-search lane ownership from `wax-bench-text-engine`.
- [x] Step 2: Keep runner and CLI call sites unchanged.
- [x] Step 3: Run `cargo test -p wax-bench-text-engine`.

## Progress Notes

- `wax-v2-vector` now owns the current compatibility loading contract for `document_ids`, `document_vectors`, optional preview vectors, optional HNSW sidecars, and query-vector sidecars.
- The first unit tests cover exact-flat search over the compatibility files plus the current auto-mode preview fallback rule.
- Fresh verification for the crate-introduction chunk: `cargo test -p wax-v2-vector` and `cargo test --workspace --quiet`.

## Completion Notes

- `wax-bench-text-engine` now delegates vector-lane loading, search, profiling, and warmup flows to `wax-v2-vector`.
- The benchmark engine no longer compiles its former local `vector_lane.rs` implementation.
- Fresh verification for the caller-migration chunk: `cargo test -p wax-bench-text-engine` and `cargo test --workspace --quiet`.
