# Rax Vector Backend Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move ANN access in `wax-v2-vector` behind an internal backend trait while preserving the current compatibility facade and benchmark behavior.

**Architecture:** Keep `VectorLane` as the facade that owns shared metadata, runtime mode selection, and public API. Extract backend-specific search behavior behind a crate-private trait so exact-flat, preview-q8 rerank, and HNSW sidecars no longer live as one large monolith inside the facade. Do not expose the trait publicly yet.

**Tech Stack:** Rust workspace crates, `hnsw_rs`, `memmap2`, `wax-v2-vector`, cargo tests

---

## Chunk 1: Introduce Internal Backend Trait

### Task 1: Split search implementations from the facade

**Files:**
- Modify: `crates/wax-v2-vector/src/lib.rs`

- [x] Step 1: Add failing tests that still prove exact-flat and preview/HNSW behavior after the internal split.
- [x] Step 2: Introduce a crate-private backend trait with the minimal search/profile/warmup hooks needed by `VectorLane`.
- [x] Step 3: Keep `VectorLane` as the stable public facade over backend selection.
- [x] Step 4: Run `cargo test -p wax-v2-vector`.

## Chunk 2: Re-verify Bench Callers

### Task 2: Confirm the migrated benchmark engine stays green

**Files:**
- Modify: `crates/wax-bench-text-engine/src/lib.rs`
- Modify: `crates/wax-bench-text-engine/src/query_support.rs`

- [x] Step 1: Keep `wax-bench-text-engine` source unchanged or reduce it further only if the trait split makes that obvious.
- [x] Step 2: Run `cargo test -p wax-bench-text-engine`.
- [x] Step 3: Run `cargo test --workspace --quiet`.

## Completion Notes

- `wax-v2-vector` now routes search, profile, and warmup through crate-private exact, preview, and HNSW backends while keeping `VectorLane` as the public facade.
- The split fixed a real compatibility bug: explicit `Hnsw` profile requests now fall back to exact-flat when the sidecar is unavailable instead of panicking.
- Fresh verification for this slice: `cargo test -p wax-v2-vector`, `cargo test -p wax-bench-text-engine`, and `cargo test --workspace --quiet`.
