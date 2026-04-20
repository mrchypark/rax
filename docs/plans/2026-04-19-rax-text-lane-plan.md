# Rax Text Lane Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first real lane crate, `wax-v2-text`, and move the current lexical text-lane boundary behind it without changing benchmark runner or CLI call sites.

**Architecture:** Treat this as a compatibility extraction, not the final persisted text-segment format. `wax-v2-text` owns loading the current postings/query-sidecar shape and exposes a small open/search boundary. `wax-bench-text-engine` becomes its caller while preserving the existing benchmark harness behavior.

**Tech Stack:** Rust workspace crates, `serde`, `wax-bench-model`, cargo tests

---

## Chunk 1: Add The Crate

### Task 1: Introduce `wax-v2-text`

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/wax-v2-text/Cargo.toml`
- Create: `crates/wax-v2-text/src/lib.rs`

- [x] Step 1: Write failing tests for loading a text lane and searching through it.
- [x] Step 2: Implement a minimal `TextLane` boundary in the new crate.
- [x] Step 3: Run `cargo test -p wax-v2-text`.

## Chunk 2: Move Benchmark Callers Behind The Boundary

### Task 2: Make `wax-bench-text-engine` call the new crate

**Files:**
- Modify: `crates/wax-bench-text-engine/Cargo.toml`
- Modify: `crates/wax-bench-text-engine/src/lib.rs`
- Modify: `crates/wax-bench-text-engine/src/query_support.rs`

- [x] Step 1: Remove direct lexical text-lane ownership from `wax-bench-text-engine`.
- [x] Step 2: Keep runner and CLI call sites unchanged.
- [x] Step 3: Run `cargo test -p wax-bench-text-engine`.

## Completion Notes

- `wax-v2-text` now owns loading the current `text_postings` and query-sidecar compatibility shape.
- `wax-bench-text-engine` now delegates lexical text-lane materialization to `wax-v2-text` and no longer keeps duplicate text-lane helpers.
- Fresh verification for this slice: `cargo test -p wax-v2-text`, `cargo test -p wax-bench-text-engine`, and `cargo test --quiet`.
