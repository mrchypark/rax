# Rax Search Crate Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first `wax-v2-search` crate so hybrid fusion and lane-level search orchestration stop living only inside `wax-bench-text-engine`.

**Architecture:** Keep the benchmark harness green, but extract the shared search-layer behavior into `wax-v2-search`. The first slice should own hybrid fusion over existing `wax-v2-text` and `wax-v2-vector` boundaries without forcing the final public runtime API.

**Tech Stack:** Rust workspace crates, `wax-v2-search`, `wax-bench-text-engine`, cargo tests

---

## Chunk 1: Introduce Search Crate

### Task 1: Move hybrid orchestration behind a real crate boundary

**Files:**
- Modify: `Cargo.toml`
- Add: `crates/wax-v2-search/Cargo.toml`
- Add: `crates/wax-v2-search/src/lib.rs`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing test for the first shared hybrid-search boundary.
- [x] Step 2: Introduce `wax-v2-search` with minimal fusion/orchestration APIs over `wax-v2-text` and `wax-v2-vector`.
- [x] Step 3: Keep the interface compatibility-first and avoid freezing the final product runtime API.
- [x] Step 4: Run `cargo test -p wax-v2-search`.

## Chunk 2: Migrate Benchmark Caller

### Task 2: Remove search-layer duplication from the benchmark engine

**Files:**
- Modify: `crates/wax-bench-text-engine/src/lib.rs`
- Modify: `crates/wax-bench-text-engine/src/query_support.rs`

- [x] Step 1: Rewire benchmark hybrid/text-vector orchestration through `wax-v2-search`.
- [x] Step 2: Keep existing benchmark outputs and auto-vector behavior stable.
- [x] Step 3: Run `cargo test -p wax-bench-text-engine`.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- Completed on 2026-04-19.
- `wax-v2-search` now owns reciprocal-rank fusion and hybrid execution while leaving benchmark query parsing and auto-mode choreography outside the crate.
- The next unchecked search slice is lane-contribution diagnostics rather than more fusion extraction.
