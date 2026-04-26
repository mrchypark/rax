# Rax Search Diagnostics Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first lane-contribution diagnostics inside `wax-v2-search` so hybrid execution can report how text and vector lanes influenced the final ranking.

**Architecture:** Extend the new `wax-v2-search` compatibility boundary without freezing the final product API. Keep benchmark outputs stable while introducing a diagnostics shape that can later feed real runtime APIs, CLI surfaces, and regression tooling.

**Tech Stack:** Rust workspace crates, `wax-v2-search`, `wax-bench-text-engine`, cargo tests

---

## Chunk 1: Add Diagnostics Shape

### Task 1: Report lane contributions from the search crate

**Files:**
- Modify: `crates/wax-v2-search/src/lib.rs`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing test for the first lane-contribution diagnostics shape.
- [x] Step 2: Introduce a compatibility-first diagnostics result alongside hybrid fusion/search.
- [x] Step 3: Keep the diagnostics shape implementation-focused and avoid prematurely locking the final product reporting API.
- [x] Step 4: Run `cargo test -p wax-v2-search`.

## Chunk 2: Expose Diagnostics Through The Benchmark Engine

### Task 2: Keep benchmark callers green while threading diagnostics through

**Files:**
- Modify: `crates/wax-bench-text-engine/src/lib.rs`

- [x] Step 1: Thread the new diagnostics output through the benchmark engine only where needed.
- [x] Step 2: Keep current ranked-result outputs and auto-vector behavior stable.
- [x] Step 3: Run `cargo test -p wax-bench-text-engine`.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- Completed on 2026-04-19.
- `wax-v2-search` now emits per-hit lane-contribution diagnostics with text rank, vector rank, and fused RRF score.
- `wax-bench-text-engine` routes through the diagnostics-aware search path internally while keeping existing ranked-result outputs unchanged.
