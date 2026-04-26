# Rax Search Metadata Filtering Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first metadata-filtering path to the real search layer so ranked search can constrain results by document metadata without routing entirely through benchmark-only helpers.

**Architecture:** Extend the compatibility-first `wax-v2-search` boundary to accept filtering inputs that can later map onto real doc segments and runtime APIs. Keep benchmark outputs green while moving filter-aware read-path behavior toward engine crates instead of caller-only logic.

**Tech Stack:** Rust workspace crates, `wax-v2-search`, `wax-v2-docstore`, `wax-bench-text-engine`, cargo tests

---

## Chunk 1: Define Filter Boundary

### Task 1: Add a first filter-aware search contract

**Files:**
- Modify: `crates/wax-v2-search/src/lib.rs`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing test for the first metadata-filtering search shape.
- [x] Step 2: Introduce a compatibility-first filter boundary that can constrain fused hits by metadata-visible document attributes.
- [x] Step 3: Keep the filter shape minimal and avoid freezing the final product query/filter API.
- [x] Step 4: Run `cargo test -p wax-v2-search`.

## Chunk 2: Thread Through Benchmark Caller

### Task 2: Keep benchmark callers green while the real read path learns filtering

**Files:**
- Modify: `crates/wax-bench-text-engine/src/lib.rs`

- [x] Step 1: Use the new filter-aware path only where needed without changing current benchmark contracts.
- [x] Step 2: Keep ranked-result outputs and auto-vector behavior stable.
- [x] Step 3: Run `cargo test -p wax-bench-text-engine`.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- Completed on 2026-04-19.
- `wax-v2-search` now exposes a trait-based top-level exact-match metadata filter boundary.
- `wax-bench-text-engine` preserves benchmark `filter_spec` through `wax-v2-text`, overfetches filtered queries to corpus size, applies metadata filtering through docstore-backed field lookups, and then truncates back to `top_k`.
