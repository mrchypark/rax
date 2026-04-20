# Rax Text Contract Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Define the first text-segment contract inside `wax-v2-text` that is more Wax-owned than the current direct `text_postings`/query-sidecar compatibility scan.

**Architecture:** Keep the current compatibility text reader working, but stop treating raw benchmark file kinds as the only text-lane contract. Introduce an owned text metadata/contract boundary inside `wax-v2-text`, similar to the vector metadata split, while deferring the final persisted binary text-segment design.

**Tech Stack:** Rust workspace crates, `wax-v2-text`, cargo tests

---

## Chunk 1: Add Owned Text Contract Boundary

### Task 1: Make text-lane identity more Wax-owned

**Files:**
- Modify: `crates/wax-v2-text/src/lib.rs`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [ ] Step 1: Add failing tests for the first owned text metadata/contract boundary.
- [x] Step 1: Add failing tests for the first owned text metadata/contract boundary.
- [x] Step 2: Stop relying on raw `text_postings` and query-sidecar scans as the only text-lane identity source.
- [x] Step 3: Keep current compatibility inputs readable while introducing the owned boundary.
- [x] Step 4: Run `cargo test -p wax-v2-text`.

## Chunk 2: Re-verify Callers

### Task 2: Keep migrated benchmark callers green

**Files:**
- Modify: `crates/wax-bench-text-engine/src/lib.rs`

- [x] Step 1: Adjust caller integration only if the new text boundary requires it.
- [x] Step 2: Run `cargo test -p wax-bench-text-engine`.
- [x] Step 3: Run `cargo test --workspace --quiet`.

## Notes

- Completed on 2026-04-19.
- `wax-v2-text` now resolves persisted text metadata through a Wax-owned boundary and exposes batch query/search over the current compatibility `query_set` JSONL shape.
- Hybrid and vector orchestration remain outside `wax-v2-text` by design; the next extraction target is `wax-v2-search`.
