# Rax Vector Metadata Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep persisted vector metadata Wax-owned instead of leaving compatibility details scattered across benchmark manifest file kinds and sidecar naming.

**Architecture:** Preserve the current `wax-v2-vector` compatibility facade, but start moving vector-lane identity and persisted metadata into Wax-owned structures. The benchmark file layout can remain as an input source for now, but metadata that future readers depend on should stop being inferred from ad hoc manifest entries where possible.

**Tech Stack:** Rust workspace crates, `wax-v2-vector`, `wax-v2-core`, cargo tests

---

## Chunk 1: Define Owned Vector Metadata Boundary

### Task 1: Add explicit vector metadata ownership inside `wax-v2-vector`

**Files:**
- Modify: `crates/wax-v2-vector/src/lib.rs`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add failing tests for the first owned vector-metadata boundary.
- [x] Step 2: Stop relying on raw manifest file-kind lookups as the only source of vector-lane identity and capabilities.
- [x] Step 3: Keep current compatibility inputs readable while moving owned metadata into `wax-v2-vector`.
- [x] Step 4: Run `cargo test -p wax-v2-vector`.

## Chunk 2: Re-verify Callers

### Task 2: Keep migrated benchmark callers green

**Files:**
- Modify: `crates/wax-bench-text-engine/src/lib.rs`

- [x] Step 1: Adjust caller integration only if the owned metadata boundary requires it.
- [x] Step 2: Run `cargo test -p wax-bench-text-engine`.
- [x] Step 3: Run `cargo test --workspace --quiet`.

## Completion Notes

- `wax-v2-vector` now resolves a Wax-owned `VectorLaneMetadata` boundary for persisted vector inputs and keeps benchmark `query_vectors` sidecars in a separate compatibility-only `VectorQueryInputs` boundary.
- This means vector-lane identity and capabilities are no longer inferred only from ad hoc manifest scans inside the loader body.
- Fresh verification for this slice: `cargo test -p wax-v2-vector`, `cargo test -p wax-bench-text-engine`, and `cargo test --workspace --quiet`.
