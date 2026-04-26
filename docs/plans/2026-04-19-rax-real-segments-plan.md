# Rax Real Segments Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the benchmark text and vector paths consume real published Wax segments so harness execution stops depending on dataset-pack sidecars for runtime reads.

**Architecture:** Preserve benchmark outputs while staging the migration behind the existing `wax-v2-text`, `wax-v2-vector`, and `wax-v2-docstore` boundaries. Prefer real manifest-visible segments when present, and keep compatibility sidecars only as fallback until the fixture-layout dependency is fully removed.

**Tech Stack:** Rust workspace crates, `wax-v2-core`, `wax-v2-docstore`, `wax-v2-text`, `wax-v2-vector`, `wax-bench-text-engine`, cargo tests

---

## Chunk 1: Text Path Prefers Real Segments

### Task 1: Prove the first manifest-backed text open and query path

**Files:**
- Modify: `crates/wax-v2-text/src/lib.rs`
- Modify: `crates/wax-bench-text-engine/src/lib.rs`
- Modify: `tests/contracts/*` as needed

- [x] Step 1: Add a failing test that proves a benchmark text query can reopen through manifest-visible store data when real text/doc segments are present.
- [x] Step 2: Make `wax-v2-text` and its caller prefer real published segment inputs over compatibility sidecars when available.
- [x] Step 3: Keep current ranked outputs stable for the same benchmark queries during the migration.
- [x] Step 4: Run `cargo test -p wax-v2-text` and `cargo test -p wax-bench-text-engine`.

## Chunk 2: Vector Path Prefers Real Segments

### Task 2: Prove the first manifest-backed vector open and query path

**Files:**
- Modify: `crates/wax-v2-vector/src/lib.rs`
- Modify: `crates/wax-bench-text-engine/src/lib.rs`
- Modify: vector-focused contracts as needed

- [x] Step 1: Add a failing test that proves vector search prefers manifest-visible store data when real vector segments are present.
- [x] Step 2: Make `wax-v2-vector` and its caller prefer real published vector metadata and payloads over compatibility sidecars when available.
- [x] Step 3: Keep exact-flat, preview, and HNSW behavior stable across the new loading boundary.
- [x] Step 4: Run `cargo test -p wax-v2-vector` and the directly affected benchmark contracts.

## Chunk 3: Remove Fixture Layout Dependence

### Task 3: Collapse remaining runtime assumptions about dataset-pack sidecars

**Files:**
- Modify: runtime crates only where the fixture-layout assumption still leaks through
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Identify the remaining runtime reads that still require dataset-pack fixture layout knowledge after text/vector segment fallback exists.
- [x] Step 2: Move those reads behind real engine crate boundaries or compatibility fallbacks, not benchmark callers.
- [x] Step 3: Run `cargo test --workspace --quiet`.

## Notes

- The first text-segment migration intentionally lives in `wax-v2-text`, not `wax-bench-text-engine`, so benchmark callers stay stable while the crate boundary learns to prefer real manifest-visible `Txt` segments.
- `publish_compatibility_text_segment` is a bridge helper for migration and tests; it serializes the current `text_postings` shape into a real segment object, but it is not the final persisted text-segment contract.
- Query-sidecar loading is still compatibility-only and remains outside this chunk's completion criteria.
- The first vector-segment migration intentionally lives in `wax-v2-vector`, not `wax-bench-text-engine`, so benchmark callers stay stable while the crate boundary learns to prefer real manifest-visible `Vec` segments.
- `publish_compatibility_vector_segment` is a bridge helper for migration and tests; it serializes the current vector sidecar payloads into a real segment object, but it is not the final persisted vector-segment contract.
- HNSW graph loading and query-vector loading are still compatibility-only and remain part of the next fixture-layout cleanup chunk.
- `wax-v2-docstore::open` now prefers the latest manifest-visible `Doc` segment in `store.wax` and falls back to dataset-pack documents only when no real doc segment exists, which removes the last direct benchmark-caller dependence on `docs.ndjson`.
- `query_vectors` and HNSW graph files still exist as compatibility inputs, but that dependence now lives behind engine crate boundaries rather than in benchmark callers.
