# Rax Runtime API Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Define the first Rust-native product runtime API surface for create/open/write/search/close over the staged Wax v2 crates without collapsing back into benchmark-only shapes.

**Architecture:** Build a thin product-facing API that composes `wax-v2-core`, `wax-v2-docstore`, `wax-v2-text`, `wax-v2-vector`, and `wax-v2-search`. Keep benchmark callers working, but do not let benchmark query or artifact shapes become the public runtime contract.

**Tech Stack:** Rust workspace crates, cargo tests, staged Wax v2 engine crates

---

## Chunk 1: Runtime Open/Search Surface

### Task 1: Introduce the first public runtime facade

**Files:**
- Modify: new or existing product-facing crate/module
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing test for the first product-facing open/search lifecycle.
- [x] Step 2: Define the initial runtime types for store open, search request, search response, and close.
- [x] Step 3: Implement the thin facade over current core/docstore/text/vector/search crates without leaking benchmark workload names.
- [x] Step 4: Run the directly affected crate tests.

## Chunk 2: Runtime Write Surface

### Task 2: Expose the first create/write/publish API boundary

**Files:**
- Modify: same runtime facade and supporting crates only as needed

- [x] Step 1: Add a failing test for create-store and first publish behavior through the runtime facade.
- [x] Step 2: Expose create/open/publish primitives through stable runtime types.
- [x] Step 3: Keep the API honest about current compatibility limits rather than implying full product parity.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- The first public facade lives in a dedicated `wax-v2-runtime` crate rather than in `wax-v2-core`, so the core container stays focused on storage primitives while the runtime crate owns orchestration and caller-facing request validation.
- The first search surface is intentionally honest: `Text` search takes `text_query`, while `Vector` and `Hybrid` search require caller-provided `vector_query` values instead of silently coupling the public API to benchmark embedding helpers.
- `wax-v2-runtime` still reads the current compatibility manifest format and composes staged engine crates underneath, so this slice is an API boundary milestone rather than final product parity.
- The first write surface uses `RuntimeStore::create` plus a `RuntimeStoreWriter` session rather than collapsing write orchestration into the main read handle. The only supported write entry point is `import_compatibility_snapshot`, which publishes compatibility `Doc`/`Txt`/`Vec` segments from the current dataset-pack inputs into `store.wax`.
- `RuntimeStore::create` is intentionally non-destructive and rejects overwriting an existing `store.wax`, while repeated compatibility imports still append new immutable generations rather than mutating earlier objects in place.
