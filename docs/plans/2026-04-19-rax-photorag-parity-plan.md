# Rax PhotoRAG Parity Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Start the first PhotoRAG-facing parity slice on top of completed multimodal bootstrap ingest without pretending that `rax` already has full upstream image understanding, OCR, embedding, or user-facing gallery orchestration parity.

**Architecture:** Keep `wax-v2-multimodal` as the owner of durable asset descriptors and payload copies. The next slice should add a thin PhotoRAG-oriented read contract above that asset layer, not a final end-user workflow. Prefer explicit image-asset querying and typed results before any embedding or ranking pipeline is introduced.

**Tech Stack:** Rust workspace crates, cargo tests, staged Wax v2 engine crates

---

## Chunk 1: Photo Asset Read Boundary

### Task 1: Expose image-focused reads over multimodal assets

**Files:**
- Modify: `crates/wax-v2-multimodal/src/lib.rs`
- Modify: root contract tests and roadmap docs

- [x] Step 1: Add a failing test for querying only image assets through a PhotoRAG-oriented API rather than generic asset listing.
- [x] Step 2: Define explicit image-facing query/result types that reuse the current multimodal asset descriptors.
- [x] Step 3: Keep the API explicit that this is an image-asset read boundary, not OCR, embedding, reranking, or retrieval-generation parity.
- [x] Step 4: Run the directly affected crate tests.

## Chunk 2: Photo Metadata Bridge

### Task 2: Preserve room for later image understanding

**Files:**
- Modify: same multimodal crate and docs only as needed

- [x] Step 1: Add a failing test for recording or reading bootstrap image metadata that can later feed richer PhotoRAG processing.
- [x] Step 2: Extend the image-facing surface with explicit optional metadata fields without inventing final embedding or OCR contracts yet.
- [x] Step 3: Keep naming explicit that this is first PhotoRAG parity scaffolding above multimodal ingest, not complete upstream behavior.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- Do not move payload ownership out of `wax-v2-multimodal`.
- Do not promise OCR, CLIP-style embeddings, captioning, or image retrieval quality yet.
- The first success condition is an image-oriented read boundary that later PhotoRAG logic can adopt without reinterpreting generic assets.
- Keep image-specific typed views in `wax-v2-multimodal`; do not push image/video-specific object enums down into `wax-v2-core` while the broader multimodal segment contract is still unsettled.
