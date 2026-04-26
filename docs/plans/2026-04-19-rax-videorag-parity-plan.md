# Rax VideoRAG Parity Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Start the first VideoRAG-facing parity slice on top of completed multimodal bootstrap ingest and PhotoRAG read scaffolding without pretending that `rax` already has transcript extraction, frame sampling, temporal retrieval, or full video understanding parity.

**Architecture:** Keep `wax-v2-multimodal` as the owner of asset descriptors, copied payloads, and typed media views. The next slice should add a thin video-oriented read contract plus minimal bootstrap temporal metadata, while keeping frame extraction, transcript generation, embeddings, and ranking outside this step.

**Tech Stack:** Rust workspace crates, cargo tests, staged Wax v2 engine crates

---

## Chunk 1: Video Asset Read Boundary

### Task 1: Expose video-focused reads over multimodal assets

**Files:**
- Modify: `crates/wax-v2-multimodal/src/lib.rs`
- Modify: root contract tests and roadmap docs

- [x] Step 1: Add a failing test for querying only video assets through a VideoRAG-oriented API rather than generic asset listing.
- [x] Step 2: Define explicit video-facing query/result types that reuse current multimodal asset descriptors.
- [x] Step 3: Keep the API explicit that this is a video-asset read boundary, not transcripts, frame sampling, embeddings, or temporal retrieval parity.
- [x] Step 4: Run the directly affected crate tests.

## Chunk 2: Video Metadata Bridge

### Task 2: Preserve room for later temporal understanding

**Files:**
- Modify: same multimodal crate and docs only as needed

- [x] Step 1: Add a failing test for recording or reading bootstrap video metadata that can later feed richer VideoRAG processing.
- [x] Step 2: Extend the video-facing surface with explicit optional metadata fields without inventing transcript or frame-index contracts yet.
- [x] Step 3: Keep naming explicit that this is first VideoRAG parity scaffolding above multimodal ingest, not complete upstream behavior.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- Do not move video payload ownership out of `wax-v2-multimodal`.
- Do not promise frame extraction, ASR transcripts, CLIP-like embeddings, or temporal retrieval quality yet.
- The first success condition is a video-oriented read boundary that later VideoRAG logic can adopt without reinterpreting generic assets.
- Keep video-specific typed views in `wax-v2-multimodal`; do not add video-specific object enums or FFmpeg-style extraction dependencies to `wax-v2-core` or the ingest path while the cross-platform multimodal processing contract is still unsettled.
