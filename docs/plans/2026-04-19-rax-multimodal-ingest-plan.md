# Rax Multimodal Ingest Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first explicit multimodal ingest boundary without pretending that `rax` already has PhotoRAG, VideoRAG, retrieval orchestration, or final binary media-segment parity.

**Architecture:** Add a dedicated `wax-v2-multimodal` crate that owns stable media asset descriptors plus compatibility ingest of external image or video files into the store root. Keep retrieval, embedding pipelines, frame extraction, and user-facing PhotoRAG or VideoRAG orchestration out of this first slice.

**Tech Stack:** Rust workspace crates, cargo tests, staged Wax v2 engine crates

---

## Chunk 1: Asset Descriptor Boundary

### Task 1: Make multimodal asset identity explicit

**Files:**
- Modify: new multimodal crate and root contract tests
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing test for opening a multimodal ingest session and recording a stable asset descriptor for at least one image-like file.
- [x] Step 2: Define explicit asset-facing types and operations above raw file-copy logic.
- [x] Step 3: Keep the first boundary explicit that asset identity and provenance are stable, while embeddings, OCR, transcripts, and retrieval semantics are still future work.
- [x] Step 4: Run the directly affected crate tests.

## Chunk 2: Compatibility Media Import

### Task 2: Persist imported assets durably

**Files:**
- Modify: same multimodal crate and docs only as needed

- [x] Step 1: Add a failing test for reopening the store and listing or reading imported multimodal asset metadata.
- [x] Step 2: Persist imported-asset descriptors through a store-owned bootstrap file or manifest-visible compatibility payload without leaking benchmark fixture assumptions.
- [x] Step 3: Keep naming explicit that this is a first multimodal ingest layer, not PhotoRAG, VideoRAG, or final media-segment parity.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- The first slice should prefer a dedicated `wax-v2-multimodal` crate rather than hiding media ingest in runtime, broker, or MCP layers.
- Do not freeze the final binary media-segment format in this slice.
- Do not promise embedding extraction, frame sampling, OCR, speech-to-text, or retrieval orchestration yet.
- The first success condition is durable asset identity plus reopen-safe ingest, not complete multimodal search parity.
- The implemented bootstrap multimodal layer persists asset descriptors in `multimodal-assets.ndjson` and copies imported asset bytes into a `multimodal-assets/` store-owned directory. This keeps asset identity and payload ownership explicit without claiming final manifest-visible media segments yet.
