# Rax To Wax Master Todo

## Rules

- Keep this file updated when scope or order changes.
- Record corrections explicitly instead of silently rewriting history.
- If a task turns out to be wrong or premature, strike it through and add a short reason.

## Current Position

- [x] Gap analysis completed against upstream Wax
- [x] Roadmap design written
- [x] Core-foundation implementation plan written
- [x] Yeoul decisions recorded
- [x] First core-engine slice started

## Phase 1: Durable Core

### Container

- [x] Add `crates/wax-v2-core`
- [x] Define binary superblock format
- [x] Define active manifest format
- [x] Define typed segment metadata records
- [x] Add create/open/validate lifecycle
- [x] Add corruption and checksum tests
- [x] Expand manifest segment descriptors to the full v2 binary-spec contract before wiring real doc/text/vector segments

### Docstore

- [x] Add `crates/wax-v2-docstore`
- [x] Add a compatibility read facade over dataset-pack `docs.ndjson` and optional `document_offsets`
- [x] Define stable Wax-owned doc ids
- [x] Define doc segment layout
- [x] Add preview and metadata fetch path
- [x] Add tombstone representation

### Text Lane

- [x] Add `crates/wax-v2-text`
- [x] Define text segment contract independent of benchmark fixture files
- [x] Add text adapter over the current lexical logic
- [x] Add batch-search boundary

### Vector Lane

- [x] Add `crates/wax-v2-vector`
- [x] Move ANN access behind a backend trait
- [x] Keep persisted vector metadata Wax-owned
- [x] Support exact and HNSW paths behind the same interface

### Search

- [x] Add `crates/wax-v2-search`
- [x] Move hybrid fusion into a real engine crate
- [x] Add diagnostics for lane contributions
- [x] Add metadata filtering in the real read path

### Write Path

- [x] Build segment publication flow
- [x] Append segment bytes to immutable region
- [x] Publish new manifest generation
- [x] Switch active superblock atomically
- [x] Add snapshot isolation tests
- [x] Add compaction design follow-up

## Harness Migration

- [x] Keep `wax-bench-*` crates green during migration
- [x] Make benchmark runner target the real core open path
- [x] Make benchmark text/vector paths consume real segments
- [x] Remove direct dependence on benchmark fixture layout from runtime code

## Product Surface

- [x] Define Rust runtime API surface for create/open/write/search/close
- [x] Add product CLI separate from `wax-bench-cli`
- [x] Add broker/session model
- [x] Add MCP-compatible surface

## Deferred Full Parity

- [x] Structured memory
- [x] Entity/fact APIs
- [x] Multimodal ingest
- [x] PhotoRAG parity
- [x] VideoRAG parity
- [x] Apple-specific acceleration parity

## Corrections And Wrong Turns

- [x] Corrected the shorthand framing from "rax is just a harness" to "rax is a harness-first prototype for a real rewrite."
- [x] Recorded that the initial `wax-v2-core` manifest descriptor is a bootstrap format and must be expanded before real segment publication.
- [x] Recorded that the first binary doc-segment codec keeps `payload_offset` authoritative, but preserves `metadata_ref` and `preview_ref` as row metadata while metadata and preview bodies are currently length-prefixed by record order. The next publication slice must either make those refs authoritative or revise the spec wording.
- [x] Recorded that the first stable Wax-owned numeric doc-id map uses 0-based `u64` ids in `docs.ndjson` file order, with the dataset-pack string `doc_id` retained only as a compatibility key.
- [x] Recorded that the first manifest-visible publication path appends raw segment and manifest bytes and flips the alternate superblock, but still lacks final `WXOB` object envelopes and page-aligned append boundaries.
- [x] Recorded that `metadata_ref` and `preview_ref` are now authoritative byte refs inside the doc segment, and `preview_ref.length == 0` currently decodes to `None`.
- [x] Recorded that the publication path now wraps manifest and segment payloads in `WXOB` envelopes and aligns object starts to 4096-byte boundaries, while journal-backed recovery is still pending.
- [x] Recorded that snapshot stability and latest-valid-generation fallback are now regression-tested as part of the alternate-superblock publication contract.
- [x] Recorded that `wax-v2-text` now owns the current lexical compatibility boundary, while persisted text-segment design and batch-search API work remain open.
- [x] Recorded that `wax-v2-vector` now exists as the vector-lane compatibility boundary, while benchmark callers still need to migrate off the local `VectorLane`.
- [x] Recorded that `wax-bench-text-engine` now delegates vector-lane loading and search to `wax-v2-vector`, while the next remaining vector task is an internal backend trait split.
- [x] Recorded that `wax-v2-vector` now routes exact, preview, and HNSW behavior through crate-private backends, and explicit HNSW profiling falls back to exact-flat when the sidecar is unavailable.
- [x] Recorded that `wax-v2-vector` now resolves persisted vector metadata through a Wax-owned metadata boundary, while benchmark `query_vectors` remain a separate compatibility input.
- [x] Recorded that `wax-v2-text` now resolves persisted text metadata through a Wax-owned boundary and owns batch query/search over the compatibility `query_set` format, while hybrid/vector orchestration remains above the text crate.
- [x] Recorded the correction that the new `query_set` batch loader in `wax-v2-text` is compatibility-only and must not become the final product query contract.
- [x] Recorded that `wax-v2-search` now owns reciprocal-rank fusion and hybrid execution over the current text/vector lane boundaries, while benchmark query parsing and auto-mode orchestration remain caller-side compatibility logic.
- [x] Recorded the correction that the first `wax-v2-search` surface is compatibility-first and must not be mistaken for the final product runtime API.
- [x] Recorded that `wax-v2-search` now emits per-hit lane-contribution diagnostics with text rank, vector rank, and fused RRF score, while `wax-bench-text-engine` keeps ranked outputs stable by using that diagnostics-aware path internally.
- [x] Recorded the correction that the first diagnostics shape is implementation-focused internal reporting and not yet the final user-facing diagnostics API.
- [x] Recorded that `wax-v2-search` now owns a trait-based top-level exact-match metadata filter boundary, while benchmark `filter_spec` stays a compatibility query input carried through `wax-v2-text`.
- [x] Recorded the correction that filtered queries currently overfetch to corpus size before metadata filtering and truncation; this is acceptable for the compatibility slice but not the final filter-aware execution plan.
- [x] Recorded that the first compaction follow-up is now documented as another manifest-generation publish with logical active-set cleanup first and physical byte reclamation deferred.
- [x] Recorded the correction that early compaction in `rax` should not promise immediate file shrinking; old compacted bytes may remain until a later rewrite or vacuum tool exists.
- [x] Recorded that the first runner/core-open migration keeps `wax-bench-runner` generic and moves the real `wax-v2-core` validation boundary into `PackedTextEngine.open`, so invalid `store.wax` files now fail benchmark open before search begins.
- [x] Recorded that `wax-v2-text` now publishes a compatibility `Txt` segment and prefers manifest-visible text segments over `text_postings` sidecars when available, while query definitions still remain compatibility-sidecar input.
- [x] Recorded that `wax-v2-vector` now publishes a compatibility `Vec` segment and prefers manifest-visible vector payloads over `document_ids`, `document_vectors`, and preview sidecars when available, while `query_vectors` and HNSW graph files still remain compatibility-sidecar input.
- [x] Recorded that `wax-v2-docstore::open` now prefers manifest-visible doc segments over `docs.ndjson` sidecars, so benchmark preview and metadata hydration no longer require direct caller knowledge of dataset-pack document files.
- [x] Recorded that the first public `wax-v2-runtime` facade now owns open/search/close over the staged engine crates, and vector/hybrid search intentionally require caller-provided `vector_query` input rather than hidden benchmark embedding.
- [x] Recorded that the completed runtime API surface now uses `RuntimeStore::create` plus a `RuntimeStoreWriter` compatibility-import session for write/publish, and `create` explicitly refuses to overwrite an existing `store.wax`.
- [x] Recorded that the first product CLI now lives in the `wax-cli` package and ships a `wax` binary with `create`, `import-compat`, and text `search`, while benchmark pack/run/reduce flows remain isolated in `wax-bench-cli`.
- [x] Recorded that the first broker/session surface now lives in `wax-v2-broker`, keeps opaque broker-owned session ids above `wax-v2-runtime`, and currently exposes text search plus compatibility import without freezing future transport or vector-input policy.
- [x] Recorded that the first MCP-compatible surface now lives in `wax-v2-mcp`, keeps transport-ready request/response enums above `wax-v2-broker`, and intentionally stops short of a real daemon or network server.
- [x] Recorded that the first structured-memory surface now lives in `wax-v2-structured-memory`, uses an explicit bootstrap `structured-memory.ndjson` persistence layer, and currently exposes bootstrap record write/read/query with explicit status and provenance rather than full entity/fact parity.
- [x] Recorded that the first explicit entity/fact API layer now also lives in `wax-v2-structured-memory`, keeps persistence in the same bootstrap `structured-memory.ndjson` file through reserved entity metadata predicates, and still does not claim alias normalization, graph traversal, evidence, or bitemporal parity.
- [x] Recorded that the first multimodal ingest surface now lives in `wax-v2-multimodal`, persists asset descriptors in `multimodal-assets.ndjson`, copies imported payloads into a store-owned `multimodal-assets/` directory, and still does not claim final media segments, OCR, transcript, embedding, or retrieval orchestration parity.
- [x] Recorded that the first PhotoRAG parity surface now lives in `wax-v2-multimodal`, exposes image-only typed read/query APIs plus optional bootstrap image metadata, and keeps image-specific typed views out of `wax-v2-core` while OCR, captioning, embeddings, and image retrieval quality remain future work.
- [x] Recorded that the first VideoRAG parity surface now lives in `wax-v2-multimodal`, exposes video-only typed read/query APIs plus optional bootstrap video metadata, and keeps video-specific typed views and processing dependencies out of `wax-v2-core` while transcripts, frame extraction, temporal retrieval, and video retrieval quality remain future work.
- [x] Recorded that the first Apple-specific acceleration parity surface now lives in `wax-v2-runtime`, reports Apple-family acceleration capability explicitly, resolves optional backend preference separately from search requests, and still does not claim linked Apple frameworks or hardware-accelerated execution parity.

## Follow-On Roadmap

- [x] Recorded that the staged roadmap is complete and remains frozen as historical execution history.
- [x] Added `docs/specs/2026-04-20-rax-raw-product-ingest-roadmap.md`, `docs/plans/2026-04-20-rax-raw-product-ingest-plan.md`, and `docs/todos/2026-04-20-rax-raw-product-ingest-todo.md` to track the next write-path roadmap separately.
