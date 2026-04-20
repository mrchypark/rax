# Rax To Wax Roadmap

Status: Approved for execution  
Date: 2026-04-19  
Scope: turn `rax` from a benchmark-first prototype into a Rust-native Wax v2 implementation with a staged path from core engine parity to product-surface parity

## Summary

`rax` already proves useful benchmark-harness pieces, dataset packing, text/vector query evaluation, and artifact reduction. It does not yet implement the durable Wax v2 engine described by the existing architecture docs, nor the product surfaces exposed by upstream Wax.

The approved direction is:

1. keep the current benchmark harness
2. add the real Wax v2 core beside it
3. move the harness to target the real core incrementally
4. add product surfaces only after the durable core exists

## Correction To Earlier Framing

An earlier shorthand framing was "rax is a benchmark harness." That is incomplete.

The corrected framing is:

- current code is harness-first
- current intent is a real Wax v2 rewrite
- the highest-risk gaps are structural engine gaps, not micro-features

This correction matters because the existing `wax-bench-*` crates are not throwaway. They should become verification infrastructure around the real engine rather than be replaced blindly.

## Current State

Today `rax` contains:

- benchmark dataset packing
- benchmark execution runner
- simplified text/vector search runtime used for evaluation
- artifact generation and reduction
- contract tests for benchmark behavior

Today `rax` does not contain:

- a real `.wax` binary container
- superblock and active-manifest publication
- append-friendly write and commit flow
- snapshot-isolated read generations
- a durable docstore separate from benchmark fixtures
- product API, broker, or MCP surface

## Goals

### Phase 1 Goals

- implement a real Rust-native Wax v2 core container
- preserve the benchmark harness as a gate around the new core
- keep backend boundaries stable enough for later swaps
- make open/search/write costs measurable against the new engine

### Later Goals

- product CLI and runtime API
- broker and MCP surface
- structured memory parity
- multimodal parity

## Non-Goals For The First Execution Slice

- full upstream product parity in one cut
- Swift API parity
- PhotoRAG or VideoRAG parity
- structured memory implementation
- Apple-specific acceleration

## Options Considered

### Option 1: Keep Improving Only The Harness

Pros:

- low immediate risk
- faster benchmark iteration

Cons:

- does not reduce the main parity gap
- risks optimizing a prototype that never becomes the engine

### Option 2: Rewrite The Whole Product Surface Immediately

Pros:

- feels closer to upstream Wax quickly

Cons:

- forces API and broker decisions before core durability exists
- mixes engine risk with product-surface risk
- weakens benchmark usefulness

### Option 3: Build The Durable Core First, Then Move Outward

Pros:

- aligns with existing architecture docs
- keeps benchmark infrastructure useful
- creates a stable substrate for API, CLI, and MCP work

Cons:

- product-visible parity arrives later
- requires temporary duplication between benchmark crates and core crates

Decision: choose Option 3.

## Approved Architecture Direction

### 1. Workspace Shape

Keep existing `wax-bench-*` crates and add real engine crates:

- `wax-v2-core`
- `wax-v2-docstore`
- `wax-v2-text`
- `wax-v2-vector`
- `wax-v2-search`

The harness remains the primary verification layer during migration.

### 2. Container

The first new implementation slice will add a real binary container skeleton:

- dual superblocks
- active manifest payload
- typed segment metadata
- generation and checksum validation

This slice will intentionally stop short of a full mutable engine. The goal is to establish the on-disk contract that later write and read paths can share.

### 3. Read Path

The read path will evolve in this order:

1. binary container open and manifest load
2. docstore segment lookup
3. text segment adapter
4. vector segment adapter
5. hybrid search coordinator

The benchmark runner will eventually exercise the same open/search code as the product runtime.

### 4. Write Path

Write support will evolve in this order:

1. segment builder helpers
2. append segment bytes
3. write new manifest generation
4. atomically switch active superblock
5. add compaction and tombstone handling later

### 5. Product Surface

Public runtime API, CLI, broker, and MCP should be added only after the core container can:

- create a store
- open a store
- publish a manifest generation
- execute stable read paths against a generation

## Execution Order

### Slice A: Core Container Skeleton

- add `wax-v2-core`
- define binary superblock and manifest encoding
- support create/open/validate for empty or minimal stores
- add format tests

### Slice B: Durable Docstore

- move documents from fixture-style sidecars into typed doc segments
- provide stable Wax-owned doc ids
- support preview and metadata lookup

Slice B starts with a compatibility sub-slice:

- introduce `wax-v2-docstore` as the read boundary over the current dataset-pack `docs.ndjson` and optional `document_offsets`
- keep benchmark runner and CLI call sites stable while `wax-bench-text-engine` delegates document reads to the new crate
- defer true binary doc-segment publication, tombstone semantics, and canonical numeric doc-id allocation to the next docstore step

This sequencing keeps the harness green while removing direct document-file knowledge from the engine caller layer.

Slice B handoff status:

- the compatibility boundary is in place
- the core manifest descriptor now matches the full 128-byte v2 contract
- `wax-v2-docstore` now has a binary doc-segment codec and tombstone row flag representation
- `wax-v2-docstore` now has a stable `DocIdMap` boundary with 0-based Wax-owned numeric ids assigned in `docs.ndjson` file order
- `wax-v2-docstore` can now publish a manifest-visible doc segment through `wax-v2-core` and reopen it successfully
- `wax-v2-core` now wraps manifest and segment payloads in `WXOB` envelopes and aligns object starts to 4096-byte boundaries

### Slice C: Real Text And Vector Segments

- wrap current benchmark search logic behind real segment adapters
- stop reading benchmark fixture layouts directly from the runner path

Slice C handoff status:

- `wax-v2-text` now exists as the first text-lane crate and owns the current lexical compatibility shape over `text_postings` and query-sidecar files
- `wax-bench-text-engine` now delegates lexical text-lane loading and search to `wax-v2-text`, with runner and CLI call sites unchanged
- `wax-v2-text` now resolves persisted text inputs through a Wax-owned metadata boundary and keeps benchmark `query_set` files classified as compatibility query input rather than persisted text metadata
- `wax-v2-text` now owns a batch query/search boundary over the current compatibility `query_set` format, and `wax-bench-text-engine` uses that boundary for text batch parsing and text-hit generation while keeping hybrid/vector orchestration outside the text crate
- `wax-v2-vector` now exists as the first vector-lane crate and owns the current compatibility shape over `document_ids`, vector payloads, preview payloads, HNSW sidecars, and query-vector sidecars
- `wax-bench-text-engine` now delegates vector-lane loading, search, profiling, and warmup flows to `wax-v2-vector`, with runner and CLI call sites unchanged
- `wax-v2-vector` now routes exact-flat, preview-q8, and HNSW behavior through crate-private backends while preserving `VectorLane` as the public facade
- `wax-v2-vector` now resolves persisted vector metadata through a Wax-owned metadata boundary and keeps benchmark `query_vectors` sidecars separate from that persisted metadata
- the remaining text-lane gap is the final persisted binary text-segment design rather than more benchmark-side parser ownership
- `wax-v2-search` now exists as the first shared search-layer crate and owns reciprocal-rank fusion plus hybrid execution over the current `wax-v2-text` and `wax-v2-vector` boundaries
- `wax-bench-text-engine` now delegates shared hybrid execution to `wax-v2-search`, while query parsing and auto-mode orchestration remain compatibility caller logic for now
- `wax-v2-search` now also produces per-hit lane-contribution diagnostics with text rank, vector rank, and fused RRF score for hybrid results
- `wax-bench-text-engine` now threads that diagnostics-aware path internally while keeping ranked-result outputs unchanged
- `wax-v2-search` now owns a first metadata-filtering path over top-level exact-match clauses, while `wax-bench-text-engine` preserves benchmark `filter_spec` input as compatibility query metadata
- filtered queries now overfetch candidates to corpus size, apply metadata filtering through a trait-based search boundary backed by docstore-loaded document fields, and then trim back to `top_k`
- the write-path compaction design follow-up now exists as [2026-04-19-wax-v2-compaction-followup.md](/Users/cypark/.codex/worktrees/0c4e/rax/docs/specs/2026-04-19-wax-v2-compaction-followup.md)
- `wax-bench-text-engine` now validates `store.wax` through `wax-v2-core::open_store` during `open` when a real core store is present, so runner-driven benchmark open now exercises the first real core-open contract without coupling `wax-bench-runner` to a specific engine crate
- `wax-v2-text` can now publish a compatibility `Txt` segment into `store.wax`, and `TextLane::load` now prefers the latest manifest-visible `Txt` segment over `text_postings` sidecars when one exists
- `wax-bench-text-engine` now inherits that text-lane preference automatically, so benchmark text queries can still run after `text_postings` is removed as long as a real `Txt` segment has been published
- `wax-v2-vector` can now publish a compatibility `Vec` segment into `store.wax`, and `VectorLane::load` now prefers the latest manifest-visible `Vec` segment for doc ids, exact vectors, and preview vectors when one exists
- `wax-bench-text-engine` now inherits that vector-lane preference automatically, so benchmark vector queries can still run after `document_ids`, `document_vectors`, and preview sidecars are removed as long as a real `Vec` segment has been published
- `wax-v2-docstore::open` now prefers the latest manifest-visible `Doc` segment in `store.wax`, so benchmark preview and metadata hydration no longer depend directly on `docs.ndjson` when a real doc segment is present
- benchmark callers no longer directly depend on benchmark fixture layout for doc/text/vector runtime reads; remaining compatibility inputs such as `query_vectors` and HNSW graph files now sit behind engine crate boundaries instead
- `wax-v2-runtime` now exists as the first product-facing Rust facade and currently exposes create/open/search/close plus a write-session boundary over the staged core, docstore, text, vector, and search crates
- the first runtime write surface is intentionally compatibility-scoped: `RuntimeStoreWriter::import_compatibility_snapshot` publishes compatibility `Doc`/`Txt`/`Vec` segments from the current dataset-pack inputs into `store.wax`
- `wax-cli` now exists as the first product CLI crate, shipping a `wax` binary with `create`, `import-compat`, and text `search` commands backed by `wax-v2-runtime`
- the first product CLI intentionally keeps compatibility import explicit and does not surface benchmark pack/run/reduce workflows or pretend to offer final raw ingest
- `wax-v2-broker` now exists as the first broker/session crate and owns opaque session ids plus in-process `RuntimeStore` reuse for text search, compatibility import, and close
- the first broker/session surface is intentionally local and text-first, so it stays reusable for later daemon or MCP transport without prematurely freezing vector-input or concurrency policy
- `wax-v2-mcp` now exists as the first MCP-compatible crate and exposes transport-ready request/response enums for session open, text search, compatibility import, and close over `wax-v2-broker`
- the first MCP slice deliberately stops short of a real network server or daemon, but it now has a stable enough tool boundary for later transport work
- `wax-v2-structured-memory` now exists as the first bootstrap structured-memory crate with explicit `subject/predicate/value/status/provenance` records and a bootstrap `structured-memory.ndjson` persistence layer
- the first structured-memory slice deliberately stops short of final Wax binary segment persistence and full upstream entity/fact parity, but it now gives deferred-parity work an explicit boundary instead of hiding structure inside document metadata
- `wax-v2-structured-memory` now also exposes first explicit `StructuredEntity` and `StructuredFact` APIs above the same bootstrap persistence layer, so later broker or MCP callers no longer need to reinterpret generic bootstrap records to express entity or fact intent
- the current entity bootstrap design deliberately remains flat: entity kind and aliases are still persisted as reserved bootstrap predicates inside `structured-memory.ndjson`, while alias normalization, graph traversal, bitemporal querying, evidence richness, and final binary-segment persistence remain future work
- `wax-v2-multimodal` now exists as the first bootstrap multimodal crate with explicit asset descriptors, copied store-owned payloads, and a `multimodal-assets.ndjson` metadata file plus `multimodal-assets/` payload directory
- the first multimodal ingest slice deliberately stops short of manifest-visible media segments, OCR, transcripts, embeddings, frame extraction, or retrieval orchestration, but it now gives later PhotoRAG and VideoRAG work a durable asset-ownership boundary
- `wax-v2-multimodal` now also exposes an explicit PhotoRAG-facing read layer with image-only typed queries/results and optional bootstrap image metadata fields for dimensions and capture time
- the current PhotoRAG scaffolding deliberately stops short of OCR, captions, CLIP-style embeddings, reranking, or end-user image retrieval workflows, and keeps image-specific typed views out of `wax-v2-core` while the broader multimodal segment contract remains unsettled
- `wax-v2-multimodal` now also exposes an explicit VideoRAG-facing read layer with video-only typed queries/results and optional bootstrap video metadata fields for duration, frame dimensions, and frame rate
- the current VideoRAG scaffolding deliberately stops short of transcript extraction, frame sampling, temporal retrieval, or video-processing dependencies in the ingest path, and keeps video-specific typed views out of `wax-v2-core` while the broader multimodal segment contract remains unsettled
- `wax-v2-runtime` now also exposes an explicit Apple-acceleration capability report plus a separate backend-resolution surface, keeping hardware-selection strategy out of `RuntimeSearchRequest`
- the current Apple-parity scaffolding deliberately stops short of linking Apple frameworks or changing current Rust-first search execution, but it now gives callers an inspectable capability boundary and an optional preference-resolution path
- the staged `rax` roadmap checklist is now complete; remaining future work is further refinement, not an unchecked parity scaffold from this execution plan

### Slice D: Commit Publication

- append new segments
- write manifest generations
- switch active generation atomically
- add snapshot isolation tests
- define compaction follow-up before executable compaction work starts

### Slice E: Product Surfaces

- runtime API
- product CLI
- broker and MCP

### Slice F: Deferred Parity

- structured memory
- multimodal ingest and retrieval
- hardware-specialized backends

## Risks

- The main failure mode is continuing to add capability to `wax-bench-*` without extracting a real shared core.
- The second failure mode is over-designing the final product surface before the container and commit model are stable.
- The third failure mode is letting benchmark-only data formats harden into the production format.

## Current Implementation Corrections

- The first `wax-v2-core` slice proved the create/open/checksum path, but its manifest segment descriptor is still intentionally narrower than the byte-level v2 binary spec.
- Before real `doc`, `txt`, and `vec` segments are published through the core manifest, the descriptor must expand to the Wax-owned metadata described in `2026-03-29-wax-v2-binary-format.md`, including family version, flags, doc-id ranges, timestamps, live/tombstone counts, and reserved bytes.
- This is a sequencing correction, not a direction change: the core slice remains valid as a bootstrap, but it is not yet the final manifest contract.
- That descriptor expansion is now complete in `wax-v2-core`, so the next remaining docstore gap is publication and stable numeric id ownership rather than manifest shape.
- Stable numeric id ownership is now defined in `wax-v2-docstore` as a 0-based `u64` mapping over document-file order, with the benchmark string `doc_id` retained only as an external compatibility key.
- The doc-segment ref contract is now tightened: `payload_offset`, `metadata_ref`, and `preview_ref` are all authoritative byte refs inside their sections.
- The first harness-to-core open migration intentionally keeps `wax-bench-runner` generic over `WaxEngine`; the real core-open boundary lives in `PackedTextEngine.open`, which now validates `store.wax` when present before continuing with compatibility manifest loading.
- This is a boundary correction, not a retreat from harness migration: the runner now proves the first real store-open contract without taking a hard dependency on `wax-v2-core`, and the next migration step is segment-backed text/vector reads.
- The first persisted `Txt` segment is a compatibility bridge, not the final text format: it is published from current `text_postings` sidecars and still carries external benchmark `doc_id` strings instead of the final Wax-owned text posting representation.
- Query definitions also remain compatibility-sidecar inputs for now, so this slice removes runtime dependence on `text_postings` reads before it removes `query_set` reads.
- The first persisted `Vec` segment is also a compatibility bridge, not the final vector format: it is published from current `document_ids`, `document_vectors`, and optional preview sidecars and still uses benchmark-aligned external `doc_id` strings inside the embedded skeleton.
- `query_vectors` and HNSW graph files remain compatibility-sidecar inputs for now, so this slice removes runtime dependence on vector payload sidecars before it removes ANN graph and query-definition sidecars.
- The first store-backed `Doc` open path is also a migration boundary, not the final document runtime API: it currently materializes the latest published doc segment eagerly in memory and assumes doc payload JSON still carries the external compatibility `doc_id`.
- The first `wax-v2-runtime` search surface is intentionally narrow and honest: vector and hybrid search require explicit `vector_query` input from the caller instead of silently depending on benchmark embedding helpers.
- The runtime facade still depends on the current compatibility manifest and staged engine crates under the hood, so it should be treated as the first public boundary rather than as full product/runtime parity.
- `preview_ref.length == 0` currently decodes to `None`, so the format does not yet distinguish no-preview from an explicitly empty preview string.
- External critique on the doc-id slice confirmed that file-order allocation is acceptable as the minimal next step, but only if the dependency is explicit. The current tests therefore prove that changing `docs.ndjson` order changes Wax numeric ids.
- External critique on the publication slice confirmed that flipping only the alternate superblock is the right minimal switch model, but also surfaced a remaining spec gap: the current helper appends raw object bytes without the final `WXOB` envelope and page-aligned append boundaries required by `2026-03-29-wax-v2-binary-format.md`.
- That raw-object gap is now closed in `wax-v2-core`: manifest and segment publication use `WXOB` envelopes and 4096-byte object-start alignment.
- Snapshot isolation and multi-generation reopen fallback are now covered by core regression tests.
- The next remaining write-path hardening gap is journal-backed recovery and compaction behavior rather than byte-level object layout.
- External critique on the ref-contract slice supported making `metadata_ref` and `preview_ref` authoritative and treating a zero-length preview ref as the minimal `None` representation.
- The text-lane compatibility extraction is now complete enough to move forward: `wax-v2-text` owns the current lexical sidecar format, the benchmark engine no longer keeps duplicate text-lane parsing logic, and persisted text metadata now resolves through a Wax-owned boundary.
- External critique on the text batch slice supported keeping lane orchestration above the text crate. The current `query_set` batch loader in `wax-v2-text` is therefore explicitly compatibility-only and must not harden into the final product query format.
- The first search-crate extraction is now in place: `wax-v2-search` owns RRF fusion and hybrid execution, but it is still a compatibility-first boundary rather than the final product runtime API.
- The search slice deliberately does not absorb benchmark query parsing yet. That keeps benchmark-shaped query-set handling from becoming the permanent search API while still removing duplicate fusion logic from `wax-bench-text-engine`.
- The first diagnostics slice is now in place too: `wax-v2-search` emits per-hit lane-contribution diagnostics, but those diagnostics are currently implementation-focused internal reporting rather than a frozen user-facing API.
- The benchmark engine intentionally keeps its ranked-result output stable while routing through the diagnostics-aware search path. That preserves harness contracts while leaving room to design a fuller reporting surface later.
- The first metadata-filtering slice is now in place as well: `wax-v2-search` owns a trait-based top-level exact-match filter boundary, and benchmark query-side `filter_spec` is preserved only as compatibility input.
- Filtered queries currently overfetch to corpus size before post-filtering and truncation. That is acceptable for the compatibility slice, but later real segment/query planning work should replace it with filter-aware candidate generation.
- The compaction gap is now explicit instead of implicit: the first follow-up design defines compaction as another manifest-generation publish with logical active-set cleanup first and physical byte reclamation deferred.
- This is a deliberate correction worth keeping visible: the first compaction target is not file shrinking. Old compacted bytes may remain in the file until a later rewrite or vacuum tool exists.
- The vector-lane migration is now in the same staged shape: `wax-v2-vector` owns the current compatibility loading and search logic, but the benchmark engine still has to delegate to it before the vector boundary is truly extracted.
- That caller-migration gap is now closed: the benchmark engine delegates to `wax-v2-vector`, so the next vector concern is internal backend decomposition rather than more caller rewiring.
- That backend-decomposition gap is now closed too: `wax-v2-vector` uses crate-private exact, preview, and HNSW backends, and explicit HNSW profile requests degrade to exact-flat instead of panicking when no sidecar exists.
- The first vector-metadata ownership gap is now closed as well: persisted vector identity and capabilities resolve through a Wax-owned metadata boundary, while `query_vectors` stay classified as benchmark-query compatibility input rather than persisted vector metadata.

## Verification Strategy

- keep `cargo test` green for existing harness contracts
- add new core-format round-trip and corruption tests
- route future benchmark slices through the core engine as soon as possible
- keep design and todo documents updated as scope changes

## External Critique Applied

An external model critique was used before freezing this direction. The main correction it contributed was to separate:

- phase-1 architectural gaps
- durable-engine gaps
- full product-parity gaps

That distinction is now part of the roadmap and todo structure.

## Follow-On Roadmap

The staged roadmap above is now complete and remains historically correct as written.

The next product-write roadmap is tracked separately in `docs/specs/2026-04-20-rax-raw-product-ingest-roadmap.md`. That follow-on work does not reopen the staged checklist above; it starts from the completed runtime, CLI, broker, and MCP surfaces and replaces compatibility-pack writes with true raw product ingest.
