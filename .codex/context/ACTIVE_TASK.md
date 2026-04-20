# Active Task

Date: 2026-04-20

Current slice: the staged rax-to-wax roadmap and the full raw product-ingest follow-on roadmap are complete. There is no remaining unchecked item in the current raw-ingest docs.

Done:

- roadmap spec written
- core-foundation plan written
- master todo written
- Yeoul roadmap and correction episodes recorded
- `wax-v2-core` added with binary superblocks, manifest encoding, and create/open validation
- `wax-v2-core` manifest descriptor expanded to the full 128-byte v2 shape
- `wax-v2-docstore` compatibility facade added and wired into `wax-bench-text-engine`
- `wax-v2-docstore` binary doc-segment codec added with tombstone row flags
- `wax-v2-docstore` `DocIdMap` added with 0-based numeric ids assigned in `docs.ndjson` file order
- `wax-v2-core` can now append a segment object, publish a new manifest generation, and switch the alternate superblock
- `wax-v2-docstore` can now publish a manifest-visible doc segment into a real store and reopen it
- `wax-v2-docstore` now treats `metadata_ref` and `preview_ref` as authoritative byte refs during encode and decode
- `wax-v2-core` now wraps manifest and segment payloads in `WXOB` envelopes and aligns object starts to 4096-byte boundaries
- `wax-v2-core` now has regression tests for snapshot stability, latest-valid-generation reopen, and fallback when the newest superblock copy is invalid
- `wax-v2-text` now owns the current lexical `text_postings` and query-sidecar compatibility boundary
- `wax-bench-text-engine` no longer keeps duplicate text-lane parsing helpers after delegating lexical search to `wax-v2-text`
- `wax-v2-text` now resolves persisted text inputs through a Wax-owned metadata boundary, while benchmark `query_set` files remain compatibility query input
- `wax-v2-text` now owns batch query/search over the compatibility `query_set` format, and `wax-bench-text-engine` uses that boundary for batch text hits
- `wax-v2-vector` now owns the current compatibility loading and search logic for `document_ids`, vector payloads, preview payloads, optional HNSW sidecars, and query-vector sidecars
- `wax-bench-text-engine` now delegates vector-lane loading, search, profiling, and warmup flows to `wax-v2-vector`
- `wax-v2-vector` now routes exact-flat, preview-q8, and HNSW behavior through crate-private backends
- explicit `Hnsw` profile requests now fall back to exact-flat when no sidecar is available
- `wax-v2-vector` now resolves persisted vector inputs through a Wax-owned metadata boundary, while benchmark `query_vectors` remain outside persisted vector metadata
- `wax-v2-search` now exists as the first shared search-layer crate
- `wax-v2-search` now owns reciprocal-rank fusion and hybrid execution over the current text/vector lane boundaries
- `wax-bench-text-engine` now delegates shared hybrid execution to `wax-v2-search`, while benchmark query parsing and auto-mode orchestration remain caller-side compatibility logic
- `wax-v2-search` now emits per-hit lane-contribution diagnostics with text rank, vector rank, and fused RRF score
- `wax-bench-text-engine` now routes through the diagnostics-aware search path internally while keeping ranked-result outputs unchanged
- `wax-v2-search` now owns a trait-based top-level exact-match metadata filter boundary
- `wax-v2-text` now preserves benchmark `filter_spec` as compatibility query metadata
- filtered benchmark queries now overfetch to corpus size, apply metadata filtering through docstore-backed field lookups, and then truncate back to `top_k`
- the write-path compaction follow-up is now documented in `2026-04-19-wax-v2-compaction-followup.md`
- the current compaction plan explicitly separates logical active-set cleanup from later physical byte reclamation
- benchmark `ContainerOpen` now validates `store.wax` through `wax-v2-core::open_store` when a real core store is present
- invalid real core stores now fail benchmark open before search begins, while compatibility manifest loading still handles the rest of the benchmark runtime
- `wax-v2-text` now publishes a compatibility `Txt` segment and prefers the latest manifest-visible text segment over `text_postings` sidecars when one exists
- benchmark text queries now survive `text_postings` removal as long as the real `Txt` segment has already been published into `store.wax`
- `wax-v2-vector` now publishes a compatibility `Vec` segment and prefers the latest manifest-visible vector segment over `document_ids`, `document_vectors`, and preview sidecars when one exists
- benchmark vector queries now survive removal of those vector payload sidecars as long as the real `Vec` segment has already been published into `store.wax`
- `wax-v2-docstore::open` now prefers the latest manifest-visible `Doc` segment over dataset-pack `docs.ndjson`
- benchmark preview and metadata hydration now survive `docs.ndjson` removal as long as the real `Doc` segment has already been published into `store.wax`
- `wax-v2-runtime` now exists as the first product-facing Rust facade
- the first runtime facade now supports open/search/close with `Text`, `Vector`, and `Hybrid` modes without leaking benchmark workload names
- vector and hybrid runtime search intentionally require caller-provided `vector_query` values
- `wax-v2-runtime` now also supports non-destructive `create` plus a `RuntimeStoreWriter` compatibility-import session for first write/publish behavior
- runtime compatibility publish now round-trips through reopen and survives removal of published doc/text/vector payload sidecars
- `wax-cli` now exists as the first product CLI crate and ships a `wax` binary distinct from `wax-bench-cli`
- the first product CLI now supports `create`, `import-compat`, and text `search` over `wax-v2-runtime`
- `wax-v2-broker` now exists as the first broker/session crate with opaque session ids over reusable `RuntimeStore` handles
- the first broker/session surface now supports `open_session`, text `search`, compatibility import, and `close_session`
- `wax-v2-mcp` now exists as the first MCP-compatible crate with transport-ready request/response enums over broker-owned sessions
- the first MCP-compatible surface now supports session open, text search, compatibility import, and close without requiring a daemon yet
- `wax-v2-structured-memory` now exists as the first bootstrap structured-memory crate with explicit record write/read/query behavior
- bootstrap structured-memory records now persist in `structured-memory.ndjson` with explicit `status` and provenance fields and survive reopen/query
- `wax-v2-structured-memory` now also exposes explicit entity and fact APIs above the same bootstrap persistence layer
- bootstrap entity metadata currently persists through reserved `__entity_kind` and `__entity_alias` predicates rather than a final dedicated binary structured-memory format
- `wax-v2-multimodal` now exists as the first bootstrap multimodal ingest crate with explicit asset descriptors and durable copied payload ownership
- bootstrap multimodal ingest currently persists metadata in `multimodal-assets.ndjson` and payload bytes in a store-owned `multimodal-assets/` directory rather than final manifest-visible media segments
- `wax-v2-multimodal` now also exposes PhotoRAG-facing image-only typed read/query APIs
- bootstrap PhotoRAG metadata currently uses optional image dimensions and capture time fields rather than OCR, captions, embeddings, or retrieval features
- `wax-v2-multimodal` now also exposes VideoRAG-facing video-only typed read/query APIs
- bootstrap VideoRAG metadata currently uses optional duration, frame dimensions, and frame rate fields rather than transcripts, frame extraction, embeddings, or temporal retrieval features
- `wax-v2-runtime` now also exposes Apple-family acceleration capability reporting and separate backend-resolution preference APIs
- current Apple acceleration parity is capability-first only; it does not yet link Apple frameworks or alter the default Rust-first execution path
- follow-on raw product ingest planning docs now exist in `docs/specs/2026-04-20-rax-raw-product-ingest-roadmap.md`, `docs/plans/2026-04-20-rax-raw-product-ingest-plan.md`, and `docs/todos/2026-04-20-rax-raw-product-ingest-todo.md`
- `wax-v2-docstore` now persists full known external-doc-id to wax-doc-id bindings inside the latest `Doc` segment and reuses them across compatibility reimports
- the new `raw_doc_id_authority_contract` proves document-order changes no longer renumber existing Wax doc ids when a later reimport adds new documents
- `wax-v2-core` now has a shared `publish_segments` helper for one-generation multi-family publication
- `wax-v2-docstore`, `wax-v2-text`, and `wax-v2-vector` now expose staged compatibility segment-preparation helpers instead of only immediate publish paths
- `wax-v2-runtime` now exposes a staged compatibility publish path that commits `Doc`/`Txt`/`Vec` in one visible generation, and `import_compatibility_snapshot` now delegates to that same staged path
- the new `raw_ingest_single_publish_contract` proves staged compatibility publish yields one visible generation and survives reopen plus sidecar removal
- `wax-v2-runtime` now also exposes `NewDocument` plus `publish_raw_documents` for the first raw product-ingest surface
- `wax-v2-docstore` now prepares raw `Doc` segments directly from runtime-owned document payloads, and `wax-v2-text` now prepares raw `Txt` segments directly from runtime-owned text bodies
- the new `raw_document_ingest_contract` proves raw `Doc` plus `Txt` publish survives reopen and text search after dataset `documents`, `document_offsets`, and `text_postings` sidecars are removed
- `wax-v2-runtime` now also exposes `NewDocumentVector` plus `publish_raw_vectors` for the first explicit raw vector-ingest surface
- `wax-v2-vector` now prepares raw `Vec` segments directly from caller-provided vectors and exposes a runtime load path that does not require benchmark `query_vectors` sidecars
- the new `raw_vector_ingest_contract` proves a vector-only publish after raw document ingest preserves earlier doc/text families, survives reopen, and serves vector search after compatibility vector and query sidecars are removed
- the new `raw_compat_equivalence_contract` now proves packer-plus-compat-import and raw product ingest produce the same doc-id map plus reopen-safe text, vector, and hybrid runtime responses for the same corpus after sidecar removal
- `product_cli_contract`, `broker_session_contract`, and `mcp_surface_contract` now also prove the existing product read surfaces can consume raw-prepared stores after sidecar removal, not just compatibility-imported stores
- `wax-cli` now exposes explicit raw ingest commands as `wax ingest docs` and `wax ingest vectors`
- `wax-v2-broker` now exposes raw document and raw vector ingest mutations over the existing session boundary
- `wax-v2-mcp` now exposes `IngestDocuments` and `IngestVectors` request variants plus `RawIngested` responses
- the new `product_raw_ingest_cli_contract` and `mcp_raw_ingest_contract` prove current product surfaces can perform the first end-to-end raw product writes
- compatibility `Txt` publication now translates dataset-pack `documents` input through the same raw text builder used by product ingest
- compatibility `Vec` publication now translates dataset-pack vector payloads through the same raw vector builder used by product ingest
- `wax-v2-runtime` now exposes a shared `publish_raw_snapshot` primitive that stages `Doc`/`Txt` and optional `Vec` publication in one generation
- the new `raw_compat_publish_semantics_contract` proves compatibility import and raw full-snapshot publish now converge on the same one-generation report and semantic segment descriptor set for equivalent inputs

Next verification target:

- none in the current raw-ingest roadmap; future work needs a new roadmap or follow-on slice definition

Notes:

- keep the staged roadmap completion docs frozen as historical execution history
- keep the benchmark harness intact while adding the real engine crates
- do not expose final-looking raw ingest runtime or CLI verbs before doc-id authority and the single-publish primitive exist
- document any design correction in both `docs/specs` and `docs/todos`
- the dataset-pack string `doc_id` is now a compatibility key, not the canonical Wax-owned identifier
- `metadata_ref` and `preview_ref` are now authoritative byte refs; `preview_ref.length == 0` is the current `None` representation
- the current publication helper now matches the object-envelope and alignment parts of the binary-format contract, but journal-backed recovery and snapshot hardening are still pending
- the new `query_set` loader in `wax-v2-text` is compatibility-only and should not become the final product query contract
- the current `wax-v2-search` surface is compatibility-first and should not be mistaken for the final product runtime API
- the current diagnostics shape in `wax-v2-search` is implementation-focused internal reporting, not yet the final user-facing diagnostics API
- the current metadata-filtering path is compatibility-first; later filter-aware candidate generation should replace the current overfetch-and-trim approach
- the compaction follow-up is design-complete, but executable compaction and byte reclamation are still future work
- the first `Txt` segment is still a compatibility bridge that serializes external benchmark `doc_id` strings rather than the final Wax-owned text posting representation
- the first `Vec` segment still uses a bootstrap binary shape without sparse partial updates, quantized preview generation, or HNSW publication
- benchmark `query_vectors` and HNSW graph files are still compatibility-sidecar inputs, but runtime vector search no longer requires `query_vectors` once the caller provides `vector_query`
- the runtime facade still depends on the current compatibility manifest and staged engine crates beneath it
- the current runtime write surface now offers raw product ingest for documents, full-snapshot documents-plus-vectors, and vector-only updates, but it still does not claim general multi-step transactional staging
- store-owned doc-id persistence is now in place through persisted binding maps carried by the latest `Doc` segment, replacing file-order allocation as the authority for repeated imports
- a staged pending-segment primitive now exists for new work, and the compatibility bridge now reuses that single-generation publish path instead of keeping a separate multi-generation import flow
- the first raw product-ingest surface now covers `Doc`, `Txt`, and caller-provided `Vec` publication, but vector ingest is still manifest-sized and does not yet claim sparse partial updates
- the first equivalence proof remains runtime-first for read behavior, and the stricter publish-semantics proof now exists separately via the shared full-snapshot publish primitive
- the first product raw-ingest surface is family-explicit (`docs` and `vectors`) rather than a generic multi-family envelope
- fresh targeted raw-ingest verification and fresh `cargo test --workspace --quiet` verification are both green when the run uses a workspace-local `TMPDIR` instead of the exhausted system temp volume
- post-completion review hardening now keeps mounted-pack versus store-segment consistency checks for `Doc`, `Txt`, and `Vec` at the benchmark harness boundary; runtime keeps `store.wax` authoritative so raw-ingested stores can legitimately diverge from leftover benchmark sidecars
- the first broker/session slice is intentionally local and in-process; it does not yet define transport, concurrency, or pooling policy beyond single-process ownership
- the first MCP slice is transport-ready but still text-first and does not yet provide a daemon, network transport, or vector/hybrid tool surface
- the first structured-memory slice uses an explicit bootstrap sidecar file and does not yet claim final Wax binary segment persistence, alias normalization, bitemporal querying, or full entity/fact parity
- the first explicit entity/fact slice still uses reserved bootstrap predicates and does not yet claim alias normalization, evidence-rich fact values, graph traversal, or bitemporal querying
- the first multimodal ingest slice uses explicit bootstrap metadata and copied payload ownership, and does not yet claim manifest-visible media segments, OCR, transcripts, embeddings, or retrieval orchestration
- the first PhotoRAG slice adds image-only typed reads and bootstrap image metadata, but does not yet claim OCR, captions, embeddings, reranking, or end-user retrieval workflows
- the first VideoRAG slice adds video-only typed reads and bootstrap video metadata, but does not yet claim transcripts, frame extraction, temporal retrieval, or video-processing dependencies in the ingest path
- the first Apple parity slice adds capability reporting and separate backend preference resolution, but does not yet claim linked Apple frameworks or hardware-accelerated execution
- there is no remaining unchecked item in the current staged roadmap documents
