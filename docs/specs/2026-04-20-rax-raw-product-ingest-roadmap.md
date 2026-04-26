# Rax Raw Product Ingest Roadmap

Status: Approved for follow-on execution  
Date: 2026-04-20  
Scope: replace compatibility-pack writes with true product ingest while preserving the completed staged rax-to-wax roadmap and keeping the benchmark harness green

## Summary

The staged rax-to-wax roadmap is complete, but the public write path is still intentionally honest about being a bridge:

- `RuntimeStoreWriter::import_compatibility_snapshot()` reads dataset-pack compatibility artifacts and publishes compatibility `Doc`/`Txt`/`Vec` segments into `store.wax`
- `wax-cli` still exposes `import-compat` as the compatibility bridge alongside the new raw `wax ingest docs` and `wax ingest vectors` verbs
- broker and MCP surfaces now expose raw document/vector writes, but compatibility import still remains as the broadest bridge path

That was the right staged endpoint because it gave `rax` a real core, product-facing read surfaces, and stable engine crates without pretending raw product ingest already existed.

The next roadmap is to remove benchmark-pack artifacts from product write surfaces. The end state is:

1. product callers provide raw documents, metadata, optional vectors, and raw multimodal assets
2. Wax-owned crates derive the publishable segment payloads from those inputs
3. one ingest session publishes one manifest generation with explicit carry-forward semantics
4. compatibility import remains as a benchmark bridge rather than the main product path

Post-completion review hardening:

- mounted benchmark-pack reads now validate store-preferred `Doc`, `Txt`, and `Vec` segments against the currently mounted dataset pack at the benchmark harness boundary
- product runtime keeps `store.wax` authoritative and does not inherit that mounted-pack validation rule, because raw-ingested stores are allowed to diverge from leftover compatibility sidecars

## Current Boundary

Today the runtime, CLI, broker, and MCP layers still depend on compatibility-pack shapes underneath:

- `wax-v2-runtime` still reads the dataset-pack `manifest.json`, and compatibility import remains one of its write paths
- `wax-v2-docstore` still retains a compatibility fallback around dataset-pack documents
- `wax-v2-text` and `wax-v2-vector` can still translate compatibility dataset artifacts into publishable bridge segments
- `wax-cli` exposes `create`, `import-compat`, raw `ingest docs`, raw `ingest vectors`, and text `search`

This means `rax` already has a real storage substrate, but not a real product ingest substrate.

## Goals

### Primary Goals

- add raw product-facing ingest contracts for documents, metadata, vectors, and multimodal assets
- make Wax-owned crates responsible for deriving publishable segments from raw inputs
- persist store-owned doc-id authority instead of relying on compatibility file order as the long-term source of truth
- publish one manifest generation per ingest session instead of a sequence of compatibility family publishes
- move product surfaces from `import-compat` to explicit ingest verbs and requests

### Secondary Goals

- keep `import_compatibility_snapshot` available as a legacy bridge for benchmark harnesses
- reuse the same builder logic under both product ingest and compatibility ingest so verification does not drift
- keep current read paths and existing staged roadmap deliverables intact while the write path evolves

## Non-Goals

- hidden embedding generation for vector or hybrid search
- pretending OCR, transcription, captioning, or reranking already exist for multimodal ingest
- replacing the benchmark harness with a product-only test strategy
- introducing a daemon or network transport rewrite in the same slice
- immediate physical file shrinking or mutable in-place segment edits

## Decision Drivers

- The product surface should no longer require callers to understand dataset-pack sidecars such as `docs.ndjson`, `text_postings`, or `document_vectors`.
- The benchmark harness still matters, but it should verify shared builders and publication semantics rather than a permanently separate write path.
- The next write surface must stay honest about what is and is not solved. External vector inputs are acceptable; hidden embedding services are not.

## Options Considered

### Option 1: Keep Compatibility Import As The Main Product Write Path

Pros:

- minimal short-term work
- no immediate contract churn for runtime or CLI

Cons:

- keeps product callers coupled to benchmark artifacts
- leaves builder ownership outside the product runtime
- prevents honest `ingest` product verbs

### Option 2: Add Raw Ingest Verbs On Top Of The Existing Compatibility Builders

Pros:

- produces user-facing ingest commands quickly
- reuses more of the existing surface

Cons:

- risks shipping empty ingest verbs before real builders exist
- hides the fact that current writes still depend on benchmark artifacts
- increases harness versus product verification drift

### Option 3: Build Real Raw-Ingest Builders First, Then Move Product Surfaces

Pros:

- keeps product verbs honest
- forces doc-id authority and carry-forward semantics to become explicit
- lets benchmark and product paths converge on shared segment builders

Cons:

- slower CLI-visible progress
- requires another round of write-path design before product verb expansion

Decision: choose Option 3.

## Approved Architecture Direction

### 1. Product Inputs Become Wax-Owned Types

The next public ingest boundary should accept Wax-owned product types rather than compatibility files.

Examples:

- `NewDocument` with external id, text body, and structured metadata
- `ExternalVectorRecord` or equivalent explicit vector input type for callers who already have embeddings
- `NewAssetImport` for raw multimodal payload import plus bootstrap metadata

The first raw-ingest contract should not claim hidden tokenization, embedding generation, OCR, or transcription services that do not exist yet.

### 2. Doc-Id Authority Moves Into The Store

The staged roadmap allowed numeric doc ids to start as compatibility file-order allocation. That is no longer sufficient for incremental product ingest.

The store must own:

- the next numeric doc-id allocation point
- the mapping from external document key to canonical numeric doc id
- supersede and reopen behavior across multiple ingest sessions

Without this slice, raw ingest risks doc-id drift and non-deterministic carry-forward behavior.

### 3. Builder Logic Lives In Wax-Owned Crates

The next builders belong under the staged engine/runtime crates, not in benchmark-only packers.

Required builder boundaries:

- raw document to `Doc` rows
- raw text bodies to `Txt` postings or equivalent first text segment payload
- explicit external vectors to `Vec` payloads
- raw multimodal asset import to durable store-owned payload references plus metadata

Compatibility import should eventually call the same builder logic after translating dataset-pack inputs into these raw builder requests.

### 4. Ingest Sessions Publish A Single Generation

The current compatibility import path appends three immutable family publishes in sequence. The follow-on ingest path should instead:

1. open an ingest session
2. accumulate or spill builder state safely
3. publish one manifest generation
4. carry forward unaffected families from the previous generation explicitly

This keeps the manifest contract honest and makes later partial-family ingest extensions easier to reason about.

### 5. Product Surfaces Shift Only After Builders Exist

`wax-cli`, broker, and MCP should not expose final-looking ingest verbs until:

- raw document ingest exists
- doc-id authority is store-owned
- manifest carry-forward semantics are regression-tested

This is a sequencing correction from the staged roadmap: product verbs follow real builders rather than leading them.

## Execution Order

### Slice A: Store-Owned Doc-Id Authority

- persist the next numeric doc-id state and external-id mapping in the store
- remove file-order allocation as the long-term source of truth for product ingest
- add reopen and multi-session regression tests

This slice is mandatory before multiple raw ingest sessions can be treated as stable product behavior.

Slice A handoff status:

- the persisted authority currently lives inside the latest manifest-visible `Doc` segment as a durable binding map, not as a separate core object
- `wax-v2-docstore` now reuses that persisted binding map across compatibility reimports and allocates new doc ids from the persisted `max + 1` state rather than current document-file order
- the new `raw_doc_id_authority_contract` proves existing doc ids remain stable after reopen and a later compatibility reimport with changed document order plus a new document

### Slice B: Staged Pending Segments And Single Publish Primitive

- add a staged pending-segment boundary that can hold new family outputs before publication
- publish one manifest generation for one ingest session rather than exposing intermediate lane generations
- add regression tests that prove readers never observe partial product ingests between family builders

This slice is mandatory before raw product ingest can be considered honest, because the current `publish_segment` path publishes each family as its own generation.

Slice B handoff status:

- `wax-v2-core` now has a `publish_segments` helper that appends multiple segment objects and publishes one manifest generation for the whole batch
- `wax-v2-docstore`, `wax-v2-text`, and `wax-v2-vector` now each expose a prepared compatibility segment helper so runtime-owned callers can stage segment writes before publication
- `wax-v2-runtime` now exposes a staged compatibility publish path that writes `Doc`/`Txt`/`Vec` in one visible generation, and `import_compatibility_snapshot` now delegates to that same staged path
- the new `raw_ingest_single_publish_contract` proves a staged compatibility batch produces one visible generation, three same-generation family descriptors, and searchable reopen behavior after sidecar removal

### Slice C: Raw Document Ingest Contract And Builder Bootstrap

- define Wax-owned raw document ingest request types in `wax-v2-runtime`
- add the first failing runtime contract tests for raw document ingest
- add the first document-to-segment builder path under Wax-owned crates
- keep current compatibility import intact

This slice deliberately couples contract definition with the first real builder so the public shape is immediately validated by executable behavior.

Slice C handoff status:

- `wax-v2-runtime` now exposes a first `NewDocument` type plus `RuntimeStoreWriter::publish_raw_documents`
- the first raw document publish path stages `Doc` and `Txt` segments directly from runtime-owned raw document inputs rather than compatibility sidecars
- `raw_document_ingest_contract` proves raw document publish survives reopen and text search after dataset `documents`, `document_offsets`, and `text_postings` sidecars are removed
- this is still a bootstrap raw-ingest shape: it keeps the existing manifest dependency and does not yet include vector ingest, multimodal ingest unification, or carry-forward of older text generations across later raw document publishes

### Slice D: Text Builder And Raw Text Publish

- derive `Txt` segment payloads from raw document text rather than `text_postings`
- keep tokenization and text normalization under Wax-owned code paths
- prove benchmark compatibility import can reuse the same builder via translation instead of a separate publication path

Slice D handoff status:

- `wax-v2-text` now exposes a raw document text-segment preparation helper that tokenizes runtime-owned text bodies directly
- the first raw text path deduplicates repeated tokens per document and publishes deterministic sorted postings without going through `text_postings`
- compatibility text publication now translates dataset-pack `documents` input through that same raw text builder, so the remaining convergence work is publish semantics rather than a separate text build stack

### Slice E: Explicit Vector Ingest Policy And Builder

- define the first honest vector ingest contract around caller-provided vectors
- derive `Vec` segment payloads from those inputs
- make carry-forward behavior explicit when one ingest session updates vectors without re-ingesting every family

This slice still does not claim hidden embedding generation.

Slice E handoff status:

- `wax-v2-runtime` now exposes a first explicit `NewDocumentVector` type plus `RuntimeStoreWriter::publish_raw_vectors`
- the first raw vector publish path stages and publishes a real `Vec` segment from caller-provided vectors without pretending hidden embedding generation exists
- the current raw vector contract is intentionally bootstrap-scoped: it requires a full manifest-sized vector set for known existing documents and does not yet claim sparse partial vector updates
- `wax-v2-vector` now exposes a runtime load path that does not depend on benchmark `query_vectors` sidecars when the caller already supplies the search vector
- the latest manifest-visible `Vec` segment is now authoritative for preview presence; runtime reopen no longer falls back to removed preview sidecars when the persisted segment omits previews
- the new `raw_vector_ingest_contract` proves a vector-only publish after raw document ingest preserves earlier text/doc families, survives reopen, and serves vector search after compatibility vector and query sidecars are removed

### Slice F: Raw Versus Compatibility Equivalence Verification

- add corpus-level equivalence tests between packer-plus-compat-import and raw product ingest
- prove doc-id bindings, preview hydration, text hits, vector hits, and hybrid fusion stay aligned for equivalent inputs
- make benchmark harness verification depend on shared builder behavior wherever practical

Slice F handoff status:

- `raw_compat_equivalence_contract` now compares packer-plus-compat-import against raw product ingest at the runtime boundary for the same corpus
- the first equivalence proof checks store-backed doc-id bindings plus reopen-safe text, vector, and hybrid search results after compatibility sidecars are removed from both stores
- the current equivalence boundary is intentionally runtime-first: it treats externally visible runtime behavior as the canonical comparison target instead of overfitting to internal generation counts or legacy publication sequencing
- product-surface contract expansion and shared-builder benchmark convergence still remain as later work inside this slice

### Slice G: Product Surface Migration

- add `wax ingest`-style product CLI verbs only after the raw builder path exists
- add raw ingest requests to broker and MCP surfaces
- keep `import-compat` available but clearly legacy on product surfaces

Slice G handoff status:

- `wax-cli` now exposes explicit family-based raw ingest commands as `wax ingest docs` and `wax ingest vectors`
- `wax-v2-broker` now exposes raw document and raw vector ingest requests above the existing session boundary
- `wax-v2-mcp` now exposes transport-ready `IngestDocuments` and `IngestVectors` requests plus `RawIngested` responses
- the first product raw-ingest migration keeps family boundaries explicit rather than inventing a generic multi-family envelope before the runtime actually has one
- `publish_raw_documents` is now incremental only against active store-owned raw document segments; it does not silently merge compatibility pack sidecars into product ingest state when the store has no `Doc` segment yet
- long-lived runtime, broker, structured-memory, and multimodal sessions now refresh read state before serving reads that may otherwise observe stale data after another session writes
- the MCP surface now requires an allowed root and preserves unknown flattened top-level document payload fields through broker raw ingest
- `import-compat` remains available as the explicit legacy bridge alongside the new raw product-ingest surface
- targeted CLI and MCP raw-ingest contracts are green, and fresh full-workspace verification is also green when `cargo test --workspace --quiet` runs under a workspace-local `TMPDIR`

### Slice H: Compatibility Bridge De-Emphasis

- make the benchmark harness translate dataset-pack artifacts into the shared raw builder path where practical
- keep a narrow compatibility bridge for benchmark-specific fixtures that still lack raw equivalents
- avoid verification drift between product and benchmark write behavior

Slice H handoff status:

- `wax-v2-runtime` now exposes a shared `publish_raw_snapshot` primitive that stages `Doc`/`Txt` and optional `Vec` publication in one manifest generation
- `import_compatibility_snapshot` now delegates to the staged single-generation compatibility publish path instead of keeping a separate multi-generation legacy publication flow
- compatibility `Txt` publication now builds from dataset-pack `documents` through the same raw text builder used by product ingest
- compatibility `Vec` publication now translates dataset-pack vector payloads through the same raw vector builder used by product ingest
- the new `raw_compat_publish_semantics_contract` proves compatibility import and raw full-snapshot publish now converge on the same one-generation report and semantic segment descriptor set for equivalent inputs
- the benchmark harness is green again under a workspace-local `TMPDIR`, and only dataset-pack-to-raw-input translation remains as the compatibility-specific write shim

## Risks

- The main new risk is doc-id drift if product ingest lands before store-owned numeric id persistence.
- The second new risk is harness versus product verification drift if compatibility import keeps a separate publication path instead of reusing shared builders.
- The third new risk is session-state bloat if ingest sessions require all derived family payloads to remain fully in memory before publish.
- The fourth new risk is overstating vector or multimodal parity by implying hidden embedding, OCR, or transcription steps that are not yet implemented.
- The fifth new risk is exposing raw runtime or CLI ingest verbs before the single-publish primitive exists, which would let readers observe partially ingested lane generations.

## Corrections Applied

- The follow-on roadmap keeps the completed staged roadmap intact rather than reopening those checklist items.
- Product ingest verbs are now explicitly sequenced after real builders and doc-id authority work, not before.
- Store-owned doc-id persistence is called out as a first-class slice rather than an implicit consequence of raw ingest.
- The staged pending-segment and single-manifest publish primitive is now called out as another mandatory early slice rather than being left implicit inside later runtime work.
- The first doc-id-authority implementation now uses the persisted `Doc` segment itself as the store-owned binding authority, which keeps the slice small while still removing product-write dependence on current file order for repeated imports.
- The first staged single-publish implementation landed as a new shared core primitive plus a staged compatibility runtime path, and `import_compatibility_snapshot` has now been folded onto that staged path as the bridge narrows.
- The first raw document ingest implementation is a bootstrap product surface over `Doc` plus `Txt` only; it intentionally stops short of vector ingest, multimodal session unification, or root-level manifest independence.
- Family-explicit product verbs remain the public partial-update surface, while a separate shared full-snapshot runtime primitive now carries the stricter equivalence and compatibility-bridge convergence semantics.
- Benchmark coverage remains mandatory, but the target is shared builder verification rather than permanent compatibility-only publication logic.
- Product raw document ingest now treats the active store `Doc` segment as the only carry-forward source. Compatibility pack documents remain a bridge input for explicit compatibility import or full-snapshot equivalence, not an implicit merge source for incremental product writes.
- Store publication paths now use generation or document-segment preconditions around merge, validation, and publish so concurrent writers fail closed instead of clobbering unseen updates.
- MCP session roots are now fail-closed to a configured allowed root; arbitrary filesystem roots are no longer part of the transport-ready surface.
- Missing HNSW sidecar files now fall back to exact-flat even when HNSW mode is explicitly requested, matching runtime search fallback behavior instead of failing during lane load.

## Verification Strategy

- start each raw-ingest slice with failing contract tests around the product-facing runtime surface
- keep `cargo test --workspace --quiet` green throughout follow-on work
- add focused reopen and multi-session regression tests for doc-id persistence and manifest carry-forward
- ensure benchmark compatibility imports migrate toward shared builders instead of growing a second write stack

## External Critique Applied

External critique on this follow-on roadmap contributed three concrete corrections:

- make store-owned doc-id persistence a dedicated early slice
- make the single-manifest staged publish primitive land before final-looking raw ingest surfaces
- do not expose final-looking ingest verbs before the real builders exist
- treat session memory pressure and carry-forward semantics as design work, not as implementation details to clean up later
