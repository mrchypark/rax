# Rax Raw Product Ingest Todo

## Rules

- Keep this file updated as the follow-on roadmap executes.
- Preserve the completed staged roadmap history in `2026-04-19-rax-to-wax-master-todo.md`.
- Record corrections explicitly instead of silently rewriting the write-path story.

## Current Position

- [x] Staged rax-to-wax roadmap completed
- [x] Raw product ingest roadmap written
- [x] Raw product ingest implementation plan written
- [x] Yeoul search and external critique reviewed before freezing direction

## Phase A: Store-Owned Doc-Id Authority

- [x] Persist next numeric doc-id allocation in the store
- [x] Persist external-id to canonical-doc-id mapping in the store
- [x] Add multi-session reopen regression tests for stable doc ids

## Phase B: Single-Publish Primitive

- [x] Stage pending family outputs before publication
- [x] Publish one manifest generation per ingest session
- [x] Prove readers do not observe partial lane generations from one product ingest

## Phase C: Product Input Contracts

- [x] Define the first Wax-owned raw document ingest request types
- [x] Keep compatibility import as an explicit bridge, not the primary product write contract
- [x] Add first runtime contract tests for raw document ingest

## Phase D: Raw Document And Text Builders

- [x] Build `Doc` segment payloads from raw documents
- [x] Build `Txt` segment payloads from raw text bodies
- [x] Remove raw-ingest dependence on `docs.ndjson` and `text_postings`

## Phase E: Explicit Vector Ingest

- [x] Define caller-provided vector ingest request types
- [x] Build `Vec` segment payloads from explicit vector input
- [x] Add carry-forward tests for sessions that change only vector families

## Phase F: Equivalence Verification

- [x] Add raw-versus-compatibility corpus equivalence tests
- [x] Prove equivalent inputs converge on the same doc-id bindings, previews, text hits, vector hits, and hybrid fusion
- [x] Keep benchmark verification attached to shared builder behavior wherever practical

## Phase G: Product Surface Migration

- [x] Add product-facing `ingest` CLI verbs after raw builders are real
- [x] Add raw ingest requests to broker/session surface
- [x] Add raw ingest requests to MCP surface
- [x] Keep `import-compat` as a legacy bridge rather than removing it immediately

## Phase H: Compatibility Bridge Convergence

- [x] Reuse shared raw builder logic from compatibility import wherever practical
- [x] Keep benchmark harness green while compatibility publication narrows
- [x] Prove equivalent raw and compatibility inputs converge on the same publish semantics

## Corrections And Wrong Turns

- [x] Corrected the next-step framing from "just add ingest verbs" to "build real raw-ingest builders and doc-id authority before product verb expansion."
- [x] Recorded that product write surfaces still depend on compatibility dataset-pack artifacts and should remain explicit about that until raw builders land.
- [x] Recorded that store-owned doc-id persistence is now a mandatory early slice rather than an implicit side effect of raw ingest.
- [x] Recorded that a staged pending-segment and single-manifest publish primitive must land before raw product ingest is exposed to readers.
- [x] Recorded that benchmark coverage must shift toward shared builder verification to avoid harness versus product publication drift.
- [x] Recorded that session memory pressure and manifest carry-forward semantics are design-level requirements, not late cleanup items.
- [x] Recorded that the first doc-id-authority implementation uses persisted binding maps embedded in the latest `Doc` segment rather than a separate core-level registry object.
- [x] Recorded that the first single-publish implementation lands as a shared `wax-v2-core` batch helper plus staged compatibility-segment preparation helpers, while the legacy compatibility import path remains intentionally unchanged.
- [x] Recorded that the first raw document ingest surface publishes only `Doc` plus `Txt` families from runtime-owned `NewDocument` inputs and intentionally keeps manifest dependency plus legacy compatibility import alongside it.
- [x] Recorded that the first raw vector ingest surface is runtime-only, accepts caller-provided vectors for existing documents, and intentionally requires a full manifest-sized vector set rather than pretending sparse partial updates already exist.
- [x] Recorded that runtime vector search must not depend on compatibility `query_vectors` sidecars once the caller already provides `vector_query`.
- [x] Recorded that the latest persisted `Vec` segment is authoritative for preview presence and must not silently fall back to removed preview sidecars.
- [x] Recorded that raw-versus-compatibility equivalence should compare runtime-observable behavior for the same corpus rather than overfitting to internal generation counts or legacy compatibility publication sequencing.
- [x] Recorded that before product raw-ingest verbs exist, product-surface equivalence should be exercised by reading raw-prepared stores through the existing CLI, broker, and MCP read surfaces rather than waiting for future write-verb migration.
- [x] Recorded that the first product raw-ingest migration should stay family-explicit (`docs` and `vectors`) instead of inventing a generic ingest envelope before the runtime has a truly shared multi-family write contract.
- [x] Recorded that the local temp-volume exhaustion was an environment-only verification blocker and that workspace-local `TMPDIR` reruns are the correct workaround while the convergence work continues.
- [x] Recorded that family-explicit product verbs can remain stable while runtime adds a separate full-snapshot shared publish primitive for equivalence and compatibility-bridge convergence.
- [x] Recorded that incremental product document ingest carries forward only active store `Doc` segments, not compatibility pack sidecars.
- [x] Recorded that raw publication must use the same generation or document-segment identity for validation and final publish preconditions.
- [x] Recorded that long-lived product sessions must refresh read state before serving reads after another handle may have written.
- [x] Recorded that MCP roots are constrained by an allowed-root boundary and raw document unknown top-level fields must survive MCP and broker ingest.
- [x] Recorded that explicit HNSW requests should fall back to exact-flat when declared sidecars are missing rather than failing lane load.
