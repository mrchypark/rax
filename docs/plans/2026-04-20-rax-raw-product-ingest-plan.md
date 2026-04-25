# Rax Raw Product Ingest Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace compatibility-pack product writes with a true raw-ingest path that accepts product-facing inputs, derives publishable segments inside Wax-owned crates, and commits one manifest generation per ingest session.

**Architecture:** Keep the completed staged roadmap intact and build a follow-on write path on top of it. Start with raw document ingest plus store-owned doc-id authority, then move text and vector builders behind the same ingest session, and only then expose final-looking product ingest verbs in `wax-cli`, broker, and MCP.

**Tech Stack:** Rust workspace crates, cargo tests, staged Wax v2 engine/runtime crates, existing benchmark harness

---

## Chunk 1: Store-Owned Doc-Id Authority

### Task 1: Persist canonical numeric doc ids in the store

**Files:**
- Modify: `crates/wax-v2-core/src/lib.rs`
- Modify: `crates/wax-v2-docstore/src/lib.rs`
- Modify: `crates/wax-v2-runtime/src/lib.rs`
- Create: `tests/contracts/raw_doc_id_authority_contract.rs`
- Modify: `docs/specs/2026-04-20-rax-raw-product-ingest-roadmap.md`
- Modify: `docs/todos/2026-04-20-rax-raw-product-ingest-todo.md`

- [x] Step 1: Add a failing contract test that proves doc ids remain stable across multiple raw ingest sessions and reopen cycles.
- [x] Step 2: Introduce a store-owned persisted doc-id registry or equivalent manifest-visible authority that allocates the next numeric doc id and maps external keys to canonical ids.
- [x] Step 3: Remove any raw-ingest dependence on compatibility file order as the long-term source of truth.
- [x] Step 4: Run `cargo test -p rax --test raw_doc_id_authority_contract` and the directly affected crate tests.

## Chunk 2: Single-Publish Primitive

### Task 2: Stage pending family outputs before one manifest publish

**Files:**
- Modify: `crates/wax-v2-core/src/lib.rs`
- Modify: `crates/wax-v2-runtime/src/lib.rs`
- Create: `tests/contracts/raw_ingest_single_publish_contract.rs`
- Modify: `docs/specs/2026-04-20-rax-raw-product-ingest-roadmap.md`
- Modify: `docs/todos/2026-04-20-rax-raw-product-ingest-todo.md`

- [x] Step 1: Add a failing contract test that proves one ingest session produces one new visible generation even when multiple families are built.
- [x] Step 2: Add a staged pending-segment boundary that can accumulate new family objects before publication.
- [x] Step 3: Publish one manifest generation per ingest session and keep current compatibility import behavior unchanged until raw builders adopt the primitive.
- [x] Step 4: Run `cargo test -p rax --test raw_ingest_single_publish_contract` and the directly affected crate tests.

## Chunk 3: Raw Document Contract Bootstrap

### Task 3: Define the first raw document ingest request and failing runtime contracts

**Files:**
- Modify: `crates/wax-v2-runtime/src/lib.rs`
- Create: `tests/contracts/raw_document_ingest_contract.rs`
- Modify: `docs/specs/2026-04-20-rax-raw-product-ingest-roadmap.md`
- Modify: `docs/todos/2026-04-20-rax-raw-product-ingest-todo.md`

- [x] Step 1: Add a failing contract test that creates a store, submits raw document inputs, and expects search to work after reopen without dataset-pack sidecars.
- [x] Step 2: Define the first Wax-owned raw ingest request types in `wax-v2-runtime`, keeping the contract honest about what is raw product input versus compatibility bridge input.
- [x] Step 3: Add the minimal runtime ingest-session API shape on top of the staged single-publish primitive without removing `import_compatibility_snapshot`.
- [x] Step 4: Run `cargo test -p rax --test raw_document_ingest_contract`.

## Chunk 4: Raw Document And Text Builders

### Task 4: Build `Doc` and `Txt` segments from raw document input

**Files:**
- Modify: `crates/wax-v2-docstore/src/lib.rs`
- Modify: `crates/wax-v2-text/src/lib.rs`
- Modify: `crates/wax-v2-runtime/src/lib.rs`
- Create: `tests/contracts/raw_text_publish_contract.rs`
- Modify: `docs/todos/2026-04-20-rax-raw-product-ingest-todo.md`

- [x] Step 1: Add a failing contract test that ingests raw documents, publishes once, and serves text search plus preview/metadata after reopen.
- [x] Step 2: Add the first Wax-owned builder path from raw document bodies and metadata into publishable `Doc` and `Txt` segment payloads.
- [x] Step 3: Refresh runtime read state through the newly published generation without relying on `docs.ndjson` or `text_postings`.
- [x] Step 4: Run `cargo test -p wax-v2-docstore`, `cargo test -p wax-v2-text`, and the targeted contract test.

## Chunk 5: Explicit Vector Ingest

### Task 5: Add honest caller-provided vector ingest without hidden embedding

**Files:**
- Modify: `crates/wax-v2-vector/src/lib.rs`
- Modify: `crates/wax-v2-runtime/src/lib.rs`
- Create: `tests/contracts/raw_vector_ingest_contract.rs`
- Modify: `docs/specs/2026-04-20-rax-raw-product-ingest-roadmap.md`
- Modify: `docs/todos/2026-04-20-rax-raw-product-ingest-todo.md`

- [x] Step 1: Add a failing contract test for ingesting explicit vectors alongside raw documents and serving vector search after reopen.
- [x] Step 2: Define the first explicit external-vector ingest request type and keep it separate from any future hidden embedding policy.
- [x] Step 3: Build `Vec` segments from those inputs and prove unaffected families carry forward correctly when only vector data changes.
- [x] Step 4: Run `cargo test -p wax-v2-vector` and the targeted contract test.

## Chunk 6: Equivalence Verification

### Task 6: Keep raw ingest and compatibility import behavior aligned

**Files:**
- Modify: `tests/contracts/product_cli_contract.rs`
- Modify: `tests/contracts/broker_session_contract.rs`
- Modify: `tests/contracts/mcp_surface_contract.rs`
- Create: `tests/contracts/raw_compat_equivalence_contract.rs`
- Modify: `crates/wax-v2-runtime/src/lib.rs`
- Modify: compatibility-bridge code as needed
- Modify: `docs/todos/2026-04-20-rax-raw-product-ingest-todo.md`

- [x] Step 1: Add a failing equivalence test that compares packer-plus-compat-import versus raw ingest for the same corpus.
- [x] Step 2: Prove doc-id bindings, preview hydration, text hits, vector hits, and hybrid fusion stay aligned for equivalent inputs.
- [x] Step 3: Update product-facing contract tests so they exercise both the raw path and the remaining compatibility bridge where appropriate.
- [x] Step 4: Run the targeted contract tests and `cargo test --workspace --quiet`.

## Chunk 7: Product Surface Migration

### Task 7: Move product verbs from `import-compat` to raw ingest

**Files:**
- Modify: `crates/wax-cli/src/main.rs`
- Modify: `crates/wax-v2-broker/src/lib.rs`
- Modify: `crates/wax-v2-mcp/src/lib.rs`
- Create: `tests/contracts/product_raw_ingest_cli_contract.rs`
- Create: `tests/contracts/mcp_raw_ingest_contract.rs`
- Modify: `docs/todos/2026-04-20-rax-raw-product-ingest-todo.md`

- [x] Step 1: Add failing contract tests for `wax ingest ...` and for broker/MCP raw-ingest requests.
- [x] Step 2: Expose product-facing raw ingest verbs and requests only after the runtime builders and session commit path exist.
- [x] Step 3: Keep `import-compat` available but clearly legacy for benchmark and fixture workflows.
- [x] Step 4: Run the targeted CLI, broker, and MCP tests plus `cargo test --workspace --quiet`.

## Chunk 8: Compatibility Bridge Convergence

### Task 8: Reuse shared builders from the compatibility bridge

**Files:**
- Modify: `crates/wax-v2-runtime/src/lib.rs`
- Modify: `crates/wax-v2-docstore/src/lib.rs`
- Modify: `crates/wax-v2-text/src/lib.rs`
- Modify: `crates/wax-v2-vector/src/lib.rs`
- Modify: compatibility-only benchmark bridge code as needed
- Modify: `docs/todos/2026-04-20-rax-raw-product-ingest-todo.md`

- [x] Step 1: Add a failing regression test that proves compatibility import and raw ingest converge on the same publish semantics for equivalent inputs.
- [x] Step 2: Translate compatibility dataset-pack inputs into the shared builder path wherever practical.
- [x] Step 3: Leave only a narrow benchmark-only compatibility shim where raw equivalents still do not exist.
- [x] Step 4: Run the directly affected tests and `cargo test --workspace --quiet`.

## Notes

- Do not ship final-looking `wax ingest` CLI verbs before raw builders and store-owned doc-id authority exist.
- Do not ship final-looking raw runtime write methods before the staged single-publish primitive exists; partially visible lane generations are not acceptable product behavior.
- The first vector ingest slice must stay honest: caller-provided vectors are acceptable, hidden embedding generation is not part of this plan.
- The benchmark harness should remain green, but it should increasingly verify shared raw builder behavior instead of a permanently separate compatibility publication path.
- Raw multimodal asset ingest is already bootstrapped in `wax-v2-multimodal`; if it needs to join the unified ingest session later, do that only after document/text/vector raw ingest is stable.
- Task 1 is now complete via persisted binding maps in the latest `Doc` segment plus the root `raw_doc_id_authority_contract`; the next executable task is the staged single-publish primitive.
- Task 2 is now complete via `wax-v2-core::publish_segments`, staged compatibility segment-preparation helpers, and the root `raw_ingest_single_publish_contract`; the next executable task is raw document contract bootstrap.
- Tasks 3 and 4 are now complete via `NewDocument`, `publish_raw_documents`, raw `Doc` segment preparation in docstore, and raw `Txt` segment preparation in text.
- Task 5 is now complete via `NewDocumentVector`, `publish_raw_vectors`, raw `Vec` segment preparation in vector, and a runtime vector-load path that no longer requires compatibility `query_vectors` sidecars.
- Task 6 is now complete via runtime-level raw-versus-compatibility equivalence, product read-surface equivalence coverage, and fresh targeted verification.
- Task 7 is now complete: `wax ingest docs`, `wax ingest vectors`, matching broker/MCP raw mutation requests, and a fresh workspace run all pass once verification uses a workspace-local `TMPDIR` instead of the exhausted system temp volume.
- Task 8 is now complete: raw full-snapshot publish exists as a shared runtime primitive, `import_compatibility_snapshot` delegates through that same one-generation raw snapshot path, and the remaining compatibility-only logic is limited to dataset-pack translation into raw inputs.
- There is no remaining unchecked item in the current raw product-ingest follow-on roadmap.

## Post-Completion Review Hardening

- Treat active store segments, not compatibility pack sidecars, as the carry-forward source for incremental product ingest.
- Keep `publish_raw_snapshot` as the explicit full-replacement primitive; use `publish_raw_documents` only for incremental document updates against existing raw store documents.
- Guard raw document and vector publication with store-generation or document-segment preconditions from the state that was actually validated.
- Refresh long-lived runtime, broker, structured-memory, and multimodal sessions before read operations that can be affected by another handle's write.
- Keep MCP session roots constrained to a configured allowed root, and preserve unknown top-level raw document fields through MCP and broker boundaries.
- Fall back to exact-flat vector search when HNSW sidecars are declared but missing, including explicit HNSW mode.
