# Wax v2 Text Lane

Status: Draft  
Date: 2026-03-29  
Scope: text-lane architecture and segment contract for Wax v2

## 1. Purpose

This document defines the text lane for Wax v2.

Its purpose is to fix the parts that should remain stable even if the first Rust-native backend is later replaced:

- what the text lane is responsible for
- what the text lane is not responsible for
- what a `txt` segment must guarantee
- how the text lane interacts with `doc` segments and unified search

This document does not choose the final library yet. It defines the contract the first library-backed implementation must satisfy.

## 2. Inputs

This text-lane design derives from:

- [2026-03-29-wax-v2-architecture.md](./2026-03-29-wax-v2-architecture.md)
- [2026-03-29-wax-v2-binary-format.md](./2026-03-29-wax-v2-binary-format.md)
- [2026-03-29-wax-v2-benchmark-plan.md](./2026-03-29-wax-v2-benchmark-plan.md)

Current design assumptions:

- Wax v2 is performance-first
- cold open and search p95 are the main gates
- text lane uses a Rust-native library backend in the first implementation
- the outer container and segment contract remain Wax-owned

## 3. Design Goals

### 3.1 Primary Goals

- fast cold open
- predictable text-query p95
- cheap filtering by Wax-owned `doc_id`
- backend replaceability

### 3.2 Secondary Goals

- explainable ranking
- stable snippet/preview behavior
- low open-time heap work

### 3.3 Non-Goals

- full-text engine feature completeness
- maximum indexing flexibility in v2 phase 1
- feature parity with arbitrary search libraries

## 4. Text Lane Responsibilities

The text lane is responsible for:

- tokenized term lookup
- postings retrieval
- BM25-style ranking
- returning matching `doc_id`s
- optional per-hit snippet inputs
- cooperating with Wax-level metadata filtering

The text lane is not responsible for:

- authoritative document storage
- final preview text ownership
- vector search
- advanced reranking
- structured memory in phase 1

This separation is deliberate.

## 5. Why the Text Lane Must Stay Separate from Docstore

The first implementation may use a Rust-native search library, but Wax should not let the search backend become the authority for document identity or stored records.

Hard rules:

- canonical `doc_id` is owned by Wax
- canonical timestamps are owned by Wax
- canonical payload references are owned by Wax
- canonical metadata filters are owned by Wax

The text lane only indexes and ranks.

This prevents future backend swaps from forcing a rewrite of the whole store.

## 6. Core Query Contract

The text lane should expose a logical interface equivalent to:

```text
open(segment_set) -> text_reader
search(query, limit, candidate_filter?) -> [text_hit]
search_batch(queries, limit, candidate_filter?) -> [[text_hit]]
```

Where each `text_hit` contains:

- `doc_id`
- `score`
- optional `snippet_ref`
- optional `term_match_summary`

The text lane should not return backend-internal row ids to the caller.

## 7. Candidate Filtering Contract

Wax-level metadata and doc filters are applied using Wax-owned `doc_id`s.

This is a critical contract.

The text lane must support at least one of these efficiently:

1. pre-filtered candidate restriction by `doc_id`
2. fast post-filtering on a bounded candidate set

The architecture must not assume "search first, filter everything later" if that causes large p95 spikes.

### 7.1 Required Filter Types

Phase 1 must support:

- include/exclude deleted docs
- explicit `doc_id` set restriction
- basic timestamp window cooperation through docstore

More advanced filters can remain in the Wax layer.

## 8. Ranking Contract

Wax v2 phase 1 should use simple, explainable lexical ranking.

Recommendation:

- BM25-style ranking
- deterministic tie-breaking
- no backend-private ranking assumptions leaking into the outer system

### 8.1 Ranking Stability Rules

The first implementation should define:

- whether scores are raw backend scores or normalized Wax scores
- deterministic tie-break order
- whether ranking diagnostics can map back to term/posting contributions

Recommended rule:

- use backend score as an internal signal
- rank output deterministically by `(score desc, doc_id asc)` unless a better justified tie-break exists

### 8.2 Guardrail

Do not let the rest of Wax depend on exact backend numeric score semantics.

Wax should depend on:

- ordering
- bounded candidate count
- optional diagnostics

This avoids lock-in.

## 9. Snippets and Stored Fields

This is one of the easiest places to paint the system into a corner.

### 9.1 Recommended Rule

The text lane may help produce snippet references, but canonical preview and stored document text should remain outside the backend-private index.

Meaning:

- backend may store term positions or snippet aids
- backend may emit offsets, windows, or lightweight snippet payloads
- final preview text should come from Wax-owned docstore data

### 9.2 Why

If snippets fully depend on backend-private stored fields:

- open path may become heavier
- backend replacement becomes harder
- the backend becomes a second document store

### 9.3 Phase 1 Contract

Phase 1 should support one of:

- `snippet_ref` with offsets/windows
- backend-produced snippet text marked as advisory

But Wax-owned docstore should remain the final authority for preview text when possible.

## 10. Text Segment Contract

The `txt` segment is a Wax-defined family.

Its contract is:

- identify which docs are indexed
- expose enough metadata to open cheaply
- expose enough metadata to validate and filter results
- optionally contain backend-private blobs

It must not require full backend reconstruction at open.

### 10.1 Minimum Segment Sections

A `txt` segment should have logical sections for:

- segment header
- lexicon or lexicon metadata
- postings or postings metadata
- doc-id map or doc-id lookup support
- optional snippet/stored-field support section
- optional backend blob

### 10.2 Required Open-Time Guarantees

At open:

- the segment can be recognized and validated cheaply
- its active document range is known
- its backend family and version are known
- its checksum can be validated

Open must not:

- rebuild token structures
- re-tokenize stored text
- reconstruct the entire index into heap memory by default

## 11. Segment Metadata That Must Be Wax-Owned

The following metadata must remain in Wax-owned fields, not only in a backend blob:

- indexed doc count
- term count or equivalent lexicon cardinality
- object checksum
- backend identifier
- backend version hint
- doc range
- snippet capability flags
- major/minor text segment version

This is necessary for tooling, migration, and later backend replacement.

## 12. Backend Blob Policy

Backend blobs are allowed, but tightly constrained.

### 12.1 Allowed

- library-generated compact search data
- auxiliary tables required by the backend
- compressed internal posting structures

### 12.2 Not Allowed

- using the backend blob as the only source of segment identity
- storing the only valid doc-id mapping only inside backend-private bytes
- requiring the whole blob to be deserialized into heap memory at open

### 12.3 Replaceability Rule

If a later backend is adopted:

- the `txt` family may gain a new family version
- `doc` and `vec` families should remain unaffected
- unified search should not need a redesign

## 13. Open-Time Behavior

Open-time behavior is a first-class design concern.

The text lane must support:

- cheap segment discovery from manifest descriptors
- mmap or lazy load where practical
- bounded heap allocations during open

### 13.1 Failure Modes to Avoid

- hidden backend warmup during first query
- rebuilding ranking state on open
- loading large stored-field blobs before they are needed

### 13.2 First-Query Guardrail

The first text query after open should not pay for:

- token dictionary rebuild
- snippet index build
- global cache reconstruction

If that happens, the backend contract is wrong, even if warm-query p95 looks good.

## 14. Batch Search

The internal text-lane interface should be batch-capable from the beginning.

This does not mean the first user-facing API must expose batch search.

It means the engine contract should support:

- multiple queries in one call
- shared lexicon access
- future SIMD or platform-specific batching

Batch capability is a future-proofing requirement.

## 15. Text Lane and Unified Search

The text lane participates in unified search as one lane among others.

Wax-level fusion should depend on:

- ordered candidate list
- stable `doc_id`
- bounded candidate count
- optional snippet refs
- optional diagnostics

Wax-level fusion should not depend on:

- the backend's internal posting representation
- the backend's persisted row numbering
- exact score comparability across backends

## 16. Performance Gates for the Text Lane

The text lane is considered successful in the current architecture if:

- it does not dominate cold open
- it does not dominate first-query cost
- it does not dominate text-only search p95 without clear evidence that the query itself is pathological

### 16.1 Rewrite Trigger

Consider replacing or redesigning the text backend if:

- text-only search becomes the persistent p95 bottleneck
- open-time text initialization is too expensive
- backend blobs force format compromises
- filtering by Wax `doc_id` is too expensive

## 17. Common Design Traps

### 17.1 Letting the Backend Own Identity

If the backend row id becomes the de facto `doc_id`, later replacement gets much harder.

### 17.2 Letting Snippets Become a Second Docstore

If all preview/stored-field behavior migrates into the search backend, the backend becomes harder to replace and the open path tends to get heavier.

### 17.3 Assuming BM25 Score Portability

Exact score values are often backend-specific.

Wax should treat score ordering as more important than raw value semantics.

### 17.4 Overfitting to the First Library

If the segment contract becomes just a wrapper around the first backend's private files, the architecture loses one of its main reasons for existing.

## 18. Recommended First Implementation Shape

For phase 1:

- choose a Rust-native library backend
- adapt it behind a narrow text-lane trait
- keep previews primarily in docstore
- keep `doc_id` mapping Wax-owned
- expose batch-capable internal interfaces
- benchmark open, first query, and text p95 before adding advanced text features

## 19. Recommended Next Docs

- `wax-v2-text-segment-layout.md`
- `wax-v2-query-parser-and-tokenization.md`
- `wax-v2-ranking-diagnostics.md`
- `sqlite-compat-vs-native-text-lane.md`
