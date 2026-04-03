# Wax v2 Vector Lane

Status: Draft  
Date: 2026-03-29  
Scope: vector-lane architecture and segment contract for Wax v2

## 1. Purpose

This document defines the vector lane for Wax v2.

Its purpose is to fix the parts that should remain stable even if the first Rust HNSW backend is later replaced:

- what the vector lane is responsible for
- what the vector lane is not responsible for
- what a `vec` segment must guarantee
- how the vector lane interacts with embedding providers, `doc` segments, and unified search

This document does not choose the final Rust crate yet. It defines the contract the first Rust HNSW-backed implementation must satisfy.

## 2. Inputs

This vector-lane design derives from:

- [2026-03-29-wax-v2-architecture.md](./2026-03-29-wax-v2-architecture.md)
- [2026-03-29-wax-v2-binary-format.md](./2026-03-29-wax-v2-binary-format.md)
- [2026-03-29-wax-v2-benchmark-plan.md](./2026-03-29-wax-v2-benchmark-plan.md)
- upstream Wax vector modules:
  - `WaxVectorSearch`
  - `WaxVectorSearchMiniLM`

Current design assumptions:

- Wax v2 is performance-first
- cold open and search p95 are the main gates
- vector search uses a Rust HNSW backend in the first implementation
- embedding generation and vector indexing are separate subsystems
- the outer container and segment contract remain Wax-owned

## 3. Design Goals

### 3.1 Primary Goals

- fast cold open
- predictable vector-query p95
- cheap mapping from ANN hits to Wax-owned `doc_id`
- backend replaceability without re-embedding the corpus

### 3.2 Secondary Goals

- bounded open-time heap work
- exact rerank support on bounded candidate sets
- future compatibility with platform-specific accelerators

### 3.3 Non-Goals

- arbitrary ANN feature parity across libraries
- multimodal orchestration in phase 1
- platform-specific acceleration in phase 1
- aggressive vector compression in phase 1

## 4. Vector Lane Responsibilities

The vector lane is responsible for:

- approximate nearest-neighbor retrieval
- vector-id to Wax `doc_id` resolution
- score production for vector hits
- optional bounded exact rerank using stored vectors
- cooperating with Wax-level metadata filtering

The vector lane is not responsible for:

- authoritative document storage
- embedding generation
- query embedding policy
- final hybrid fusion
- advanced reranking beyond vector-local scoring

This separation is deliberate.

## 5. Why Vector Search Must Stay Separate from Embedding Runtime

Upstream Wax combines vector search with on-device embedding providers in one broader subsystem. Wax v2 should keep these more strongly separated.

Hard rules:

- canonical `doc_id` is owned by Wax
- embedding model identity is owned by Wax
- vector dimensions and normalization policy are declared by Wax-owned metadata
- the vector backend does not decide how embeddings are produced

The vector lane indexes and searches vectors. It does not own the embedding runtime.

This keeps future backend work open:

- swap HNSW library without changing embedding generation
- add platform-specific vector backends later
- migrate to a different embedding family without rewriting the whole container

## 6. Why Backend Graph Must Stay Separate from Canonical Vector Payload

This is the most important design choice in the vector lane.

Wax v2 should not let a backend-private HNSW blob become the only durable copy of the corpus vectors.

Hard rules:

- canonical stored vectors remain recoverable without interpreting backend-private graph internals
- canonical `doc_id` mapping remains outside backend-private graph state
- exact rerank must remain possible without backend-private APIs

Recommended rule:

- every `vec` segment contains a Wax-owned vector payload block
- every `vec` segment may also contain a backend-private ANN acceleration blob

This makes backend replacement expensive in I/O, but not expensive in product architecture.

## 7. Core Query Contract

The vector lane should expose a logical interface equivalent to:

```text
open(segment_set) -> vector_reader
search(query_vector, limit, candidate_filter?) -> [vector_hit]
search_batch(query_vectors, limit, candidate_filter?) -> [[vector_hit]]
```

Where each `vector_hit` contains:

- `doc_id`
- `score`
- optional `distance`
- optional `segment_ref`

The vector lane should not return backend-internal node ids to the caller.

## 8. Candidate Filtering Contract

Wax-level metadata and doc filters are applied using Wax-owned `doc_id`s.

This is a critical contract.

The vector lane must support at least one of these efficiently:

1. pre-filtered candidate restriction by `doc_id`
2. bounded oversample plus fast post-filtering on candidate hits

The architecture must not assume "ANN first, filter everything later" if that causes broad-query p95 spikes.

If filter selectivity is expected to be very low, the planner should be allowed to avoid the vector lane entirely or require a more restrictive candidate set before ANN search begins.

### 8.1 Required Filter Types

Phase 1 must support:

- include/exclude deleted docs
- explicit `doc_id` set restriction
- basic timestamp-window cooperation through docstore

More advanced metadata filters can remain in the Wax layer.

### 8.2 Guardrail for High-Selectivity Filters

Bounded oversample plus post-filter is not sufficient as the only strategy.

For highly selective filters, such as rare tags or small explicit subsets, Wax must support at least one of:

- true pre-restricted ANN search over a provided candidate set
- planner-level fallback that reduces or skips ANN work until the candidate set is small enough

The system must not rely on unbounded oversampling to recover hits under rare-filter workloads.

## 9. Metric and Normalization Contract

Wax v2 must treat metric semantics as an explicit contract, not an implicit backend choice.

Every active vector segment must declare:

- metric family
- embedding dimensions
- normalization expectation
- numeric element type

### 9.1 Phase 1 Recommendation

Phase 1 should standardize on:

- `Float32` vectors
- little-endian encoding
- cosine-oriented search
- explicit normalization flag in segment metadata

This leaves room for later dot-product or L2 variants without making v2 phase 1 more complex than necessary.

### 9.2 Guardrail

Do not let the rest of Wax depend on exact backend score semantics.

Wax should depend on:

- ordering
- bounded candidate count
- known metric family
- deterministic tie-breaks

Recommended tie-break rule:

- `(score desc, doc_id asc)`

unless a stronger justification is proven later.

## 10. Exact Rerank Contract

Approximate retrieval is allowed. Unbounded score ambiguity is not.

Phase 1 should support bounded exact rerank on the top candidate set when needed for:

- hybrid fusion stability
- metric consistency across backends
- future GPU or flat-search fallback

Exact rerank should operate over the Wax-owned vector payload block, not a backend-private graph API.

This is one of the main reasons to keep canonical vectors outside the graph blob.

## 11. Vector Segment Contract

The `vec` segment is a Wax-defined family.

Its contract is:

- identify which docs are indexed
- expose enough metadata to open cheaply
- expose enough metadata to validate query vectors
- support exact rerank from Wax-owned vector payloads
- optionally contain backend-private acceleration blobs

It must not require full graph reconstruction at open.

### 11.1 Minimum Segment Sections

A `vec` segment should have logical sections for:

- segment header
- doc-id block
- vector payload block
- optional backend-private ANN blob
- optional auxiliary rerank or routing metadata

### 11.2 Required Open-Time Guarantees

At open:

- the segment can be recognized and validated cheaply
- its active document range is known
- its metric and dimension contract are known
- its checksum can be validated
- its vector payload block can be mapped or read without rebuild
- its ANN blob can be opened without graph reconstruction by default

Open must not:

- recompute embeddings
- rebuild the HNSW graph
- hydrate all nodes into heap memory by default
- scan all vectors to reconstruct id mappings

## 12. Segment Metadata That Must Be Wax-Owned

The following metadata must remain in Wax-owned fields, not only in a backend blob:

- indexed doc count
- dimensions
- metric family
- normalization flag
- numeric element type
- object checksum
- backend identifier
- backend version hint
- doc range
- exact-rerank capability flag
- major/minor vector segment version
- deletion mask or live-doc accounting metadata

This is necessary for tooling, validation, migration, and backend replacement.

## 13. Canonical Vector Payload Rules

The canonical vector payload is the Wax-owned representation of stored embeddings for the segment.

### 13.1 Phase 1 Recommendation

Phase 1 should use:

- row-major `Float32` vector storage
- one contiguous vector block per segment
- one parallel `doc_id` block per segment
- page-aligned large payloads when possible
- per-vector alignment suitable for SIMD-friendly rerank paths

Recommended minimum:

- 32-byte alignment for vector rows when feasible on disk
- section boundaries aligned so mmap-backed reads do not straddle small unaligned fragments unnecessarily

This keeps rerank, scan-based fallback, and migration simple.

### 13.2 Why This Is Worth the Space

The storage overhead is justified because it preserves:

- backend independence
- deterministic rerank
- future flat-search fallback
- migration without re-embedding

If benchmarks later prove this cost is too high, compression or quantized payload variants can be added as a new segment version.

## 14. Backend Blob Policy

Backend blobs are allowed, but tightly constrained.

### 14.1 Allowed

- persisted HNSW graph state
- neighbor lists
- backend-private routing structures
- backend-private compact lookup metadata

### 14.2 Not Allowed

- owning the only durable `doc_id` mapping
- requiring the original vectors to be absent
- forcing full graph hydration into heap memory at open
- storing mutable runtime cache state as committed durable format

The backend blob is an accelerator, not the canonical store.

## 15. Multi-Segment Search and Compaction

Wax v2 will eventually have multiple active `vec` segments.

The vector lane must therefore assume:

- search can span multiple immutable segments
- results need per-segment merging
- deleted docs may still exist in older segments but be masked by Wax-level state

Phase 1 recommendation:

- publish immutable segment batches
- merge search results in Wax-owned query orchestration
- compact older segments in the background

Compaction may rebuild ANN blobs, but it must not change the logical query contract.

### 15.1 Dead-Node Guardrail

Immutable segments do not remove the need for ANN hygiene.

If deleted or superseded docs remain represented in active ANN blobs for too long:

- traversal waste increases
- oversample pressure increases
- vector-query p95 becomes harder to predict

Wax should therefore track live-doc ratio or tombstone ratio per `vec` segment and trigger compaction before dead-node accumulation becomes a dominant query cost.

### 15.2 Segment-Count Guardrail

Search fan-out across too many active `vec` segments will eventually dominate the backend search cost.

Wax should define a maximum recommended active `vec` segment count per search tier and trigger compaction or tiering before merge overhead becomes a primary contributor to p95.

## 16. Write and Commit Rules

The hot path is read-mostly. The write path should respect that.

Recommended rules:

- build new `vec` segments off the active read path
- write canonical vectors first
- build ANN blob from those vectors
- publish only through manifest switch

This keeps open and search simple:

- readers see immutable snapshots
- writers never mutate the active graph in place
- crash recovery remains manifest-based, not graph-repair-based

## 17. Interaction with Unified Search

The vector lane provides candidates. Unified search decides how to combine them.

That means:

- vector lane returns hits in its own stable ordering
- hybrid fusion weights are not baked into vector segment format
- cross-lane normalization rules belong above the vector backend

This avoids coupling the vector backend to text ranking policy.

## 18. Rewrite Triggers

The first Rust HNSW-backed implementation should be replaced only if benchmarks show one of these is persistently true:

- cold open requires large graph hydration
- broad filtered queries cause large oversample waste
- exact rerank cannot be done cheaply from stored vectors
- multi-segment merge cost dominates vector-query p95
- resident memory per indexed vector is too high on iOS-class devices
- backend persistence format blocks future accelerator backends

Until then, the vector lane contract should remain stable and the backend should be treated as replaceable.

## 19. Summary

The Wax v2 vector lane should be built around four stable rules:

1. Wax owns `doc_id`, embedding identity, and segment metadata
2. canonical vectors remain available outside backend-private graph blobs
3. open must not rebuild the ANN graph
4. unified search depends on hit ordering and bounded candidate sets, not backend score internals

If those four rules hold, Wax can start with a Rust HNSW backend now and still retain room for future backend swaps, exact rerank improvements, and platform-specific accelerators later.
