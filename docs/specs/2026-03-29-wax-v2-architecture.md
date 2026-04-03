# Wax v2 Architecture

Status: Draft  
Date: 2026-03-29  
Scope: performance-first Wax v2 architecture, intentionally not compatible with Wax v1 internals

## 1. Decision Summary

This document captures the currently approved direction for Wax v2.

- Wax v2 prioritizes performance over Wax v1 internal compatibility.
- The first two benchmark priorities are:
  - cold open
  - search p95
- Text search uses a Rust-native library backend in the first implementation.
- Vector search uses Rust HNSW in the first implementation.
- Platform-specific APIs and backend specializations are deferred.
- Advanced RAG, structured memory, and complex reranking are out of the first performance-critical scope.
- If a subsystem becomes the dominant bottleneck later, it may be replaced behind a stable interface rather than over-optimizing the first implementation.

## 2. Goals

### 2.1 Primary Goals

- beat or justify replacement against the current Wax experience on iOS for cold open and search p95
- preserve a single-file distribution model
- keep the read path minimal and predictable
- keep write/commit append-friendly
- keep backend boundaries clean enough that future Apple or non-Apple specialized backends can be added without rewriting the whole system

### 2.2 Secondary Goals

- remain cross-platform first
- keep the core implementation understandable and benchmarkable
- avoid accidental lock-in to one search library's private on-disk format

### 2.3 Non-Goals for v2 Phase 1

- Wax v1 binary compatibility
- structured memory parity
- full RAG orchestration parity
- platform-specific acceleration
- maximally optimized ingest throughput

## 3. External Critique Incorporated

An external-model critique was used as a discussion partner before fixing this direction. The main takeaways incorporated here are:

- cold open will fail if v2 requires full HNSW hydration or index rebuild at open
- append-friendly write paths can still become read-hostile if segment metadata grows without strong manifest rules
- a performance-first v2 must avoid binding the container format directly to a single text library or ANN library's opaque bytes
- iOS failure modes are likely to come from memory pressure, mmap/page-fault behavior, and excessive per-query allocations rather than only raw compute
- the active manifest should be zero-copy or near-zero-copy readable
- in-flight readers should not block on new commit publication
- internal search interfaces should be batch-capable from the start

This architecture is designed to avoid those traps.

## 4. Recommended Approach

Three approaches were considered:

1. aggressive custom engine from day one
2. pragmatic v2 with library-backed text and Rust HNSW
3. build-up approach with a simpler vector path first

The selected approach is `pragmatic v2`.

Reasoning:

- it matches the current decisions already made
- it keeps the file format under Wax control
- it keeps the first implementation focused on the read path
- it allows later backend replacement without changing the outer system model

## 5. Architecture Principles

### 5.1 The Container Owns the Format

Wax v2 must define its own segment and manifest format.

That means:

- the outer `.wax` file format is not the private on-disk format of the text library
- the outer `.wax` file format is not the private on-disk format of the HNSW library
- backends may populate segments, but the container owns naming, versioning, checksums, and commit semantics

### 5.2 Open Must Be Cheap

Opening a store must not require:

- replaying a large WAL
- rebuilding postings
- rebuilding the vector graph
- loading every segment fully into heap memory

Open should load only:

- superblock
- active manifest
- segment table
- enough metadata to route the first query

### 5.3 Search Must Be Mostly Read-Only

The hot search path should read immutable segment structures and avoid mutable shared state.

That implies:

- immutable segments
- append-only or replace-by-manifest commit
- background compaction instead of in-place mutation

### 5.4 Backends Are Replaceable

Text and vector engines must sit behind stable interfaces.

The interfaces must separate:

- logical search features
- runtime backend implementation
- persisted segment representation

## 6. High-Level System Layout

Wax v2 is organized into five layers:

1. `wax-v2-core`
   - file I/O
   - superblock/manifest
   - segment registry
   - atomic commit
2. `wax-v2-docstore`
   - document metadata
   - payload references
   - lightweight filters
3. `wax-v2-text`
   - text search backend abstraction
   - Rust-native backend adapter
4. `wax-v2-vector`
   - vector backend abstraction
   - Rust HNSW backend adapter
5. `wax-v2-search`
   - unified query planning
   - hybrid fusion
   - diagnostics and ranking control

The first phase excludes dedicated structured memory and advanced RAG layers.

## 7. Proposed `.wax` v2 File Structure

This is a proposed architecture-level layout, not a byte-finalized binary spec.

```text
┌────────────────────────────────────────────────────────────┐
│ Superblock A                                              │
├────────────────────────────────────────────────────────────┤
│ Superblock B                                              │
├────────────────────────────────────────────────────────────┤
│ Commit Journal / Tiny Recovery Area                       │
├────────────────────────────────────────────────────────────┤
│ Immutable Segment Region                                  │
│  ├─ Docstore segments                                     │
│  ├─ Text index segments                                   │
│  ├─ Vector graph/vector segments                          │
│  └─ Optional future segment kinds                         │
├────────────────────────────────────────────────────────────┤
│ Active Manifest                                           │
├────────────────────────────────────────────────────────────┤
│ Optional older manifests / compaction metadata            │
└────────────────────────────────────────────────────────────┘
```

### 7.1 Superblocks

Two superblocks provide atomic pointer switching.

They should contain:

- file magic
- format version
- active manifest offset and length
- generation
- checksum
- feature flags

The role is similar to the v1 header pair, but the semantics are simpler:

- point to the latest complete manifest
- avoid long recovery work

### 7.2 Commit Journal

The journal is intentionally small.

It exists to make manifest publication crash-safe, not to act as a long-lived replay log.

Design rule:

- journal size must stay bounded and independent of document corpus size

This is a guardrail against the v1-style replay cost growing into the open path.

### 7.3 Immutable Segment Region

All query-serving data lives in immutable segments.

Initial segment families:

- `doc`
  - document ids
  - timestamps
  - metadata filters
  - payload references
- `txt`
  - postings
  - lexicon
  - optional stored snippets or preview data
- `vec`
  - persisted HNSW graph
  - vector payload blocks
  - id mapping

### 7.4 Manifest

The manifest is the authoritative snapshot of the active store state.

It must contain:

- active segment list
- logical segment generations
- document id ranges
- text/vector backend identifiers
- checksums and sizes
- compaction lineage

This keeps open-time work small:

- read superblock
- read manifest
- open active segments

Guardrails:

- the manifest should be designed for zero-copy or near-zero-copy loading
- open-time manifest work must stay effectively bounded relative to active segment count, not corpus size

## 8. Document Storage Model

The docstore must remain logically separate from text and vector indexing.

Each stored item should have:

- stable `doc_id`
- timestamps
- lightweight metadata fields
- payload pointer or inline short text policy
- deletion/tombstone state

Important guardrail:

- `doc_id` must be a Wax-controlled identifier, not a backend-internal row id

This avoids coupling search library internals to the file format.

## 9. Text Lane

### 9.1 First Implementation Direction

The first implementation uses a Rust-native library backend.

This is the chosen compromise:

- not a custom engine yet
- not SQLite
- not tied to one opaque library format at the container level

### 9.2 Responsibilities

The text lane is responsible for:

- term dictionary / lexicon
- postings lookup
- BM25-style ranking
- optional snippet support
- filtering by doc ids returned from the manifest/docstore layer

### 9.3 Persistence Rule

The persisted text segment must be a Wax-defined segment family.

Allowed:

- library-assisted build process
- internal subfiles or blobs packaged inside the segment

Not allowed:

- defining the whole outer `.wax` format as "whatever the text library writes to disk"

### 9.4 Future Rewrite Rule

If the Rust-native library later becomes the bottleneck, the rewrite boundary is:

- keep the `txt` segment contract if possible
- replace only the builder/runtime backend

If that proves impossible, the segment family itself may be versioned without disturbing `doc` and `vec`.

## 10. Vector Lane

### 10.1 First Implementation Direction

The first implementation uses Rust HNSW.

Reason:

- search p95 matters more than ingest simplicity
- vector-only or hybrid latency should not depend on brute-force scan once the corpus grows

### 10.2 Responsibilities

The vector lane owns:

- vector storage
- graph adjacency
- entry points / layer metadata
- doc_id mapping
- ANN search primitives

### 10.3 Cold Open Guardrail

Cold open must not rebuild HNSW in memory from raw vectors.

The graph and required metadata must be persisted in a form that can be:

- memory-mapped directly, or
- loaded in bounded slices with predictable cost

This guardrail is mandatory.

### 10.4 Backend Independence

The vector lane must expose a trait like:

- load segment
- search
- search_batch
- build/update segment
- expose diagnostics

The container stores Wax-owned metadata around the backend, so a future backend swap remains possible.

## 11. Query Execution Path

The first query path should be:

1. open superblock and manifest
2. open doc/text/vector active segments
3. parse query
4. run text lane
5. run vector lane
6. fuse results
7. fetch previews from docstore

### 11.1 Initial Search Features

Phase 1 search should include only:

- text search
- vector search
- hybrid fusion
- simple metadata filtering
- diagnostics

Delayed:

- structured memory lane
- timeline lane if it complicates core design
- complex reranking
- answer-focused context assembly

### 11.2 Batch-First Interface Rule

Even if Phase 1 product behavior focuses on one query at a time, the internal engine boundaries should support batch execution.

Required internal capability:

- text batch search hook
- vector batch search hook
- hybrid coordinator that can consume batched lane results later

This keeps future hardware-specific or alternate backends from forcing an API redesign.

### 11.3 Fusion

The default should remain simple and inspectable.

Recommendation:

- keep weighted RRF or another deterministic fusion rule
- keep diagnostics for lane contribution

Even in v2, ranking should be explainable before it becomes clever.

## 12. Write and Commit Path

The write path should optimize for cheap publication of new immutable state.

Recommended pattern:

1. accumulate or batch incoming docs
2. build new `doc`, `txt`, and `vec` segments off the active read path
3. write segment bytes to the immutable segment region
4. write a new manifest that references the new active segment set
5. atomically switch superblock generation to the new manifest

This makes reads stable while writes happen.

### 12.1 Append-Friendly Means

For v2, append-friendly does not mean "long replay log forever."

It means:

- new segment bytes append
- manifest append
- atomic pointer switch

### 12.2 Compaction

Compaction is a background maintenance operation.

It should:

- merge small segments
- drop deleted docs
- rebalance text/vector segment counts
- publish a new manifest atomically

Queries should not block on compaction.

### 12.3 Snapshot Isolation

Commit publication must use snapshot isolation.

Required behavior:

- a query runs against the manifest generation it started with
- a new commit publishes a new manifest generation
- readers switch generations only between queries

This prevents commit spikes from contaminating search p95.

## 13. Benchmark Gates

These gates must be in the first architecture spec, not added later.

### 13.1 Cold Open Gate

Measure on-device:

- empty store open p50/p95
- medium store open p50/p95
- large store open p50/p95
- first query after open p50/p95

### 13.2 Search Gate

Measure separately:

- text-only query p50/p95
- vector-only query p50/p95
- hybrid query p50/p95
- metadata-filtered query p50/p95

### 13.3 Memory Gate

Track:

- resident memory after open
- resident memory during first query
- resident memory after repeated queries

This matters because iOS failure modes often come from memory pressure before pure compute.

### 13.4 Regression Gate

Every architectural change should be evaluated against:

- cold open regression
- first-query regression
- warm-query regression
- compaction impact on read latency
- memory growth across repeated manifest generations

## 14. Architecture Guardrails

These are the hard rules to avoid painting the system into a corner.

### 14.1 Do Not Bind the Container to Backend Private Formats

Wax v2 owns:

- file magic
- manifest
- segment metadata
- doc ids
- checksums
- generation semantics

Backends may assist with segment building, but they do not define the whole store.

### 14.2 Do Not Require Open-Time Rebuilds

Any design that requires:

- text index rebuild on open
- vector graph rebuild on open
- replaying a large mutation history on open

fails the v2 performance goal.

Related rule:

- the active manifest and active segment metadata must be directly readable without reconstructing the whole catalog into heap objects

### 14.3 Keep Search Features Layered

Do not entangle:

- text retrieval
- vector retrieval
- advanced ranking
- RAG assembly
- future structured memory

The first version should keep these as explicit layers, not a single monolith.

### 14.4 Keep Segment Kinds Versioned

Each segment family must be independently versioned.

At minimum:

- `doc-v1`
- `txt-v1`
- `vec-v1`

This allows future subsystem rewrites without throwing away the entire container design.

### 14.5 Prefer Page-Aligned Large Segments

Large segments should be page-aligned where practical.

Reason:

- better mmap behavior
- lower risk of page-fault amplification on iOS-class devices

### 14.6 Prefer Mmap-First Vector Access

The default vector path should work from persisted graph/vector structures without mandatory full heap hydration.

Small caches are acceptable.

Full open-time graph rebuilds are not.

## 15. Likely Risks

### 15.1 Text Backend Lock-In

Risk:

- first Rust-native library choice becomes de facto permanent

Mitigation:

- Wax-owned segment metadata
- Wax-owned doc ids
- narrow backend trait

### 15.2 HNSW Open Cost

Risk:

- persisted graph still loads too much state eagerly

Mitigation:

- benchmark cold open from the start
- require persisted graph metadata that supports direct load

### 15.3 Compaction Debt

Risk:

- append-friendly writes create too many small segments and hurt query p95

Mitigation:

- define compaction triggers in the architecture
- expose segment-count diagnostics from day one

### 15.4 iOS Memory Pressure

Risk:

- mmap and page-fault behavior look good in microbenchmarks but fail under app memory pressure

Mitigation:

- benchmark on real iOS devices
- include resident-memory gates, not just latency gates

### 15.5 Adoption Friction

Risk:

- dropping v1 internal compatibility slows adoption if migration is missing

Mitigation:

- provide a dedicated v1-to-v2 migration utility outside the hot search path
- keep migration concerns out of the v2 runtime architecture

## 16. Phase Plan

### Phase 1

- finalize v2 container architecture
- define superblock, manifest, and segment families
- implement doc/text/vector minimal path
- establish benchmark harness

### Phase 2

- tune cold open
- tune hybrid search p95
- add compaction
- validate iOS memory behavior

### Phase 3

- revisit structured memory
- revisit advanced reranking
- add optional platform-specific backends only if benchmark data justifies them
- ship a migration utility if not already available

## 17. Recommended Next Docs

The next documents should be:

- `wax-v2-binary-format.md`
  - byte-level superblock and manifest spec
- `wax-v2-text-lane.md`
  - text segment contract and backend boundary
- `wax-v2-vector-lane.md`
  - HNSW persistence contract and open-time guarantees
- `wax-v2-benchmark-plan.md`
  - exact datasets, devices, and pass/fail criteria
