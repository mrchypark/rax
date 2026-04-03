# Wax Upstream Benchmark Analysis

Status: Draft  
Date: 2026-03-29  
Scope: what upstream Wax actually does today, how it achieves its published numbers, and what should be benchmarked against it

## 1. Purpose

This document answers two questions:

1. how upstream Wax actually solves open, text search, vector search, and hybrid search
2. what "benchmarking against Wax" should mean for the Rust rewrite

This is not a rewrite proposal. It is an upstream analysis document.

## 2. Evidence Levels

This document separates claims into three classes:

- `Measured`
  - directly stated in upstream benchmark reports
- `Code-evidenced`
  - directly supported by current upstream source code
- `Inference`
  - reasoned from code structure and published results

This matters because upstream Wax publishes strong performance claims, but not every claim is backed by the same kind of evidence.

## 3. Primary Sources

Measured benchmark source:

- [2026-03-06-performance-results.md](https://github.com/christopherkarani/Wax/blob/main/Resources/docs/benchmarks/2026-03-06-performance-results.md)

Core implementation sources:

- `Sources/WaxCore/Wax.swift`
- `Sources/WaxTextSearch/FTS5SearchEngine.swift`
- `Sources/WaxTextSearch/FTS5Serializer.swift`
- `Sources/WaxVectorSearch/USearchVectorEngine.swift`
- `Sources/WaxVectorSearch/AccelerateVectorEngine.swift`
- `Sources/WaxVectorSearch/MetalANNSVectorEngine.swift`
- `Sources/WaxVectorSearch/LoadedVectorSearchEngine.swift`
- `Sources/Wax/UnifiedSearch/UnifiedSearch.swift`
- `Sources/Wax/UnifiedSearch/UnifiedSearchEngineCache.swift`
- `Sources/Wax/UnifiedSearch/AdaptiveFusionConfig.swift`
- `Sources/Wax/UnifiedSearch/RuleBasedQueryClassifier.swift`

Supporting docs:

- [README.md](https://github.com/christopherkarani/Wax/blob/main/README.md)
- [wal-crash-recovery.md](https://github.com/christopherkarani/Wax/blob/main/Resources/website/docs/core/wal-crash-recovery.md)
- [mini-lm-embedder.md](https://github.com/christopherkarani/Wax/blob/main/Resources/website/docs/mini-lm/mini-lm-embedder.md)
- [vector-search-engines.md](https://github.com/christopherkarani/Wax/blob/main/Resources/website/docs/vector-search/vector-search-engines.md)

## 4. Headline Conclusion

Upstream Wax solves the problem with a layered strategy, not one magic engine.

At a high level:

- `WaxCore` makes open and commit cheap enough for a single-file store
- `WaxTextSearch` uses SQLite FTS5 as a serialized blob for lexical and structured search
- `WaxVectorSearch` uses multiple vector backends, not one
- `UnifiedSearch` uses deterministic heuristics, weighted RRF, bounded candidate expansion, and fallback behavior to keep hybrid search fast
- `MiniLM` performance is managed by prewarm, batch sizing, CoreML configuration, and daemon reuse

The most important benchmarking takeaway is this:

- Wax's published `cold open` numbers are primarily `container open` numbers, not "fully hydrated text+vector hybrid engine ready" numbers

That distinction matters a lot for the Rust rewrite.

An even more precise framing is:

- the upstream headline omits the full `time to first query` cost for lanes that require engine materialization after container open

## 5. What Upstream Wax Measured

### 5.1 Published Measured Numbers

`Measured`

From the upstream benchmark report on `macOS, Apple Silicon`:

- cold open mean: `8.8 ms`
- cold open p95: `9.2 ms`
- cold open p99: `9.2 ms`
- warm hybrid with previews p95: `6.1 ms`
- warm hybrid with previews p99: `6.5 ms`
- unified hybrid search average: `0.006 s`
- text-only ingest average: `0.082 s`
- hybrid ingest average: `0.228 s`
- `MemoryOrchestrator` ingest average: `0.339 s`
- WAL `large_hybrid_10k` commit p95: `34.25 ms`
- WAL `large_hybrid_10k` commit p99: `40.03 ms`

Additional measured values:

- Metal vector search average at `1K x 128d`: `1.58 ms`
- Metal cold search with GPU sync at `10K x 384d`: `4.87 ms`
- Metal warm search average without sync: `0.91 ms`
- MiniLM batch size `32`: `220.1 ms total`, `6.88 ms/text`, `145.4 texts/sec`
- MiniLM prewarm typical latency in docs: `~500 ms - 1 s`

### 5.2 Important Benchmark Caveat

`Measured + Code-evidenced`

The benchmark report is explicitly for:

- `macOS`
- `Apple Silicon`

It is not an iPhone benchmark report.

So upstream gives us:

- strong evidence for Apple Silicon local behavior
- weaker direct evidence for iOS device behavior

This means Wax is a valid benchmark target, but not yet a complete iOS proof point.

It also means any fair rewrite benchmark must distinguish:

- container open
- first text query after open
- first vector query after open
- first hybrid query after open

## 6. How Wax Makes Cold Open Fast

### 6.1 Container Open Path

`Code-evidenced`

`Wax.open(...)` does the following:

1. acquires a file lock
2. reads header page A and B
3. selects the valid header by generation and checksum
4. tries a fast footer lookup at the header-indicated offset
5. optionally checks a replay snapshot footer
6. only scans for the last valid footer if the fast path looks stale
7. reconstructs pending WAL state
8. uses replay snapshots to skip WAL scanning when possible
9. truncates trailing garbage safely if repair is enabled

Important details from `Wax.swift`:

- dual header pages
- footer fast path
- `findLastValidFooter` only when needed
- WAL replay snapshot short-circuit
- no large logical rebuild during `Wax.open(...)`

### 6.2 Why This Produces Single-Digit Milliseconds

`Inference`

The upstream `8.8 ms` to `9.2 ms` cold open is plausible because the open path is dominated by:

- a few fixed-offset reads
- footer validation
- small WAL state checks

and avoids:

- full frame scans
- index rebuilds inside `WaxCore.open`
- loading the entire `.wax` body into heap memory

### 6.3 What That Number Does Not Mean

`Code-evidenced + Inference`

That published cold-open number does not mean:

- SQLite FTS5 is already deserialized and ready
- vector ANN structures are always already hydrated
- the first hybrid query has no additional engine cost

It means the `Wax` container itself opens quickly.

This is the single most important caveat when benchmarking against Wax.

For the rewrite, `TTFQ` should be treated as at least as important as container-open latency.

## 7. How Wax Solves Text Search

### 7.1 Storage Model

`Code-evidenced`

Wax text search is built on `FTS5SearchEngine`.

It uses:

- SQLite FTS5 virtual table for text search
- a `frame_mapping` table from SQLite rowid to Wax `frame_id`
- the same SQLite database for structured-memory tables

Committed text index bytes are staged into `.wax` via:

- `wax.stageLexIndexForNextCommit(bytes: blob, docCount: docCount)`

where `blob` is produced by:

- `sqlite3_serialize`

### 7.2 Query Path

`Code-evidenced`

Text search SQL:

- `MATCH ?`
- `ORDER BY bm25(frames_fts) ASC, m.frame_id ASC`
- `snippet(...)`

Then Wax converts SQLite BM25 rank to its own score representation.

So upstream Wax text search is:

- real SQLite FTS5
- rank-driven
- snippet-capable
- tied to serialized SQLite state

### 7.3 Read Path Caveat

`Code-evidenced`

On deserialize, upstream Wax does not directly run the serialized SQLite blob purely in memory in the main open path.

`FTS5SearchEngine.deserialize(from:)`:

- writes the blob to a temporary directory
- creates `fts.sqlite`
- opens a GRDB `DatabaseQueue(path:)`
- validates or upgrades schema

This matters because:

- first lexical engine load is more expensive than `Wax.open(...)`
- repeated searches in the same process are helped heavily by engine caching
- the first lexical load pays a double-I/O style cost:
  - read SQLite blob from `.wax`
  - write a temporary `fts.sqlite`
  - reopen that file via GRDB

### 7.4 Why Wax Still Feels Fast

`Code-evidenced + Inference`

`UnifiedSearchEngineCache` caches text engines by:

- Wax object identity
- committed checksum
- staged stamp

So repeated searches avoid repeated SQLite deserialize work inside the same process.

This is a major reason warm hybrid search can stay around `~6 ms`.

It also means warm-search results should not be read as proof that the first lexical query is equally cheap.

## 8. How Wax Solves Vector Search

### 8.1 There Is Not One Vector Engine

`Code-evidenced`

This is where README-level simplifications hide important implementation detail.

Upstream Wax currently has multiple vector paths:

- `USearchVectorEngine`
  - CPU HNSW via USearch
- `AccelerateVectorEngine`
  - CPU flat vector scan via Accelerate
- `MetalANNSVectorEngine`
  - MetalANNS backend
- `MetalVectorEngine`
  - GPU brute-force Metal search, used in docs and benchmarks

`LoadedVectorSearchEngine` selects among these based on:

- stored encoding
- projected vector count
- platform availability
- caller preference

### 8.2 Persisted Format Behavior

`Code-evidenced`

There are two materially different persistence stories:

`USearch path`

- commits a `uSearch`-encoded `MV2V` blob
- can deserialize the ANN payload directly

`Accelerate / MetalANNS path`

- commits a `flat` `MV2V` blob
- stores vectors plus `frameIds`
- rebuilds runtime index structures from flat vectors on load

This means upstream Wax does not always persist a directly reusable ANN graph.

### 8.3 Why This Matters

`Code-evidenced + Inference`

If the stored vector encoding is `flat`:

- `Wax.open(...)` can still be fast
- first vector-engine load may still need backend reconstruction
- as corpus size grows, the gap between container-open time and vector-ready first-query time can widen substantially

So again:

- Wax's published cold-open number is not equivalent to "vector lane fully ready with zero rebuild"

### 8.4 Engine Selection Heuristic

`Code-evidenced`

`LoadedVectorSearchEngine.resolveKind(...)` roughly does this:

- if stored encoding is `uSearch`, use `USearch`
- else if GPU backend is available and projected vector count is at least `10_000`, use `MetalANNS`
- else use `Accelerate`

That means Wax already has a pragmatic runtime strategy:

- CPU HNSW when explicitly persisted as HNSW
- GPU or CPU flat-search family when the persisted format is flat

### 8.5 Benchmark Takeaway

`Measured + Code-evidenced`

The upstream vector numbers are good, but they mix:

- warm runtime behavior
- backend-specific selection
- cache effects

They should not be interpreted as proof that all vector paths are equally open-cheap.

## 9. How Wax Solves Hybrid Search

### 9.1 Planner Shape

`Code-evidenced`

`UnifiedSearch` is more conservative than the README makes it sound.

It does not do an expensive learned planner.

Instead it uses:

- `RuleBasedQueryClassifier`
- `AdaptiveFusionConfig`
- explicit search modes
- bounded candidate expansion
- vector timeout handling

Query types:

- factual
- semantic
- temporal
- exploratory

Adaptive weights:

- factual: more BM25-heavy
- semantic: more vector-heavy
- temporal: includes timeline weight
- exploratory: more balanced

### 9.2 Candidate Budget

`Code-evidenced`

Candidate budget is:

- `topK * 3`, capped at `1000`

That is simple, deterministic, and very much in line with a p95-first design.

### 9.3 Fusion

`Code-evidenced`

Hybrid fusion is:

- weighted reciprocal rank fusion

not raw score blending.

This avoids:

- BM25 score calibration problems
- vector score calibration problems
- cross-backend score comparability assumptions

### 9.4 Tail Protection

`Code-evidenced`

Vector search can be wrapped with `AsyncTimeout`.

If vector times out:

- `vectorOnly` fails
- `hybrid` degrades to non-vector lanes

That is a real tail-latency protection mechanism, not just a design note.

### 9.5 Post-Fusion Work

`Code-evidenced`

After lane fusion, Wax still does:

- metadata filtering
- lazy vs batch metadata loading depending on threshold
- preview fetch
- intent-aware rerank over a bounded window
- optional timeline fallback

So upstream hybrid is not only "text + vector".

It is:

- text/vector retrieval
- rank-based fusion
- lightweight semantic post-processing
- lane-level timeout and degrade behavior

## 10. How Wax Solves Commit And Ingest

### 10.1 Core Write Path

`Code-evidenced`

WaxCore write behavior is optimized in two distinct ways:

- payload bytes are appended directly
- logical mutations go through WAL entries

For batched puts:

- payloads are precomputed
- a single writable mmap region is used for frame payload writes
- WAL records are appended in batch

This reduces:

- actor hops
- per-document syscalls
- copy overhead

### 10.2 WAL Pressure Management

`Code-evidenced`

Wax uses:

- proactive auto-commit under WAL pressure
- replay snapshots
- bounded WAL region
- segment staging for text/vector indexes before commit

This is why their commit numbers stay relatively controlled even with mixed hybrid workloads.

### 10.3 Why Ingest Is Not "Just SQLite + HNSW"

`Inference`

The ingest speed comes from combining:

- batch embedding
- batch FTS writes
- mmap payload append
- batch WAL append
- staged index blob commit

This is broader than any single library choice.

## 11. How Wax Solves Embedding Runtime Cost

### 11.1 MiniLM Strategy

`Code-evidenced`

Upstream Wax explicitly manages CoreML embedding costs with:

- prewarm
- batch embedding
- sequence-length buckets
- ANE-friendly compute-unit defaults
- daemon reuse for CLI flows

Published docs say:

- `.cpuAndNeuralEngine` is preferred over `.all`
- prewarm avoids first-inference JIT surprise
- tokenizer and model setup are treated as first-class performance issues

### 11.2 Benchmark Caveat

`Measured`

The MiniLM benchmark fix report says the deterministic benchmark path forced CPU-only configuration in XCTest.

So the published MiniLM benchmark numbers are:

- stable
- useful
- not peak ANE-path throughput numbers

That caveat should carry into any rewrite comparison.

## 12. What Wax Is Actually Good At

### 12.1 Strongest Upstream Advantages

`Measured + Code-evidenced`

The areas where upstream Wax is clearly strong today are:

- container open path
- local warm hybrid search latency
- Apple-platform embedding integration
- pragmatic tail handling in hybrid search
- append-oriented commit path

### 12.2 What Is More Conditional Than It Looks

`Code-evidenced + Inference`

The areas that are more conditional than the README implies are:

- "cold open" versus "first fully ready hybrid search"
- persisted ANN reuse across all vector backends
- published benchmark portability from macOS to iPhone
- exact meaning of "Metal HNSW" versus actual runtime backend mix

## 13. What The Rust Rewrite Should Benchmark Against

This is the most important section for the rewrite.

### 13.1 Do Not Benchmark Against The README Slogan

Benchmark against actual upstream behavior:

- `Wax.open(...)` cold open
- first text query after open
- first vector query after open
- first hybrid query after open
- warm hybrid p95
- commit p95 under sustained mixed writes

If we only compare against the published `9 ms cold open`, we will misread the target.

### 13.2 Wax Benchmark Targets Worth Keeping

Recommended benchmark targets copied from Wax behavior:

- `container cold open`
- `warm hybrid p95`
- `WAL/commit p95`
- `single-doc ingest`
- `batch ingest`
- `vector-only warm search`
- `preview-enabled hybrid search`

### 13.3 Extra Benchmarks Wax Does Not Fully Give Us

Because upstream evidence is incomplete for our needs, the Rust rewrite should additionally benchmark:

- iPhone-class first hybrid query after cold open
- iPhone-class resident memory before and after first vector query
- first query after vector backend selection
- selective-filter hybrid p95
- multi-segment vector fan-out p95
- page-cache-controlled cold open and cold first-query runs

These are the places where upstream Wax's published numbers leave uncertainty.

The page-cache-controlled runs matter because process restart alone does not guarantee storage-cache-cold measurement.

## 14. Direct Lessons For Wax v2

The Rust rewrite should benchmark and copy the following ideas first, before trying to outperform them with custom code:

1. cheap container open via fixed-offset metadata and recovery shortcuts
2. checksum-keyed in-process engine caching
3. bounded candidate expansion
4. RRF-style fusion instead of raw score mixing
5. vector timeout and degrade behavior
6. lazy versus batch metadata loading threshold
7. mmap-heavy batch append on ingest
8. explicit embedder prewarm and batch planning

These are high-leverage design ideas already proven useful in upstream Wax.

## 15. Bottom Line

Upstream Wax is not "fast because it picked SQLite and HNSW."

It is fast because it combines:

- a very cheap container open path
- serialized search-engine state
- aggressive in-process caching
- deterministic hybrid orchestration
- Apple-optimized embedding runtime management
- append-friendly write mechanics

The benchmark to beat is therefore not a single number.

It is this combination:

- `~9 ms` container cold open on Apple Silicon
- `~6 ms` warm hybrid p95 on Apple Silicon
- bounded hybrid tails via timeout and degrade
- moderate commit tails under sustained WAL pressure

But the Rust rewrite should treat one upstream claim with care:

- Wax's published cold-open result is not the same thing as "fully warmed cross-lane first-query latency"

That distinction should drive the next benchmark plan for Rust.

It should also promote two benchmark requirements to first-class status:

- `time to first query`
- iOS resident-memory pressure during first query
