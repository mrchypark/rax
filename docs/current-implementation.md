# Current Implementation

Status: source-derived snapshot
Date: 2026-05-29

This file summarizes the current code surface. Historical dated plans remain useful
for execution history, but this page is the shorter reference for what exists now.

## Product CLI

The `rax-cli` package ships the current `rax` product binary. Its current
commands are:

- `create`
- `remember`
- `recall`
- `ingest docs`
- `ingest vectors`
- `search`

There is no current `import-compat` product CLI command.

`remember` and `recall` are the `rax` memory facade. `ingest docs`,
`ingest vectors`, and `search` are the raw projection-store path for callers that
own document ids and, for vector search, caller-provided embeddings.

## Runtime

The product raw-ingest types exposed by `rax-runtime` are:

- `NewDocument`
- `NewDocumentVector`

Raw document publish derives `Doc` and `Txt` segments from caller-owned document
inputs. Raw vector publish accepts explicit 384-dimensional vectors for existing
documents. The runtime still has compatibility bridge APIs for benchmark-pack
translation, but those are not exposed as a product CLI command.

Vector and hybrid runtime search require an explicit caller-provided query vector.
The runtime does not generate hidden embeddings.

## Broker

`rax-broker` owns in-process sessions over `RuntimeStore`. It currently exposes
session search plus raw ingest operations:

- `search`
- `ingest_documents`
- `ingest_vectors`

Raw broker documents map to `NewDocument`; raw broker vectors map to
`NewDocumentVector`.

## Removed MCP Support

There is no MCP crate, stdio server, JSON-RPC tool surface, or trusted
in-process MCP adapter in the current build. Product access is through the
`rax` CLI, `rax-runtime`, and the in-process `rax-broker`
session surface.

## Benchmark CLI

The `rax-bench-cli` commands are:

- `pack`
- `pack-adhoc`
- `run`
- `query`
- `query-batch`
- `profile-vector-query`
- `quality-report`
- `reduce`
- `matrix-report`
- `mode-compare-report`
- `replay`

Benchmark vector modes are:

- `auto`
- `exact_flat`
- `hnsw`
- `preview_q8`

The benchmark runner also recognizes these environment variables:

- `RAX_BENCH_TEST_MODE=1` selects deterministic test measurement plumbing.
- `RAX_BENCH_ARTIFACT_DIR` supplies the default artifact directory when
  `run --artifact-dir` is omitted.

## Bootstrap And Parity Scope

Structured memory currently has a bootstrap `structured-memory.ndjson` persistence
layer with record, entity, and fact APIs. It is not a final binary structured
memory format.

Multimodal support currently has asset import, copied store-owned payloads,
`multimodal-assets.ndjson`, image-only typed read/query scaffolds, and video-only
typed read/query scaffolds. It does not claim OCR, captioning, transcript
extraction, embeddings, temporal retrieval, or full PhotoRAG/VideoRAG retrieval
parity.

Apple acceleration support is an explicit capability and backend-preference
resolution surface. Current execution still falls back to the Rust default backend
when Apple-specific backends are unavailable or not compiled.
