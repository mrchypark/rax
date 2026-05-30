# Usage Guide

This guide describes the current `rax` user-facing surfaces. Historical dated
plans under `docs/specs`, `docs/plans`, and `docs/todos` remain useful execution
history, but this file is the practical usage entry point.

## Overview

`rax` is a Rust workspace inspired by upstream Wax. The current product model is
still local-first: one `.wax` store file, no server required for CLI use, and
local text/vector/hybrid retrieval.

There are two command-line surfaces:

- `wax`, from the `wax-cli` package, is the product CLI.
- `wax-bench-cli` is the benchmark harness.

For a code-derived inventory of crates and current scope, see
[current-implementation.md](current-implementation.md).

## Build And Test

```bash
cargo build --workspace
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

During local test runs that create many temp files, a workspace-local temp
directory keeps artifacts contained:

```bash
mkdir -p .tmp
TMPDIR=$PWD/.tmp cargo test --workspace --all-targets
```

## Product CLI Commands

The current `wax` commands are:

```text
create
remember
recall
ingest docs
ingest vectors
search
```

There is no current `import-compat` product CLI command.

## Memory CLI Quick Start

Use `remember` and `recall` when you want Wax-style memory with generated
`mem-*` document ids.

```bash
install -d -m 700 ~/.local/share/rax

cargo run -p wax-cli -- remember \
  --store ~/.local/share/rax/agent.wax \
  "The user is building a habit tracker in Rust."
```

The command prints JSON:

```json
{
  "doc_id": "mem-1"
}
```

Recall from the same store:

```bash
cargo run -p wax-cli -- recall \
  --store ~/.local/share/rax/agent.wax \
  "What is the user building?" \
  --top-k 5
```

`recall` uses the product memory facade and hybrid search over documents written
by `remember`.

## Raw Projection Stores

Use raw projection stores when another system owns canonical records and stable
document ids. This path preserves caller-provided `doc_id` values.

Create a JSONL document file:

```jsonl
{"doc_id":"doc-1","text":"alpha product notes","metadata":{"kind":"note"}}
{"doc_id":"doc-2","text":"beta launch checklist","metadata":{"kind":"task"}}
```

Each row requires:

- `doc_id`: stable external document id
- `text`: searchable text body

Optional fields:

- `metadata`: JSON object, defaults to `{}`
- `timestamp_ms`: unsigned millisecond timestamp
- extra top-level fields, preserved in the runtime document payload

Ingest documents:

```bash
cargo run -p wax-cli -- ingest docs \
  --store ~/.local/share/rax/projection.wax \
  --input /tmp/docs.jsonl
```

The command prints a publish report:

```json
{
  "generation": 1,
  "published_families": ["doc", "text"]
}
```

Search text:

```bash
cargo run -p wax-cli -- search \
  --store ~/.local/share/rax/projection.wax \
  --mode text \
  --text "launch checklist" \
  --top-k 5 \
  --preview
```

The search output is a JSON array:

```json
[
  {
    "doc_id": "doc-2",
    "preview": "beta launch checklist"
  }
]
```

## Vector Ingest

Vector ingest is explicit. The runtime does not generate hidden embeddings.

Publish documents first, then ingest vectors for the current document set. Each
row has a `doc_id` and a finite 384-float `values` array:

```text
{"doc_id":"doc-1","values":[384 floats]}
{"doc_id":"doc-2","values":[384 floats]}
```

```bash
cargo run -p wax-cli -- ingest vectors \
  --store ~/.local/share/rax/projection.wax \
  --input /tmp/vectors.jsonl
```

The current bootstrap vector contract requires vectors for known existing
documents; sparse partial vector updates are not a product guarantee.

## Search Modes

Text search requires `--text`:

```bash
cargo run -p wax-cli -- search \
  --store ~/.local/share/rax/projection.wax \
  --mode text \
  --text "launch checklist"
```

Vector search requires `--vector-input`:

```bash
cargo run -p wax-cli -- search \
  --store ~/.local/share/rax/projection.wax \
  --mode vector \
  --vector-input /tmp/query-vector.json \
  --top-k 5 \
  --preview
```

The query vector file can be either a JSON array or an object with a single
`values` field:

```json
{"values":[0.1, 0.2, 0.3]}
```

Hybrid search requires both text and a query vector:

```bash
cargo run -p wax-cli -- search \
  --store ~/.local/share/rax/projection.wax \
  --mode hybrid \
  --text "launch checklist" \
  --vector-input /tmp/query-vector.json \
  --top-k 5
```

Use `--preview` to include hydrated document previews in results. `recall`
includes previews by default; pass `--no-preview` to disable them.

## Current Limitations

- No current `import-compat` product CLI command.
- No hidden embedding generation for product vector or hybrid search.
- No MCP crate, stdio server, JSON-RPC tool surface, or MCP-focused contract
  tests are part of the current build.
- Structured memory currently uses bootstrap `structured-memory.ndjson`
  persistence, not final Wax binary structured-memory parity.
- Multimodal support currently owns asset import and typed image/video read
  scaffolds, not OCR, captions, transcripts, embeddings, or retrieval parity.
- Apple acceleration is an explicit capability/preference surface; execution
  falls back to the Rust default backend when Apple-specific backends are
  unavailable or not compiled.

## Focused Verification

Useful focused contract tests:

```bash
cargo test --test product_cli_contract
cargo test --test product_raw_ingest_cli_contract
cargo test --test broker_session_contract
```

Full verification:

```bash
mkdir -p .tmp
TMPDIR=$PWD/.tmp cargo test --workspace --all-targets
```
