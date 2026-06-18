# Usage Guide

This guide describes the current `rax` user-facing surfaces. Historical dated
plans under `docs/specs`, `docs/plans`, and `docs/todos` remain useful execution
history, but this file is the practical usage entry point.

## Overview

`rax` is a Rust workspace inspired by upstream Wax. The current product model is
local-first: one `.rax` store file, no server required for CLI use, and
local text/vector/hybrid retrieval.

There are two command-line surfaces:

- `rax`, from the `rax-cli` package, is the current `rax` product CLI.
- `rax-bench-cli` is the current `rax` benchmark harness.

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

## Install Product CLI

Install the product CLI from this workspace:

```bash
cargo install --path crates/rax-cli --locked
```

This installs the `rax` binary. The examples below use that installed command.

## Product CLI Commands

The current `rax` commands are:

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

Use `remember` and `recall` when you want `rax` memory with generated
`mem-*` document ids. For evaluation, keep the store in a temp directory so the
quickstart does not write to `~/.local/share/rax`.

```bash
tmpdir=$(mktemp -d)
store="$tmpdir/agent.rax"

rax remember \
  --store "$store" \
  "The user is building a habit tracker in Rust."
```

The command prints JSON:

```json
{
  "doc_id": "mem-0000000000000001"
}
```

Recall from the same store:

```bash
rax recall \
  --store "$store" \
  "What is the user building?" \
  --top-k 5
```

`recall` uses the product memory facade and hybrid search over documents written
by `remember`. Remove the temp store when finished:

```bash
rm -rf "$tmpdir"
```

For a long-lived local store:

```bash
install -d -m 700 ~/.local/share/rax
rax remember \
  --store ~/.local/share/rax/agent.rax \
  "The user is building a habit tracker in Rust."
```

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
rax ingest docs \
  --store ~/.local/share/rax/projection.rax \
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
rax search \
  --store ~/.local/share/rax/projection.rax \
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
rax ingest vectors \
  --store ~/.local/share/rax/projection.rax \
  --input /tmp/vectors.jsonl
```

The current bootstrap vector contract requires vectors for known existing
documents; sparse partial vector updates are not a product guarantee.

## Search Modes

Text search requires `--text`:

```bash
rax search \
  --store ~/.local/share/rax/projection.rax \
  --mode text \
  --text "launch checklist"
```

Vector search requires `--vector-input`:

```bash
rax search \
  --store ~/.local/share/rax/projection.rax \
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
rax search \
  --store ~/.local/share/rax/projection.rax \
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
