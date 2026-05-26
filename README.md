# rax

`rax` is a Rust workspace inspired by
https://github.com/christopherkarani/Wax. The goal is to keep the Wax user model
portable across platforms: one local memory store, no server, remember text, and
recall it with hybrid local search. The product CLI/runtime store path is
supported on Unix and Windows; the untrusted MCP store-file surface is
intentionally narrower and documented below. It has two main surfaces:

- `wax`: the product-facing CLI for creating a store, ingesting documents or
  vectors, and searching the store.
- `wax-bench-cli`: the benchmark harness for packing fixtures, running
  workloads, reducing artifacts, and producing reports.

The repository is organized as a Cargo workspace. Runtime crates live under
`crates/wax-v2-*`; benchmark crates live under `crates/wax-bench-*`.

## Build And Test

```bash
cargo build --workspace
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

## Use The Product CLI Like Wax Memory

Remember text into a single `.wax` file. If the file does not exist, `wax`
creates it.

```bash
install -d -m 700 ~/.local/share/rax
cargo run -p wax-cli -- remember \
  --store ~/.local/share/rax/agent.wax \
  "The user is building a habit tracker in Rust."
```

Recall from the same file:

```bash
cargo run -p wax-cli -- recall \
  --store ~/.local/share/rax/agent.wax \
  "What is the user building?" \
  --top-k 5
```

`recall` is the product-memory query path. It uses the memory facade and hybrid
search over memories written by `remember`.

`search --store` is the raw runtime search path. Use it for caller-owned
projection stores populated with `wax ingest docs --store`; it preserves external
document ids instead of generating `mem-*` ids:

```bash
cargo run -p wax-cli -- search \
  --store ~/.local/share/rax/projection.wax \
  --text "habit tracker" \
  --top-k 5 \
  --preview
```

## Use The MCP Server

`wax-mcp` exposes the same memory flow over stdio JSON-RPC tools on Linux:

- `remember`
- `recall`
- `search`

The CLI/runtime store path is cross-platform for Unix and Windows. The
untrusted MCP store-file surface is intentionally Linux-only for now because it
relies on fd-relative opening through `/proc/self/fd` to avoid symlink,
hard-link, and path-swap races inside `WAX_MCP_ALLOWED_ROOT`. On non-Linux
targets the server initializes without tool capabilities instead of exposing an
unsafe partial implementation.

Run it with a private allowed root for store paths. The directory must be owned
by the server user and mode `0700` or stricter; do not use `/tmp` directly. The
MCP product tools only accept a `.wax` store file directly under this trusted
root; nested paths, symlink leaf paths, hard-linked files, and group/world
readable store files are rejected. Unlike the product CLI/runtime path, MCP may
create hidden `.wax-mcp-lock-*` coordination files inside the allowed root.

```bash
install -d -m 700 ~/.local/share/rax-mcp
WAX_MCP_ALLOWED_ROOT=$HOME/.local/share/rax-mcp cargo run -p wax-v2-mcp --bin wax-mcp
```

After the JSON-RPC `initialize` request succeeds and the client sends
`notifications/initialized`, this is a valid `tools/call` payload:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"remember","arguments":{"store":"/home/alice/.local/share/rax-mcp/agent.wax","content":"The user is building a habit tracker in Rust."}}}
```

## Use Raw Projection Stores

Raw projection stores are the product path when another system owns canonical
records and wants Wax to own retrieval indexing. The caller supplies stable
document ids, text, metadata, and optional timestamps; Wax stores those ids and
returns them from search.

Prepare raw documents as JSONL. Each row needs `doc_id` and `text`; `metadata`,
`timestamp_ms`, and extra top-level fields are optional.

```jsonl
{"doc_id":"doc-1","text":"alpha product notes","metadata":{"kind":"note"}}
{"doc_id":"doc-2","text":"beta launch checklist","metadata":{"kind":"task"}}
```

Ingest documents into a direct `.wax` store. The store is created if it does not
exist:

```bash
cargo run -p wax-cli -- ingest docs \
  --store ~/.local/share/rax/projection.wax \
  --input /tmp/docs.jsonl
```

Search text:

```bash
cargo run -p wax-cli -- search \
  --store ~/.local/share/rax/projection.wax \
  --text "launch checklist" \
  --top-k 5 \
  --preview
```

Optionally ingest explicit vectors for existing document ids. The vector input
must cover the current document set; publish documents first. Each `values`
array must contain 384 floats, matching the product store embedding profile.
Rows have this shape:

```text
{"doc_id":"doc-1","values":[384 floats]}
```

```bash
cargo run -p wax-cli -- ingest vectors \
  --store ~/.local/share/rax/projection.wax \
  --input /tmp/vectors.jsonl
```

Search with an external query vector when the caller owns embeddings:
the query file can be either a 384-float JSON array or an object with a single
`values` field containing 384 floats.

```bash
cargo run -p wax-cli -- search \
  --store ~/.local/share/rax/projection.wax \
  --mode vector \
  --vector-input /tmp/query-vector.json \
  --top-k 5 \
  --preview
```

Hybrid search accepts both `--text` and `--vector-input`:

```bash
cargo run -p wax-cli -- search \
  --store ~/.local/share/rax/projection.wax \
  --mode hybrid \
  --text "launch checklist" \
  --vector-input /tmp/query-vector.json \
  --top-k 5
```

## Use The Benchmark Harness

Pack a source fixture:

```bash
cargo run -p wax-bench-cli -- pack \
  --source fixtures/bench/source/minimal \
  --out /tmp/rax-pack \
  --tier small \
  --variant clean
```

Run a workload:

```bash
cargo run -p wax-bench-cli -- run \
  --dataset /tmp/rax-pack \
  --workload ttfq_text \
  --sample-count 5 \
  --artifact-dir /tmp/rax-artifacts
```

Reduce artifacts:

```bash
cargo run -p wax-bench-cli -- reduce --input /tmp/rax-artifacts
```

Query a packed dataset directly:

```bash
cargo run -p wax-bench-cli -- query \
  --dataset /tmp/rax-pack \
  --text "alpha" \
  --top-k 5
```

## Current Scope

The product surface supports direct `.wax` store creation, raw document ingest,
explicit vector ingest, text/vector/hybrid search, broker store sessions, and an
MCP-compatible in-process surface. The benchmark harness validates dataset
packing, runner lifecycle metrics, search quality summaries, vector mode
profiling, and artifact reduction.
