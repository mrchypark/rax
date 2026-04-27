# rax

`rax` is a cross-platform Rust workspace inspired by
https://github.com/christopherkarani/Wax. The goal is to keep the Wax user model
portable: one local memory store, no server, remember text, and recall it with
hybrid local search. It has two main surfaces:

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
cargo run -p wax-cli -- remember \
  --store /tmp/agent.wax \
  "The user is building a habit tracker in Rust."
```

Recall from the same file:

```bash
cargo run -p wax-cli -- recall \
  --store /tmp/agent.wax \
  "What is the user building?" \
  --top-k 5
```

`search --store` is the same product-memory path with explicit search naming:

```bash
cargo run -p wax-cli -- search \
  --store /tmp/agent.wax \
  --text "habit tracker" \
  --top-k 5 \
  --preview
```

## Use The MCP Server

`wax-mcp` exposes the same memory flow over stdio JSON-RPC tools:

- `remember`
- `recall`
- `search`

Run it with an allowed root for store paths. The MCP product tools only accept a
`.wax` store file directly under this trusted root; nested paths and symlink leaf
paths are rejected.

```bash
WAX_MCP_ALLOWED_ROOT=/tmp cargo run -p wax-v2-mcp --bin wax-mcp
```

Example `tools/call` payload:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"remember","arguments":{"store":"/tmp/agent.wax","content":"The user is building a habit tracker in Rust."}}}
```

## Use Lower-Level Ingest Commands

The lower-level ingest commands are useful when you already have document ids or
external vectors. These commands use a dataset root and are kept for migration
and compatibility workflows.

Prepare raw documents as JSONL. Each row needs `doc_id` and `text`; `metadata`,
`timestamp_ms`, and extra top-level fields are optional.

```jsonl
{"doc_id":"doc-1","text":"alpha product notes","metadata":{"kind":"note"}}
{"doc_id":"doc-2","text":"beta launch checklist","metadata":{"kind":"task"}}
```

Ingest documents:

```bash
cargo run -p wax-cli -- create --root fixtures/bench/minimal-dataset-pack
cargo run -p wax-cli -- ingest docs \
  --root fixtures/bench/minimal-dataset-pack \
  --input /tmp/docs.jsonl
```

Search text:

```bash
cargo run -p wax-cli -- search \
  --root fixtures/bench/minimal-dataset-pack \
  --text "launch checklist" \
  --top-k 5 \
  --preview
```

Optionally ingest explicit vectors for existing document ids:

```jsonl
{"doc_id":"doc-1","values":[0.1,0.2,0.3]}
{"doc_id":"doc-2","values":[0.2,0.1,0.4]}
```

```bash
cargo run -p wax-cli -- ingest vectors \
  --root fixtures/bench/minimal-dataset-pack \
  --input /tmp/vectors.jsonl
```

For legacy dataset-pack inputs, create the store and publish a compatibility
snapshot from a dataset root:

```bash
cargo run -p wax-cli -- create --root fixtures/bench/minimal-dataset-pack
cargo run -p wax-cli -- import-compat --root fixtures/bench/minimal-dataset-pack
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

The runtime supports store creation, raw document ingest, explicit vector ingest,
text search, vector-backed runtime paths, compatibility import, broker/session
APIs, and an MCP-compatible in-process surface. The benchmark harness validates
dataset packing, runner lifecycle metrics, search quality summaries, vector mode
profiling, and artifact reduction.
