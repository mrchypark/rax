# rax

`rax` is a Rust workspace inspired by
[Wax](https://github.com/christopherkarani/Wax). It keeps the core `rax` user
model local for CLI/runtime use: one `.rax` memory store, no server required,
and local retrieval over text plus caller-provided vectors.

The repository has two main command-line surfaces:

- `rax`: `rax` product CLI for local memory, raw document/vector ingest, and search.
- `rax-bench-cli`: `rax` benchmark harness for packing datasets, running workloads,
  reducing artifacts, and producing reports.

Runtime crates currently live under `crates/rax-*`; benchmark crates live
under `crates/rax-bench-*`.

## Choose Your Path

| Path | Start here | Use when |
| --- | --- | --- |
| Memory CLI | [docs/usage.md](docs/usage.md#memory-cli-quick-start) | You want a single local `.rax` file for `remember` and `recall`. |
| Raw projection store | [docs/usage.md](docs/usage.md#raw-projection-stores) | Another system owns document ids, metadata, and optional vectors. |
| Benchmark harness | [docs/benchmarks/current-guide.md](docs/benchmarks/current-guide.md) | You need to pack fixtures, run workloads, reduce artifacts, or compare vector modes. |

For a source-derived inventory of the current crate and API surface, see
[docs/current-implementation.md](docs/current-implementation.md).

## Quick Start

Remember text into a local `.rax` file:

```bash
install -d -m 700 ~/.local/share/rax

cargo run -p rax-cli -- remember \
  --store ~/.local/share/rax/agent.rax \
  "The user is building a habit tracker in Rust."
```

Recall from the same file:

```bash
cargo run -p rax-cli -- recall \
  --store ~/.local/share/rax/agent.rax \
  "What is the user building?" \
  --top-k 5
```

The current product CLI commands are:

```text
create
remember
recall
ingest docs
ingest vectors
search
```

There is no current `import-compat` product CLI command.

## Build And Test

```bash
cargo build --workspace
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

## Current Constraints

- There is no current `import-compat` product CLI command.
- Product vector and hybrid search require explicit caller-provided query
  vectors; `rax` does not generate hidden embeddings.
- There is no MCP crate, stdio server, or tool surface in the current build.

## Documentation

- [docs/usage.md](docs/usage.md): product CLI, raw stores, output formats,
  limitations, and focused tests.
- [docs/current-implementation.md](docs/current-implementation.md): current
  source-derived implementation snapshot.
- [docs/benchmarks/current-guide.md](docs/benchmarks/current-guide.md):
  benchmark command matrix, workloads, artifacts, metrics, quality reports, and
  comparison caveats.
- [docs/specs](docs/specs), [docs/plans](docs/plans), and
  [docs/todos](docs/todos): historical roadmap, design, and execution records.
