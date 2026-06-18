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

Install the product CLI from this workspace:

```bash
cargo install --path crates/rax-cli --locked
```

This installs the `rax` binary.

Try `remember` and `recall` against a disposable temp store:

```bash
tmpdir=$(mktemp -d)
store="$tmpdir/agent.rax"

rax remember \
  --store "$store" \
  "The user is building a habit tracker in Rust."

rax recall \
  --store "$store" \
  "What is the user building?" \
  --top-k 5

rm -rf "$tmpdir"
```

For a long-lived local store:

```bash
install -d -m 700 ~/.local/share/rax
rax remember \
  --store ~/.local/share/rax/agent.rax \
  "The user is building a habit tracker in Rust."
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
