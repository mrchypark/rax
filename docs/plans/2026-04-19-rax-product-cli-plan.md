# Rax Product CLI Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first product CLI separate from `wax-bench-cli`, backed by `wax-v2-runtime`, without leaking benchmark workload language into the user-facing command surface.

**Architecture:** Introduce a dedicated product CLI crate that wraps `wax-v2-runtime` for create, compatibility import, and search. Keep `wax-bench-cli` unchanged as benchmark infrastructure.

**Tech Stack:** Rust workspace crates, `clap`, cargo tests, staged Wax v2 engine crates

---

## Chunk 1: CLI Skeleton And Runtime Wiring

### Task 1: Add a dedicated product CLI crate

**Files:**
- Modify: `Cargo.toml`
- Modify: new product CLI crate files
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing CLI contract test for create and search through the product-facing command surface.
- [x] Step 2: Introduce a new CLI crate separate from `wax-bench-cli` and wire argument parsing for root path plus subcommands.
- [x] Step 3: Route the first commands through `wax-v2-runtime` without leaking benchmark workload terms.
- [x] Step 4: Run the directly affected crate tests.

## Chunk 2: Honest Compatibility Write Commands

### Task 2: Expose compatibility import commands without overstating parity

**Files:**
- Modify: same product CLI crate and docs only as needed

- [x] Step 1: Add a failing test for runtime-backed `create` plus compatibility import commands.
- [x] Step 2: Expose CLI commands for `create`, `import-compat`, and `search`.
- [x] Step 3: Keep the command naming explicit that current writes are compatibility-pack imports, not final raw ingest.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- The product CLI should stay thin and let `wax-v2-runtime` own request validation and orchestration.
- Do not mix benchmark-only commands such as workload runners, packers, reducers, or matrix reports into the product CLI.
- The first CLI slice should prefer stable nouns and verbs over short benchmark shorthands, even if the underlying implementation still depends on compatibility manifests and sidecars.
- The package is `wax-cli` and the product binary name is `wax`, so the user-facing entry point is distinct from `wax-bench-cli` while the workspace package name stays explicit.
- The first command surface stays intentionally narrow: `create`, `import-compat`, and text `search`. Vector or hybrid CLI search is still deferred until the runtime has a better public vector-input story than raw caller-provided embedding arrays.
