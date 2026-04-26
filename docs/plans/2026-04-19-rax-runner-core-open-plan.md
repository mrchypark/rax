# Rax Runner Core Open Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the benchmark runner target the real `wax-v2-core` open path so harness execution starts proving the same store-open contract the product runtime will depend on.

**Architecture:** Preserve current benchmark behavior and metrics while replacing any remaining direct dataset-pack open assumptions with the real core container open path. Keep the harness green and stage this migration so text/vector compatibility crates still provide the search behavior behind the new open boundary.

**Tech Stack:** Rust workspace crates, `wax-bench-runner`, `wax-bench-text-engine`, `wax-v2-core`, cargo tests

---

## Chunk 1: Route Runner Open Through Core

### Task 1: Replace runner-side open assumptions with the real store open path

**Files:**
- Modify: `crates/wax-bench-runner/src/lib.rs`
- Modify: `crates/wax-bench-text-engine/src/lib.rs`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing test for the first runner/core open integration boundary.
- [x] Step 2: Make the runner exercise `wax-v2-core` open validation as part of benchmark store open.
- [x] Step 3: Keep benchmark outputs and current search behavior stable while the open boundary shifts.
- [x] Step 4: Run `cargo test -p wax-bench-runner`.

## Chunk 2: Re-verify Harness Contracts

### Task 2: Keep the benchmark harness green after the open-path migration

**Files:**
- Modify: contract tests or benchmark integration points only if needed

- [x] Step 1: Re-run the directly affected benchmark engine and runner contracts.
- [x] Step 2: Run `cargo test --workspace --quiet`.

## Notes

- The first integration point lives in `PackedTextEngine.open`, not `wax-bench-runner`, because the runner must stay generic over `WaxEngine::Error` and should not hard-code a `wax-v2-core` dependency.
- The new contract test proves that an invalid `store.wax` now fails benchmark `ContainerOpen` before any search workload begins.
- Compatibility manifest loading and search behavior remain in place after core-open validation; real segment-backed reads are the next unchecked migration slice.
