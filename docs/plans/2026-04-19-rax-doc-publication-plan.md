# Rax Doc Publication Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `wax-v2-docstore` from an internal binary codec plus compatibility facade to a real publication boundary with stable Wax-owned numeric doc ids and manifest-visible doc segments.

**Architecture:** Keep the dataset-pack compatibility path alive for the benchmark harness until the real publication flow is wired. The next slice assigns stable numeric doc ids, publishes binary doc segments through `wax-v2-core`, and decides whether `metadata_ref` and `preview_ref` become authoritative byte pointers or remain logical row references backed by a different directory structure.

**Tech Stack:** Rust workspace crates, `wax-v2-core`, `wax-v2-docstore`, existing benchmark dataset pack manifest, cargo tests

---

## Chunk 1: Stable Doc Id Ownership

### Task 1: Define and persist Wax-owned numeric doc ids

**Files:**
- Modify: `crates/wax-v2-docstore/src/lib.rs`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`
- Modify: `docs/todos/2026-04-19-rax-to-wax-master-todo.md`

- [x] Step 1: Write failing tests for deterministic numeric doc-id allocation independent of dataset-pack string ids.
- [x] Step 2: Define the persisted mapping boundary that later text/vector segments can consume.
- [x] Step 3: Run `cargo test -p wax-v2-docstore`.

## Chunk 2: Real Doc Segment Publication

### Task 2: Publish doc segments through the core manifest

**Files:**
- Modify: `crates/wax-v2-core/src/lib.rs`
- Modify: `crates/wax-v2-docstore/src/lib.rs`
- Add or Modify tests in the same crates

- [x] Step 1: Write failing tests for manifest-visible doc segment publication and reopen.
- [x] Step 2: Append binary doc segment bytes and publish them through the expanded core descriptor contract.
- [x] Step 3: Run `cargo test -p wax-v2-core` and `cargo test -p wax-v2-docstore`.

## Chunk 3: Resolve The Current Ref Contract Gap

### Task 3: Decide the final meaning of `metadata_ref` and `preview_ref`

**Files:**
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`
- Modify: `crates/wax-v2-docstore/src/lib.rs`
- Modify: `.codex/context/ACTIVE_TASK.md`

- [x] Step 1: Either make row refs authoritative byte pointers or explicitly redefine them as logical row references with a separate section directory.
- [x] Step 2: Add tests that prove the chosen contract on decode.
- [x] Step 3: Refresh the active task note for the next lane migration.

Decision note:
- The first stable Wax-owned numeric doc-id boundary uses 0-based `u64` ids assigned in `docs.ndjson` file order.
- The compatibility string `doc_id` remains an external key only; future text/vector lanes should consume the `DocIdMap` boundary instead of assuming string ids are canonical.
- The `DocIdMap` persistence boundary currently round-trips as JSON for determinism tests and handoff safety. Real publication can replace the storage format later without changing the logical contract.

Risk note:
- This choice makes dataset-pack file order a visible source of truth. If the packer reorders documents, Wax numeric ids change too. That dependency is now explicit in tests and must be preserved or intentionally revised in a later slice.
- The current publication helper appends raw segment bytes and raw manifest bytes, then flips only the alternate superblock to the higher generation. This is enough for reopen tests, but it is still narrower than the final binary-format contract because `WXOB` object envelopes and page-aligned append boundaries are not implemented yet.
- The final ref contract now treats `metadata_ref` and `preview_ref` as authoritative section-local byte pointers. `preview_ref.length == 0` decodes to `None`, which means the format currently does not distinguish `None` from `Some(\"\")`.

Current blocker:
- Yeoul reads from the user-level database succeed, but write commands such as `ingest episode` and `migrate` still fail with `YEOUL_CONFIG_INVALID: open ladybug database`. Durable status updates for this slice are therefore mirrored in docs until Yeoul write support is restored.

Completion note:
- `metadata_ref` and `preview_ref` are now authoritative on decode and encode.
- The next pending slice is tightening the raw publication helper toward the binary-format spec by adding `WXOB` envelopes and page-aligned append boundaries.
