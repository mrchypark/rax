# Rax Docstore Compatibility Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce `wax-v2-docstore` as a compatibility read facade over the current dataset-pack document files, and route `wax-bench-text-engine` document reads through it without changing runner or CLI boundaries.

**Architecture:** Treat this as a migration slice, not the final doc-segment design. The new crate owns dataset-pack document lookup and doc-id parsing. `wax-bench-text-engine` becomes a caller of the new crate, while true binary doc segments, tombstone semantics, and canonical numeric doc ids remain deferred.

**Tech Stack:** Rust workspace crates, `serde_json`, existing dataset-pack manifest schema, cargo tests

---

## Chunk 1: Introduce The Compatibility Crate

### Task 1: Add `wax-v2-docstore`

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/wax-v2-docstore/Cargo.toml`
- Create: `crates/wax-v2-docstore/src/lib.rs`

- [x] Step 1: Add the new crate to the workspace.
- [x] Step 2: Write failing tests for dataset-pack open and document lookup.
- [x] Step 3: Implement `open_dataset_pack`, `load_documents_by_id`, and `load_document_ids`.
- [x] Step 4: Run `cargo test -p wax-v2-docstore`.

## Chunk 2: Move Text-Engine Callers Behind The Boundary

### Task 2: Delegate benchmark document reads to the new crate

**Files:**
- Modify: `crates/wax-bench-text-engine/Cargo.toml`
- Modify: `crates/wax-bench-text-engine/src/documents.rs`
- Modify: `crates/wax-bench-text-engine/src/lib.rs`

- [x] Step 1: Replace direct document-file helpers with docstore wrappers.
- [x] Step 2: Keep `runner` and CLI call sites unchanged.
- [x] Step 3: Run `cargo test -p wax-bench-text-engine`.

## Chunk 3: Hand Off To Real Doc Segments

### Task 3: Record what remains out of scope

**Files:**
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`
- Modify: `docs/todos/2026-04-19-rax-to-wax-master-todo.md`
- Modify: `.codex/context/ACTIVE_TASK.md`

- [x] Step 1: Keep docstore compatibility scope explicit in the roadmap.
- [x] Step 2: Leave unchecked the durable doc-segment items that still remain.
- [x] Step 3: Hand off to the next slice: binary doc segment layout plus core descriptor expansion.

Blocked note:
- `.codex/context/ACTIVE_TASK.md` refresh is currently blocked by the workspace sandbox in this heartbeat environment even though the file exists in the worktree. The next slice is still recorded in the roadmap and todo documents.

Completion note:
- The handoff slice is now complete: `wax-v2-core` uses the full 128-byte manifest segment descriptor shape, and `wax-v2-docstore` has a binary doc segment codec with tombstone row flags.
- The next pending slice is `docs/plans/2026-04-19-rax-doc-publication-plan.md`: stable Wax-owned numeric doc ids plus real doc-segment publication through the core manifest.
