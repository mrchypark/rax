# Rax Snapshot Isolation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove that the current publication path preserves snapshot isolation properties for already-opened views while later generations publish through the alternate superblock.

**Architecture:** Stay within `wax-v2-core` first. Treat `OpenedStore` as the read snapshot and add regression tests around publish/reopen behavior before widening the API surface or moving into the text lane.

**Tech Stack:** Rust workspace crates, `wax-v2-core`, cargo tests

---

## Chunk 1: Snapshot Stability

### Task 1: Add snapshot isolation tests for already-opened views

**Files:**
- Modify: `crates/wax-v2-core/src/lib.rs`
- Modify: `.codex/context/ACTIVE_TASK.md`

- [x] Step 1: Write failing tests that keep an old `OpenedStore` value alive across a later publish and prove its visible generation and segment set do not change.
- [x] Step 2: Add any helper adjustments needed to express that contract clearly without mutating opened snapshots.
- [x] Step 3: Run `cargo test -p wax-v2-core`.

## Chunk 2: Generation Progression

### Task 2: Verify multi-generation reopen behavior

**Files:**
- Modify: `crates/wax-v2-core/src/lib.rs`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`
- Modify: `docs/todos/2026-04-19-rax-to-wax-master-todo.md`

- [x] Step 1: Write failing tests for publishing more than one generation and reopening the highest valid generation.
- [x] Step 2: Confirm the alternate-superblock switch model still selects the latest valid generation after multiple publishes.
- [x] Step 3: Run `cargo test --quiet`.

Completion note:
- `OpenedStore` values now have explicit regression tests proving they remain stable after later publishes.
- Multi-generation reopen now has tests for both highest-valid-generation selection and fallback to the previous valid generation when the newest superblock copy is corrupted.

External critique applied:
- The fallback-to-previous-valid-generation case was kept in this slice rather than deferred because it is part of the minimal safe alternate-superblock contract.

Next pending slice:
- `wax-v2-text` as the first real lane crate, starting with a compatibility adapter over the current lexical search path.
