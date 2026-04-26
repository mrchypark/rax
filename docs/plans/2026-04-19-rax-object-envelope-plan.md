# Rax Object Envelope Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tighten the current raw publication path so appended segment and manifest objects move toward the `wax-v2-binary-format` contract with explicit `WXOB` envelopes and page-aligned append boundaries.

**Architecture:** Keep the current reopen-safe publication helper shape, but wrap appended objects in a Wax-owned envelope and align object starts before publication. This remains below the benchmark caller layer and should not change the compatibility docstore or text-engine interfaces.

**Tech Stack:** Rust workspace crates, `wax-v2-core`, existing binary-format spec, cargo tests

---

## Chunk 1: Object Envelope

### Task 1: Add a Wax object envelope for appended objects

**Files:**
- Modify: `crates/wax-v2-core/src/lib.rs`
- Add or Modify tests in the same crate

- [x] Step 1: Write failing tests for writing and reading a segment object through a `WXOB` envelope.
- [x] Step 2: Wrap appended segment and manifest bytes in a typed envelope with checksum validation.
- [x] Step 3: Run `cargo test -p wax-v2-core`.

## Chunk 2: Page-Aligned Publication

### Task 2: Align appended objects on page boundaries

**Files:**
- Modify: `crates/wax-v2-core/src/lib.rs`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`
- Modify: `.codex/context/ACTIVE_TASK.md`

- [x] Step 1: Write failing tests for page-aligned object append offsets.
- [x] Step 2: Add alignment padding before object publication and keep reopen/readback tests green.
- [x] Step 3: Refresh the active task note for the next post-alignment slice.

Completion note:
- `wax-v2-core` now wraps manifest and segment payloads in a 64-byte `WXOB` envelope.
- Published object starts are now aligned to 4096-byte boundaries, and descriptor/superblock offsets point at the envelope start.
- The remaining write-path hardening gap is snapshot isolation and recovery behavior rather than raw object layout.

Next pending slice:
- `docs/plans/2026-04-19-rax-snapshot-isolation-plan.md`

External critique applied:
- The object-envelope slice kept `descriptor.object_length` as header-plus-payload length, excluding trailing alignment gaps.
- The alternate-superblock switch model remains the minimal safe publication path, while journal-backed recovery still remains out of scope.
