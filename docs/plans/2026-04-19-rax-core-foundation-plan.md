# Rax Core Foundation Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first real Wax v2 core crate to `rax`, centered on binary superblocks and manifest encoding/validation, without breaking the existing benchmark harness.

**Architecture:** Keep the benchmark crates intact and introduce a new `wax-v2-core` crate that owns the binary container contract. The first slice focuses on format definition, file create/open validation, and tests. Later slices can build doc/text/vector segment publication on top of that stable binary substrate.

**Tech Stack:** Rust workspace crates, `serde`, `sha2`, standard file I/O, existing cargo test workflow

---

## Chunk 1: Workspace And Format Skeleton

### Task 1: Add the new core crate to the workspace

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/wax-v2-core/Cargo.toml`
- Create: `crates/wax-v2-core/src/lib.rs`

- [x] Step 1: Add `crates/wax-v2-core` to the workspace members.
- [x] Step 2: Create the crate manifest with only the dependencies needed for the container format.
- [x] Step 3: Create a minimal public API centered on create/open/validate helpers.
- [x] Step 4: Run `cargo test -p wax-v2-core`.

### Task 2: Define superblock and manifest types

**Files:**
- Modify: `crates/wax-v2-core/src/lib.rs`
- Test: `crates/wax-v2-core/src/lib.rs`

- [x] Step 1: Write failing tests for binary superblock encode/decode and checksum validation.
- [x] Step 2: Implement typed structs for superblock, manifest header, and segment metadata.
- [x] Step 3: Implement binary encode/decode helpers with explicit magic/version checks.
- [x] Step 4: Run `cargo test -p wax-v2-core superblock`.

## Chunk 2: Minimal Store Lifecycle

### Task 3: Create and open a minimal store file

**Files:**
- Modify: `crates/wax-v2-core/src/lib.rs`
- Test: `crates/wax-v2-core/src/lib.rs`

- [x] Step 1: Write a failing test that creates an empty store and reopens it.
- [x] Step 2: Implement store initialization with dual superblocks and a minimal active manifest.
- [x] Step 3: Implement open-time validation for generation, checksum, and active pointer resolution.
- [x] Step 4: Run `cargo test -p wax-v2-core store_create_open`.

### Task 4: Detect corruption and invalid generations

**Files:**
- Modify: `crates/wax-v2-core/src/lib.rs`
- Test: `crates/wax-v2-core/src/lib.rs`

- [x] Step 1: Write failing corruption tests for bad magic, bad checksum, and broken manifest offsets.
- [x] Step 2: Implement explicit error types and validation paths.
- [x] Step 3: Run `cargo test -p wax-v2-core corruption`.

## Chunk 3: Documentation And Harness Integration Boundary

### Task 5: Document the new core format boundary

**Files:**
- Modify: `docs/todos/2026-04-19-rax-to-wax-master-todo.md`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Update the todo document to mark the core crate as in progress.
- [x] Step 2: Note any design corrections discovered during implementation.
- [x] Step 3: Run `cargo test --quiet`.

### Task 6: Prepare the next migration slice

**Files:**
- Modify: `.codex/context/ACTIVE_TASK.md`
- Modify: `docs/plans/2026-04-19-rax-core-foundation-plan.md`

- [x] Step 1: Refresh the active task note with the next pending slice.
- [x] Step 2: Record any blockers or spec mismatches discovered in the first slice.
- [x] Step 3: Hand off to the docstore slice only after the format tests are green.
