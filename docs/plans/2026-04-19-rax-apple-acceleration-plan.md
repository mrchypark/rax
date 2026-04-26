# Rax Apple Acceleration Parity Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Start the first Apple-specific acceleration parity slice without pretending that `rax` already has full upstream Apple framework integration, hardware-tuned embedding pipelines, or platform-exclusive retrieval behavior.

**Architecture:** Keep the existing Rust-first lane contracts intact and add a narrow capability and backend-selection layer above them. The first slice should make Apple-specific acceleration discoverable and optional rather than rewriting text, vector, or multimodal lanes around a hard Apple dependency.

**Tech Stack:** Rust workspace crates, cargo tests, staged Wax v2 engine crates

---

## Chunk 1: Capability Surface

### Task 1: Make Apple acceleration capabilities explicit

**Files:**
- Modify: runtime-facing crates and roadmap docs as needed
- Modify: root contract tests

- [x] Step 1: Add a failing test for reporting Apple acceleration capability or unavailability through an explicit API rather than hidden platform assumptions.
- [x] Step 2: Define a stable capability/result type that callers can inspect without binding to Apple frameworks at compile time everywhere.
- [x] Step 3: Keep the first surface explicit that this is capability reporting and optional backend selection, not full Apple embedding or search parity.
- [x] Step 4: Run the directly affected crate tests.

## Chunk 2: Optional Backend Hinting

### Task 2: Preserve room for later Apple-tuned backends

**Files:**
- Modify: same crates and docs only as needed

- [x] Step 1: Add a failing test for selecting an Apple-acceleration preference or hint without changing existing default behavior on non-Apple paths.
- [x] Step 2: Extend the capability surface with an explicit optional backend preference contract while keeping current Rust-first implementations as the default.
- [x] Step 3: Keep naming explicit that this is first Apple parity scaffolding above current lanes, not complete upstream hardware-accelerated execution.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- Do not let Apple-specific concerns leak into `wax-v2-core` on-disk format decisions.
- Do not require Apple frameworks or platform SDKs for default workspace builds or tests.
- The first success condition is an explicit, inspectable acceleration capability boundary that later platform-tuned implementations can adopt.
- The implemented first slice keeps backend preference out of `RuntimeSearchRequest`: `RuntimeStore::capabilities()` reports Apple-family acceleration status explicitly, and `RuntimeStore::resolve_acceleration()` handles optional strategy selection separately from search intent.
