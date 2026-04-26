# Rax Structured Memory Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Start the first structured-memory slice above the completed runtime, CLI, broker, and MCP surfaces without pretending full upstream entity/fact parity already exists.

**Architecture:** Keep the first structured-memory boundary compatibility-light and local to Rust crates. Reuse the current durable core and product-surface stack, but define a new entity/fact-oriented layer explicitly instead of smuggling structure into existing document or search contracts.

**Tech Stack:** Rust workspace crates, cargo tests, staged Wax v2 engine crates

---

## Chunk 1: Structured Session Boundary

### Task 1: Introduce the first structured-memory session facade

**Files:**
- Modify: new structured-memory crate or module
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing test for opening a structured-memory session and recording a simple entity or fact-shaped item.
- [x] Step 2: Define stable structured session types for open, write, read, and close.
- [x] Step 3: Keep the first surface explicit that this is a bootstrap structured-memory layer rather than full upstream parity.
- [x] Step 4: Run the directly affected crate tests.

## Chunk 2: Search And Provenance Follow-Through

### Task 2: Make the first structured-memory records queryable

**Files:**
- Modify: same structured-memory crate and docs only as needed

- [x] Step 1: Add a failing test for retrieving the new structured-memory records through a stable query path.
- [x] Step 2: Expose a query/read boundary for the first structured records.
- [x] Step 3: Keep provenance and status explicit enough that later Yeoul-style entity/fact parity can grow from the same boundary.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- The first structured-memory slice should not force final entity/fact schema choices too early.
- Avoid encoding structured memory as hidden metadata blobs inside compatibility document rows.
- Reuse the existing runtime/broker/MCP surfaces where helpful, but keep the structured-memory boundary explicit.
- The implemented crate is `wax-v2-structured-memory`, and the first persistence layer is an explicit bootstrap file `structured-memory.ndjson` in the store root rather than a final Wax binary segment format.
- The first record shape is intentionally narrow: `subject`, `predicate`, `value`, explicit `status`, and explicit provenance `{source, asserted_at_ms}`. This is enough for bootstrap structured memory, but it is not yet full upstream entity/fact API parity.
