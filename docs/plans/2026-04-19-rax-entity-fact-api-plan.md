# Rax Entity Fact API Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first explicit entity/fact APIs on top of the bootstrap structured-memory layer without pretending complete upstream graph semantics or bitemporal parity already exist.

**Architecture:** Keep `wax-v2-structured-memory` as the persistence owner and layer clearer entity and fact operations above the current bootstrap record model. The first cut should make entity and fact intent explicit while leaving deeper graph traversal, bitemporal querying, and evidence richness for later slices.

**Tech Stack:** Rust workspace crates, cargo tests, staged Wax v2 engine crates

---

## Chunk 1: Explicit Entity Boundary

### Task 1: Add the first entity-facing API

**Files:**
- Modify: `crates/wax-v2-structured-memory/src/lib.rs`
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing test for creating or upserting an explicit entity record with aliases or kind metadata.
- [x] Step 2: Define stable entity-facing types and operations above the bootstrap record model.
- [x] Step 3: Keep the API explicit that entity identity is now first-class, but alias normalization and full graph traversal are still future work.
- [x] Step 4: Run the directly affected crate tests.

## Chunk 2: Explicit Fact Boundary

### Task 2: Add the first fact-facing API

**Files:**
- Modify: same structured-memory crate and docs only as needed

- [x] Step 1: Add a failing test for asserting and reading a fact through dedicated fact APIs rather than generic record calls.
- [x] Step 2: Expose fact assertion and read helpers that map onto the bootstrap structured-memory persistence boundary.
- [x] Step 3: Keep naming explicit that this is the first entity/fact API layer, not full upstream bitemporal/evidence parity.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- Keep the first entity/fact API explicit enough that broker or MCP callers could adopt it later without reinterpreting generic bootstrap records.
- Do not promise full bitemporal semantics, dedup hashes, or evidence span parity in this slice.
- Reuse the bootstrap provenance and status fields instead of inventing a second hidden persistence path.
- The implemented bootstrap entity layer uses reserved record predicates for kind and aliases inside the same `structured-memory.ndjson` file. This keeps persistence flat and explicit, but alias normalization, dedup hashes, entity-object value kinds, and graph traversal are still future work.
