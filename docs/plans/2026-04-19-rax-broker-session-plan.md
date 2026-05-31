# Rax Broker Session Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first broker/session model on top of `wax-v2-runtime` so product callers can hold a durable session boundary without coupling directly to benchmark-era request files or one-shot CLI orchestration.

**Architecture:** Introduce a small broker-facing crate or module that owns session lifecycle, store handle reuse, and future request routing. Keep it above `wax-v2-runtime`; there is no current MCP integration layer.

**Current status note, 2026-05-29:** This plan records the first broker slice.
The current broker surface includes raw `ingest_documents` and `ingest_vectors`
session methods above `NewDocument` and `NewDocumentVector`, not only the
historical compatibility bridge.

**Tech Stack:** Rust workspace crates, cargo tests, staged Wax v2 engine crates

---

## Chunk 1: Session Lifecycle Boundary

### Task 1: Introduce the first broker/session facade

**Files:**
- Modify: new broker/session crate or module
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing test for opening a session against a store root and reusing it across multiple text searches.
- [x] Step 2: Define stable broker/session types for open, execute search, and close.
- [x] Step 3: Keep the session boundary thin over `wax-v2-runtime` without leaking benchmark workload or query-set concepts.
- [x] Step 4: Run the directly affected crate tests.

## Chunk 2: Compatibility-Aware Write Entry

### Task 2: Route the historical compatibility import path through the session boundary

**Files:**
- Modify: same broker/session crate and supporting docs only as needed

- [x] Step 1: Add a failing test for session-driven compatibility import followed by search.
- [x] Step 2: Expose a session-level compatibility import action that delegates to `wax-v2-runtime`.
- [x] Step 3: Keep the naming explicit that this is compatibility import, not final raw ingest or structured memory mutation.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- The first broker/session slice should focus on lifecycle and boundary ownership, not network transport details.
- Session identity, concurrency, and pooling can stay local/in-process for this slice as long as the surface is reusable by a later daemon.
- Do not let benchmark runner concepts become the session contract.
- The implemented crate is `wax-v2-broker`. It owns opaque `SessionId` values and a local in-process map of active `RuntimeStore` handles rather than pushing session identity into `wax-v2-runtime`.
- Historical first-slice note: the first broker search surface was intentionally text-only through `SessionSearchRequest::text(...)`.
- Current surface note: `SessionSearchRequest` now supports text, vector, and hybrid modes, and raw document/vector ingest is exposed through `ingest_documents` and `ingest_vectors`.
