# Rax MCP Surface Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first MCP-compatible surface on top of the new broker/session model so product callers can drive search and compatibility import through tool-style requests without coupling directly to benchmark CLI flows.

**Architecture:** Keep `wax-v2-broker` as the session owner and add a small MCP-facing adapter crate or module above it. The first slice should stay transport-light and focus on request/response shape plus lifecycle mapping.

**Tech Stack:** Rust workspace crates, cargo tests, staged Wax v2 engine crates

---

## Chunk 1: MCP-Friendly Session Tools

### Task 1: Introduce the first MCP-compatible tool surface

**Files:**
- Modify: new MCP-facing crate or module
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`

- [x] Step 1: Add a failing test for opening a broker session and executing text search through an MCP-style tool boundary.
- [x] Step 2: Define stable request/response shapes for session open, text search, and close.
- [x] Step 3: Keep the tool surface thin over `wax-v2-broker` without leaking benchmark CLI verbs or raw runtime internals.
- [x] Step 4: Run the directly affected crate tests.

## Chunk 2: Compatibility Import Tooling

### Task 2: Route compatibility import through the MCP surface

**Files:**
- Modify: same MCP-facing crate or module and docs only as needed

- [x] Step 1: Add a failing test for MCP-style compatibility import followed by search.
- [x] Step 2: Expose a tool action for current compatibility import via the broker session boundary.
- [x] Step 3: Keep naming explicit that this is compatibility import rather than final raw ingest or structured-memory mutation.
- [x] Step 4: Run `cargo test --workspace --quiet`.

## Notes

- The first MCP slice does not need a full daemon or network server as long as the request/response surface is transport-ready.
- Session ids should remain opaque and broker-owned.
- Do not let benchmark pack/run/reduce/query-batch commands become the MCP contract.
- The implemented crate is `wax-v2-mcp`. It stays transport-light but uses serializable request/response enums so a later server can forward the same shapes instead of inventing a second contract.
- The first MCP-compatible tool surface is intentionally text-first and compatibility-write-explicit: `open_session`, `search_text`, `import_compatibility_snapshot`, and `close_session`.
