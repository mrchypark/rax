# Rax Compaction Follow-up Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write the first concrete compaction follow-up for the Wax v2 write path so the current publication model has an explicit next design target for segment cleanup, tombstone handling, and generation rewriting.

**Architecture:** Keep the current append-only publication path intact, but document and test the next compaction design boundary rather than improvising it inside the benchmark harness. The outcome of this slice should be a design-backed plan that later code can implement against the existing `wax-v2-core` and `wax-v2-docstore` contracts.

**Tech Stack:** Rust workspace docs, existing `wax-v2-core` and `wax-v2-docstore` contracts, cargo tests only if code changes become necessary

---

## Chunk 1: Define The Compaction Follow-up

### Task 1: Turn the remaining write-path gap into an explicit design target

**Files:**
- Modify: `docs/specs/2026-04-19-rax-to-wax-roadmap.md`
- Modify: `docs/todos/2026-04-19-rax-to-wax-master-todo.md`
- Modify: `.codex/context/ACTIVE_TASK.md`
- Add or modify: follow-up compaction design doc under `docs/specs/`

- [x] Step 1: Re-read the current core/docstore publication contracts and list the concrete compaction gaps.
- [x] Step 2: Write the first compaction follow-up design in docs, including segment selection, rewrite shape, tombstone handling, and publication expectations.
- [x] Step 3: Record any design correction or wrong turn in roadmap/todo/Yeoul if the compaction plan changes earlier assumptions.
- [x] Step 4: Run any targeted verification commands needed if code or executable examples change.

## Notes

- Completed on 2026-04-19.
- The new design doc is [2026-04-19-wax-v2-compaction-followup.md](/Users/cypark/.codex/worktrees/0c4e/rax/docs/specs/2026-04-19-wax-v2-compaction-followup.md).
- The follow-up explicitly separates logical active-set compaction from later physical byte reclamation.
- No executable code changed in this slice, so verification was a targeted document and contract review rather than cargo test execution.
