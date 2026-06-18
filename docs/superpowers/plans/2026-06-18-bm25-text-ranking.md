# BM25 Text Ranking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add BM25-style text ranking, ranking diagnostics, and a v2 text segment that stores term frequency, document length metadata, and analyzer profile/version metadata.

**Architecture:** Keep the public `TextLane::search*` APIs returning `Vec<String>` for existing callers, and add a diagnostics API for scored hits. Decode both v1 and v2 text segments; write v2 segments for new store-backed text publishes.

**Tech Stack:** Rust stdlib, existing rax text binary segment format, existing tokenizer.

---

### Task 1: Ranking Diagnostics

**Files:**
- Modify: `crates/rax-text/src/lib.rs`

- [x] Write a failing unit test in `crates/rax-text/src/lib.rs` showing a repeated-term document outranks a single-term document for query `alpha`.
- [x] Run `rustup run 1.85.0 cargo test -p rax-text bm25 -- --nocapture` and confirm it fails.
- [x] Add `TextSearchDiagnostic`, `TextSearchTermDiagnostic`, `TextPosting`, and `TextLane::search_with_diagnostics`.
- [x] Keep `TextLane::search_with_limit` as a thin wrapper over diagnostics.
- [x] Run `rustup run 1.85.0 cargo test -p rax-text bm25 -- --nocapture` and confirm it passes.

### Task 2: V2 Text Segment

**Files:**
- Modify: `crates/rax-text/src/lib.rs`

- [x] Write a failing unit test proving `BinaryTextSegment::from_documents` encodes a v2 segment and decodes term frequency plus document lengths.
- [x] Run `rustup run 1.85.0 cargo test -p rax-text text_segment_v2 -- --nocapture` and confirm it fails.
- [x] Extend binary segment encoding with version 2 records: token, doc count, repeated `(doc_id, term_frequency)`, followed by document length records.
- [x] Keep v1 decode support by treating old postings as `term_frequency = 1` and deriving document lengths from postings.
- [x] Run `rustup run 1.85.0 cargo test -p rax-text text_segment_v2 -- --nocapture` and confirm it passes.

### Task 3: Verification

**Files:**
- Modify: `rust-toolchain.toml`
- Modify: `docs/superpowers/plans/2026-06-18-bm25-text-ranking.md`
- Modify: `crates/rax-text/src/lib.rs`

- [x] Run `rustup run 1.85.0 cargo test -p rax-text`.
- [x] Run `rustup run 1.85.0 cargo check --workspace --all-targets`.
- [x] Check `git diff --stat` and ensure the change stays scoped.

### Task 4: Analyzer Profile And Versioning

**Files:**
- Modify: `crates/rax-text/src/lib.rs`

- [x] Write a failing unit test proving unsupported text analyzer profiles are rejected before indexing.
- [x] Extend v2 text segment encoding with analyzer profile name and profile version metadata.
- [x] Decode v1 text segments as a legacy analyzer profile for backwards compatibility.
- [x] Keep new writes on the current simple alnum lowercase analyzer profile.
- [x] Run `rustup run 1.85.0 cargo test -p rax-text analyzer_profile -- --nocapture`.
- [x] Run `rustup run 1.85.0 cargo test -p rax-text text_segment_v2 -- --nocapture`.
- [x] Run `rustup run 1.85.0 cargo test --workspace --all-targets`.

### Task 5: Experimental Alyze Analyzer Profile

**Files:**
- Modify: `Cargo.lock`
- Modify: `crates/rax-text/Cargo.toml`
- Modify: `crates/rax-text/src/lib.rs`

- [x] Add `alyze = "0.1.3"` to `rax-text`.
- [x] Keep the default text profile on `rax-simple-alnum-lower`.
- [x] Add an experimental `rax-alyze-uax29-ascii-fold` profile for segment-building experiments.
- [x] Use the segment profile for query analysis so experimental segments search with matching tokenization.
- [x] Add a regression test proving the experimental profile is recorded and ASCII-folds `Café` to `cafe`.
- [x] Add an ignored release microbenchmark for simple vs experimental alyze segment build/search.
- [x] Run `rustup run 1.85.0 cargo test -p rax-text experimental_alyze_profile_microbench --release -- --ignored --nocapture`.
- [x] Record the 2,000-document microbench result: simple build+encode 9.480981ms, alyze build+encode 12.65731ms, simple search batch 1.797054ms, alyze search batch 1.671837ms.
- [x] Add an ignored English/Korean quality benchmark for simple vs experimental alyze.
- [x] Run `rustup run 1.85.0 cargo test -p rax-text experimental_alyze_profile_quality_bench_english_and_korean --release -- --ignored --nocapture`.
- [x] Expand the quality benchmark to actively search for alyze-winning cases: Café/cafe, naïve/façade/résumé/coöperate/jalapeño, São/sao, Straße/strasse, hyphen, apostrophe, snake_case, Korean whitespace, and Korean no-space morphology.
- [x] Record expanded quality result: English simple nDCG/MRR/recall/success@1 = 0.4286, English alyze = 0.7143; ASCII-folding subset simple = 0.25, alyze = 1.0; punctuation subset simple = 0.6667, alyze = 0.3333; Korean whitespace subset both = 1.0; Korean no-morphology subset both = 0.0.
