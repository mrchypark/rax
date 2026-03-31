# Wax v2 Search Fusion

Status: Draft  
Date: 2026-03-29  
Scope: unified query planning, candidate budgeting, and fusion rules for Wax v2

## 1. Purpose

This document defines how Wax v2 combines results from the text lane, vector lane, and docstore-level filtering.

Its purpose is to fix the parts that should remain stable even if the first text backend or first vector backend is later replaced:

- what unified search is responsible for
- what unified search is not responsible for
- how lane selection works
- how candidate budgets are assigned
- how fusion and rerank are constrained

This document does not lock Wax v2 into one ranking formula forever. It defines the planner and fusion contract that the first implementation must satisfy.

## 2. Inputs

This search-fusion design derives from:

- [2026-03-29-wax-v2-architecture.md](/Users/cypark/Documents/project/rax/docs/specs/2026-03-29-wax-v2-architecture.md)
- [2026-03-29-wax-v2-benchmark-plan.md](/Users/cypark/Documents/project/rax/docs/specs/2026-03-29-wax-v2-benchmark-plan.md)
- [2026-03-29-wax-v2-text-lane.md](/Users/cypark/Documents/project/rax/docs/specs/2026-03-29-wax-v2-text-lane.md)
- [2026-03-29-wax-v2-vector-lane.md](/Users/cypark/Documents/project/rax/docs/specs/2026-03-29-wax-v2-vector-lane.md)
- upstream Wax unified-search references:
  - `SearchMode`
  - `HybridSearch`
  - `AdaptiveFusionConfig`
  - `SearchRequest`

Current design assumptions:

- Wax v2 is performance-first
- cold open and search p95 are the main gates
- phase 1 search lanes are text and vector
- structured memory is out of phase 1 hot-path scope
- exact backend score semantics should not leak upward

## 3. Design Goals

### 3.1 Primary Goals

- predictable search p95 across text, vector, and hybrid queries
- stable result ordering under backend replacement
- cheap planner decisions that avoid obviously bad lane choices
- bounded candidate counts before expensive rerank or preview work

### 3.2 Secondary Goals

- support query-adaptive behavior without large hidden latency spikes
- keep diagnostics explainable
- keep fusion policy separable from backend internals

### 3.3 Non-Goals

- full ML query understanding in phase 1
- expensive cross-encoder reranking in phase 1
- maximizing recall at any latency cost
- lane-specific ranking hacks leaking into the public contract

## 4. Unified Search Responsibilities

Unified search is responsible for:

- selecting search mode for a request
- deciding which lanes should run
- assigning candidate budgets per lane
- merging lane-local candidates
- applying bounded rerank or fusion
- returning final ordered hits and diagnostics

Unified search is not responsible for:

- implementing text index internals
- implementing ANN internals
- storing canonical document payloads
- generating embeddings

This separation is deliberate.

## 5. Search Modes

Wax v2 phase 1 should support three explicit logical modes:

- `text_only`
- `vector_only`
- `hybrid`

These are planner-visible modes, not backend implementation details.

### 5.1 Mode Semantics

`text_only`

- query executes only in the text lane
- no vector work is attempted

`vector_only`

- query executes only in the vector lane
- requires a query vector or an embedding policy that can provide one

`hybrid`

- both text and vector lanes may contribute
- fusion combines lane-local candidate lists under Wax-owned rules

### 5.2 Fallback Rules

Phase 1 must define deterministic fallback behavior.

Required rules:

- `vector_only` falls back to `text_only` only if the caller explicitly allows degraded execution
- `hybrid` may degrade to `text_only` if no query vector is available
- `hybrid` may degrade to `text_only` or `vector_only` if planner rules decide the skipped lane is predictably wasteful

The caller should be able to distinguish:

- requested mode
- effective mode

## 6. Request Contract

Unified search should expose a logical interface equivalent to:

```text
search(request) -> search_response
search_batch(requests) -> [search_response]
```

Where `request` may include:

- raw query text
- optional query vector
- explicit mode
- result limit
- filter set
- diagnostics flags

And `search_response` should include:

- ordered hits
- effective mode
- lane contribution summary
- optional ranking diagnostics

## 7. Planner Contract

The planner is the most important part of unified search for phase 1.

Its job is not to be clever. Its job is to avoid expensive mistakes.

### 7.1 Planner Inputs

The planner may use:

- requested mode
- presence or absence of query text
- presence or absence of query vector
- filter shape
- requested limit
- segment availability
- cheap heuristics about query shape

### 7.2 Planner Outputs

The planner must decide:

- effective mode
- which lanes execute
- per-lane candidate budget
- whether exact rerank is enabled
- whether preview generation is eager or deferred

### 7.3 Phase 1 Recommendation

Phase 1 should use simple deterministic planner rules, not opaque learned routing.

Recommended examples:

- exact keyword-heavy queries favor text budget
- query-vector absence disables vector lane
- highly selective filters reduce or skip vector lane
- very small requested limits keep rerank budget small

The planner must remain cheap enough that it does not become measurable in p95 on its own.

### 7.4 Cost-Aware Guardrail

The planner should not rely only on binary heuristics such as "filter is selective, so skip vector."

It should also consider cheap cost signals such as:

- lane availability
- expected candidate-set size
- requested limit
- recent lane latency or coldness hints

The goal is not sophisticated optimization. The goal is to avoid avoidable performance cliffs.

## 8. Candidate Budgeting

Candidate budgeting is a first-class contract.

Wax v2 should not allow each lane to return arbitrarily large hit lists and hope fusion sorts it out.

### 8.1 Required Concepts

Every hybrid request should track:

- final limit
- text candidate budget
- vector candidate budget
- optional rerank budget

### 8.2 Phase 1 Recommendation

Phase 1 should use bounded over-fetch per lane.

Example policy shape:

- `final_limit = K`
- `text_budget = min(text_cap, K * text_multiplier)`
- `vector_budget = min(vector_cap, K * vector_multiplier)`
- `rerank_budget = min(rerank_cap, text_budget + vector_budget)`

The actual constants can remain implementation-tunable, but the existence of these budgets must be fixed.

### 8.3 Guardrail

Candidate budgets must be visible in diagnostics and benchmark output.

Without this, p95 regressions become hard to attribute.

## 9. Filtering Order

Filtering order strongly affects p95.

Wax v2 must not leave this implicit.

### 9.1 Required Rules

- cheap global filters should constrain lane work as early as possible
- lane-local candidate sets must be filtered before preview work
- fusion should not spend work on candidates already known to be invalid

### 9.2 High-Selectivity Guardrail

If filters imply a very small eligible set, the planner should prefer:

- text over vector when lexical evidence is available
- vector pre-restriction if supported cheaply
- lane skipping when the expected cost of ANN oversampling is high

Hybrid does not mean both lanes always run.

## 10. Fusion Contract

Fusion combines lane-local ordered candidates into a final list.

Wax v2 should prefer a fusion rule that is stable under backend changes.

### 10.1 Phase 1 Recommendation

Phase 1 should use rank-based fusion by default.

Recommended choice:

- weighted reciprocal rank fusion

Reason:

- it avoids raw score calibration problems between BM25-like scores and vector similarity scores
- it is deterministic
- it is easy to diagnose
- it is robust against backend score-scale changes

### 10.1.1 Lane-Local Quality Guardrail

Rank-based fusion is not enough by itself.

Before a lane contributes candidates to fusion, it should apply a lane-local quality floor when possible.

Examples:

- text lane minimum lexical quality threshold
- vector lane minimum similarity or maximum distance threshold

This prevents clearly weak lane-local results from polluting hybrid output just because they happen to be rank 1 in a weak list.

### 10.2 Fusion Inputs

Fusion may depend on:

- lane-local rank order
- lane-local inclusion
- Wax-owned per-lane weights

Fusion should not depend on:

- exact backend numeric comparability between text and vector scores

### 10.3 Tie-Breaking

Fusion output should be deterministic.

Recommended tie-break order:

1. fused score descending
2. best lane-local rank ascending
3. `doc_id` ascending

### 10.4 Fusion Execution Guardrail

Fusion should be implemented with bounded, low-allocation data structures.

The contract should assume:

- pre-sized buffers when candidate budgets are known
- no unnecessary materialization of large intermediate lists
- deterministic merge cost proportional to bounded candidate budgets

Fusion itself must not become a measurable share of hot-path latency.

## 11. Weighting Contract

Wax v2 may expose user-facing or caller-facing weighting, but this must remain constrained.

### 11.1 Phase 1 Recommendation

Phase 1 may allow:

- explicit `alpha`-style text/vector balance
- a small set of planner heuristics that adjust effective weights

But it should not allow:

- arbitrary score algebra from the caller
- backend-specific weight knobs that leak into the public request model

### 11.2 Effective Weights

The final effective weights may depend on:

- requested mode
- explicit alpha
- planner heuristics
- lane availability

The response diagnostics should make these effective weights visible when diagnostics are enabled.

## 12. Rerank Boundary

Fusion and rerank are not the same thing.

Wax v2 phase 1 should keep them separate.

### 12.1 Fusion

Fusion:

- combines lane-local candidate lists
- should stay cheap
- should mostly use ranks and small metadata

### 12.2 Rerank

Rerank:

- is optional
- operates on a bounded candidate set
- may use additional preview text or exact vector similarity
- must be disabled or capped if it threatens p95

### 12.3 Guardrail

Phase 1 should not introduce an expensive general-purpose reranker in the default hot path.

If rerank is used, it should be:

- bounded
- deterministic
- diagnosable

## 13. Preview and Metadata Loading

Preview generation is not free.

Unified search must not fetch or build full previews for every pre-fusion candidate.

### 13.1 Required Rule

Preview text and richer metadata should be loaded:

- after invalid candidates are removed
- after fusion narrows the list
- before final response assembly only for bounded hits

### 13.2 Guardrail

Preview generation must not silently dominate hybrid-query p95.

Benchmark output should therefore separate:

- search time before preview
- preview assembly time

## 14. Diagnostics Contract

Diagnostics are useful, but they must not reshape the production hot path.

When enabled, diagnostics may include:

- requested mode
- effective mode
- lane candidate budgets
- lane hit counts
- lane weights
- fusion method
- rerank enabled or disabled

Diagnostics should remain optional and bounded.

## 15. Batch Search Contract

Batch interfaces should exist from the start, even if the first implementation is simple.

Required goal:

- avoid designing all planner and fusion APIs around one-query-at-a-time assumptions

Phase 1 does not need maximum batching sophistication, but the contract should permit:

- shared planner setup
- shared lane opens
- amortized preview and metadata loading

Where feasible, batch execution should also avoid head-of-line blocking:

- one heavy hybrid query should not force all light text-only queries to wait for its slow lane if the API shape can avoid that

## 16. Failure and Degradation Rules

Unified search must degrade predictably.

Required cases:

- missing query vector
- vector backend unavailable
- text backend unavailable
- timeout or budget exhaustion in one lane

Recommended rule:

- if one lane fails in `hybrid`, the request may still succeed in the surviving lane if the caller allows degraded execution
- the response must indicate degraded execution explicitly

### 16.1 Slow-Lane Guardrail

In `hybrid`, the final latency is often determined by the slower lane.

Wax v2 should therefore support a bounded lane wait policy, such as:

- lane-local timeout
- planner-declared early-exit policy
- degrade-to-surviving-lane behavior when allowed by the caller

The exact timeout values may remain implementation-tunable, but the existence of a bounded wait policy should be fixed by the contract.

Silent lane disappearance is not acceptable.

## 17. Performance Guardrails

Wax v2 unified search should be designed around these hot-path limits:

- planner work is cheap and deterministic
- lane candidate budgets remain bounded
- fusion is rank-based and low-allocation
- rerank is optional and bounded
- preview assembly is deferred

If any of those become false, search p95 will drift upward quickly.

## 18. Rewrite Triggers

The phase 1 fusion and planner design should be replaced only if benchmarks show one of these is persistently true:

- rank-based fusion is materially harming relevance relative to its latency cost
- candidate budgeting is too blunt to handle real filtered workloads
- planner heuristics are too weak and repeatedly choose the wrong lane mix
- preview assembly or rerank dominates end-to-end p95
- hybrid fusion overhead becomes comparable to lane search cost

Until then, Wax should keep the fusion contract stable and treat ranking formulas as replaceable internals under that contract.

## 19. Summary

The Wax v2 search-fusion layer should be built around five stable rules:

1. unified search owns planning, budgets, fusion, and degradation
2. hybrid does not mean both lanes always run
3. rank-based fusion should be the default because backend score scales are not stable
4. rerank and preview work must stay bounded and visible
5. diagnostics must expose effective mode, budgets, and lane contributions

If those five rules hold, Wax can start with a simple, deterministic hybrid strategy now and still leave room for stronger query adaptation or better rerankers later.
