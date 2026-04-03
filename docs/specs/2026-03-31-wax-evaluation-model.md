# Wax Evaluation Model For Rax

Status: Draft  
Date: 2026-03-31  
Scope: performance evaluation inherited from upstream Wax where possible, plus a search-accuracy evaluation model for the Rust rewrite

## 1. Purpose

This document fixes the evaluation model for `rax`.

It does two things:

1. preserve as much of Wax upstream's performance benchmark shape as possible
2. define a retrieval-accuracy evaluation model that Wax upstream does not publish in sufficient detail

This is an evaluation-model document, not an engine-design document.

## 2. Source Basis

### 2.1 Upstream Wax Performance Sources

- [Wax benchmark report, 2026-03-06](https://github.com/christopherkarani/Wax/blob/main/Resources/docs/benchmarks/2026-03-06-performance-results.md)
- [Wax upstream benchmark analysis](./2026-03-29-wax-upstream-benchmark-analysis.md)
- [RuleBasedQueryClassifier.swift](https://github.com/christopherkarani/Wax/blob/main/Sources/Wax/UnifiedSearch/RuleBasedQueryClassifier.swift)

### 2.2 Search Evaluation Sources

- [TREC 2021 Deep Learning Track overview](https://trec.nist.gov/pubs/trec30/papers/Overview-DL.pdf)
- [Elasticsearch ranking evaluation docs](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/search-rank-eval)
- [Elasticsearch rank-eval API reference](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rank-eval)
- [TREC 2024 RAG Track guidelines](https://trec-rag.github.io/annoucements/2024-track-guidelines/)

## 3. Headline Decision

The evaluation model for `rax` should be split cleanly:

- performance should inherit Wax upstream's workload structure nearly as-is
- accuracy should use graded retrieval judgments with ranked IR metrics
- answer-generation metrics, if added later, should be layered on top of retrieval metrics, not replace them

This means:

- we keep Wax's workload-first and tail-latency-first benchmark style
- we do **not** copy Wax's current public gap on search-quality measurement

## 4. What To Inherit From Wax Upstream

Wax upstream already gives a strong performance-evaluation shape.

From the 2026-03-06 benchmark report, the required sweep includes:

- ingest and search aggregate averages
- warm tail latency
- cold open
- stage / commit latency
- vector-engine-specific measurements
- WAL compaction workload matrix

The concrete upstream categories are:

- ingest text-only
- ingest hybrid
- ingest hybrid batched
- `MemoryOrchestrator` ingest
- unified hybrid search
- hybrid warm with previews
- hybrid warm without previews
- hybrid warm CPU-only
- cold open / close
- stage for commit
- commit
- Metal vector engine
- WAL compaction workload matrix

This workload decomposition is the right model to keep.

## 5. Rax Performance Evaluation Model

### 5.1 Core Rule

`rax` should keep Wax's benchmark style, but make `TTFQ` first-class.

So the performance suite should be:

- `container_open`
- `ttfq_text`
- `ttfq_vector`
- `ttfq_hybrid`
- `warm_text`
- `warm_vector`
- `warm_hybrid`
- `materialize_vector`
- `ingest_text`
- `ingest_hybrid`
- `stage_for_commit`
- `commit`
- `wal_compaction_matrix`

### 5.2 Reporting Shape

For every workload, report:

- mean
- p50
- p95
- p99
- stdev when applicable
- sample count
- cache state / cold state label

This preserves the upstream focus on tail behavior instead of average-only reporting.

### 5.3 Query Stratification

Wax upstream already classifies queries as:

- factual
- semantic
- temporal
- exploratory

`rax` should preserve this stratification in benchmark reporting.

At minimum, every performance run should be sliced by:

- query class
- text-only / vector-only / hybrid lane eligibility
- filter-selectivity family

This is important because hybrid improvements can hide regressions on one query class.

## 6. Why Wax's Current Public Accuracy Model Is Not Enough

Wax upstream publishes strong performance numbers, but the publicly visible benchmark material does not define a comparably rigorous search-accuracy benchmark.

What is missing publicly:

- no stable graded relevance benchmark spec
- no published `nDCG@k`, `Recall@k`, or `MRR@k` suite for Wax search itself
- no judgment protocol for query sets
- no explicit handling policy for unrated hits

So `rax` should add an explicit accuracy model instead of inferring one from anecdotal search behavior.

## 7. Accuracy Evaluation Model

### 7.1 Evaluation Unit

The unit of evaluation is:

- one query
- one ranked result list
- one set of graded relevance judgments

This follows standard ranked retrieval evaluation.

### 7.2 Judgment Scale

Use a 4-point graded relevance scale:

- `0 = not relevant`
- `1 = weakly relevant`
- `2 = relevant`
- `3 = highly relevant`

Why:

- TREC-style evaluation benefits from graded labels, not only binary relevance
- `nDCG` requires graded gain to be maximally useful
- the scale is small enough to keep judging practical

### 7.3 Query Classes

Every judged query must carry:

- `query_id`
- `query_class`
- `difficulty`
- `lane_eligibility`
- `top_k`

`query_class` must use:

- factual
- semantic
- temporal
- exploratory

This keeps accuracy reporting aligned with Wax's actual planner model.

## 8. Primary Accuracy Metrics

### 8.1 Primary Overall Metric

Use `nDCG@10` as the primary overall retrieval metric.

Reason:

- it rewards getting highly relevant results near the top
- it handles graded judgments directly
- TREC Deep Learning track widely uses `nDCG@10` as a central metric

### 8.2 Recall Metric

Use `Recall@100` as the primary coverage metric.

Reason:

- it detects whether the system can surface relevant material at all
- it is important for hybrid and multi-stage retrieval
- it prevents a system from looking good only because it optimizes top-1 ranking on easy queries

### 8.3 Factual Metric

Use `MRR@10` as the primary factual-query metric.

Reason:

- factual search is highly sensitive to the rank of the first right answer
- TREC and search systems commonly use reciprocal-rank-style metrics for this query shape

### 8.4 Product-Facing Sanity Metric

Use `Precision@10` as a product-facing sanity metric.

Reason:

- it is easy to interpret
- it is useful for regression dashboards
- it should not be the primary tuning target on its own

## 9. Secondary Accuracy Metrics

Track these as secondary:

- `Success@1`
- `Success@3`
- `Recall@10`
- `nDCG@20`
- top-hit agreement vs exact baseline

These are useful because:

- `Success@1` matters for direct-answer style search
- `Recall@10` catches truncation effects
- `nDCG@20` is helpful for broader exploratory search
- top-hit agreement is useful when comparing ANN approximations against exact search

## 10. Metric Policy By Query Class

The evaluation dashboard should not stop at one global number.

Required slices:

- factual: `MRR@10`, `nDCG@10`, `Success@1`
- semantic: `nDCG@10`, `Recall@100`
- temporal: `nDCG@10`, `Recall@100`
- exploratory: `nDCG@10`, `nDCG@20`, `Recall@100`

This prevents a single metric from hiding planner tradeoffs.

## 11. Judgment Protocol

### 11.1 Gold Judgments

The default gold set should be human-authored.

Required rules:

- judge on the document, not on the engine's score
- judge against the query intent, not only token overlap
- preserve the original rank list used for judgment
- keep a query-level rationale for `2` and `3` labels

### 11.2 Unrated Documents

The system must report unrated hits explicitly.

This follows the rank-eval style used in Elasticsearch:

- overall metric
- per-query detail
- list of unrated docs surfaced by the system

Policy:

- offline benchmark scoring should treat unlabeled hits as incomplete evaluation state, not silently as irrelevant forever
- leaderboard runs should use a frozen judged pool
- development runs may optionally count unrated docs as irrelevant, but that policy must be labeled

### 11.3 Pool Expansion

Use pooled judgments:

- take top results from multiple retrieval modes
- judge the pooled set
- freeze that pool for comparisons

This avoids overfitting to one retrieval mode's visible hits.

## 12. Accuracy Evaluation For ANN And Hybrid

### 12.1 ANN Approximation Checks

For approximate vector search, always track:

- top-hit agreement with `exact_flat`
- `Recall@k` against exact candidate set
- downstream `nDCG@10` after hybrid fusion

This is necessary because ANN can look faster while silently losing recall.

### 12.2 Hybrid-Specific Checks

For hybrid search, report:

- text-only metrics
- vector-only metrics
- hybrid metrics
- hybrid uplift or regression vs the stronger single lane

This matters because hybrid should not be assumed to help every query class.

## 13. Response-Level Evaluation

If `rax` later evaluates generated answers, keep it separate from retrieval.

Recommended future layering:

1. retrieval metrics first
2. citation support / grounding checks
3. answer nugget recall / precision
4. fluency only last

This follows the broad direction of TREC RAG-style evaluation, where answer quality is not treated as a substitute for retrieval quality.

## 14. Minimum Evaluation Bundle

Every benchmark bundle should eventually contain:

- performance summary
- per-query ranked hits
- judged qrels
- overall `nDCG@10`
- overall `Recall@100`
- factual `MRR@10`
- per-class slices
- unrated-hit report
- ANN exact-agreement report

This is the minimum needed to make both performance and quality decisions defensible.

## 15. Immediate Adoption Plan

Near-term adoption order:

1. keep the current Wax-style performance sweep
2. add graded qrels to dataset packs
3. add offline metric computation for `nDCG@10`, `Recall@100`, `MRR@10`, `Precision@10`
4. add per-query detail and unrated-hit reporting
5. add exact-vs-ANN agreement reporting

This sequence keeps the performance harness intact while filling the current quality-evaluation gap.

## 16. Bottom Line

The right evaluation strategy is:

- copy Wax upstream's performance benchmark structure as closely as possible
- add a formal retrieval-accuracy benchmark that Wax does not currently publish

The primary evaluation contract for `rax` should therefore be:

- performance: Wax-style workload matrix with `TTFQ` added explicitly
- accuracy: graded relevance with `nDCG@10` primary, `Recall@100` coverage, `MRR@10` for factual queries, and exact-vs-ANN agreement for vector evaluation
