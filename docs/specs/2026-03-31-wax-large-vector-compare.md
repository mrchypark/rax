# Wax Large Vector Compare

Status: Draft  
Date: 2026-03-31  
Scope: large-dataset, 30-sample comparison of `exact_flat` vs `hnsw`, plus comparison against Wax public benchmark numbers

## 1. Purpose

This document records three things:

1. the current large-dataset vector benchmark result for the Rust benchmark harness
2. the current algorithm choice between `exact_flat` and `hnsw`
3. how the current result compares to Wax's published upstream benchmark numbers

This is a benchmark decision note, not a final product claim.

## 2. Inputs

### 2.1 Dataset

Dataset pack:

- pack path: `/tmp/wax-pack-large-hnsw1`
- dataset id: `knowledge-large-clean-v1`
- tier: `large`
- doc count: `50,000`
- vector count: `50,000`
- embedding dimensions: `384`
- distance metric: `cosine`
- total text bytes: `3,138,894`

Source of truth:

- `/tmp/wax-pack-large-hnsw1/manifest.json`

### 2.2 Command

The comparison was run with:

```bash
scripts/bench-vector-mode-compare.sh /tmp/wax-pack-large-hnsw1 /tmp/wax-vector-mode-compare-large-30 30
```

Primary generated artifact:

- `/tmp/wax-vector-mode-compare-large-30/vector-mode-compare.md`

### 2.3 Upstream Wax Reference

The upstream baseline used here is the measured benchmark report recorded in:

- `docs/specs/2026-03-29-wax-upstream-benchmark-analysis.md`

Important public numbers from that document:

- Wax cold open p95: `9.2 ms`
- Wax warm hybrid with previews p95: `6.1 ms`

## 3. Current Large-Dataset Result

### 3.1 Mode Compare

From `/tmp/wax-vector-mode-compare-large-30/vector-mode-compare.md`:

| Metric | exact_flat p95 | hnsw p95 | Delta (hnsw - exact_flat) |
| --- | ---: | ---: | ---: |
| `materialize_vector.vector_materialization_ms` | 84.504 | 89.656 | 5.152 |
| `ttfq_vector.total_ttfq_ms` | 95.099 | 75.687 | -19.412 |
| `warm_vector.search_latency_ms` | 16.685 | 0.297 | -16.388 |
| `warm_hybrid.search_latency_ms` | 25.289 | 8.962 | -16.327 |

### 3.2 Interpretation

The current shape is clear:

- `exact_flat` has slightly cheaper vector materialization on this large pack
- `hnsw` is materially better for first vector query tail
- `hnsw` is overwhelmingly better for warm vector search
- `hnsw` is overwhelmingly better for warm hybrid search

At the current scale, the algorithm decision is therefore:

- keep `hnsw` as the default ANN candidate
- keep `exact_flat` as the exact baseline and quality reference

## 4. Comparison Against Wax Public Numbers

### 4.1 Directional Proxy Only

The nearest public Wax number for directional comparison is:

- Wax warm hybrid with previews p95: `6.1 ms`

The nearest local result is:

- Rust `hnsw` warm hybrid p95 search latency: `8.962 ms`

This does **not** support a fair apples-to-apples ratio comparison.

Why:

- the local value is `search_latency_ms`
- the Wax public value is `warm hybrid with previews`
- the local value therefore excludes overhead that the public Wax number includes

The safest interpretation is only this:

- `8.962 ms` is a lower bound for the current local warm-hybrid end-to-end path
- even that lower bound is already `2.862 ms` above Wax's published `6.1 ms`
- the real product-shaped local gap is therefore not smaller than `2.862 ms`, and may be larger

### 4.2 What This Does And Does Not Mean

This is still directionally useful, but it is not a fair headline-to-headline comparison.

Differences that still matter:

- Wax public benchmark is from Apple Silicon macOS
- the current Rust benchmark uses a synthetic `50,000 x 384d` dataset
- the current local warm workload records `search_latency_ms`, not full product preview/render overhead
- Wax's public `cold open` number is a container-open metric, not a full first-query metric

So the current conclusion is:

- the Rust path is not yet at the Wax public warm-hybrid headline
- the Rust path has not matched or exceeded Wax yet
- the result is strong enough to justify keeping `hnsw`
- the result is not strong enough to claim parity

## 5. Metric Semantics Caveat

The current benchmark harness exposes separate workloads:

- `materialize_vector`
- `ttfq_vector`
- `warm_vector`
- `warm_hybrid`

These are separate benchmark contracts and should not be summed as if they were one timing decomposition.

In particular:

- `materialize_vector` is a dedicated lane-materialization workload
- `ttfq_vector` is a first-query workload with its own end-to-end timing contract

So values such as:

- `materialize_vector p95 = 89.656 ms`
- `ttfq_vector p95 = 75.687 ms`

do not imply a contradiction by themselves. They reflect different workloads, not additive slices of the same sample.

This distinction must stay explicit in any future benchmark discussion.

## 6. External Critique

This algorithm choice was reviewed with an external model before being treated as the current direction.

External critique summary:

- keeping `hnsw` as the current default is directionally correct
- the remaining gap vs Wax public `warm_hybrid p95 = 6.1 ms` is real and should not be minimized
- the current benchmark discussion must keep workload semantics explicit
- before treating the decision as stable, quality must be measured alongside speed

Practical implication:

- `Recall@K` and exact-match agreement against `exact_flat` should be treated as required follow-up metrics

## 7. Decision

Current decision:

- default vector mode: `hnsw`
- retained baseline: `exact_flat`
- rejected as default for now: `preview_q8`

Reason:

- `hnsw` wins on the two metrics that matter most for product behavior on the large dataset:
  - `ttfq_vector p95`
  - `warm_hybrid p95`

## 8. Next Checks

Before locking the vector lane direction more strongly, the next checks should be:

1. add `Recall@K` and top-hit agreement reporting against `exact_flat`
2. repeat the same comparison on a more realistic corpus, not only the synthetic knowledge pack
3. run the same benchmark on iOS hardware
4. continue reducing the remaining gap between `8.962 ms` and Wax's public `6.1 ms`
5. investigate why large-pack `materialize_vector` remains high even when `warm_vector` is already very low

## 9. Referenced Artifacts

- compare report: `/tmp/wax-vector-mode-compare-large-30/vector-mode-compare.md`
- hnsw `ttfq_vector`: `/tmp/wax-vector-mode-compare-large-30/hnsw/ttfq_vector/reduced-summary.json`
- exact `ttfq_vector`: `/tmp/wax-vector-mode-compare-large-30/exact_flat/ttfq_vector/reduced-summary.json`
- hnsw `warm_hybrid`: `/tmp/wax-vector-mode-compare-large-30/hnsw/warm_hybrid/reduced-summary.json`
- dataset manifest: `/tmp/wax-pack-large-hnsw1/manifest.json`
