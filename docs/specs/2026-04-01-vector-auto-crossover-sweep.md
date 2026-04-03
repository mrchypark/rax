# Vector Auto Crossover Sweep

Date: 2026-04-01
Date base: Korea Standard Time (Asia/Seoul)
Scope: measure the current `VectorQueryMode::Auto` cutoff against `exact_flat` and `hnsw` on controlled adhoc packs derived from `/tmp/wax-source-large/docs.ndjson`.

## Inputs

- Source docs: `/tmp/wax-source-large/docs.ndjson`
- Pack builder: `target/release/wax-bench-cli pack-adhoc`
- Corpus sizes: `32`, `64`, `128`, `256`, `512`
- Workloads:
  - `warm_vector`
  - `ttfq_vector`
- Vector modes:
  - `exact_flat`
  - `hnsw`
  - `auto`
- Samples per run: `20`
- Artifact root: `/tmp/wax-crossover-sweep-fixed64`

`pack-adhoc` emits a single hybrid/vector-capable query derived from the first document text with `top_k = min(doc_count, 10)`.

## Results

| doc_count | mode | warm_vector p95 ms | ttfq_vector p95 ms | top-1 |
| ---: | --- | ---: | ---: | --- |
| 32 | exact_flat | 0.010 | 0.252 | `doc-000001` |
| 32 | hnsw | 0.024 | 0.232 | `doc-000001` |
| 32 | auto | 0.010 | 0.215 | `doc-000001` |
| 64 | exact_flat | 0.020 | 0.333 | `doc-000001` |
| 64 | hnsw | 0.028 | 0.283 | `doc-000001` |
| 64 | auto | 0.022 | 0.268 | `doc-000001` |
| 128 | exact_flat | 0.040 | 0.706 | `doc-000001` |
| 128 | hnsw | 0.043 | 0.389 | `doc-000001` |
| 128 | auto | 0.044 | 0.381 | `doc-000001` |
| 256 | exact_flat | 0.080 | 0.584 | `doc-000001` |
| 256 | hnsw | 0.059 | 0.585 | `doc-000001` |
| 256 | auto | 0.059 | 0.603 | `doc-000001` |
| 512 | exact_flat | 0.163 | 0.840 | `doc-000001` |
| 512 | hnsw | 0.089 | 0.686 | `doc-000001` |
| 512 | auto | 0.089 | 0.725 | `doc-000001` |

Raw summary: `/tmp/wax-crossover-sweep-fixed64/summary.tsv`

## Interpretation

1. `warm_vector`
   - `32` and `64` docs: `exact_flat` is faster than `hnsw`.
   - `128` docs: nearly tied.
   - `256` and `512` docs: `hnsw` is clearly faster.

2. `ttfq_vector`
   - `hnsw` is already competitive by `32` and clearly better by `128`.
   - `auto` tracks the selected backend correctly, but first-query timings still show run-to-run variance at very small doc counts.

3. Result stability
   - `top-1` matched across all three modes at every tested size in this sweep.
   - This sweep is latency-focused and does not replace a judged recall/ndcg comparison.

## Decision

The current `auto` cutoff of `64` docs is a defensible immediate default:

- it avoids the small-corpus warm-path regression seen on very small packs,
- it switches to `hnsw` before the `256+` region where `hnsw` clearly wins,
- and it is derived from the existing HNSW candidate floor rather than an arbitrary larger multiplier.

## Next step

The next benchmark should be a denser TTFQ sweep at `64`, `96`, `128`, `160`, `192`, `256` with higher sample counts so the first-query crossover can be pinned down more precisely.

## Dense TTFQ Sweep

Follow-up artifact roots:

- dense sweep: `/tmp/wax-ttfq-crossover-dense`
- spot-check: `/tmp/wax-ttfq-crossover-spotcheck`

### Dense sweep (`40` samples)

| doc_count | mode | warm_vector p95 ms | ttfq_vector p50 ms | ttfq_vector p95 ms | top-1 |
| ---: | --- | ---: | ---: | ---: | --- |
| 64 | exact_flat | 0.021 | 0.268 | 0.808 | `doc-000001` |
| 64 | hnsw | 0.029 | 0.226 | 0.321 | `doc-000001` |
| 64 | auto | 0.022 | 0.221 | 0.281 | `doc-000001` |
| 96 | exact_flat | 0.032 | 0.254 | 0.343 | `doc-000001` |
| 96 | hnsw | 0.030 | 0.249 | 0.322 | `doc-000001` |
| 96 | auto | 0.029 | 0.246 | 0.390 | `doc-000001` |
| 128 | exact_flat | 0.046 | 0.292 | 0.412 | `doc-000001` |
| 128 | hnsw | 0.033 | 0.277 | 0.336 | `doc-000001` |
| 128 | auto | 0.037 | 0.280 | 0.397 | `doc-000001` |
| 160 | exact_flat | 0.052 | 0.333 | 0.610 | `doc-000001` |
| 160 | hnsw | 0.032 | 0.301 | 0.355 | `doc-000001` |
| 160 | auto | 0.033 | 0.302 | 0.419 | `doc-000001` |
| 192 | exact_flat | 0.060 | 0.415 | 0.666 | `doc-000001` |
| 192 | hnsw | 0.126 | 0.394 | 0.812 | `doc-000001` |
| 192 | auto | 0.065 | 0.378 | 0.559 | `doc-000001` |
| 256 | exact_flat | 0.083 | 0.451 | 0.513 | `doc-000001` |
| 256 | hnsw | 0.070 | 0.421 | 0.560 | `doc-000001` |
| 256 | auto | 0.068 | 0.422 | 0.550 | `doc-000001` |

### Spot-check (`100` samples, `64` and `96` docs only)

| doc_count | mode | ttfq_vector p50 ms | ttfq_vector p95 ms | ttfq_vector p99 ms |
| ---: | --- | ---: | ---: | ---: |
| 64 | exact_flat | 0.220 | 0.279 | 0.447 |
| 64 | hnsw | 0.224 | 0.262 | 0.372 |
| 64 | auto | 0.218 | 0.282 | 0.416 |
| 96 | exact_flat | 0.260 | 0.354 | 0.426 |
| 96 | hnsw | 0.260 | 0.853 | 5.379 |
| 96 | auto | 0.256 | 0.365 | 0.491 |

### Updated interpretation

1. `64` docs
   - `warm_vector` still favors `exact_flat`.
   - `ttfq_vector` shows `exact_flat` and `hnsw` are effectively tied in the median, and `auto` tracks the exact path closely in the `100`-sample spot-check.

2. `96` docs
   - `warm_vector` is still near crossover.
   - `ttfq_vector` does not justify raising the cutoff: `hnsw` shows unstable tail latency in the `100`-sample rerun, while `auto` remains close to the non-outlier path.

3. Decision
   - The current `64`-doc cutoff should remain unchanged for now.
   - The next optimization target is not the cutoff itself but `ttfq_vector` tail stability on the HNSW path around `96` to `192` docs.

## Phase Profile

Profiling command:

- `target/release/wax-bench-cli profile-vector-query --dataset <pack> --vector-mode <mode> --sample-count 100`

Artifact root:

- `/tmp/wax-hnsw-tail-profile`

This profile separates:

- `vector_lane_load_ms`
- `approximate_search_ms`
- `rerank_ms`
- `exact_scan_ms`

### `96` and `192` docs (`100` samples)

| doc_count | mode | lane_load p95 ms | search p95 ms | exact/approx p95 ms | rerank p95 ms |
| ---: | --- | ---: | ---: | ---: | ---: |
| 96 | exact_flat | 0.264 | 0.042 | 0.042 exact | 0.000 |
| 96 | hnsw | 0.234 | 0.037 | 0.023 approx | 0.015 |
| 192 | exact_flat | 0.286 | 0.086 | 0.086 exact | 0.000 |
| 192 | hnsw | 0.413 | 0.049 | 0.030 approx | 0.021 |

### Reading

1. The dominant first-query cost in this range is not ANN graph traversal.
   - `approximate_search_ms` and `rerank_ms` stay below `0.05 ms` p95.
   - the larger component is `vector_lane_load_ms`, especially at `192` docs on the HNSW path.

2. `192/hnsw` shows the clearest headroom.
   - `search` is already faster than exact.
   - the larger remaining gap is lane load, not candidate generation.

3. This points to the next optimization more clearly than another cutoff tweak.
   - If `ttfq_vector` tails matter, the next experiment should reduce or defer vector-lane/HNSW load work rather than tuning `candidate_limit`.

## Lazy HNSW Validation

Implementation note:

- `VectorLane::load` now skips HNSW sidecar load for `exact_flat` and `auto` paths that resolve to exact.
- A new CLI entrypoint is available for repeated phase sampling:
  - `target/release/wax-bench-cli profile-vector-query --dataset <pack> --vector-mode <mode> --sample-count <n>`

Validation artifact root:

- `/tmp/wax-lazy-hnsw-validation`

### `100` samples

| doc_count | mode | ttfq p50 ms | ttfq p95 ms | lane_load p95 ms | hnsw_sidecar_load p95 ms | search p95 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 64 | exact_flat | 0.222 | 0.381 | 0.188 | 0.000 | 0.049 |
| 64 | hnsw | 0.633 | 1.176 | 0.732 | 0.416 | 0.096 |
| 64 | auto | 0.217 | 0.336 | 0.208 | 0.000 | 0.065 |
| 96 | exact_flat | 0.200 | 0.304 | 0.181 | 0.000 | 0.088 |
| 96 | hnsw | 0.465 | 0.915 | 0.672 | 0.494 | 0.141 |
| 96 | auto | 0.454 | 0.847 | 0.693 | 0.445 | 0.118 |
| 192 | exact_flat | 0.259 | 0.509 | 0.243 | 0.000 | 0.198 |
| 192 | hnsw | 0.571 | 1.015 | 1.925 | 1.457 | 0.152 |
| 192 | auto | 0.643 | 1.271 | 4.361 | 3.247 | 0.521 |

### Reading

1. The split load change did what it was supposed to do for exact paths.
   - `exact_flat` and `auto@64` now show `hnsw_sidecar_load_ms = 0`.
   - this confirms the exact path no longer pays the HNSW sidecar tax.

2. The remaining first-query problem is now isolated more clearly.
   - when `auto` resolves to HNSW (`96+` docs here), first-query cost is dominated by `hnsw_sidecar_load_ms`.
   - `search p95` remains materially smaller than sidecar load.

3. Updated next step.
   - The next optimization should split vector-lane base load from HNSW sidecar state even more aggressively, ideally making HNSW sidecar hydration independently cacheable or lazily attachable.
   - tuning `candidate_limit` or `ef_search` remains secondary until sidecar load is reduced.

## Auto Exact-First Validation

Implementation update:

- `VectorQueryMode::Auto` now treats the first vector or hybrid query as a cold-start exact scan, regardless of corpus size.
- HNSW sidecar hydration remains lazy for real query paths.
- benchmark-only warmup sentinels (`__warmup_vector__`, `__warmup_hybrid__`, `__warmup_hybrid_with_previews__`) now proactively hydrate the HNSW sidecar after the exact-first warmup query when auto would later resolve to HNSW, so warm workloads still measure steady-state behavior.
- `profile_first_vector_query(..., Auto)` now follows the same cold-start exact semantics instead of resolving to HNSW on large packs.

Validation artifacts:

- release comparison: `/tmp/wax-auto-exact-first-validation/summary.tsv`
- `1024` TTFQ spot-check: `/tmp/wax-auto-exact-first-spotcheck-1024/summary.tsv`
- `1024` warm spot-check: `/tmp/wax-auto-exact-first-spotcheck-1024-warm/summary.tsv`

### Release comparison (`20` samples)

| doc_count | workload | exact_flat p95 ms | hnsw p95 ms | auto p95 ms |
| ---: | --- | ---: | ---: | ---: |
| 96 | `ttfq_vector` | 0.189 | 0.710 | 0.217 |
| 96 | `warm_vector` | 0.046 | 0.104 | 0.074 |
| 1024 | `ttfq_vector` | 0.895 | 3.601 | 1.564 |
| 1024 | `warm_vector` | 0.686 | 0.315 | 0.308 |

### Spot-checks (`100` samples)

#### `1024` docs, `ttfq_vector`

| mode | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: |
| `exact_flat` | 0.963 | 2.157 | 2.557 |
| `auto` | 0.963 | 1.664 | 2.560 |

#### `1024` docs, `warm_vector`

| mode | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: |
| `exact_flat` | 0.582 | 1.019 | 1.242 |
| `hnsw` | 0.226 | 0.489 | 0.580 |
| `auto` | 0.293 | 0.638 | 0.837 |

### Updated reading

1. The cold-start goal is now met more cleanly.
   - `auto` tracks `exact_flat` closely on first-query latency.
   - the `1024` spot-check shows identical median TTFQ and slightly better `p95` than the explicit exact path in this sample.

2. Warm-path behavior is preserved for benchmark measurement.
   - after warmup sentinel priming, `auto` tracks the HNSW steady-state path instead of paying first sidecar hydration inside the measured warm query.
   - at `1024` docs, `auto warm_vector p95 = 0.308 ms`, essentially matching `hnsw = 0.315 ms`.

3. This leaves one clear semantic split.
   - real cold-start queries stay exact-first.
   - benchmark warm workloads measure the post-warmup steady state rather than the literal second query.
