# Wax Upstream-Real Comparison (1/2/3) Report

Date: 2026-03-31  
Date base: Korea Standard Time (Asia/Seoul)  
Scope: execute and compare current local harness against upstream-public references on the largest reliable local pack available.

## 1) What was run

### Inputs
- Dataset pack: `/tmp/wax-pack-upstream-real`
  - `dataset_id`: `upstream-large-clean-v1`
  - `doc_count`: 31, `vector_count`: 31
  - `query_count`: 39
- Command set used:
  - `scripts/bench-release-matrix.sh /tmp/wax-pack-upstream-real <artifact_root> 39`
  - `scripts/bench-vector-mode-compare.sh /tmp/wax-pack-upstream-real <artifact_root> 39`
  - `wax-bench-cli quality-report --query-set /private/tmp/wax-upstream-real-query-top100.jsonl --qrels /tmp/wax-upstream-real-qrels.jsonl --results /private/tmp/wax-upstream-real-results-top100-*.json`

### Artifact roots used
- Release matrix: `/tmp/wax-release-matrix-upstream-39`
- Vector mode compare: `/tmp/wax-vector-mode-compare-upstream-39`
- Quality outputs:
  - `/tmp/wax-upstream-real-quality-groundtruth-exact.json`
  - `/tmp/wax-upstream-real-quality-groundtruth-hnsw.json`

## 2) Performance (local)

### Release matrix (39 samples, upstream-real)
| Workload | p95 container_open_ms | p95 vector_materialization_ms | p95 total_ttfq_ms | p95 search_latency_ms |
|---|---:|---:|---:|---:|
| container_open | 0.074 | unavailable | unavailable | unavailable |
| ttfq_text | 0.052 | unavailable | 0.915 | unavailable |
| materialize_vector | 0.036 | 0.221 | 0.241 | unavailable |
| ttfq_vector | 0.045 | unavailable | 0.329 | unavailable |
| warm_text | 0.044 | unavailable | unavailable | 0.008 |
| warm_vector | 0.034 | unavailable | unavailable | 0.028 |
| warm_hybrid | 0.034 | unavailable | unavailable | 0.031 |
| warm_hybrid_with_previews | 0.034 | unavailable | unavailable | 0.032 |

> `sample_count` for each workload: 39

### Vector mode compare (exact_flat vs hnsw)
| Metric / workload | exact_flat | hnsw | Delta (hnsw - exact_flat) |
|---|---:|---:|---:|
| materialize_vector p95 vector_materialization_ms | 0.183 | 0.180 | -0.003 |
| materialize_vector p95 total_ttfq_ms | 0.220 | 0.214 | -0.006 |
| warm_vector p95 search_latency_ms | 0.009 | 0.026 | +0.017 |
| warm_hybrid p95 search_latency_ms | 0.016 | 0.042 | +0.026 |
| warm_hybrid_with_previews p95 search_latency_ms | 0.019 | 0.032 | +0.013 |

## 3) 정확도 / 품질 비교

### exact vs hnsw 질의 품질 (qrels 기반)
- 입력 qrels: `/tmp/wax-upstream-real-qrels.jsonl` (39 queries × 3 judged docs/query)
- qrels 커버리지: 117 judged docs (3 × 39)
- 핵심 지표 (exact + hnsw 동일):
  - `ndcg_at_10`: 1.000
  - `ndcg_at_20`: 1.000
  - `recall_at_10`: 1.000
  - `recall_at_100`: 1.000
  - `precision_at_10`: 0.300
  - `mrr_at_10`: 1.000
  - `success_at_1`: 1.000
  - `success_at_3`: 1.000
  - `query_count`: 39
  - `unrated_hit_count`: 273

### exact vs hnsw 결과 집합 겹침(내부 분석)
- top-1 일치율: 100%
- top-2 overlap@2: 100%
- top-5 overlap@5: 100%
- top-10 overlap@10: 100%
- top-20 overlap@20: 100%
- top-30 overlap@30: 100%
- top-100 overlap@100: 31.0% (문헌: 전체 31개 doc만 존재)

### Top-100 질의셋 기반 정확도 (`/private/tmp/wax-upstream-real-quality-top100-exact.json`)

- `query_count`: 39
- `ndcg_at_10`: 0.99476
- `ndcg_at_20`: 0.99476
- `recall_at_10`: 1.0
- `recall_at_100`: 1.0
- `precision_at_10`: 0.30
- `mrr_at_10`: 1.0
- `success_at_1`: 1.0
- `success_at_3`: 1.0
- `unrated_hit_count`: 1092

Top-100 기준 exact/hnsw 비교:

- `overlap@1/2/3/5/10/20`: 100%
- `overlap@50`: 62.0%
- `overlap@100`: 31.0%

## 4) 공정 비교 판단

### 결론 요약
1. **현재 local 성능 수치 자체는 유의미한 추세를 보여주지만**, upstream 공개 수치와 직접 비교는 성능 수치 정의/범위가 다릅니다.
2. **upstream와 fair하게 비교하려면** dataset scale, text index/embedding 파이프라인, 벡터 임베딩 품질, 측정 구간 정의를 맞춰야 합니다.
3. **정확도는 현재 qrels 희소성 때문에 과대평가 위험**이 있으며, perfect-like 지표는 신뢰 가능한 일반화 결론으로 보기 어렵습니다.
4. **hnsw는 warm/preview 경로에서 현재 local에서는 exact 대비 악화**(warm_vector/search latency), 그러나 materialize/ttfq에서는 소폭 개선.

### 비교의 불일치 포인트 (우선 수정 필요)
- upstream 공개수치는 `cold open 9.2 ms`, `warm hybrid with previews 6.1 ms` 수준에서 보고되었으나, local은 `container_open p95 0.074 ms`, `warm_hybrid_with_previews p95 0.032 ms`처럼 단위/워크로드가 다릅니다.
- 샘플 수는 39라 통계 신뢰구간이 매우 얇습니다.
- qrels가 3개/쿼리로 희소하며 `precision@10=0.300`, `unrated_hit_count=273`으로 미평가 히트가 많습니다.

## 5) 동일 조건 확장 비교 (large-realistic)

### Inputs
- Dataset pack: `/tmp/wax-pack-large-hnsw1`
  - `dataset_id`: `knowledge-large-clean-v1`
  - `doc_count`: 50,000

### 성능 (release matrix + vector mode)

- `container_open p95`: 0.115 ms
- `materialize_vector p95 total_ttfq_ms`: 98.298 ms
- `ttfq_vector p95`: 95.030 ms
- `warm_vector p95 search_latency_ms`: 0.258 ms
- `warm_hybrid p95 search_latency_ms`: 29.998 ms

### 정확도
- exact-top100: `ndcg_at_10=0.88546`, `precision_at_10=0.30`, `unrated_hit_count=14`
- hnsw-top100: `ndcg_at_10=0.88546`, `precision_at_10=0.30`, `unrated_hit_count=14`
- exact/hnsw 집합 일치:
  - `overlap@1`: 100%
  - `overlap@2`: 75%
  - `overlap@10`: 65%
  - `overlap@50`: 13%
  - `overlap@100`: 6.5%

### 결론(공정 비교)
- `knowledge-large-clean-v1`에서는 `hnsw`가 `warm_vector`와 `warm_hybrid`에서 큰 이득을 보여 기존 결정(`hnsw` default 유지)에 정합
- 현재 `upstream-real`(31 docs)에서는 워크로드 크기가 극도로 작아서 `warm` 경로 성능 우열이 고정밀 비교로 보기 어려움
- `top-k` 평가의 `unrated_hit`가 많아 정량값은 유효성 경고 플래그가 있으므로, 공정 비교를 위해서는 판단 문항 확장(각 쿼리별 더 많은 judged doc, 최소 깊이 보장)이 필수

1. **1단계(성능):** large 현실 pack(10k+)으로 동일 스크립트 재실행
2. **2단계(정확도):** qrels 확장(최소 각 클래스별 분할 + judged depth 보장), `top_k >= 100`로 품질보고 지표 정합성 확보
3. **3단계(최적화):** hnsw warm-path를 31개 데이터셋 기준으로 과도하게 느린 병목 분석(특히 후보 제한/재료화 캐시) 후 large-corpus 상에서 고정밀 비교 재실행

## 6) 실행 재현 위치(원본)
- `/tmp/wax-release-matrix-upstream-39`
- `/tmp/wax-vector-mode-compare-upstream-39`
- `/tmp/wax-upstream-real-quality-groundtruth-exact.json`
- `/tmp/wax-upstream-real-quality-groundtruth-hnsw.json`
