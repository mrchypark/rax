use std::collections::HashSet;
use std::time::{Duration, Instant};

use rax_vector_search::embedvec_backend::EmbedVecVectorEngine;
use rax_vector_search::engine::VectorSearch;
use rax_vector_search::high_perf_backend::HighPerfVectorEngine;
use rax_vector_search::hnsw_rs_backend::HnswRsVectorEngine;
use rax_vector_search::hybrid_backend::HybridVectorEngine;
use rax_vector_search::usearch_backend::USearchVectorEngine;

const DATASET_SIZE: usize = 20_000;
const QUERY_COUNT: usize = 200;
const DIMS: usize = 384;
const TOP_K: usize = 10;

#[derive(Debug, Clone)]
struct BenchResult {
    name: &'static str,
    ingest: Duration,
    query: Duration,
    qps: f64,
    recall_at_k: f64,
}

fn main() {
    let corpus = build_vectors(DATASET_SIZE, DIMS, 7);
    let queries = build_vectors(QUERY_COUNT, DIMS, 19_911);

    let mut exact = HighPerfVectorEngine::new();
    for (id, vector) in corpus.iter().enumerate() {
        exact.upsert(id as u64, vector.clone());
    }
    let truth: Vec<Vec<u64>> = queries
        .iter()
        .map(|q| exact.search(q, TOP_K).into_iter().map(|h| h.id).collect())
        .collect();

    let mut results = Vec::new();
    results.push(run_bench(
        "high_perf_exact",
        HighPerfVectorEngine::new(),
        &corpus,
        &queries,
        &truth,
    ));
    results.push(run_bench(
        "hnsw_rs",
        HnswRsVectorEngine::new(),
        &corpus,
        &queries,
        &truth,
    ));
    results.push(run_bench(
        "usearch",
        USearchVectorEngine::new(),
        &corpus,
        &queries,
        &truth,
    ));
    results.push(run_bench(
        "embedvec",
        EmbedVecVectorEngine::new(),
        &corpus,
        &queries,
        &truth,
    ));
    results.push(run_bench(
        "hybrid",
        HybridVectorEngine::new(),
        &corpus,
        &queries,
        &truth,
    ));

    println!(
        "dataset={}, queries={}, dims={}, top_k={}",
        DATASET_SIZE, QUERY_COUNT, DIMS, TOP_K
    );
    println!("backend,ingest_ms,query_ms,qps,recall_at_{}", TOP_K);
    for r in results {
        println!(
            "{},{:.3},{:.3},{:.2},{:.4}",
            r.name,
            r.ingest.as_secs_f64() * 1_000.0,
            r.query.as_secs_f64() * 1_000.0,
            r.qps,
            r.recall_at_k
        );
    }
}

fn run_bench<E: VectorSearch>(
    name: &'static str,
    mut engine: E,
    corpus: &[Vec<f32>],
    queries: &[Vec<f32>],
    truth: &[Vec<u64>],
) -> BenchResult {
    let ingest_start = Instant::now();
    for (id, vector) in corpus.iter().enumerate() {
        engine.upsert(id as u64, vector.clone());
    }
    let ingest = ingest_start.elapsed();

    // Warmup
    for query in queries.iter().take(10) {
        let _ = engine.search(query, TOP_K);
    }

    let query_start = Instant::now();
    let mut hit_sum = 0usize;
    let mut total = 0usize;

    for (query, expected) in queries.iter().zip(truth.iter()) {
        let hits = engine.search(query, TOP_K);
        let actual_ids: HashSet<u64> = hits.into_iter().map(|h| h.id).collect();
        for id in expected {
            if actual_ids.contains(id) {
                hit_sum = hit_sum.saturating_add(1);
            }
        }
        total = total.saturating_add(expected.len());
    }
    let query = query_start.elapsed();

    let qps = if query.is_zero() {
        0.0
    } else {
        queries.len() as f64 / query.as_secs_f64()
    };
    let recall_at_k = if total == 0 {
        0.0
    } else {
        hit_sum as f64 / total as f64
    };

    BenchResult {
        name,
        ingest,
        query,
        qps,
        recall_at_k,
    }
}

fn build_vectors(count: usize, dims: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut state = 1469598103934665603u64 ^ seed;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let mut vector = Vec::with_capacity(dims);
        for _ in 0..dims {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state = state.wrapping_mul(2685821657736338717u64);
            vector.push(((state >> 40) as u32) as f32 / u32::MAX as f32);
        }
        out.push(vector);
    }
    out
}
