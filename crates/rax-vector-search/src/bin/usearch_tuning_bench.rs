use std::collections::HashSet;
use std::time::{Duration, Instant};

use rax_vector_search::engine::VectorSearch;
use rax_vector_search::high_perf_backend::HighPerfVectorEngine;
use rax_vector_search::usearch_backend::{USearchConfig, USearchVectorEngine};

const DEFAULT_DATASET_SIZE: usize = 20_000;
const DEFAULT_QUERY_COUNT: usize = 200;
const DEFAULT_DIMS: usize = 384;
const DEFAULT_TOP_K: usize = 10;

#[derive(Debug, Clone)]
struct USearchVariant {
    name: &'static str,
    config: USearchConfig,
}

#[derive(Debug, Clone)]
struct BenchResult {
    name: String,
    ingest: Duration,
    query: Duration,
    qps: f64,
    recall_at_k: f64,
    config: USearchConfig,
}

fn main() {
    let dataset_size = read_usize_env("RAX_VECTOR_BENCH_DATASET_SIZE", DEFAULT_DATASET_SIZE);
    let query_count = read_usize_env("RAX_VECTOR_BENCH_QUERY_COUNT", DEFAULT_QUERY_COUNT);
    let dims = read_usize_env("RAX_VECTOR_BENCH_DIMS", DEFAULT_DIMS);
    let top_k = read_usize_env("RAX_VECTOR_BENCH_TOP_K", DEFAULT_TOP_K);
    let full_profile = std::env::var("RAX_VECTOR_BENCH_PROFILE")
        .ok()
        .map(|v| v.trim().eq_ignore_ascii_case("full"))
        .unwrap_or(false);

    let corpus = build_vectors(dataset_size, dims, 7);
    let queries = build_vectors(query_count, dims, 19_911);

    let mut exact = HighPerfVectorEngine::new();
    for (id, vector) in corpus.iter().enumerate() {
        exact.upsert(id as u64, vector.clone());
    }
    let truth: Vec<Vec<u64>> = queries
        .iter()
        .map(|q| exact.search(q, top_k).into_iter().map(|h| h.id).collect())
        .collect();

    let mut variants = vec![
        USearchVariant {
            name: "c16_add64_search32",
            config: USearchConfig {
                connectivity: 16,
                expansion_add: 64,
                expansion_search: 32,
            },
        },
        USearchVariant {
            name: "c16_add200_search80",
            config: USearchConfig {
                connectivity: 16,
                expansion_add: 200,
                expansion_search: 80,
            },
        },
        USearchVariant {
            name: "c24_add256_search96",
            config: USearchConfig {
                connectivity: 24,
                expansion_add: 256,
                expansion_search: 96,
            },
        },
        USearchVariant {
            name: "c32_add384_search128",
            config: USearchConfig {
                connectivity: 32,
                expansion_add: 384,
                expansion_search: 128,
            },
        },
    ];
    if full_profile {
        variants.push(USearchVariant {
            name: "c48_add800_search192",
            config: USearchConfig {
                connectivity: 48,
                expansion_add: 800,
                expansion_search: 192,
            },
        });
        variants.push(USearchVariant {
            name: "c64_add1200_search256",
            config: USearchConfig {
                connectivity: 64,
                expansion_add: 1_200,
                expansion_search: 256,
            },
        });
    }

    let mut results = Vec::with_capacity(variants.len());
    for variant in variants {
        eprintln!(
            "[bench] running {} (connectivity={}, add_expansion={}, search_expansion={})",
            variant.name,
            variant.config.connectivity,
            variant.config.expansion_add,
            variant.config.expansion_search
        );
        let result = run_usearch_bench(variant, &corpus, &queries, &truth, top_k);
        eprintln!(
            "[bench] done {}: recall@{}={:.4}, qps={:.2}",
            result.name, top_k, result.recall_at_k, result.qps
        );
        results.push(result);
    }

    println!(
        "dataset={}, queries={}, dims={}, top_k={}",
        dataset_size, query_count, dims, top_k
    );
    println!(
        "variant,ingest_ms,query_ms,qps,recall_at_k,connectivity,expansion_add,expansion_search"
    );
    for result in &results {
        println!(
            "{},{:.3},{:.3},{:.2},{:.4},{},{},{}",
            result.name,
            result.ingest.as_secs_f64() * 1_000.0,
            result.query.as_secs_f64() * 1_000.0,
            result.qps,
            result.recall_at_k,
            result.config.connectivity,
            result.config.expansion_add,
            result.config.expansion_search
        );
    }

    if let Some(best) = results.iter().max_by(|a, b| {
        a.recall_at_k
            .total_cmp(&b.recall_at_k)
            .then_with(|| a.qps.total_cmp(&b.qps))
    }) {
        println!(
            "best_recall_variant={},recall_at_k={:.4},qps={:.2},connectivity={},expansion_add={},expansion_search={}",
            best.name,
            best.recall_at_k,
            best.qps,
            best.config.connectivity,
            best.config.expansion_add,
            best.config.expansion_search
        );
    }
}

fn run_usearch_bench(
    variant: USearchVariant,
    corpus: &[Vec<f32>],
    queries: &[Vec<f32>],
    truth: &[Vec<u64>],
    top_k: usize,
) -> BenchResult {
    let mut engine = USearchVectorEngine::with_config(variant.config);

    let ingest_start = Instant::now();
    for (id, vector) in corpus.iter().enumerate() {
        engine.upsert(id as u64, vector.clone());
    }
    let ingest = ingest_start.elapsed();

    for query in queries.iter().take(10) {
        let _ = engine.search(query, top_k);
    }

    let query_start = Instant::now();
    let mut hit_sum = 0usize;
    let mut total = 0usize;
    for (query, expected) in queries.iter().zip(truth.iter()) {
        let hits = engine.search(query, top_k);
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
        name: variant.name.to_string(),
        ingest,
        query,
        qps,
        recall_at_k,
        config: variant.config,
    }
}

fn read_usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
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
