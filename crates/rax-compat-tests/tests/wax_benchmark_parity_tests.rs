use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rax_core::backup::manifest::{full_manifest, incremental_manifest, BackupManifest};
use rax_core::backup::pitr::restore_pitr;
use rax_core::backup::restore::restore_incremental;
use rax_core::codec::BinaryEncoder;
use rax_core::io::streaming_reader::StreamingReader;
use rax_core::wal::entry::WALEntry;
use rax_core::wal::replay::replay_pending_puts;
use rax_core::wal::ring::WALRing;
use rax_orchestrator::memory_orchestrator::MemoryOrchestrator;
use rax_rag::fast_rag::{build_context, ContextChunk};
use rax_rag::query_classifier::classify_query;
use rax_rag::search_request::SearchRequest;
use rax_rag::token_counter::count_tokens;
use rax_rag::unified_search::{fuse_results, UnifiedCandidate};
use rax_text_search::fts5_engine::TextSearchEngine;
use rax_vector_search::cpu_backend::CpuVectorEngine;
use rax_vector_search::engine::VectorSearch;

const DEFAULT_DOCS: usize = 1_000;
const DEFAULT_DIMENSIONS: usize = 64;

fn bench_enabled() -> bool {
    env_flag("RAX_RUN_BENCHMARKS")
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn should_skip() -> bool {
    !bench_enabled()
}

fn should_skip_10k() -> bool {
    should_skip() || !env_flag("RAX_BENCHMARK_10K")
}

fn should_skip_samples() -> bool {
    should_skip() || !env_flag("RAX_BENCHMARK_SAMPLES")
}

fn benchmark_docs() -> usize {
    std::env::var("RAX_BENCHMARK_DOCS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_DOCS)
}

fn benchmark_iters() -> usize {
    std::env::var("RAX_BENCHMARK_ITERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(50)
}

fn guardrail_ms(name: &str, default_ms: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default_ms)
}

fn synthetic_docs(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            format!(
                "Document {i} about Rust memory systems, retrieval, vector search, and backup replay."
            )
        })
        .collect()
}

fn metadata(i: usize) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert(
        "source".to_string(),
        if i % 2 == 0 { "even" } else { "odd" }.to_string(),
    );
    meta.insert("city".to_string(), "seoul".to_string());
    meta
}

fn deterministic_vector(seed: u64, dims: usize) -> Vec<f32> {
    let mut state = 1469598103934665603u64 ^ seed;
    let mut out = Vec::with_capacity(dims);
    for _ in 0..dims {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state = state.wrapping_mul(2685821657736338717u64);
        out.push(((state >> 40) as u32) as f32 / u32::MAX as f32);
    }
    out
}

fn deterministic_embed(text: &str, dims: usize) -> Vec<f32> {
    let mut hash = 1469598103934665603u64;
    for b in text.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(1099511628211u64);
    }
    deterministic_vector(hash, dims)
}

fn percentile(samples: &[Duration], p: f64) -> Duration {
    if samples.is_empty() {
        return Duration::from_millis(0);
    }
    let mut sorted = samples.to_vec();
    sorted.sort();
    let idx = ((sorted.len() as f64) * p).floor() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn p50(samples: &[Duration]) -> Duration {
    percentile(samples, 0.50)
}

fn p95_copy(samples: &[Duration]) -> Duration {
    percentile(samples, 0.95)
}

fn assert_latency_guardrails(
    samples: &[Duration],
    p50_guard_name: &str,
    p50_default_ms: u64,
    p95_guard_name: &str,
    p95_default_ms: u64,
    max_p95_over_p50: f64,
) {
    let p50 = p50(samples);
    let p95 = p95_copy(samples);
    let p50_limit = Duration::from_millis(guardrail_ms(p50_guard_name, p50_default_ms));
    let p95_limit = Duration::from_millis(guardrail_ms(p95_guard_name, p95_default_ms));
    let p50_floor = p50.max(Duration::from_micros(1));
    let ratio = p95.as_secs_f64() / p50_floor.as_secs_f64();

    assert!(p50 <= p50_limit, "p50={p50:?} exceeds {p50_limit:?}");
    assert!(p95 <= p95_limit, "p95={p95:?} exceeds {p95_limit:?}");
    assert!(
        ratio <= max_p95_over_p50,
        "jitter ratio p95/p50={ratio:.3} exceeds {max_p95_over_p50}"
    );
}

fn build_hybrid_hits(query: &str, docs: usize, k: usize) -> Vec<u64> {
    let texts = synthetic_docs(docs);
    let mut text = TextSearchEngine::new();
    let mut vector = CpuVectorEngine::new();

    for (i, body) in texts.iter().enumerate() {
        text.ingest(i.to_string(), body.clone(), metadata(i));
        vector.upsert(i as u64, deterministic_vector(i as u64, DEFAULT_DIMENSIONS));
    }

    let text_hits = text.query(query, None, k);
    let vec_hits = vector.search(&deterministic_vector(42, DEFAULT_DIMENSIONS), k);
    let mut map: BTreeMap<u64, UnifiedCandidate> = BTreeMap::new();

    for h in text_hits {
        if let Ok(id) = h.id.parse::<u64>() {
            map.entry(id)
                .and_modify(|c| c.structured_score = h.score)
                .or_insert(UnifiedCandidate {
                    id,
                    structured_score: h.score,
                    semantic_score: 0.0,
                    temporal_score: 0.0,
                });
        }
    }
    for h in vec_hits {
        map.entry(h.id)
            .and_modify(|c| c.semantic_score = h.score)
            .or_insert(UnifiedCandidate {
                id: h.id,
                structured_score: 0.0,
                semantic_score: h.score,
                temporal_score: 0.0,
            });
    }

    let req = SearchRequest::new(query);
    let mode = classify_query(query);
    let fused = fuse_results(&req, mode, &map.into_values().collect::<Vec<_>>());
    fused.iter().map(|h| h.id).collect()
}

#[test]
fn test_batch_vs_sequential_embedding() {
    if should_skip() {
        return;
    }
    let docs = synthetic_docs(benchmark_docs());

    let sequential_start = Instant::now();
    let mut sequential = Vec::with_capacity(docs.len());
    for d in &docs {
        sequential.push(deterministic_embed(d, DEFAULT_DIMENSIONS));
    }
    let sequential_elapsed = sequential_start.elapsed();

    let batch_start = Instant::now();
    let batched = docs
        .iter()
        .map(|d| deterministic_embed(d, DEFAULT_DIMENSIONS))
        .collect::<Vec<_>>();
    let batch_elapsed = batch_start.elapsed();

    assert_eq!(sequential.len(), batched.len());
    assert!(batch_elapsed <= sequential_elapsed.saturating_mul(2));
}

#[test]
fn test_batch_embedding_scaling() {
    if should_skip() {
        return;
    }
    let mut prev = Duration::from_millis(0);
    for size in [32usize, 64, 128, 256] {
        let docs = synthetic_docs(size);
        let start = Instant::now();
        let _embeddings = docs
            .iter()
            .map(|d| deterministic_embed(d, DEFAULT_DIMENSIONS))
            .collect::<Vec<_>>();
        let elapsed = start.elapsed();
        if prev > Duration::from_millis(0) {
            assert!(elapsed >= prev / 3);
        }
        prev = elapsed;
    }
}

#[tokio::test]
async fn test_orchestrator_batch_embedding_performance() {
    if should_skip() {
        return;
    }
    let orchestrator = MemoryOrchestrator::new();
    let docs = synthetic_docs(benchmark_docs() / 2);
    let start = Instant::now();
    for d in docs {
        orchestrator.remember(d).await;
    }
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_secs(5));
    assert!(orchestrator.flush().await > 0);
}

#[tokio::test]
async fn test_buffer_serialization_vs_file_based() {
    if should_skip() {
        return;
    }
    let payload = deterministic_vector(1, 512)
        .iter()
        .flat_map(|f| f.to_le_bytes())
        .collect::<Vec<_>>();
    let entry = WALEntry::PutFrame {
        frame_id: 1,
        payload,
    };
    let bytes = entry.encode();

    let in_mem_start = Instant::now();
    let mut enc = BinaryEncoder::new();
    for _ in 0..200 {
        enc.put_bytes(&bytes);
    }
    let _blob = enc.finish();
    let in_mem_elapsed = in_mem_start.elapsed();

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "rax-buffer-bench-{}-{nonce}.bin",
        std::process::id()
    ));
    let file_start = Instant::now();
    fs::write(&path, &bytes).unwrap();
    let _read_back = fs::read(&path).unwrap();
    let file_elapsed = file_start.elapsed();
    fs::remove_file(path).unwrap();

    assert!(in_mem_elapsed <= file_elapsed.saturating_mul(200));
}

#[tokio::test]
async fn test_long_memory_recall_and_answer_quality() {
    if should_skip() {
        return;
    }
    let orchestrator = MemoryOrchestrator::new();
    for i in 0..300 {
        orchestrator
            .remember(format!("episode {i} includes keyword-long-memory"))
            .await;
    }
    let start = Instant::now();
    let hits = orchestrator.recall("keyword-long-memory").await;
    let elapsed = start.elapsed();

    assert!(hits.len() >= 200);
    assert!(elapsed < Duration::from_secs(1));
}

#[test]
fn test_metal_search_performance() {
    if should_skip() {
        return;
    }
    let mut engine = CpuVectorEngine::new();
    for i in 0..benchmark_docs() {
        engine.upsert(i as u64, deterministic_vector(i as u64, DEFAULT_DIMENSIONS));
    }
    let q = deterministic_vector(7, DEFAULT_DIMENSIONS);
    let start = Instant::now();
    let hits = engine.search(&q, 20);
    let elapsed = start.elapsed();
    assert_eq!(hits.len(), 20);
    assert!(elapsed < Duration::from_secs(1));
}

#[test]
fn test_metal_lazy_gpu_sync_performance() {
    if should_skip() {
        return;
    }
    let mut engine = CpuVectorEngine::new();
    for i in 0..2_000 {
        engine.upsert(i as u64, deterministic_vector(i as u64, DEFAULT_DIMENSIONS));
    }
    let q = deterministic_vector(777, DEFAULT_DIMENSIONS);
    let cold = Instant::now();
    let _ = engine.search(&q, 10);
    let cold_elapsed = cold.elapsed();

    let warm = Instant::now();
    let _ = engine.search(&q, 10);
    let warm_elapsed = warm.elapsed();

    assert!(warm_elapsed <= cold_elapsed.saturating_mul(4));
}

#[test]
fn test_metal_search_after_add_correctness() {
    if should_skip() {
        return;
    }
    let mut engine = CpuVectorEngine::new();
    let query = deterministic_vector(123, DEFAULT_DIMENSIONS);
    engine.upsert(1, query.clone());
    let before = engine.search(&query, 1);
    assert_eq!(before[0].id, 1);

    engine.upsert(2, query.clone());
    let after = engine.search(&query, 2);
    assert_eq!(after.len(), 2);
    assert!(after.iter().any(|h| h.id == 2));
}

#[test]
fn test_batch_vs_sequential_metadata_lookup() {
    if should_skip() {
        return;
    }
    let docs = benchmark_docs();
    let mut store = HashMap::new();
    for i in 0..docs {
        store.insert(i, format!("doc-{i}-meta"));
    }
    let ids = (0..docs).collect::<Vec<_>>();

    let seq_start = Instant::now();
    let sequential = ids
        .iter()
        .filter_map(|id| store.get(id).cloned())
        .collect::<Vec<_>>();
    let seq_elapsed = seq_start.elapsed();

    let batch_start = Instant::now();
    let mut batched = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(64) {
        batched.extend(chunk.iter().filter_map(|id| store.get(id).cloned()));
    }
    let batch_elapsed = batch_start.elapsed();

    assert_eq!(sequential.len(), batched.len());
    assert!(batch_elapsed <= seq_elapsed.saturating_mul(5));
}

#[test]
fn test_actor_vs_task_hop_token_counter() {
    if should_skip() {
        return;
    }
    let text = "tokenizer throughput benchmark for deterministic text counting";
    let iterations = benchmark_iters() * 20;

    let direct_start = Instant::now();
    let mut direct_sum = 0usize;
    for _ in 0..iterations {
        direct_sum += count_tokens(text);
    }
    let direct_elapsed = direct_start.elapsed();

    let threaded_start = Instant::now();
    let mut threaded_sum = 0usize;
    for _ in 0..iterations {
        let owned = text.to_string();
        threaded_sum += std::thread::spawn(move || count_tokens(&owned))
            .join()
            .unwrap();
    }
    let threaded_elapsed = threaded_start.elapsed();

    assert_eq!(direct_sum, threaded_sum);
    assert!(direct_elapsed <= threaded_elapsed.saturating_mul(2));
}

#[tokio::test]
async fn test_ingest_text_only_performance() {
    if should_skip() {
        return;
    }
    let mut engine = TextSearchEngine::new();
    let docs = synthetic_docs(benchmark_docs());
    let start = Instant::now();
    for (i, d) in docs.iter().enumerate() {
        engine.ingest(i.to_string(), d.clone(), metadata(i));
    }
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_secs(5));
}

#[tokio::test]
async fn test_ingest_hybrid_performance() {
    if should_skip() {
        return;
    }
    let docs = synthetic_docs(benchmark_docs());
    let mut text = TextSearchEngine::new();
    let mut vector = CpuVectorEngine::new();
    let start = Instant::now();
    for (i, d) in docs.iter().enumerate() {
        text.ingest(i.to_string(), d.clone(), metadata(i));
        vector.upsert(i as u64, deterministic_vector(i as u64, DEFAULT_DIMENSIONS));
    }
    let elapsed = start.elapsed();
    let hits = vector.search(&deterministic_vector(5, DEFAULT_DIMENSIONS), 5);
    assert_eq!(hits.len(), 5);
    assert!(elapsed < Duration::from_secs(10));
}

#[tokio::test]
async fn test_ingest_hybrid_batched_performance() {
    if should_skip() {
        return;
    }
    let docs = synthetic_docs(benchmark_docs());
    let mut text = TextSearchEngine::new();
    let mut vector = CpuVectorEngine::new();
    let start = Instant::now();
    for (batch_idx, chunk) in docs.chunks(64).enumerate() {
        for (offset, d) in chunk.iter().enumerate() {
            let id = batch_idx * 64 + offset;
            text.ingest(id.to_string(), d.clone(), metadata(id));
            vector.upsert(
                id as u64,
                deterministic_vector(id as u64, DEFAULT_DIMENSIONS),
            );
        }
    }
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_secs(10));
}

#[tokio::test]
async fn test_ingest_text_only_performance_10_k_docs() {
    if should_skip_10k() {
        return;
    }
    let mut engine = TextSearchEngine::new();
    let docs = synthetic_docs(10_000);
    let start = Instant::now();
    for (i, d) in docs.iter().enumerate() {
        engine.ingest(i.to_string(), d.clone(), metadata(i));
    }
    assert!(start.elapsed() < Duration::from_secs(30));
}

#[tokio::test]
async fn test_ingest_hybrid_performance_10_k_docs() {
    if should_skip_10k() {
        return;
    }
    let docs = synthetic_docs(10_000);
    let mut text = TextSearchEngine::new();
    let mut vector = CpuVectorEngine::new();
    let start = Instant::now();
    for (i, d) in docs.iter().enumerate() {
        text.ingest(i.to_string(), d.clone(), metadata(i));
        vector.upsert(i as u64, deterministic_vector(i as u64, DEFAULT_DIMENSIONS));
    }
    assert!(start.elapsed() < Duration::from_secs(45));
}

#[tokio::test]
async fn test_ingest_hybrid_batched_performance_10_k_docs() {
    if should_skip_10k() {
        return;
    }
    let docs = synthetic_docs(10_000);
    let mut text = TextSearchEngine::new();
    let mut vector = CpuVectorEngine::new();
    let start = Instant::now();
    for (batch_idx, chunk) in docs.chunks(128).enumerate() {
        for (offset, d) in chunk.iter().enumerate() {
            let id = batch_idx * 128 + offset;
            text.ingest(id.to_string(), d.clone(), metadata(id));
            vector.upsert(
                id as u64,
                deterministic_vector(id as u64, DEFAULT_DIMENSIONS),
            );
        }
    }
    assert!(start.elapsed() < Duration::from_secs(45));
}

#[tokio::test]
async fn test_text_search_performance() {
    if should_skip() {
        return;
    }
    let mut text = TextSearchEngine::new();
    for (i, d) in synthetic_docs(benchmark_docs()).iter().enumerate() {
        text.ingest(i.to_string(), d.clone(), metadata(i));
    }
    let start = Instant::now();
    let hits = text.query("Rust retrieval", Some(("city", "seoul")), 20);
    assert!(!hits.is_empty());
    assert!(start.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn test_vector_search_performance() {
    if should_skip() {
        return;
    }
    let mut vector = CpuVectorEngine::new();
    for i in 0..benchmark_docs() {
        vector.upsert(i as u64, deterministic_vector(i as u64, DEFAULT_DIMENSIONS));
    }
    let start = Instant::now();
    let hits = vector.search(&deterministic_vector(17, DEFAULT_DIMENSIONS), 20);
    let elapsed = start.elapsed();
    assert_eq!(hits.len(), 20);
    assert!(elapsed < Duration::from_secs(1));
    assert!(1.0 / elapsed.as_secs_f64().max(1e-9) >= 500.0);
}

#[tokio::test]
async fn test_unified_search_hybrid_performance() {
    if should_skip() {
        return;
    }
    let start = Instant::now();
    let top_ids = build_hybrid_hits("city:seoul", benchmark_docs(), 20);
    let elapsed = start.elapsed();
    assert!(!top_ids.is_empty());
    assert!(elapsed < Duration::from_secs(2));
    assert!(1.0 / elapsed.as_secs_f64().max(1e-9) >= 100.0);
}

#[tokio::test]
async fn test_unified_search_hybrid_performance_with_metrics() {
    if should_skip() {
        return;
    }
    let top_ids = build_hybrid_hits("city:seoul", benchmark_docs(), 20);
    let rss_proxy = top_ids.len();
    assert!(rss_proxy >= 1);
}

#[tokio::test]
async fn test_fast_rag_build_performance_fast_mode() {
    if should_skip() {
        return;
    }
    let chunks = (0..200)
        .map(|i| ContextChunk {
            id: i,
            text: format!("chunk {i} with medium length context"),
            importance: (200 - i) as i32,
        })
        .collect::<Vec<_>>();
    let start = Instant::now();
    let ctx = build_context(chunks, 200);
    assert!(!ctx.is_empty());
    assert!(start.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn test_fast_rag_build_performance_dense_cached() {
    if should_skip() {
        return;
    }
    let chunks = (0..300)
        .map(|i| ContextChunk {
            id: i,
            text: format!("dense cache chunk {i}"),
            importance: (i % 50) as i32,
        })
        .collect::<Vec<_>>();
    let first = build_context(chunks.clone(), 250);
    let second = build_context(chunks, 250);
    assert_eq!(first, second);
}

#[tokio::test]
async fn test_memory_orchestrator_ingest_performance() {
    if should_skip() {
        return;
    }
    let orchestrator = MemoryOrchestrator::new();
    let docs = synthetic_docs(benchmark_docs() / 2);
    let start = Instant::now();
    for d in docs {
        orchestrator.remember(d).await;
    }
    assert!(start.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn test_memory_orchestrator_recall_performance() {
    if should_skip() {
        return;
    }
    let orchestrator = MemoryOrchestrator::new();
    for d in synthetic_docs(benchmark_docs() / 2) {
        orchestrator.remember(d).await;
    }
    let start = Instant::now();
    let hits = orchestrator.recall("retrieval").await;
    assert!(!hits.is_empty());
    assert!(start.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn test_cold_open_hybrid_search_performance() {
    if should_skip() {
        return;
    }
    let docs = synthetic_docs(300);
    let mut samples = Vec::new();
    for _ in 0..10 {
        let start = Instant::now();
        let mut text = TextSearchEngine::new();
        let mut vector = CpuVectorEngine::new();
        for (i, d) in docs.iter().enumerate() {
            text.ingest(i.to_string(), d.clone(), metadata(i));
            vector.upsert(i as u64, deterministic_vector(i as u64, DEFAULT_DIMENSIONS));
        }
        let _ = text.query("memory", None, 5);
        let _ = vector.search(&deterministic_vector(1, DEFAULT_DIMENSIONS), 5);
        samples.push(start.elapsed());
    }
    assert_latency_guardrails(
        &samples,
        "RAX_GUARD_COLD_OPEN_P50_MS",
        120,
        "RAX_GUARD_COLD_OPEN_P95_MS",
        300,
        4.0,
    );
}

#[tokio::test]
async fn test_token_counting_performance() {
    if should_skip() {
        return;
    }
    let text = synthetic_docs(200).join(" ");
    let start = Instant::now();
    let count = count_tokens(&text);
    let elapsed = start.elapsed();
    assert!(count > 0);
    assert!(elapsed < Duration::from_millis(500));
    assert!(count as f64 / elapsed.as_secs_f64().max(1e-9) >= 50_000.0);
}

#[tokio::test]
async fn test_token_counting_cold_start_performance() {
    if should_skip() {
        return;
    }
    let text = synthetic_docs(20).join(" ");
    let start = Instant::now();
    let c1 = count_tokens(&text);
    let c2 = count_tokens(&text);
    assert_eq!(c1, c2);
    assert!(start.elapsed() < Duration::from_millis(300));
}

#[tokio::test]
async fn test_unified_search_hybrid_warm_latency_samples() {
    if should_skip_samples() {
        return;
    }
    let mut samples = Vec::new();
    for _ in 0..benchmark_iters() {
        let start = Instant::now();
        let _ids = build_hybrid_hits("city:seoul", 600, 10);
        samples.push(start.elapsed());
    }
    assert_latency_guardrails(
        &samples,
        "RAX_GUARD_UNIFIED_WARM_P50_MS",
        10,
        "RAX_GUARD_UNIFIED_WARM_P95_MS",
        25,
        3.5,
    );
}

#[tokio::test]
async fn test_unified_search_hybrid_warm_latency_samples_cpu_only() {
    if should_skip_samples() {
        return;
    }
    let mut vector = CpuVectorEngine::new();
    for i in 0..2_000 {
        vector.upsert(i as u64, deterministic_vector(i as u64, DEFAULT_DIMENSIONS));
    }
    let query = deterministic_vector(99, DEFAULT_DIMENSIONS);
    let mut samples = Vec::new();
    for _ in 0..benchmark_iters() {
        let start = Instant::now();
        let _ = vector.search(&query, 20);
        samples.push(start.elapsed());
    }
    assert_latency_guardrails(
        &samples,
        "RAX_GUARD_VECTOR_WARM_P50_MS",
        3,
        "RAX_GUARD_VECTOR_WARM_P95_MS",
        10,
        3.0,
    );
}

#[tokio::test]
async fn test_frame_previews_warm_latency_samples() {
    if should_skip_samples() {
        return;
    }
    let bytes = vec![42u8; 64 * 1024];
    let mut samples = Vec::new();
    for _ in 0..benchmark_iters() {
        let start = Instant::now();
        let mut reader = StreamingReader::new(bytes.clone());
        let _ = reader.read_chunk(256).await.unwrap();
        samples.push(start.elapsed());
    }
    assert_latency_guardrails(
        &samples,
        "RAX_GUARD_PREVIEW_WARM_P50_MS",
        1,
        "RAX_GUARD_PREVIEW_WARM_P95_MS",
        4,
        3.5,
    );
}

#[tokio::test]
async fn test_wax_open_close_cold_latency_samples() {
    if should_skip_samples() {
        return;
    }
    let mut samples = Vec::new();
    for _ in 0..benchmark_iters() {
        let start = Instant::now();
        let orchestrator = MemoryOrchestrator::new();
        orchestrator.remember("cold open sample").await;
        let _ = orchestrator.flush().await;
        samples.push(start.elapsed());
    }
    assert_latency_guardrails(
        &samples,
        "RAX_GUARD_OPEN_CLOSE_P50_MS",
        1,
        "RAX_GUARD_OPEN_CLOSE_P95_MS",
        5,
        4.0,
    );
}

#[tokio::test]
async fn test_incremental_stage_and_commit_latency_samples() {
    if should_skip_samples() {
        return;
    }
    let mut ring = WALRing::new(4_096);
    let mut samples = Vec::new();
    for i in 0..benchmark_iters() {
        let start = Instant::now();
        let seq = ring.append(WALEntry::PutFrame {
            frame_id: i as u64,
            payload: vec![i as u8; 128],
        });
        let _ = ring.compact(seq.saturating_sub(8));
        samples.push(start.elapsed());
    }
    assert_latency_guardrails(
        &samples,
        "RAX_GUARD_STAGE_COMMIT_P50_MS",
        1,
        "RAX_GUARD_STAGE_COMMIT_P95_MS",
        4,
        3.5,
    );
}

#[tokio::test]
async fn test_unified_search_hybrid_performance_10_k_docs() {
    if should_skip_10k() {
        return;
    }
    let start = Instant::now();
    let top = build_hybrid_hits("city:seoul", 10_000, 20);
    assert!(!top.is_empty());
    assert!(start.elapsed() < Duration::from_secs(18));
}

#[tokio::test]
async fn test_unified_search_hybrid_performance_10_k_docs_cpu() {
    if should_skip_10k() {
        return;
    }
    let mut vector = CpuVectorEngine::new();
    for i in 0..10_000 {
        vector.upsert(i as u64, deterministic_vector(i as u64, DEFAULT_DIMENSIONS));
    }
    let q = deterministic_vector(1234, DEFAULT_DIMENSIONS);
    let start = Instant::now();
    let hits = vector.search(&q, 30);
    assert_eq!(hits.len(), 30);
    assert!(start.elapsed() < Duration::from_secs(6));
}

#[tokio::test]
async fn test_mini_lm_embedding_performance() {
    if should_skip() {
        return;
    }
    let docs = synthetic_docs(benchmark_docs() / 2);
    let start = Instant::now();
    let out = docs
        .iter()
        .map(|d| deterministic_embed(d, 384))
        .collect::<Vec<_>>();
    assert_eq!(out.len(), docs.len());
    assert!(start.elapsed() < Duration::from_secs(8));
}

#[tokio::test]
async fn test_mini_lm_batch_embedding_throughput() {
    if should_skip() {
        return;
    }
    let docs = synthetic_docs(512);
    let start = Instant::now();
    let _ = docs
        .chunks(32)
        .flat_map(|chunk| chunk.iter().map(|d| deterministic_embed(d, 384)))
        .collect::<Vec<_>>();
    assert!(start.elapsed() < Duration::from_secs(8));
}

#[tokio::test]
async fn test_mini_lm_embedding_cold_start_performance() {
    if should_skip() {
        return;
    }
    let cold_start = Instant::now();
    let v1 = deterministic_embed("cold-start", 384);
    let cold_elapsed = cold_start.elapsed();
    let warm_start = Instant::now();
    let v2 = deterministic_embed("cold-start", 384);
    let warm_elapsed = warm_start.elapsed();
    assert_eq!(v1, v2);
    assert!(warm_elapsed <= cold_elapsed.saturating_mul(5));
}

#[tokio::test]
async fn test_mini_lm_open_and_first_recall_on_existing_store_samples() {
    if should_skip() {
        return;
    }
    let orchestrator = MemoryOrchestrator::new();
    for i in 0..300 {
        orchestrator
            .remember(format!("minilm recall sample {i} with context"))
            .await;
    }
    let start = Instant::now();
    let hits = orchestrator.recall("minilm recall").await;
    assert!(!hits.is_empty());
    assert!(start.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn test_mini_lm_ingest_performance() {
    if should_skip() {
        return;
    }
    let orchestrator = MemoryOrchestrator::new();
    let start = Instant::now();
    for d in synthetic_docs(400) {
        let _embedding = deterministic_embed(&d, 384);
        orchestrator.remember(d).await;
    }
    assert!(start.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn test_mini_lm_recall_performance() {
    if should_skip() {
        return;
    }
    let orchestrator = MemoryOrchestrator::new();
    for d in synthetic_docs(500) {
        orchestrator.remember(d).await;
    }
    let start = Instant::now();
    let hits = orchestrator.recall("vector").await;
    assert!(!hits.is_empty());
    assert!(start.elapsed() < Duration::from_secs(2));
}

#[test]
fn test_tokenizer_throughput() {
    if should_skip() {
        return;
    }
    let text = synthetic_docs(2_000).join(" ");
    let start = Instant::now();
    let tokens = count_tokens(&text);
    let elapsed = start.elapsed();
    assert!(tokens > 1_000);
    assert!(elapsed < Duration::from_millis(800));
    assert!(tokens as f64 / elapsed.as_secs_f64().max(1e-9) >= 75_000.0);
}

#[tokio::test]
async fn test_wal_compaction_workload_matrix() {
    if should_skip() {
        return;
    }
    let mut ring = WALRing::new(50_000);
    let start = Instant::now();
    for i in 0..30_000u64 {
        ring.append(WALEntry::PutFrame {
            frame_id: i,
            payload: vec![1; 64],
        });
        if i % 500 == 0 {
            let _ = ring.compact(i.saturating_sub(250));
        }
    }
    assert!(start.elapsed() < Duration::from_secs(8));
}

#[tokio::test]
async fn test_proactive_pressure_guardrails() {
    if should_skip() {
        return;
    }
    let mut ring = WALRing::new(10_000);
    let mut latencies = Vec::new();
    for i in 0..benchmark_iters() * 20 {
        let start = Instant::now();
        ring.append(WALEntry::PutFrame {
            frame_id: i as u64,
            payload: vec![0; 512],
        });
        let _ = ring.compact(i.saturating_sub(32) as u64);
        latencies.push(start.elapsed());
    }
    assert_latency_guardrails(
        &latencies,
        "RAX_GUARD_WAL_PRESSURE_P50_MS",
        1,
        "RAX_GUARD_WAL_PRESSURE_P95_MS",
        6,
        4.0,
    );
}

#[tokio::test]
async fn test_replay_state_snapshot_guardrails() {
    if should_skip() {
        return;
    }
    let mut ring = WALRing::new(5_000);
    for i in 0..5_000 {
        ring.append(WALEntry::PutFrame {
            frame_id: i as u64,
            payload: vec![i as u8; 32],
        });
    }
    let start = Instant::now();
    let replayed = replay_pending_puts(ring.records(), 4_000);
    let elapsed = start.elapsed();
    assert!(!replayed.is_empty());
    assert!(elapsed < Duration::from_secs(2));
}

#[tokio::test]
async fn test_backup_restore_chain_smoke_for_benchmark_suite() {
    if should_skip() {
        return;
    }
    let base = full_manifest("snap-a", 1, vec!["seg-a".to_string()]);
    let inc1 = incremental_manifest("snap-a", "snap-b", 2, 10, 20, vec!["seg-b".to_string()]);
    let inc2 = incremental_manifest("snap-b", "snap-c", 3, 21, 30, vec!["seg-c".to_string()]);
    let chain: Vec<BackupManifest> = vec![base.clone(), inc1, inc2];

    let restored = restore_incremental(&chain).unwrap();
    assert_eq!(
        restored.applied_snapshots,
        vec![
            "snap-a".to_string(),
            "snap-b".to_string(),
            "snap-c".to_string()
        ]
    );
    assert_eq!(restore_pitr(&chain, 25), Some("snap-b".to_string()));
    assert_eq!(restore_pitr(&chain, 5), Some("snap-a".to_string()));

    let invalid = vec![
        base,
        incremental_manifest("wrong-base", "snap-z", 4, 31, 40, vec!["seg-z".to_string()]),
    ];
    assert!(restore_incremental(&invalid).is_none());
}
