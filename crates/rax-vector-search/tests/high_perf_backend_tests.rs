use rax_vector_search::cpu_backend::CpuVectorEngine;
use rax_vector_search::engine::VectorSearch;
use rax_vector_search::high_perf_backend::HighPerfVectorEngine;

fn deterministic_vec(seed: u64, dims: usize) -> Vec<f32> {
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

#[test]
fn high_perf_engine_returns_expected_neighbor() {
    let mut eng = HighPerfVectorEngine::new();
    eng.upsert(1, vec![1.0, 0.0]);
    eng.upsert(2, vec![0.0, 1.0]);
    eng.upsert(3, vec![0.9, 0.1]);

    let hits = eng.search(&[1.0, 0.0], 2);
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].id, 1);
    assert_eq!(hits[1].id, 3);
}

#[test]
fn high_perf_engine_matches_cpu_engine_top_k_ids() {
    let mut cpu = CpuVectorEngine::new();
    let mut hp = HighPerfVectorEngine::new();

    for id in 0..2_000u64 {
        let v = deterministic_vec(id, 128);
        cpu.upsert(id, v.clone());
        hp.upsert(id, v);
    }

    let query = deterministic_vec(42, 128);
    let cpu_hits = cpu.search(&query, 20);
    let hp_hits = hp.search(&query, 20);

    assert_eq!(
        cpu_hits.iter().map(|h| h.id).collect::<Vec<_>>(),
        hp_hits.iter().map(|h| h.id).collect::<Vec<_>>()
    );
}
