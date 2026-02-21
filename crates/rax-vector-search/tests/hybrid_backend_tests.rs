use rax_vector_search::engine::VectorSearch;
use rax_vector_search::hybrid_backend::{HybridConfig, HybridVectorEngine};

#[test]
fn hybrid_returns_expected_neighbor_order() {
    let mut eng = HybridVectorEngine::new();
    eng.upsert(1, vec![1.0, 0.0]);
    eng.upsert(2, vec![0.0, 1.0]);
    eng.upsert(3, vec![0.9, 0.1]);

    let hits = eng.search(&[1.0, 0.0], 2);
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].id, 1);
    assert_eq!(hits[1].id, 3);
}

#[test]
fn hybrid_remove_and_upsert_override_live_results() {
    let mut eng = HybridVectorEngine::new();
    eng.upsert(10, vec![1.0, 0.0]);
    eng.upsert(11, vec![0.0, 1.0]);
    eng.remove(10);

    let after_remove = eng.search(&[1.0, 0.0], 2);
    assert_eq!(after_remove.first().map(|h| h.id), Some(11));

    eng.upsert(11, vec![1.0, 0.0]);
    let after_override = eng.search(&[1.0, 0.0], 1);
    assert_eq!(after_override[0].id, 11);
}

#[test]
fn hybrid_accepts_custom_config() {
    let config = HybridConfig {
        enable_conditional_fallback: false,
        embedvec_primary_multiplier: 4,
        embedvec_confident_kth_score: 0.9,
        embedvec_candidate_multiplier: 30,
        hnsw_candidate_multiplier: 15,
        rerank_candidate_cap: 128,
    };
    let eng = HybridVectorEngine::with_config(config);
    assert_eq!(eng.config(), config);
}
