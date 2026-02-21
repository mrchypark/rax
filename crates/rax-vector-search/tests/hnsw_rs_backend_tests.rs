use rax_vector_search::engine::VectorSearch;
use rax_vector_search::hnsw_rs_backend::{HnswRsConfig, HnswRsVectorEngine};

#[test]
fn hnsw_rs_returns_expected_neighbor_order() {
    let mut eng = HnswRsVectorEngine::new();
    eng.upsert(1, vec![1.0, 0.0]);
    eng.upsert(2, vec![0.0, 1.0]);
    eng.upsert(3, vec![0.9, 0.1]);

    let hits = eng.search(&[1.0, 0.0], 2);
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].id, 1);
    assert_eq!(hits[1].id, 3);
}

#[test]
fn hnsw_rs_remove_and_upsert_override_live_results() {
    let mut eng = HnswRsVectorEngine::new();
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
fn hnsw_rs_accepts_custom_config() {
    let config = HnswRsConfig {
        max_nb_connection: 24,
        max_elements_hint: 128,
        max_layer: 8,
        ef_construction: 400,
        search_oversampling: 12,
    };
    let eng = HnswRsVectorEngine::with_config(config);
    assert_eq!(eng.config(), config);
}
