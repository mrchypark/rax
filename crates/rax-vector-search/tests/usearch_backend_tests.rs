use rax_vector_search::engine::VectorSearch;
use rax_vector_search::usearch_backend::{USearchConfig, USearchVectorEngine};

#[test]
fn usearch_returns_expected_neighbor_order() {
    let mut eng = USearchVectorEngine::new();
    eng.upsert(1, vec![1.0, 0.0]);
    eng.upsert(2, vec![0.0, 1.0]);
    eng.upsert(3, vec![0.9, 0.1]);

    let hits = eng.search(&[1.0, 0.0], 2);
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].id, 1);
    assert_eq!(hits[1].id, 3);
}

#[test]
fn usearch_remove_and_upsert_override_live_results() {
    let mut eng = USearchVectorEngine::new();
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
fn usearch_accepts_custom_config() {
    let config = USearchConfig {
        connectivity: 24,
        expansion_add: 256,
        expansion_search: 128,
    };
    let eng = USearchVectorEngine::with_config(config);
    assert_eq!(eng.config(), config);
}
