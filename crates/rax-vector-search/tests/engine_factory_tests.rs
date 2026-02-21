use rax_vector_search::engine::VectorSearch;
use rax_vector_search::factory::{
    create_default_engine, create_engine, VectorBackend, VectorSearchConfig,
};

#[test]
fn default_engine_uses_usearch_backend() {
    let engine = create_default_engine();
    assert_eq!(engine.backend(), VectorBackend::USearch);
}

#[test]
fn config_can_select_usearch_backend() {
    let engine = create_engine(VectorSearchConfig {
        backend: VectorBackend::USearch,
    });
    assert_eq!(engine.backend(), VectorBackend::USearch);
}

#[test]
fn factory_engine_executes_vector_search() {
    let mut engine = create_default_engine();
    engine.upsert(1, vec![1.0, 0.0]);
    engine.upsert(2, vec![0.0, 1.0]);

    let hits = engine.search(&[1.0, 0.0], 1);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, 1);
}
