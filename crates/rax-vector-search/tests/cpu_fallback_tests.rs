use rax_vector_search::cpu_backend::CpuVectorEngine;
use rax_vector_search::engine::VectorSearch;

#[test]
fn cpu_fallback_engine_works_without_external_backend() {
    let mut eng = CpuVectorEngine::new();
    eng.upsert(1, vec![1.0, 0.0]);
    let hits = eng.search(&[1.0, 0.0], 1);
    assert_eq!(hits[0].id, 1);
}
