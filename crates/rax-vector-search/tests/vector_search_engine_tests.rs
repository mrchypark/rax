use rax_vector_search::cpu_backend::CpuVectorEngine;
use rax_vector_search::engine::VectorSearch;

#[test]
fn cosine_knn_returns_expected_neighbor() {
    let mut eng = CpuVectorEngine::new();
    eng.upsert(1, vec![1.0, 0.0]);
    eng.upsert(2, vec![0.0, 1.0]);
    eng.upsert(3, vec![0.9, 0.1]);

    let hits = eng.search(&[1.0, 0.0], 2);
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].id, 1);
    assert_eq!(hits[1].id, 3);

    eng.remove(1);
    let hits2 = eng.search(&[1.0, 0.0], 1);
    assert_eq!(hits2[0].id, 3);
}
