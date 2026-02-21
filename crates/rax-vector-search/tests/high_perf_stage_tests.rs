use rax_vector_search::engine::VectorSearch;
use rax_vector_search::high_perf_backend::HighPerfVectorEngine;

#[test]
fn stage_for_commit_clears_pending_mutations() {
    let mut eng = HighPerfVectorEngine::new();
    eng.upsert(1, vec![1.0, 0.0]);
    eng.upsert(2, vec![0.0, 1.0]);
    eng.remove(2);

    assert_eq!(eng.pending_mutations(), 3);
    let snapshot = eng.stage_for_commit();

    assert_eq!(snapshot.dimension, 2);
    assert_eq!(snapshot.vector_count, 1);
    assert_eq!(eng.pending_mutations(), 0);
}

#[test]
fn restore_from_snapshot_recovers_search_state() {
    let mut source = HighPerfVectorEngine::new();
    source.upsert(1, vec![1.0, 0.0]);
    source.upsert(3, vec![0.9, 0.1]);
    let snapshot = source.stage_for_commit();

    let mut restored = HighPerfVectorEngine::new();
    restored.restore_from_snapshot(snapshot);

    let hits = restored.search(&[1.0, 0.0], 2);
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].id, 1);
    assert_eq!(hits[1].id, 3);
}
