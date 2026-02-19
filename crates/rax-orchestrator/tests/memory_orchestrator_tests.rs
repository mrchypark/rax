use rax_orchestrator::memory_orchestrator::MemoryOrchestrator;

#[tokio::test]
async fn remember_then_recall_round_trip() {
    let o = MemoryOrchestrator::new();
    o.remember("first memory").await;
    o.remember("second note").await;

    let hits = o.recall("memory").await;
    assert_eq!(hits, vec!["first memory".to_string()]);

    let flushed = o.flush().await;
    assert_eq!(flushed, 2);
}
