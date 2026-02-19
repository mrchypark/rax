use std::collections::HashMap;

use rax_text_search::fts5_engine::TextSearchEngine;

#[test]
fn bm25_query_returns_ranked_hits() {
    let mut engine = TextSearchEngine::new();

    let mut m1 = HashMap::new();
    m1.insert("kind".to_string(), "note".to_string());
    engine.ingest("a", "memory memory engine", m1);

    let mut m2 = HashMap::new();
    m2.insert("kind".to_string(), "note".to_string());
    engine.ingest("b", "memory", m2);

    let hits = engine.query("memory", None, 10);
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].id, "a");
    assert!(hits[0].score > hits[1].score);
    assert!(hits[0].snippet.is_some());
}
