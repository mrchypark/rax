use rax_rag::query_classifier::classify_query;
use rax_rag::search_request::SearchRequest;
use rax_rag::unified_search::{fuse_results, UnifiedCandidate};

#[test]
fn constraint_query_prioritizes_structured_lane() {
    let mode = classify_query("city:seoul");
    let req = SearchRequest::new("city:seoul");

    let candidates = vec![
        UnifiedCandidate {
            id: 1,
            structured_score: 0.1,
            semantic_score: 0.9,
            temporal_score: 0.0,
        },
        UnifiedCandidate {
            id: 2,
            structured_score: 0.6,
            semantic_score: 0.1,
            temporal_score: 0.0,
        },
    ];

    let hits = fuse_results(&req, mode, &candidates);
    assert_eq!(hits[0].id, 2);
}
