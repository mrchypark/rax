use rax_rag::query_classifier::{classify_query, QueryMode};
use rax_rag::search_request::SearchRequest;
use rax_rag::unified_search::{fuse_results, UnifiedCandidate};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct GoldenCase {
    name: String,
    query: String,
    expected_mode: String,
    candidates: Vec<UnifiedCandidateFixture>,
    expected_order: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct UnifiedCandidateFixture {
    id: u64,
    structured_score: f32,
    semantic_score: f32,
    temporal_score: f32,
}

#[test]
fn golden_query_fixtures_match_expected_order_and_mode() {
    let raw = include_str!("../../../fixtures/rag/golden_queries.json");
    let fixtures: Vec<GoldenCase> = serde_json::from_str(raw).unwrap();

    for case in fixtures {
        let mode = classify_query(&case.query);
        let expected_mode = parse_mode(&case.expected_mode);
        assert_eq!(mode, expected_mode, "mode mismatch in {}", case.name);

        let req = SearchRequest::new(case.query.clone());
        let candidates = case
            .candidates
            .iter()
            .map(|c| UnifiedCandidate {
                id: c.id,
                structured_score: c.structured_score,
                semantic_score: c.semantic_score,
                temporal_score: c.temporal_score,
            })
            .collect::<Vec<_>>();
        let hits = fuse_results(&req, mode, &candidates);
        let got_order = hits.iter().map(|h| h.id).collect::<Vec<_>>();

        assert_eq!(
            got_order, case.expected_order,
            "order mismatch in {}",
            case.name
        );
    }
}

fn parse_mode(raw: &str) -> QueryMode {
    match raw {
        "constraint" => QueryMode::Constraint,
        "semantic" => QueryMode::Semantic,
        other => panic!("unknown expected_mode: {other}"),
    }
}
