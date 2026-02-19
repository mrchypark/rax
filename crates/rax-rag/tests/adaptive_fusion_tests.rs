use rax_rag::query_classifier::{classify_query, QueryMode};

#[test]
fn classifies_query_mode_for_adaptive_fusion() {
    assert_eq!(classify_query("city:seoul"), QueryMode::Constraint);
    assert_eq!(
        classify_query("what happened yesterday"),
        QueryMode::Semantic
    );
}
