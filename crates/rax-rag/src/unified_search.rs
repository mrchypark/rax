use crate::query_classifier::QueryMode;
use crate::search_request::SearchRequest;

#[derive(Debug, Clone, PartialEq)]
pub struct UnifiedCandidate {
    pub id: u64,
    pub structured_score: f32,
    pub semantic_score: f32,
    pub temporal_score: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnifiedHit {
    pub id: u64,
    pub score: f32,
}

pub fn fuse_results(
    request: &SearchRequest,
    mode: QueryMode,
    candidates: &[UnifiedCandidate],
) -> Vec<UnifiedHit> {
    let structured_boost = if mode == QueryMode::Constraint {
        2.0
    } else {
        1.0
    };

    let mut out: Vec<UnifiedHit> = candidates
        .iter()
        .map(|c| UnifiedHit {
            id: c.id,
            score: (c.structured_score * request.structured_weight * structured_boost)
                + (c.semantic_score * request.semantic_weight)
                + (c.temporal_score * request.temporal_weight),
        })
        .collect();

    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });
    out
}
