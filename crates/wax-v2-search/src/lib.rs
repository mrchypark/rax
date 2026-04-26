use std::collections::HashMap;

use wax_bench_model::VectorQueryMode;
use wax_v2_text::TextLane;
use wax_v2_vector::VectorLane;

const RRF_K: f64 = 60.0;

#[derive(Debug, Clone, PartialEq)]
pub struct HybridHitDiagnostic {
    pub doc_id: String,
    pub text_rank: Option<usize>,
    pub vector_rank: Option<usize>,
    pub rrf_score: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HybridSearchReport {
    pub fused_hits: Vec<String>,
    pub diagnostics: Vec<HybridHitDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataFilter {
    clauses: Vec<MetadataFilterClause>,
}

impl MetadataFilter {
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            clauses: pairs
                .into_iter()
                .map(|(field, value)| MetadataFilterClause {
                    field: field.into(),
                    value: value.into(),
                })
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataFilterClause {
    field: String,
    value: String,
}

pub trait MetadataSource {
    fn field_value(&self, doc_id: &str, field: &str) -> Option<&str>;
}

pub fn search_first_hybrid_query(
    text_lane: &TextLane,
    vector_lane: &mut VectorLane,
    vector_mode: VectorQueryMode,
    auto_force_exact: bool,
) -> Result<Vec<String>, String> {
    let Some(hybrid_text_query) = text_lane.first_hybrid_query() else {
        return Ok(Vec::new());
    };
    let Some(hybrid_vector_query) = vector_lane.first_hybrid_query.as_ref() else {
        return Ok(Vec::new());
    };
    let limit = text_lane
        .first_hybrid_top_k()
        .max(vector_lane.first_hybrid_top_k)
        .max(1);
    let text_hits = text_lane.search_with_limit(hybrid_text_query, limit);
    let hybrid_vector_query = hybrid_vector_query.clone();
    let report = hybrid_search_with_diagnostics(
        &text_hits,
        vector_lane,
        &hybrid_vector_query,
        limit,
        vector_mode,
        auto_force_exact,
    )?;
    Ok(report.fused_hits)
}

pub fn hybrid_search(
    text_hits: &[String],
    vector_lane: &mut VectorLane,
    query_vector: &[f32],
    limit: usize,
    vector_mode: VectorQueryMode,
    auto_force_exact: bool,
) -> Result<Vec<String>, String> {
    let report = hybrid_search_with_diagnostics(
        text_hits,
        vector_lane,
        query_vector,
        limit,
        vector_mode,
        auto_force_exact,
    )?;
    Ok(report.fused_hits)
}

pub fn hybrid_search_with_diagnostics(
    text_hits: &[String],
    vector_lane: &mut VectorLane,
    query_vector: &[f32],
    limit: usize,
    vector_mode: VectorQueryMode,
    auto_force_exact: bool,
) -> Result<HybridSearchReport, String> {
    let vector_hits =
        vector_lane.search_with_query(query_vector, limit, vector_mode, auto_force_exact)?;
    Ok(hybrid_search_report(text_hits, &vector_hits, limit))
}

pub fn hybrid_search_report(
    text_hits: &[String],
    vector_hits: &[String],
    limit: usize,
) -> HybridSearchReport {
    let mut scores = HashMap::<String, f64>::new();
    let mut text_ranks = HashMap::<String, usize>::new();
    let mut vector_ranks = HashMap::<String, usize>::new();

    for (rank, doc_id) in text_hits.iter().enumerate() {
        let rank = rank + 1;
        text_ranks.insert(doc_id.clone(), rank);
        *scores.entry(doc_id.clone()).or_insert(0.0) += 1.0 / (RRF_K + rank as f64);
    }
    for (rank, doc_id) in vector_hits.iter().enumerate() {
        let rank = rank + 1;
        vector_ranks.insert(doc_id.clone(), rank);
        *scores.entry(doc_id.clone()).or_insert(0.0) += 1.0 / (RRF_K + rank as f64);
    }

    let mut fused = scores.into_iter().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });

    let diagnostics = fused
        .iter()
        .take(limit)
        .map(|(doc_id, score)| HybridHitDiagnostic {
            doc_id: doc_id.clone(),
            text_rank: text_ranks.get(doc_id).copied(),
            vector_rank: vector_ranks.get(doc_id).copied(),
            rrf_score: *score,
        })
        .collect::<Vec<_>>();
    let fused_hits = diagnostics
        .iter()
        .map(|diagnostic| diagnostic.doc_id.clone())
        .collect::<Vec<_>>();

    HybridSearchReport {
        fused_hits,
        diagnostics,
    }
}

pub fn reciprocal_rank_fusion(
    text_hits: &[String],
    vector_hits: &[String],
    limit: usize,
) -> Vec<String> {
    hybrid_search_report(text_hits, vector_hits, limit).fused_hits
}

pub fn filter_hits_by_metadata(
    hits: &[String],
    metadata_source: &impl MetadataSource,
    filter: &MetadataFilter,
) -> Vec<String> {
    if filter.is_empty() {
        return hits.to_vec();
    }

    hits.iter()
        .filter(|doc_id| {
            filter.clauses.iter().all(|clause| {
                metadata_source
                    .field_value(doc_id, &clause.field)
                    .is_some_and(|value| value == clause.value)
            })
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        filter_hits_by_metadata, hybrid_search_report, reciprocal_rank_fusion, MetadataFilter,
        MetadataSource,
    };

    struct TestMetadataSource {
        docs: HashMap<String, HashMap<String, String>>,
    }

    impl MetadataSource for TestMetadataSource {
        fn field_value(&self, doc_id: &str, field: &str) -> Option<&str> {
            self.docs
                .get(doc_id)
                .and_then(|document| document.get(field))
                .map(String::as_str)
        }
    }

    #[test]
    fn reciprocal_rank_fusion_prefers_combined_rank_signal_and_doc_id_tie_breaks() {
        let hits = reciprocal_rank_fusion(
            &["doc-2".to_owned(), "doc-1".to_owned()],
            &["doc-1".to_owned(), "doc-3".to_owned()],
            3,
        );

        assert_eq!(hits, vec!["doc-1", "doc-2", "doc-3"]);
    }

    #[test]
    fn hybrid_search_report_captures_lane_ranks_and_scores() {
        let report = hybrid_search_report(
            &["doc-2".to_owned(), "doc-1".to_owned()],
            &["doc-1".to_owned(), "doc-3".to_owned()],
            3,
        );

        assert_eq!(report.fused_hits, vec!["doc-1", "doc-2", "doc-3"]);
        assert_eq!(report.diagnostics.len(), 3);
        assert_eq!(report.diagnostics[0].doc_id, "doc-1");
        assert_eq!(report.diagnostics[0].text_rank, Some(2));
        assert_eq!(report.diagnostics[0].vector_rank, Some(1));
        assert!(report.diagnostics[0].rrf_score > report.diagnostics[1].rrf_score);
        assert_eq!(report.diagnostics[1].doc_id, "doc-2");
        assert_eq!(report.diagnostics[1].text_rank, Some(1));
        assert_eq!(report.diagnostics[1].vector_rank, None);
        assert_eq!(report.diagnostics[2].doc_id, "doc-3");
        assert_eq!(report.diagnostics[2].text_rank, None);
        assert_eq!(report.diagnostics[2].vector_rank, Some(2));
    }

    #[test]
    fn filter_hits_by_metadata_keeps_docs_matching_top_level_string_clauses() {
        let filter = MetadataFilter::from_pairs([("workspace_id", "w1")]);
        let docs = TestMetadataSource {
            docs: HashMap::from([
                (
                    "doc-1".to_owned(),
                    HashMap::from([
                        ("workspace_id".to_owned(), "w1".to_owned()),
                        ("text".to_owned(), "alpha".to_owned()),
                    ]),
                ),
                (
                    "doc-2".to_owned(),
                    HashMap::from([
                        ("workspace_id".to_owned(), "w2".to_owned()),
                        ("text".to_owned(), "beta".to_owned()),
                    ]),
                ),
                (
                    "doc-3".to_owned(),
                    HashMap::from([("text".to_owned(), "missing".to_owned())]),
                ),
            ]),
        };

        let filtered = filter_hits_by_metadata(
            &["doc-2".to_owned(), "doc-1".to_owned(), "doc-3".to_owned()],
            &docs,
            &filter,
        );

        assert_eq!(filtered, vec!["doc-1"]);
    }
}
