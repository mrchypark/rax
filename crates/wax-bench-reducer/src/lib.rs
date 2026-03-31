use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use wax_bench_artifacts::{
    list_sample_artifact_paths, read_run_summary, render_markdown_summary, MetricValue,
    RunSummaryArtifact, SampleArtifact,
};
use wax_bench_model::{QrelRecord, RankedQueryResult};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReducedSummary {
    pub run_id: String,
    pub sample_count: u32,
    pub fairness_fingerprint: String,
    pub p50_container_open_ms: MetricValue<f64>,
    pub p95_container_open_ms: MetricValue<f64>,
    pub p99_container_open_ms: MetricValue<f64>,
    pub p50_vector_materialization_ms: MetricValue<f64>,
    pub p95_vector_materialization_ms: MetricValue<f64>,
    pub p99_vector_materialization_ms: MetricValue<f64>,
    pub p50_total_ttfq_ms: MetricValue<f64>,
    pub p95_total_ttfq_ms: MetricValue<f64>,
    pub p99_total_ttfq_ms: MetricValue<f64>,
    pub p50_search_latency_ms: MetricValue<f64>,
    pub p95_search_latency_ms: MetricValue<f64>,
    pub p99_search_latency_ms: MetricValue<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaselineComparison {
    pub baseline_run_id: String,
    pub p50_container_open_delta_ms: MetricValue<f64>,
    pub p95_container_open_delta_ms: MetricValue<f64>,
    pub p99_container_open_delta_ms: MetricValue<f64>,
    pub p50_vector_materialization_delta_ms: MetricValue<f64>,
    pub p95_vector_materialization_delta_ms: MetricValue<f64>,
    pub p99_vector_materialization_delta_ms: MetricValue<f64>,
    pub p50_total_ttfq_delta_ms: MetricValue<f64>,
    pub p95_total_ttfq_delta_ms: MetricValue<f64>,
    pub p99_total_ttfq_delta_ms: MetricValue<f64>,
    pub p50_search_latency_delta_ms: MetricValue<f64>,
    pub p95_search_latency_delta_ms: MetricValue<f64>,
    pub p99_search_latency_delta_ms: MetricValue<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReducedRunReport {
    pub run_summary: RunSummaryArtifact,
    pub summary: ReducedSummary,
    pub baseline_comparison: Option<BaselineComparison>,
    pub markdown: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorLaneMatrixRow {
    pub workload_id: String,
    pub p50_vector_materialization_ms: MetricValue<f64>,
    pub p95_vector_materialization_ms: MetricValue<f64>,
    pub p50_total_ttfq_ms: MetricValue<f64>,
    pub p95_total_ttfq_ms: MetricValue<f64>,
    pub p50_search_latency_ms: MetricValue<f64>,
    pub p95_search_latency_ms: MetricValue<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorLaneMatrixReport {
    pub dataset_id: String,
    pub rows: Vec<VectorLaneMatrixRow>,
    pub markdown: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchQualitySummary {
    pub query_count: u32,
    pub unrated_hit_count: u32,
    pub ndcg_at_10: f64,
    pub ndcg_at_20: f64,
    pub recall_at_10: f64,
    pub recall_at_100: f64,
    pub precision_at_10: f64,
    pub mrr_at_10: f64,
    pub success_at_1: f64,
    pub success_at_3: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReduceError {
    pub message: String,
}

impl ReduceError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub fn detect_fairness_mismatch(
    candidate: &RunSummaryArtifact,
    baseline: &RunSummaryArtifact,
) -> bool {
    candidate.fairness_fingerprint != baseline.fairness_fingerprint
}

pub fn reduce_run_dir(
    run_dir: &Path,
    baseline_dir: Option<&Path>,
) -> Result<ReducedRunReport, ReduceError> {
    let mut candidate = build_report(run_dir)?;
    if let Some(baseline_dir) = baseline_dir {
        let baseline = build_report(baseline_dir)?;
        if detect_fairness_mismatch(&candidate.run_summary, &baseline.run_summary) {
            return Err(ReduceError::new("fairness fingerprint mismatch"));
        }
        candidate.baseline_comparison = Some(BaselineComparison {
            baseline_run_id: baseline.run_summary.run_id,
            p50_container_open_delta_ms: delta_metric(
                &candidate.summary.p50_container_open_ms,
                &baseline.summary.p50_container_open_ms,
            ),
            p95_container_open_delta_ms: delta_metric(
                &candidate.summary.p95_container_open_ms,
                &baseline.summary.p95_container_open_ms,
            ),
            p99_container_open_delta_ms: delta_metric(
                &candidate.summary.p99_container_open_ms,
                &baseline.summary.p99_container_open_ms,
            ),
            p50_vector_materialization_delta_ms: delta_metric(
                &candidate.summary.p50_vector_materialization_ms,
                &baseline.summary.p50_vector_materialization_ms,
            ),
            p95_vector_materialization_delta_ms: delta_metric(
                &candidate.summary.p95_vector_materialization_ms,
                &baseline.summary.p95_vector_materialization_ms,
            ),
            p99_vector_materialization_delta_ms: delta_metric(
                &candidate.summary.p99_vector_materialization_ms,
                &baseline.summary.p99_vector_materialization_ms,
            ),
            p50_total_ttfq_delta_ms: delta_metric(
                &candidate.summary.p50_total_ttfq_ms,
                &baseline.summary.p50_total_ttfq_ms,
            ),
            p95_total_ttfq_delta_ms: delta_metric(
                &candidate.summary.p95_total_ttfq_ms,
                &baseline.summary.p95_total_ttfq_ms,
            ),
            p99_total_ttfq_delta_ms: delta_metric(
                &candidate.summary.p99_total_ttfq_ms,
                &baseline.summary.p99_total_ttfq_ms,
            ),
            p50_search_latency_delta_ms: delta_metric(
                &candidate.summary.p50_search_latency_ms,
                &baseline.summary.p50_search_latency_ms,
            ),
            p95_search_latency_delta_ms: delta_metric(
                &candidate.summary.p95_search_latency_ms,
                &baseline.summary.p95_search_latency_ms,
            ),
            p99_search_latency_delta_ms: delta_metric(
                &candidate.summary.p99_search_latency_ms,
                &baseline.summary.p99_search_latency_ms,
            ),
        });
    }

    Ok(candidate)
}

pub fn render_vector_lane_matrix_report(artifact_root: &Path) -> Result<String, ReduceError> {
    Ok(build_vector_lane_matrix_report(artifact_root)?.markdown)
}

pub fn render_vector_mode_compare_report(artifact_root: &Path) -> Result<String, ReduceError> {
    let compare_workload_order = [
        "materialize_vector",
        "ttfq_vector",
        "warm_vector",
        "warm_hybrid",
    ];
    let exact =
        build_named_workload_report(&artifact_root.join("exact_flat"), &compare_workload_order)?;
    let hnsw = build_named_workload_report(&artifact_root.join("hnsw"), &compare_workload_order)?;

    if exact.dataset_id != hnsw.dataset_id {
        return Err(ReduceError::new(
            "vector mode compare reports must share dataset_id",
        ));
    }
    if exact.rows.len() != hnsw.rows.len() {
        return Err(ReduceError::new(
            "vector mode compare reports must share workload rows",
        ));
    }

    Ok(render_vector_mode_compare_markdown(&exact, &hnsw))
}

pub fn compute_search_quality_summary(
    qrels: &[QrelRecord],
    results: &[RankedQueryResult],
) -> Result<SearchQualitySummary, ReduceError> {
    let qrels_by_query = group_qrels(qrels)?;
    let results_by_query = group_results(results, &qrels_by_query)?;
    let mut query_ids = qrels_by_query.keys().cloned().collect::<Vec<_>>();
    query_ids.sort();

    if query_ids.is_empty() {
        return Err(ReduceError::new("qrels must not be empty"));
    }

    let mut unrated_hit_count = 0u32;
    let mut ndcg_at_10 = 0.0;
    let mut ndcg_at_20 = 0.0;
    let mut recall_at_10 = 0.0;
    let mut recall_at_100 = 0.0;
    let mut precision_at_10 = 0.0;
    let mut mrr_at_10 = 0.0;
    let mut success_at_1 = 0.0;
    let mut success_at_3 = 0.0;

    for query_id in &query_ids {
        let qrels_for_query = qrels_by_query
            .get(query_id)
            .expect("query id derived from qrels map");
        let hits = results_by_query
            .get(query_id)
            .map(|result| result.as_slice())
            .unwrap_or(&[]);
        let relevant_doc_count = qrels_for_query
            .values()
            .filter(|relevance| **relevance > 0)
            .count();

        unrated_hit_count += hits
            .iter()
            .filter(|doc_id| !qrels_for_query.contains_key((*doc_id).as_str()))
            .count() as u32;

        ndcg_at_10 += ndcg_at_k(qrels_for_query, hits, 10);
        ndcg_at_20 += ndcg_at_k(qrels_for_query, hits, 20);
        recall_at_10 += recall_at_k(qrels_for_query, hits, 10, relevant_doc_count);
        recall_at_100 += recall_at_k(qrels_for_query, hits, 100, relevant_doc_count);
        precision_at_10 += precision_at_k(qrels_for_query, hits, 10);
        mrr_at_10 += reciprocal_rank_at_k(qrels_for_query, hits, 10);
        success_at_1 += success_at_k(qrels_for_query, hits, 1);
        success_at_3 += success_at_k(qrels_for_query, hits, 3);
    }

    let query_count = query_ids.len() as f64;
    Ok(SearchQualitySummary {
        query_count: query_ids.len() as u32,
        unrated_hit_count,
        ndcg_at_10: ndcg_at_10 / query_count,
        ndcg_at_20: ndcg_at_20 / query_count,
        recall_at_10: recall_at_10 / query_count,
        recall_at_100: recall_at_100 / query_count,
        precision_at_10: precision_at_10 / query_count,
        mrr_at_10: mrr_at_10 / query_count,
        success_at_1: success_at_1 / query_count,
        success_at_3: success_at_3 / query_count,
    })
}

pub fn compute_search_quality_summary_from_paths(
    query_set_path: &Path,
    qrels_path: &Path,
    results_path: &Path,
) -> Result<SearchQualitySummary, ReduceError> {
    let query_ids = read_query_ids(query_set_path)?;
    let qrels = read_qrels(qrels_path)?;
    validate_qrel_query_coverage(&query_ids, &qrels)?;
    let results = read_ranked_query_results(results_path)?;
    validate_result_query_coverage(&query_ids, &results)?;
    compute_search_quality_summary(&qrels, &results)
}

pub fn build_vector_lane_matrix_report(
    artifact_root: &Path,
) -> Result<VectorLaneMatrixReport, ReduceError> {
    let workload_order = ["materialize_vector", "ttfq_vector", "warm_vector"];
    let (dataset_id, rows) = build_named_workload_rows(artifact_root, &workload_order)?;
    let markdown = render_vector_lane_matrix_markdown(&dataset_id, &rows);
    Ok(VectorLaneMatrixReport {
        dataset_id,
        rows,
        markdown,
    })
}

fn build_report(run_dir: &Path) -> Result<ReducedRunReport, ReduceError> {
    let run_summary = read_run_summary(&run_dir.join("summary.json")).map_err(ReduceError::new)?;
    let sample_paths = list_sample_artifact_paths(run_dir).map_err(ReduceError::new)?;

    let mut container_opens = Vec::new();
    let mut vector_materializations = Vec::new();
    let mut totals = Vec::new();
    let mut search_latencies = Vec::new();
    let mut sample_indices = Vec::new();
    for sample_path in sample_paths {
        let sample: SampleArtifact =
            wax_bench_artifacts::read_sample_artifact(&sample_path).map_err(ReduceError::new)?;
        sample_indices.push(sample.benchmark_id.sample_index);
        match sample.metrics.container_open_ms {
            MetricValue::Available { value } => container_opens.push(value),
            MetricValue::Unavailable { .. } => {}
        }
        match sample.metrics.total_ttfq_ms {
            MetricValue::Available { value } => totals.push(value),
            MetricValue::Unavailable { .. } => {}
        }
        match sample.metrics.vector_materialization_ms {
            MetricValue::Available { value } => vector_materializations.push(value),
            MetricValue::Unavailable { .. } => {}
        }
        match sample.metrics.search_latency_ms {
            MetricValue::Available { value } => search_latencies.push(value),
            MetricValue::Unavailable { .. } => {}
        }
    }
    validate_sample_bundle(&run_summary, &sample_indices)?;
    container_opens.sort_by(|left, right| left.partial_cmp(right).unwrap());
    vector_materializations.sort_by(|left, right| left.partial_cmp(right).unwrap());
    totals.sort_by(|left, right| left.partial_cmp(right).unwrap());
    search_latencies.sort_by(|left, right| left.partial_cmp(right).unwrap());

    let summary = ReducedSummary {
        run_id: run_summary.run_id.clone(),
        sample_count: run_summary.sample_count,
        fairness_fingerprint: run_summary.fairness_fingerprint.clone(),
        p50_container_open_ms: percentile_metric(&container_opens, 0.50, 1),
        p95_container_open_ms: percentile_metric(&container_opens, 0.95, 1),
        p99_container_open_ms: percentile_metric(&container_opens, 0.99, 4),
        p50_vector_materialization_ms: percentile_metric(&vector_materializations, 0.50, 1),
        p95_vector_materialization_ms: percentile_metric(&vector_materializations, 0.95, 1),
        p99_vector_materialization_ms: percentile_metric(&vector_materializations, 0.99, 4),
        p50_total_ttfq_ms: percentile_metric(&totals, 0.50, 1),
        p95_total_ttfq_ms: percentile_metric(&totals, 0.95, 1),
        p99_total_ttfq_ms: percentile_metric(&totals, 0.99, 4),
        p50_search_latency_ms: percentile_metric(&search_latencies, 0.50, 1),
        p95_search_latency_ms: percentile_metric(&search_latencies, 0.95, 1),
        p99_search_latency_ms: percentile_metric(&search_latencies, 0.99, 4),
    };

    Ok(ReducedRunReport {
        markdown: render_markdown_summary(&run_summary),
        run_summary,
        summary,
        baseline_comparison: None,
    })
}

fn build_named_workload_report(
    artifact_root: &Path,
    workload_order: &[&str],
) -> Result<VectorLaneMatrixReport, ReduceError> {
    let (dataset_id, rows) = build_named_workload_rows(artifact_root, workload_order)?;
    let markdown = render_vector_lane_matrix_markdown(&dataset_id, &rows);
    Ok(VectorLaneMatrixReport {
        dataset_id,
        rows,
        markdown,
    })
}

fn read_query_ids(path: &Path) -> Result<Vec<String>, ReduceError> {
    #[derive(Deserialize)]
    struct QuerySetRecord {
        query_id: String,
    }

    let text =
        fs::read_to_string(path).map_err(|_| ReduceError::new("failed to read query_set file"))?;
    parse_jsonl::<QuerySetRecord>(&text, "query_set file contains invalid json").map(|records| {
        records
            .into_iter()
            .map(|record| record.query_id)
            .collect::<Vec<_>>()
    })
}

fn read_qrels(path: &Path) -> Result<Vec<QrelRecord>, ReduceError> {
    let text =
        fs::read_to_string(path).map_err(|_| ReduceError::new("failed to read qrels file"))?;
    parse_jsonl::<QrelRecord>(&text, "qrels file contains invalid json")
}

fn read_ranked_query_results(path: &Path) -> Result<Vec<RankedQueryResult>, ReduceError> {
    let text =
        fs::read_to_string(path).map_err(|_| ReduceError::new("failed to read results file"))?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ReduceError::new("results file must not be empty"));
    }
    if trimmed.starts_with('[') {
        serde_json::from_str(trimmed)
            .map_err(|_| ReduceError::new("results file contains invalid json"))
    } else {
        parse_jsonl::<RankedQueryResult>(&text, "results file contains invalid json")
    }
}

fn parse_jsonl<T>(text: &str, invalid_message: &str) -> Result<Vec<T>, ReduceError>
where
    T: for<'de> Deserialize<'de>,
{
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(|_| ReduceError::new(invalid_message)))
        .collect()
}

fn group_qrels(
    qrels: &[QrelRecord],
) -> Result<BTreeMap<String, BTreeMap<String, u8>>, ReduceError> {
    let mut grouped = BTreeMap::<String, BTreeMap<String, u8>>::new();
    for qrel in qrels {
        if qrel.relevance > 3 {
            return Err(ReduceError::new("qrels contain invalid relevance"));
        }
        let docs = grouped.entry(qrel.query_id.clone()).or_default();
        if docs.insert(qrel.doc_id.clone(), qrel.relevance).is_some() {
            return Err(ReduceError::new("qrels contain duplicate query_id/doc_id"));
        }
    }
    Ok(grouped)
}

fn validate_qrel_query_coverage(
    query_ids: &[String],
    qrels: &[QrelRecord],
) -> Result<(), ReduceError> {
    let expected = query_ids.iter().map(String::as_str).collect::<HashSet<_>>();
    let judged = qrels
        .iter()
        .map(|qrel| qrel.query_id.as_str())
        .collect::<HashSet<_>>();
    if expected != judged {
        return Err(ReduceError::new("qrels file must align with query ids"));
    }
    Ok(())
}

fn validate_result_query_coverage(
    query_ids: &[String],
    results: &[RankedQueryResult],
) -> Result<(), ReduceError> {
    let expected = query_ids.iter().map(String::as_str).collect::<HashSet<_>>();
    let returned = results
        .iter()
        .map(|result| result.query_id.as_str())
        .collect::<HashSet<_>>();
    if expected != returned {
        return Err(ReduceError::new("results file must align with query ids"));
    }
    Ok(())
}

fn group_results(
    results: &[RankedQueryResult],
    qrels_by_query: &BTreeMap<String, BTreeMap<String, u8>>,
) -> Result<BTreeMap<String, Vec<String>>, ReduceError> {
    let mut grouped = BTreeMap::<String, Vec<String>>::new();
    for result in results {
        if !qrels_by_query.contains_key(result.query_id.as_str()) {
            return Err(ReduceError::new("results contain unknown query_id"));
        }
        let mut seen_doc_ids = HashSet::new();
        let doc_ids = result
            .hits
            .iter()
            .map(|hit| {
                if !seen_doc_ids.insert(hit.doc_id.as_str()) {
                    Err(ReduceError::new(
                        "results contain duplicate doc_id within query_id",
                    ))
                } else {
                    Ok(hit.doc_id.clone())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if grouped.insert(result.query_id.clone(), doc_ids).is_some() {
            return Err(ReduceError::new("results contain duplicate query_id"));
        }
    }
    Ok(grouped)
}

fn ndcg_at_k(qrels: &BTreeMap<String, u8>, hits: &[String], k: usize) -> f64 {
    let dcg = discounted_gain(
        &hits
            .iter()
            .take(k)
            .map(|doc_id| *qrels.get(doc_id.as_str()).unwrap_or(&0))
            .collect::<Vec<_>>(),
    );
    let mut ideal = qrels.values().copied().collect::<Vec<_>>();
    ideal.sort_unstable_by(|left, right| right.cmp(left));
    let idcg = discounted_gain(&ideal.into_iter().take(k).collect::<Vec<_>>());
    if idcg == 0.0 {
        0.0
    } else {
        dcg / idcg
    }
}

fn discounted_gain(relevances: &[u8]) -> f64 {
    relevances
        .iter()
        .enumerate()
        .map(|(index, relevance)| {
            let gain = (2_u32.pow(*relevance as u32) - 1) as f64;
            let discount = (index as f64 + 2.0).log2();
            gain / discount
        })
        .sum()
}

fn recall_at_k(
    qrels: &BTreeMap<String, u8>,
    hits: &[String],
    k: usize,
    relevant_doc_count: usize,
) -> f64 {
    if relevant_doc_count == 0 {
        return 0.0;
    }
    let retrieved = hits
        .iter()
        .take(k)
        .filter(|doc_id| qrels.get(doc_id.as_str()).copied().unwrap_or(0) > 0)
        .count();
    retrieved as f64 / relevant_doc_count as f64
}

fn precision_at_k(qrels: &BTreeMap<String, u8>, hits: &[String], k: usize) -> f64 {
    let retrieved = hits
        .iter()
        .take(k)
        .filter(|doc_id| qrels.get(doc_id.as_str()).copied().unwrap_or(0) > 0)
        .count();
    retrieved as f64 / k as f64
}

fn reciprocal_rank_at_k(qrels: &BTreeMap<String, u8>, hits: &[String], k: usize) -> f64 {
    hits.iter()
        .take(k)
        .position(|doc_id| qrels.get(doc_id.as_str()).copied().unwrap_or(0) > 0)
        .map(|index| 1.0 / (index as f64 + 1.0))
        .unwrap_or(0.0)
}

fn success_at_k(qrels: &BTreeMap<String, u8>, hits: &[String], k: usize) -> f64 {
    if hits
        .iter()
        .take(k)
        .any(|doc_id| qrels.get(doc_id.as_str()).copied().unwrap_or(0) > 0)
    {
        1.0
    } else {
        0.0
    }
}

fn build_named_workload_rows(
    artifact_root: &Path,
    workload_order: &[&str],
) -> Result<(String, Vec<VectorLaneMatrixRow>), ReduceError> {
    let mut rows = Vec::new();
    let mut dataset_id = None::<String>;

    for workload_id in workload_order {
        let report = reduce_run_dir(&artifact_root.join(workload_id), None)?;
        if let Some(existing) = &dataset_id {
            if existing != &report.run_summary.benchmark.dataset_id {
                return Err(ReduceError::new(
                    "vector lane matrix workloads must share dataset_id",
                ));
            }
        } else {
            dataset_id = Some(report.run_summary.benchmark.dataset_id.clone());
        }

        rows.push(VectorLaneMatrixRow {
            workload_id: (*workload_id).to_owned(),
            p50_vector_materialization_ms: report.summary.p50_vector_materialization_ms,
            p95_vector_materialization_ms: report.summary.p95_vector_materialization_ms,
            p50_total_ttfq_ms: report.summary.p50_total_ttfq_ms,
            p95_total_ttfq_ms: report.summary.p95_total_ttfq_ms,
            p50_search_latency_ms: report.summary.p50_search_latency_ms,
            p95_search_latency_ms: report.summary.p95_search_latency_ms,
        });
    }

    let dataset_id = dataset_id.ok_or_else(|| ReduceError::new("vector lane matrix is empty"))?;
    Ok((dataset_id, rows))
}

fn validate_sample_bundle(
    run_summary: &RunSummaryArtifact,
    sample_indices: &[u32],
) -> Result<(), ReduceError> {
    if sample_indices.len() != run_summary.sample_count as usize {
        return Err(ReduceError::new(
            "sample_count does not match sample artifacts",
        ));
    }

    let mut sorted_indices = sample_indices.to_vec();
    sorted_indices.sort_unstable();
    for (expected, actual) in sorted_indices.iter().enumerate() {
        if *actual != expected as u32 {
            return Err(ReduceError::new("sample indices must be contiguous"));
        }
    }

    Ok(())
}

fn percentile_metric(values: &[f64], percentile: f64, min_samples: usize) -> MetricValue<f64> {
    if values.len() < min_samples || values.is_empty() {
        return MetricValue::unavailable("insufficient_samples");
    }

    let index = ((values.len() as f64 * percentile).ceil() as usize).saturating_sub(1);
    MetricValue::available(values[index.min(values.len() - 1)])
}

fn delta_metric(candidate: &MetricValue<f64>, baseline: &MetricValue<f64>) -> MetricValue<f64> {
    match (candidate, baseline) {
        (MetricValue::Available { value: left }, MetricValue::Available { value: right }) => {
            MetricValue::available(left - right)
        }
        _ => MetricValue::unavailable("comparison_not_available"),
    }
}

fn render_vector_lane_matrix_markdown(dataset_id: &str, rows: &[VectorLaneMatrixRow]) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Vector Lane Matrix\n\n");
    markdown.push_str(&format!("- Dataset: {dataset_id}\n\n"));
    markdown.push_str(
        "| Workload | p50 vector_materialization_ms | p95 vector_materialization_ms | p50 total_ttfq_ms | p95 total_ttfq_ms | p50 search_latency_ms | p95 search_latency_ms |\n",
    );
    markdown.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for row in rows {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} |\n",
            row.workload_id,
            metric_value_markdown(&row.p50_vector_materialization_ms),
            metric_value_markdown(&row.p95_vector_materialization_ms),
            metric_value_markdown(&row.p50_total_ttfq_ms),
            metric_value_markdown(&row.p95_total_ttfq_ms),
            metric_value_markdown(&row.p50_search_latency_ms),
            metric_value_markdown(&row.p95_search_latency_ms),
        ));
    }
    markdown
}

fn metric_value_markdown(value: &MetricValue<f64>) -> String {
    match value {
        MetricValue::Available { value } => format!("{value:.3}"),
        MetricValue::Unavailable { .. } => "-".to_owned(),
    }
}

fn render_vector_mode_compare_markdown(
    exact: &VectorLaneMatrixReport,
    hnsw: &VectorLaneMatrixReport,
) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Vector Mode Compare\n\n");
    markdown.push_str(&format!("- Dataset: {}\n\n", exact.dataset_id));
    markdown.push_str(&render_compare_section(
        "p95 vector_materialization_ms",
        exact,
        hnsw,
        |row| &row.p95_vector_materialization_ms,
    ));
    markdown.push('\n');
    markdown.push_str(&render_compare_section(
        "p95 total_ttfq_ms",
        exact,
        hnsw,
        |row| &row.p95_total_ttfq_ms,
    ));
    markdown.push('\n');
    markdown.push_str(&render_compare_section(
        "p95 search_latency_ms",
        exact,
        hnsw,
        |row| &row.p95_search_latency_ms,
    ));
    markdown
}

fn render_compare_section<F>(
    title: &str,
    exact: &VectorLaneMatrixReport,
    hnsw: &VectorLaneMatrixReport,
    metric: F,
) -> String
where
    F: Fn(&VectorLaneMatrixRow) -> &MetricValue<f64>,
{
    let mut section = String::new();
    section.push_str(&format!("## {title}\n\n"));
    section.push_str("| Workload | exact_flat p95 | hnsw p95 | delta_ms (hnsw-exact_flat) |\n");
    section.push_str("| --- | ---: | ---: | ---: |\n");
    for (exact_row, hnsw_row) in exact.rows.iter().zip(&hnsw.rows) {
        section.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            exact_row.workload_id,
            metric_value_markdown(metric(exact_row)),
            metric_value_markdown(metric(hnsw_row)),
            metric_delta_markdown(metric(hnsw_row), metric(exact_row)),
        ));
    }
    section
}

fn metric_delta_markdown(candidate: &MetricValue<f64>, baseline: &MetricValue<f64>) -> String {
    match delta_metric(candidate, baseline) {
        MetricValue::Available { value } => format!("{value:.3}"),
        MetricValue::Unavailable { .. } => "-".to_owned(),
    }
}
