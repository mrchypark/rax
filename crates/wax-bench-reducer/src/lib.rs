use std::path::Path;

use serde::{Deserialize, Serialize};
use wax_bench_artifacts::{
    list_sample_artifact_paths, read_run_summary, render_markdown_summary, MetricValue,
    RunSummaryArtifact, SampleArtifact,
};

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

pub fn render_vector_lane_matrix_report(
    artifact_root: &Path,
) -> Result<String, ReduceError> {
    Ok(build_vector_lane_matrix_report(artifact_root)?.markdown)
}

pub fn render_vector_mode_compare_report(
    artifact_root: &Path,
) -> Result<String, ReduceError> {
    let compare_workload_order = ["materialize_vector", "ttfq_vector", "warm_vector", "warm_hybrid"];
    let exact = build_named_workload_report(&artifact_root.join("exact_flat"), &compare_workload_order)?;
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
    markdown.push_str(
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
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
