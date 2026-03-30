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

fn build_report(run_dir: &Path) -> Result<ReducedRunReport, ReduceError> {
    let run_summary = read_run_summary(&run_dir.join("summary.json")).map_err(ReduceError::new)?;
    let sample_paths = list_sample_artifact_paths(run_dir).map_err(ReduceError::new)?;

    let mut container_opens = Vec::new();
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
        match sample.metrics.search_latency_ms {
            MetricValue::Available { value } => search_latencies.push(value),
            MetricValue::Unavailable { .. } => {}
        }
    }
    validate_sample_bundle(&run_summary, &sample_indices)?;
    container_opens.sort_by(|left, right| left.partial_cmp(right).unwrap());
    totals.sort_by(|left, right| left.partial_cmp(right).unwrap());
    search_latencies.sort_by(|left, right| left.partial_cmp(right).unwrap());

    let summary = ReducedSummary {
        run_id: run_summary.run_id.clone(),
        sample_count: run_summary.sample_count,
        fairness_fingerprint: run_summary.fairness_fingerprint.clone(),
        p50_container_open_ms: percentile_metric(&container_opens, 0.50, 1),
        p95_container_open_ms: percentile_metric(&container_opens, 0.95, 1),
        p99_container_open_ms: percentile_metric(&container_opens, 0.99, 4),
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
