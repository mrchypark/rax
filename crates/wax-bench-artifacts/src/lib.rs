use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use wax_bench_metrics::{MemoryReading, SampleMetrics};
use wax_bench_model::BenchmarkId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MetricValue<T> {
    Available { value: T },
    Unavailable { reason: String },
}

impl<T> MetricValue<T> {
    pub fn available(value: T) -> Self {
        Self::Available { value }
    }

    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self::Unavailable {
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SampleMetricSlices {
    pub container_open_ms: MetricValue<f64>,
    pub metadata_readiness_ms: MetricValue<f64>,
    pub total_ttfq_ms: MetricValue<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SampleArtifact {
    pub benchmark_id: BenchmarkId,
    pub metrics: SampleMetricSlices,
    pub resident_memory_bytes: MetricValue<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSummaryArtifact {
    pub run_id: String,
    pub benchmark: BenchmarkId,
    pub sample_count: u32,
    pub p50_total_ttfq_ms: MetricValue<f64>,
    pub p95_total_ttfq_ms: MetricValue<f64>,
    pub p99_total_ttfq_ms: MetricValue<f64>,
}

pub fn render_markdown_summary(summary: &RunSummaryArtifact) -> String {
    format!(
        "# Benchmark Summary\n\n- Run: {}\n- Dataset: {}\n- Workload: {}\n- Samples: {}\n",
        summary.run_id,
        summary.benchmark.dataset_id,
        summary.benchmark.workload_id,
        summary.sample_count
    )
}

pub fn write_run_bundle(
    out_dir: &Path,
    run_id: &str,
    benchmark: &BenchmarkId,
    measured_runs: &[SampleMetrics],
) -> Result<(), String> {
    fs::create_dir_all(out_dir).map_err(|error| error.to_string())?;

    let mut sample_artifacts = Vec::new();
    for (index, metrics) in measured_runs.iter().enumerate() {
        let artifact = SampleArtifact {
            benchmark_id: BenchmarkId {
                dataset_id: benchmark.dataset_id.clone(),
                workload_id: benchmark.workload_id.clone(),
                sample_index: index as u32,
            },
            metrics: SampleMetricSlices {
                container_open_ms: MetricValue::available(metrics.container_open_ms as f64),
                metadata_readiness_ms: MetricValue::available(
                    metrics.metadata_readiness_ms as f64,
                ),
                total_ttfq_ms: MetricValue::available(metrics.total_ttfq_ms as f64),
            },
            resident_memory_bytes: memory_metric(&metrics.resident_memory_bytes),
        };
        let sample_path = out_dir.join(format!("sample-{index:03}.json"));
        fs::write(
            sample_path,
            serde_json::to_string_pretty(&artifact).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        sample_artifacts.push(artifact);
    }

    let summary = build_run_summary(run_id, benchmark, &sample_artifacts);
    fs::write(
        out_dir.join("summary.json"),
        serde_json::to_string_pretty(&summary).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    fs::write(out_dir.join("summary.md"), render_markdown_summary(&summary))
        .map_err(|error| error.to_string())?;

    Ok(())
}

fn build_run_summary(
    run_id: &str,
    benchmark: &BenchmarkId,
    sample_artifacts: &[SampleArtifact],
) -> RunSummaryArtifact {
    let mut totals: Vec<f64> = sample_artifacts
        .iter()
        .filter_map(|artifact| match artifact.metrics.total_ttfq_ms {
            MetricValue::Available { value } => Some(value),
            MetricValue::Unavailable { .. } => None,
        })
        .collect();
    totals.sort_by(|left, right| left.partial_cmp(right).unwrap());

    RunSummaryArtifact {
        run_id: run_id.to_owned(),
        benchmark: benchmark.clone(),
        sample_count: sample_artifacts.len() as u32,
        p50_total_ttfq_ms: percentile_metric(&totals, 0.50, 1),
        p95_total_ttfq_ms: percentile_metric(&totals, 0.95, 1),
        p99_total_ttfq_ms: percentile_metric(&totals, 0.99, 4),
    }
}

fn percentile_metric(values: &[f64], percentile: f64, min_samples: usize) -> MetricValue<f64> {
    if values.len() < min_samples || values.is_empty() {
        return MetricValue::unavailable("insufficient_samples");
    }

    let index = ((values.len() as f64 * percentile).ceil() as usize).saturating_sub(1);
    MetricValue::available(values[index.min(values.len() - 1)])
}

fn memory_metric(reading: &MemoryReading) -> MetricValue<u64> {
    match reading {
        MemoryReading::Available { value } => MetricValue::available(*value),
        MemoryReading::Unavailable { reason } => MetricValue::unavailable(reason.clone()),
    }
}
