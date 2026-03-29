use serde::{Deserialize, Serialize};
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
