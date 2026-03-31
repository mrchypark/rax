use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use wax_bench_metrics::{MemoryReading, SampleMetrics};
use wax_bench_model::{BenchmarkId, MaterializationMode, VectorQueryMode};

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
    pub vector_materialization_ms: MetricValue<f64>,
    pub total_ttfq_ms: MetricValue<f64>,
    pub search_latency_ms: MetricValue<f64>,
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
    pub fairness_fingerprint: String,
    pub sample_count: u32,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayConfigArtifact {
    pub dataset_path: Option<String>,
    pub workload_id: String,
    pub sample_count: u32,
    pub materialization_mode: MaterializationMode,
    pub vector_mode: VectorQueryMode,
    pub artifact_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileDigest {
    pub path: String,
    pub checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunManifestArtifact {
    pub run_id: String,
    pub benchmark: BenchmarkId,
    pub fairness_fingerprint: String,
    pub replay: ReplayConfigArtifact,
    pub files: Vec<ArtifactFileDigest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactBundleStatus {
    Complete,
    Partial { missing_files: Vec<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunBundleArtifact {
    pub manifest: RunManifestArtifact,
    pub summary: Option<RunSummaryArtifact>,
    pub samples: Vec<SampleArtifact>,
    pub status: ArtifactBundleStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactError {
    pub message: String,
}

impl ArtifactError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub fn render_markdown_summary(summary: &RunSummaryArtifact) -> String {
    format!(
        "# Benchmark Summary\n\n- Run: {}\n- Dataset: {}\n- Workload: {}\n- Samples: {}\n- p50 container_open_ms: {}\n- p95 container_open_ms: {}\n- p99 container_open_ms: {}\n- p50 vector_materialization_ms: {}\n- p95 vector_materialization_ms: {}\n- p99 vector_materialization_ms: {}\n- p50 total_ttfq_ms: {}\n- p95 total_ttfq_ms: {}\n- p99 total_ttfq_ms: {}\n- p50 search_latency_ms: {}\n- p95 search_latency_ms: {}\n- p99 search_latency_ms: {}\n",
        summary.run_id,
        summary.benchmark.dataset_id,
        summary.benchmark.workload_id,
        summary.sample_count,
        metric_value_label(&summary.p50_container_open_ms),
        metric_value_label(&summary.p95_container_open_ms),
        metric_value_label(&summary.p99_container_open_ms),
        metric_value_label(&summary.p50_vector_materialization_ms),
        metric_value_label(&summary.p95_vector_materialization_ms),
        metric_value_label(&summary.p99_vector_materialization_ms),
        metric_value_label(&summary.p50_total_ttfq_ms),
        metric_value_label(&summary.p95_total_ttfq_ms),
        metric_value_label(&summary.p99_total_ttfq_ms),
        metric_value_label(&summary.p50_search_latency_ms),
        metric_value_label(&summary.p95_search_latency_ms),
        metric_value_label(&summary.p99_search_latency_ms)
    )
}

pub fn write_run_bundle(
    out_dir: &Path,
    run_id: &str,
    benchmark: &BenchmarkId,
    fairness_fingerprint: &str,
    measured_runs: &[SampleMetrics],
) -> Result<(), String> {
    let replay = ReplayConfigArtifact {
        dataset_path: None,
        workload_id: benchmark.workload_id.clone(),
        sample_count: measured_runs.len() as u32,
        materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
        vector_mode: VectorQueryMode::Auto,
        artifact_dir: out_dir.display().to_string(),
    };
    write_run_bundle_with_replay_config(
        out_dir,
        run_id,
        benchmark,
        fairness_fingerprint,
        measured_runs,
        &replay,
    )
}

pub fn write_run_bundle_with_replay_config(
    out_dir: &Path,
    run_id: &str,
    benchmark: &BenchmarkId,
    fairness_fingerprint: &str,
    measured_runs: &[SampleMetrics],
    replay: &ReplayConfigArtifact,
) -> Result<(), String> {
    fs::create_dir_all(out_dir).map_err(|error| error.to_string())?;

    let mut sample_artifacts = Vec::new();
    let mut file_digests = Vec::new();
    for (index, metrics) in measured_runs.iter().enumerate() {
        let artifact = SampleArtifact {
            benchmark_id: BenchmarkId {
                dataset_id: benchmark.dataset_id.clone(),
                workload_id: benchmark.workload_id.clone(),
                sample_index: index as u32,
            },
            metrics: SampleMetricSlices {
                container_open_ms: MetricValue::available(metrics.container_open_ms),
                metadata_readiness_ms: MetricValue::available(metrics.metadata_readiness_ms),
                vector_materialization_ms: metric_value_from_option(
                    metrics.vector_materialization_ms,
                    "not_measured",
                ),
                total_ttfq_ms: metric_value_from_option(
                    metrics.total_ttfq_recorded.then_some(metrics.total_ttfq_ms),
                    "not_measured",
                ),
                search_latency_ms: metric_value_from_option(
                    metrics.search_latency_ms,
                    "not_measured",
                ),
            },
            resident_memory_bytes: memory_metric(&metrics.resident_memory_bytes),
        };
        let sample_path = out_dir.join(format!("sample-{index:03}.json"));
        fs::write(
            sample_path,
            serde_json::to_string_pretty(&artifact).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        file_digests.push(digest_entry(out_dir, &format!("sample-{index:03}.json"))?);
        sample_artifacts.push(artifact);
    }

    let summary = build_run_summary(run_id, benchmark, fairness_fingerprint, &sample_artifacts);
    let summary_json = serde_json::to_string_pretty(&summary).map_err(|error| error.to_string())?;
    fs::write(out_dir.join("summary.json"), summary_json).map_err(|error| error.to_string())?;
    file_digests.push(digest_entry(out_dir, "summary.json")?);

    let markdown = render_markdown_summary(&summary);
    fs::write(out_dir.join("summary.md"), markdown).map_err(|error| error.to_string())?;
    file_digests.push(digest_entry(out_dir, "summary.md")?);

    let manifest = RunManifestArtifact {
        run_id: run_id.to_owned(),
        benchmark: benchmark.clone(),
        fairness_fingerprint: fairness_fingerprint.to_owned(),
        replay: replay.clone(),
        files: file_digests,
    };
    fs::write(
        out_dir.join("run-manifest.json"),
        serde_json::to_string_pretty(&manifest).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;

    Ok(())
}

pub fn read_sample_artifact(path: &Path) -> Result<SampleArtifact, String> {
    read_json(path)
}

pub fn read_run_summary(path: &Path) -> Result<RunSummaryArtifact, String> {
    read_json(path)
}

pub fn list_sample_artifact_paths(run_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(run_dir).map_err(|error| error.to_string())? {
        let path = entry.map_err(|error| error.to_string())?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with("sample-") && name.ends_with(".json") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

pub fn read_run_manifest(path: &Path) -> Result<RunManifestArtifact, String> {
    read_json(path)
}

pub fn read_run_bundle(run_dir: &Path) -> Result<RunBundleArtifact, ArtifactError> {
    let manifest =
        read_run_manifest(&run_dir.join("run-manifest.json")).map_err(ArtifactError::new)?;
    let mut missing_files = Vec::new();
    let mut summary = None;
    let mut samples = Vec::new();

    for entry in &manifest.files {
        let path = run_dir.join(&entry.path);
        if !path.exists() {
            missing_files.push(entry.path.clone());
            continue;
        }

        let actual = checksum_file(&path).map_err(ArtifactError::new)?;
        if actual != entry.checksum {
            return Err(ArtifactError::new("artifact checksum mismatch"));
        }

        if entry.path == "summary.json" {
            summary = Some(read_run_summary(&path).map_err(ArtifactError::new)?);
        } else if entry.path.starts_with("sample-") && entry.path.ends_with(".json") {
            samples.push(read_sample_artifact(&path).map_err(ArtifactError::new)?);
        }
    }

    samples.sort_by_key(|sample| sample.benchmark_id.sample_index);
    let status = if missing_files.is_empty() {
        ArtifactBundleStatus::Complete
    } else {
        ArtifactBundleStatus::Partial { missing_files }
    };

    Ok(RunBundleArtifact {
        manifest,
        summary,
        samples,
        status,
    })
}

pub fn render_replay_command(replay: &ReplayConfigArtifact) -> Result<String, String> {
    let dataset_path = replay
        .dataset_path
        .as_deref()
        .ok_or_else(|| "replay dataset_path missing".to_owned())?;
    Ok(format!(
        "cargo run -p wax-bench-cli -- run --dataset {} --workload {} --sample-count {} --vector-mode {} --artifact-dir {}",
        dataset_path,
        replay.workload_id,
        replay.sample_count,
        vector_mode_label(replay.vector_mode),
        replay.artifact_dir
    ))
}

fn vector_mode_label(mode: VectorQueryMode) -> &'static str {
    match mode {
        VectorQueryMode::Auto => "auto",
        VectorQueryMode::ExactFlat => "exact_flat",
        VectorQueryMode::Hnsw => "hnsw",
        VectorQueryMode::PreviewQ8 => "preview_q8",
    }
}

fn build_run_summary(
    run_id: &str,
    benchmark: &BenchmarkId,
    fairness_fingerprint: &str,
    sample_artifacts: &[SampleArtifact],
) -> RunSummaryArtifact {
    let mut container_opens: Vec<f64> = sample_artifacts
        .iter()
        .filter_map(|artifact| match artifact.metrics.container_open_ms {
            MetricValue::Available { value } => Some(value),
            MetricValue::Unavailable { .. } => None,
        })
        .collect();
    let mut totals: Vec<f64> = sample_artifacts
        .iter()
        .filter_map(|artifact| match artifact.metrics.total_ttfq_ms {
            MetricValue::Available { value } => Some(value),
            MetricValue::Unavailable { .. } => None,
        })
        .collect();
    let mut vector_materializations: Vec<f64> = sample_artifacts
        .iter()
        .filter_map(|artifact| match artifact.metrics.vector_materialization_ms {
            MetricValue::Available { value } => Some(value),
            MetricValue::Unavailable { .. } => None,
        })
        .collect();
    let mut search_latencies: Vec<f64> = sample_artifacts
        .iter()
        .filter_map(|artifact| match artifact.metrics.search_latency_ms {
            MetricValue::Available { value } => Some(value),
            MetricValue::Unavailable { .. } => None,
        })
        .collect();
    container_opens.sort_by(|left, right| left.partial_cmp(right).unwrap());
    totals.sort_by(|left, right| left.partial_cmp(right).unwrap());
    vector_materializations.sort_by(|left, right| left.partial_cmp(right).unwrap());
    search_latencies.sort_by(|left, right| left.partial_cmp(right).unwrap());

    RunSummaryArtifact {
        run_id: run_id.to_owned(),
        benchmark: benchmark.clone(),
        fairness_fingerprint: fairness_fingerprint.to_owned(),
        sample_count: sample_artifacts.len() as u32,
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

fn metric_value_from_option(value: Option<f64>, unavailable_reason: &str) -> MetricValue<f64> {
    match value {
        Some(value) => MetricValue::available(value),
        None => MetricValue::unavailable(unavailable_reason),
    }
}

fn metric_value_label(value: &MetricValue<f64>) -> String {
    match value {
        MetricValue::Available { value } => format!("{value:.3}"),
        MetricValue::Unavailable { reason } => format!("unavailable ({reason})"),
    }
}

fn read_json<T>(path: &Path) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    serde_json::from_str(&text).map_err(|error| error.to_string())
}

fn digest_entry(root: &Path, relative_path: &str) -> Result<ArtifactFileDigest, String> {
    Ok(ArtifactFileDigest {
        path: relative_path.to_owned(),
        checksum: checksum_file(&root.join(relative_path))?,
    })
}

fn checksum_file(path: &Path) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}
