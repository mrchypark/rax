use tempfile::tempdir;
use wax_bench_artifacts::{
    read_run_summary, write_run_bundle, MetricValue, RunSummaryArtifact, SampleArtifact,
    SampleMetricSlices,
};
use wax_bench_metrics::{CompilerOptimization, MemoryReading, SampleMetrics, ThermalState};
use wax_bench_model::BenchmarkId;
use wax_bench_reducer::{detect_fairness_mismatch, reduce_run_dir};

#[test]
fn reducer_computes_percentiles_from_sample_bundle() {
    let run_dir = tempdir().unwrap();
    write_run_bundle(
        run_dir.path(),
        "run-001",
        &BenchmarkId {
            dataset_id: "knowledge-small-clean-v1".to_owned(),
            workload_id: "ttfq_text".to_owned(),
            sample_index: 0,
        },
        "sha256:fairness-a",
        &[
            sample(1.0),
            sample(2.0),
            sample(3.0),
            sample(4.0),
            sample(5.0),
        ],
    )
    .unwrap();

    let report = reduce_run_dir(run_dir.path(), None).unwrap();
    assert_eq!(
        report.summary.p50_container_open_ms,
        MetricValue::available(1.0)
    );
    assert_eq!(
        report.summary.p50_total_ttfq_ms,
        MetricValue::available(3.0)
    );
    assert_eq!(
        report.summary.p95_total_ttfq_ms,
        MetricValue::available(5.0)
    );
    assert_eq!(
        report.summary.p95_vector_materialization_ms,
        MetricValue::available(0.5)
    );
    assert_eq!(
        report.summary.p99_total_ttfq_ms,
        MetricValue::available(5.0)
    );
    assert_eq!(
        report.summary.p95_search_latency_ms,
        MetricValue::unavailable("insufficient_samples")
    );
    assert!(report.markdown.contains("p95"));
}

#[test]
fn reducer_detects_and_rejects_fairness_mismatch() {
    let candidate_dir = tempdir().unwrap();
    let baseline_dir = tempdir().unwrap();

    write_run_bundle(
        candidate_dir.path(),
        "run-001",
        &BenchmarkId {
            dataset_id: "candidate".to_owned(),
            workload_id: "ttfq_text".to_owned(),
            sample_index: 0,
        },
        "sha256:fairness-a",
        &[sample(3.0)],
    )
    .unwrap();
    write_run_bundle(
        baseline_dir.path(),
        "run-000",
        &BenchmarkId {
            dataset_id: "baseline".to_owned(),
            workload_id: "ttfq_text".to_owned(),
            sample_index: 0,
        },
        "sha256:fairness-b",
        &[sample(2.0)],
    )
    .unwrap();

    let candidate_summary = reduce_run_dir(candidate_dir.path(), None)
        .unwrap()
        .run_summary;
    let baseline_summary = reduce_run_dir(baseline_dir.path(), None)
        .unwrap()
        .run_summary;
    assert!(detect_fairness_mismatch(
        &candidate_summary,
        &baseline_summary
    ));
    assert_eq!(
        reduce_run_dir(candidate_dir.path(), Some(baseline_dir.path()))
            .unwrap_err()
            .message,
        "fairness fingerprint mismatch"
    );
}

#[test]
fn reducer_preserves_unavailable_percentiles_for_empty_measurements() {
    let run_dir = tempdir().unwrap();
    let sample = SampleArtifact {
        benchmark_id: BenchmarkId {
            dataset_id: "knowledge-small-clean-v1".to_owned(),
            workload_id: "ttfq_text".to_owned(),
            sample_index: 0,
        },
        metrics: SampleMetricSlices {
            container_open_ms: MetricValue::available(1.0),
            metadata_readiness_ms: MetricValue::available(1.0),
            vector_materialization_ms: MetricValue::unavailable("not_measured"),
            total_ttfq_ms: MetricValue::unavailable("not_measured"),
            search_latency_ms: MetricValue::available(0.7),
        },
        resident_memory_bytes: MetricValue::unavailable("test"),
    };
    let summary = RunSummaryArtifact {
        run_id: "run-001".to_owned(),
        benchmark: sample.benchmark_id.clone(),
        fairness_fingerprint: "sha256:fairness-a".to_owned(),
        sample_count: 1,
        p50_container_open_ms: MetricValue::available(1.0),
        p95_container_open_ms: MetricValue::available(1.0),
        p99_container_open_ms: MetricValue::unavailable("insufficient_samples"),
        p50_vector_materialization_ms: MetricValue::unavailable("insufficient_samples"),
        p95_vector_materialization_ms: MetricValue::unavailable("insufficient_samples"),
        p99_vector_materialization_ms: MetricValue::unavailable("insufficient_samples"),
        p50_total_ttfq_ms: MetricValue::unavailable("insufficient_samples"),
        p95_total_ttfq_ms: MetricValue::unavailable("insufficient_samples"),
        p99_total_ttfq_ms: MetricValue::unavailable("insufficient_samples"),
        p50_search_latency_ms: MetricValue::available(0.7),
        p95_search_latency_ms: MetricValue::available(0.7),
        p99_search_latency_ms: MetricValue::unavailable("insufficient_samples"),
    };
    std::fs::write(
        run_dir.path().join("sample-000.json"),
        serde_json::to_string_pretty(&sample).unwrap(),
    )
    .unwrap();
    std::fs::write(
        run_dir.path().join("summary.json"),
        serde_json::to_string_pretty(&summary).unwrap(),
    )
    .unwrap();

    let report = reduce_run_dir(run_dir.path(), None).unwrap();
    assert_eq!(
        report.summary.p50_total_ttfq_ms,
        MetricValue::unavailable("insufficient_samples")
    );
}

#[test]
fn reducer_rejects_inconsistent_sample_bundle() {
    let run_dir = tempdir().unwrap();
    write_run_bundle(
        run_dir.path(),
        "run-001",
        &BenchmarkId {
            dataset_id: "knowledge-small-clean-v1".to_owned(),
            workload_id: "ttfq_text".to_owned(),
            sample_index: 0,
        },
        "sha256:fairness-a",
        &[sample(3.0)],
    )
    .unwrap();

    let mut summary = read_run_summary(&run_dir.path().join("summary.json")).unwrap();
    summary.sample_count = 2;
    std::fs::write(
        run_dir.path().join("summary.json"),
        serde_json::to_string_pretty(&summary).unwrap(),
    )
    .unwrap();

    assert_eq!(
        reduce_run_dir(run_dir.path(), None).unwrap_err().message,
        "sample_count does not match sample artifacts"
    );
}

fn sample(total_ttfq_ms: f64) -> SampleMetrics {
    SampleMetrics {
        container_open_ms: 1.0,
        metadata_readiness_ms: 1.0,
        vector_materialization_ms: Some(0.5),
        total_ttfq_ms,
        total_ttfq_recorded: true,
        search_latency_ms: None,
        resident_memory_bytes: MemoryReading::Unavailable {
            reason: "test".to_owned(),
        },
        compiler_optimization: Some(CompilerOptimization::Debug),
        thermal_state: Some(ThermalState::Nominal),
    }
}
