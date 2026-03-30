use std::fs;

use wax_bench_artifacts::{
    render_markdown_summary, MetricValue, RunSummaryArtifact, SampleArtifact, SampleMetricSlices,
};
use wax_bench_model::BenchmarkId;

#[test]
fn sample_artifact_serializes_identity_and_explicit_missing_metrics() {
    let artifact = SampleArtifact {
        benchmark_id: BenchmarkId {
            dataset_id: "knowledge-small-clean-v1".to_owned(),
            workload_id: "container_open".to_owned(),
            sample_index: 0,
        },
        metrics: SampleMetricSlices {
            container_open_ms: MetricValue::available(4.2),
            metadata_readiness_ms: MetricValue::available(4.8),
            total_ttfq_ms: MetricValue::unavailable("not_measured"),
        },
        resident_memory_bytes: MetricValue::unavailable("platform_not_supported"),
    };

    let value = serde_json::to_value(&artifact).unwrap();
    assert_eq!(
        value["benchmark_id"]["dataset_id"],
        "knowledge-small-clean-v1"
    );
    assert_eq!(value["metrics"]["container_open_ms"]["status"], "available");
    assert_eq!(value["metrics"]["total_ttfq_ms"]["status"], "unavailable");
    assert_eq!(
        value["resident_memory_bytes"]["reason"],
        "platform_not_supported"
    );
}

#[test]
fn run_summary_matches_expected_fixture() {
    let summary = RunSummaryArtifact {
        run_id: "run-001".to_owned(),
        benchmark: BenchmarkId {
            dataset_id: "knowledge-small-clean-v1".to_owned(),
            workload_id: "container_open".to_owned(),
            sample_index: 0,
        },
        fairness_fingerprint: "sha256:fairness-a".to_owned(),
        sample_count: 3,
        p50_total_ttfq_ms: MetricValue::available(6.1),
        p95_total_ttfq_ms: MetricValue::available(7.4),
        p99_total_ttfq_ms: MetricValue::unavailable("insufficient_samples"),
    };

    let expected =
        fs::read_to_string("fixtures/bench/expected-artifacts/sample-summary.json").unwrap();
    let actual = serde_json::to_string_pretty(&summary).unwrap();

    assert_eq!(actual.trim_end(), expected.trim_end());
    assert!(render_markdown_summary(&summary).contains("run-001"));
}
