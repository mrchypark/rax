use tempfile::tempdir;
use wax_bench_artifacts::write_run_bundle;
use wax_bench_metrics::{CompilerOptimization, MemoryReading, SampleMetrics, ThermalState};
use wax_bench_model::BenchmarkId;
use wax_bench_reducer::{
    render_vector_lane_matrix_report, render_vector_mode_compare_report,
};

#[test]
fn reducer_renders_vector_lane_matrix_for_release_artifacts() {
    let artifact_root = tempdir().unwrap();
    write_run(
        artifact_root.path(),
        "materialize_vector",
        sample(0.8, 1.2, None),
    );
    write_run(artifact_root.path(), "ttfq_vector", sample(0.3, 4.0, None));
    write_run(artifact_root.path(), "warm_vector", sample(0.0, 0.0, Some(0.7)));

    let markdown = render_vector_lane_matrix_report(artifact_root.path()).unwrap();

    assert!(markdown.contains("| Workload |"));
    assert!(markdown.contains("| materialize_vector |"));
    assert!(markdown.contains("| ttfq_vector |"));
    assert!(markdown.contains("| warm_vector |"));
    assert!(markdown.contains("1.200"));
    assert!(markdown.contains("4.000"));
    assert!(markdown.contains("0.700"));
}

#[test]
fn reducer_renders_vector_mode_compare_report_for_hnsw_and_exact_flat() {
    let artifact_root = tempdir().unwrap();
    for mode in ["exact_flat", "hnsw"] {
        write_run(
            &artifact_root.path().join(mode),
            "materialize_vector",
            sample(if mode == "hnsw" { 0.4 } else { 0.8 }, 1.2, None),
        );
        write_run(
            &artifact_root.path().join(mode),
            "ttfq_vector",
            sample(0.0, if mode == "hnsw" { 2.0 } else { 4.0 }, None),
        );
        write_run(
            &artifact_root.path().join(mode),
            "warm_vector",
            sample(0.0, 0.0, Some(if mode == "hnsw" { 0.5 } else { 0.9 })),
        );
        write_run(
            &artifact_root.path().join(mode),
            "warm_hybrid",
            sample(0.0, 0.0, Some(if mode == "hnsw" { 0.7 } else { 1.1 })),
        );
    }

    let markdown = render_vector_mode_compare_report(artifact_root.path()).unwrap();

    assert!(markdown.contains("| Workload | exact_flat p95 | hnsw p95 | delta_ms (hnsw-exact_flat) |"));
    assert!(markdown.contains("## p95 total_ttfq_ms"));
    assert!(markdown.contains("## p95 search_latency_ms"));
    assert!(markdown.contains("| ttfq_vector | 4.000 | 2.000 | -2.000 |"));
    assert!(markdown.contains("| warm_vector | 0.900 | 0.500 | -0.400 |"));
    assert!(markdown.contains("| warm_hybrid | 1.100 | 0.700 | -0.400 |"));
}

fn write_run(root: &std::path::Path, workload_id: &str, sample: SampleMetrics) {
    let run_dir = root.join(workload_id);
    write_run_bundle(
        &run_dir,
        "run-001",
        &BenchmarkId {
            dataset_id: "knowledge-large-clean-v1".to_owned(),
            workload_id: workload_id.to_owned(),
            sample_index: 0,
        },
        "sha256:fairness-a",
        &[sample],
    )
    .unwrap();
}

fn sample(
    vector_materialization_ms: f64,
    total_ttfq_ms: f64,
    search_latency_ms: Option<f64>,
) -> SampleMetrics {
    SampleMetrics {
        container_open_ms: 0.1,
        metadata_readiness_ms: 0.2,
        vector_materialization_ms: Some(vector_materialization_ms),
        total_ttfq_ms,
        total_ttfq_recorded: true,
        search_latency_ms,
        resident_memory_bytes: MemoryReading::Unavailable {
            reason: "test".to_owned(),
        },
        compiler_optimization: Some(CompilerOptimization::Release),
        thermal_state: Some(ThermalState::Nominal),
    }
}
