use tempfile::tempdir;
use wax_bench_artifacts::{
    read_run_bundle, render_replay_command, write_run_bundle_with_replay_config,
    ArtifactBundleStatus, ReplayConfigArtifact,
};
use wax_bench_metrics::{CompilerOptimization, MemoryReading, SampleMetrics, ThermalState};
use wax_bench_model::{BenchmarkId, MaterializationMode};

#[test]
fn artifact_checksum_mismatch_is_detected() {
    let run_dir = tempdir().unwrap();
    let replay = replay_config(
        run_dir
            .path()
            .join("rerun-artifacts")
            .to_string_lossy()
            .as_ref(),
    );
    write_run_bundle_with_replay_config(
        run_dir.path(),
        "run-001",
        &benchmark_id(),
        "sha256:fairness-a",
        &[sample(3)],
        &replay,
    )
    .unwrap();

    std::fs::write(
        run_dir.path().join("sample-000.json"),
        "{\"tampered\":true}\n",
    )
    .unwrap();

    assert_eq!(
        read_run_bundle(run_dir.path()).unwrap_err().message,
        "artifact checksum mismatch"
    );
}

#[test]
fn incomplete_run_is_still_readable_as_partial() {
    let run_dir = tempdir().unwrap();
    let replay = replay_config(
        run_dir
            .path()
            .join("rerun-artifacts")
            .to_string_lossy()
            .as_ref(),
    );
    write_run_bundle_with_replay_config(
        run_dir.path(),
        "run-001",
        &benchmark_id(),
        "sha256:fairness-a",
        &[sample(3)],
        &replay,
    )
    .unwrap();

    std::fs::remove_file(run_dir.path().join("summary.md")).unwrap();

    let bundle = read_run_bundle(run_dir.path()).unwrap();
    assert_eq!(
        bundle.status,
        ArtifactBundleStatus::Partial {
            missing_files: vec!["summary.md".to_owned()],
        }
    );
    assert!(bundle.summary.is_some());
    assert_eq!(bundle.samples.len(), 1);
}

#[test]
fn run_config_can_be_replayed_exactly() {
    let run_dir = tempdir().unwrap();
    let replay = replay_config("/tmp/replayed-artifacts");
    write_run_bundle_with_replay_config(
        run_dir.path(),
        "run-001",
        &benchmark_id(),
        "sha256:fairness-a",
        &[sample(3), sample(5)],
        &replay,
    )
    .unwrap();

    let bundle = read_run_bundle(run_dir.path()).unwrap();
    assert_eq!(bundle.manifest.replay, replay);
    assert_eq!(
        render_replay_command(&bundle.manifest.replay).unwrap(),
        "cargo run -p wax-bench-cli -- run --dataset fixtures/bench/out/minimal-pack --workload ttfq_text --sample-count 2 --artifact-dir /tmp/replayed-artifacts"
    );
}

fn benchmark_id() -> BenchmarkId {
    BenchmarkId {
        dataset_id: "knowledge-small-clean-v1".to_owned(),
        workload_id: "ttfq_text".to_owned(),
        sample_index: 0,
    }
}

fn replay_config(artifact_dir: &str) -> ReplayConfigArtifact {
    ReplayConfigArtifact {
        dataset_path: Some("fixtures/bench/out/minimal-pack".to_owned()),
        workload_id: "ttfq_text".to_owned(),
        sample_count: 2,
        materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
        artifact_dir: artifact_dir.to_owned(),
    }
}

fn sample(total_ttfq_ms: u64) -> SampleMetrics {
    SampleMetrics {
        container_open_ms: 1,
        metadata_readiness_ms: 1,
        total_ttfq_ms,
        resident_memory_bytes: MemoryReading::Unavailable {
            reason: "test".to_owned(),
        },
        compiler_optimization: Some(CompilerOptimization::Debug),
        thermal_state: Some(ThermalState::Nominal),
    }
}
