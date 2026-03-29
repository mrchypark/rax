use std::process::Command;

use tempfile::tempdir;
use wax_bench_packer::{pack_dataset, PackRequest};

#[test]
fn local_e2e_smoke_emits_sample_and_summary_artifacts() {
    let dataset_dir = tempdir().unwrap();
    let work_dir = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let artifact_dir = work_dir.path().join("artifacts/latest");
    let status = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("WAX_BENCH_TEST_MODE", "1")
        .env("WAX_BENCH_ARTIFACT_DIR", artifact_dir.to_str().unwrap())
        .args([
            "run",
            "-p",
            "wax-bench-cli",
            "--",
            "run",
            "--dataset",
            dataset_dir.path().to_str().unwrap(),
            "--workload",
            "ttfq_text",
            "--sample-count",
            "2",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(artifact_dir.join("sample-000.json").exists());
    assert!(artifact_dir.join("summary.json").exists());
    assert!(artifact_dir.join("summary.md").exists());
}
