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

#[test]
fn local_e2e_smoke_renders_vector_lane_matrix_report() {
    let dataset_dir = tempdir().unwrap();
    let work_dir = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let artifact_root = work_dir.path().join("release-matrix");
    for workload in ["materialize_vector", "ttfq_vector", "warm_vector"] {
        let run_dir = artifact_root.join(workload);
        let status = Command::new("cargo")
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("WAX_BENCH_TEST_MODE", "1")
            .args([
                "run",
                "-p",
                "wax-bench-cli",
                "--",
                "run",
                "--dataset",
                dataset_dir.path().to_str().unwrap(),
                "--workload",
                workload,
                "--sample-count",
                "2",
                "--artifact-dir",
                run_dir.to_str().unwrap(),
            ])
            .status()
            .unwrap();
        assert!(status.success());

        let reduce = Command::new("cargo")
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args([
                "run",
                "-p",
                "wax-bench-cli",
                "--",
                "reduce",
                "--input",
                run_dir.to_str().unwrap(),
            ])
            .status()
            .unwrap();
        assert!(reduce.success());
    }

    let matrix_path = artifact_root.join("vector-lane-summary.md");
    let status = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-bench-cli",
            "--",
            "matrix-report",
            "--input",
            artifact_root.to_str().unwrap(),
            "--output",
            matrix_path.to_str().unwrap(),
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(matrix_path.exists());
    let matrix = std::fs::read_to_string(matrix_path).unwrap();
    assert!(matrix.contains("| materialize_vector |"));
    assert!(matrix.contains("| ttfq_vector |"));
    assert!(matrix.contains("| warm_vector |"));
}

#[test]
fn local_e2e_smoke_queries_packed_dataset_with_document_preview() {
    let dataset_dir = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let output = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-bench-cli",
            "--",
            "query",
            "--dataset",
            dataset_dir.path().to_str().unwrap(),
            "--text",
            "rust benchmark",
            "--top-k",
            "2",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"doc_id\": \"doc-001\""));
    assert!(stdout.contains("\"text\": \"rust benchmark guide\""));
    assert!(stdout.contains("\"workspace_id\": \"w1\""));
}

#[test]
fn local_e2e_smoke_runs_warm_hybrid_with_previews_workload() {
    let dataset_dir = tempdir().unwrap();
    let artifact_dir = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let run = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("WAX_BENCH_TEST_MODE", "1")
        .args([
            "run",
            "-p",
            "wax-bench-cli",
            "--",
            "run",
            "--dataset",
            dataset_dir.path().to_str().unwrap(),
            "--workload",
            "warm_hybrid_with_previews",
            "--sample-count",
            "1",
            "--artifact-dir",
            artifact_dir.path().to_str().unwrap(),
        ])
        .status()
        .unwrap();

    assert!(run.success());
    assert!(artifact_dir.path().join("summary.json").exists());
}

#[test]
fn local_e2e_smoke_batches_queries_for_realistic_judged_dataset() {
    let dataset_dir = tempdir().unwrap();
    let output_dir = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/realistic",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let results_path = output_dir.path().join("results.json");
    let batch = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-bench-cli",
            "--",
            "query-batch",
            "--dataset",
            dataset_dir.path().to_str().unwrap(),
            "--query-set",
            dataset_dir
                .path()
                .join("queries/core.jsonl")
                .to_str()
                .unwrap(),
            "--output",
            results_path.to_str().unwrap(),
        ])
        .status()
        .unwrap();
    assert!(batch.success());

    let quality = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-bench-cli",
            "--",
            "quality-report",
            "--query-set",
            dataset_dir
                .path()
                .join("queries/core.jsonl")
                .to_str()
                .unwrap(),
            "--qrels",
            dataset_dir
                .path()
                .join("queries/core-qrels.jsonl")
                .to_str()
                .unwrap(),
            "--results",
            results_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(quality.status.success());
    let stdout = String::from_utf8(quality.stdout).unwrap();
    assert!(stdout.contains("\"query_count\": 5"));
    assert!(stdout.contains("\"ndcg_at_10\""));
    assert!(stdout.contains("\"unrated_hits_by_query\""));
    assert!(stdout.contains("\"q-101\""));
}

#[test]
fn local_e2e_smoke_packs_adhoc_docs_then_queries_them() {
    let source_dir = tempdir().unwrap();
    let dataset_dir = tempdir().unwrap();
    let artifact_dir = tempdir().unwrap();
    std::fs::write(
        source_dir.path().join("docs.ndjson"),
        concat!(
            "{\"doc_id\":\"real-001\",\"text\":\"rust vector lane notes\",\"workspace_id\":\"prod\"}\n",
            "{\"doc_id\":\"real-002\",\"text\":\"ios hybrid latency checklist\",\"workspace_id\":\"prod\"}\n",
        ),
    )
    .unwrap();

    let pack = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-bench-cli",
            "--",
            "pack-adhoc",
            "--docs",
            source_dir.path().join("docs.ndjson").to_str().unwrap(),
            "--out",
            dataset_dir.path().to_str().unwrap(),
            "--tier",
            "small",
        ])
        .status()
        .unwrap();
    assert!(pack.success());

    let run = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("WAX_BENCH_TEST_MODE", "1")
        .args([
            "run",
            "-p",
            "wax-bench-cli",
            "--",
            "run",
            "--dataset",
            dataset_dir.path().to_str().unwrap(),
            "--workload",
            "ttfq_vector",
            "--sample-count",
            "1",
            "--artifact-dir",
            artifact_dir.path().to_str().unwrap(),
        ])
        .status()
        .unwrap();
    assert!(run.success());

    let output = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-bench-cli",
            "--",
            "query",
            "--dataset",
            dataset_dir.path().to_str().unwrap(),
            "--text",
            "rust vector",
            "--top-k",
            "1",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"doc_id\": \"real-001\""));
    assert!(stdout.contains("\"workspace_id\": \"prod\""));
}
