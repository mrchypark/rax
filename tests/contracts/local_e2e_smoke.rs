use rax_bench_packer::{pack_dataset, PackRequest};
use tempfile::tempdir;

#[path = "support/cargo.rs"]
mod cargo_support;

use cargo_support::{cargo_output, cargo_status, cargo_status_with_env};

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
    let status = cargo_status_with_env(
        [
            "run",
            "-p",
            "rax-bench-cli",
            "--",
            "run",
            "--dataset",
            dataset_dir.path().to_str().unwrap(),
            "--workload",
            "ttfq_text",
            "--sample-count",
            "2",
        ],
        [
            ("RAX_BENCH_TEST_MODE", "1"),
            ("RAX_BENCH_ARTIFACT_DIR", artifact_dir.to_str().unwrap()),
        ],
    );

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
        let status = cargo_status_with_env(
            [
                "run",
                "-p",
                "rax-bench-cli",
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
            ],
            [("RAX_BENCH_TEST_MODE", "1")],
        );
        assert!(status.success());

        let reduce = cargo_status([
            "run",
            "-p",
            "rax-bench-cli",
            "--",
            "reduce",
            "--input",
            run_dir.to_str().unwrap(),
        ]);
        assert!(reduce.success());
    }

    let matrix_path = artifact_root.join("vector-lane-summary.md");
    let status = cargo_status([
        "run",
        "-p",
        "rax-bench-cli",
        "--",
        "matrix-report",
        "--input",
        artifact_root.to_str().unwrap(),
        "--output",
        matrix_path.to_str().unwrap(),
    ]);

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

    let output = cargo_output([
        "run",
        "-p",
        "rax-bench-cli",
        "--",
        "query",
        "--dataset",
        dataset_dir.path().to_str().unwrap(),
        "--text",
        "rust benchmark",
        "--top-k",
        "2",
    ]);

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

    let run = cargo_status_with_env(
        [
            "run",
            "-p",
            "rax-bench-cli",
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
        ],
        [("RAX_BENCH_TEST_MODE", "1")],
    );

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
    let batch = cargo_status([
        "run",
        "-p",
        "rax-bench-cli",
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
    ]);
    assert!(batch.success());

    let quality = cargo_output([
        "run",
        "-p",
        "rax-bench-cli",
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
    ]);

    assert!(quality.status.success());
    let stdout = String::from_utf8(quality.stdout).unwrap();
    assert!(stdout.contains("\"query_count\": 5"));
    assert!(stdout.contains("\"ndcg_at_10\""));
    assert!(stdout.contains("\"unrated_hits_by_query\""));
    assert!(stdout.contains("\"q-101\""));
}

#[test]
fn local_e2e_smoke_search_bench_uses_runtime_store_built_from_pack() {
    let dataset_dir = tempdir().unwrap();
    let output_dir = tempdir().unwrap();
    let artifact_dir = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/realistic",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let results_path = output_dir.path().join("search-bench.json");
    let output = cargo_output([
        "run",
        "-p",
        "rax-bench-cli",
        "--",
        "search-bench",
        "--dataset",
        dataset_dir.path().to_str().unwrap(),
        "--query-set",
        dataset_dir
            .path()
            .join("queries/core.jsonl")
            .to_str()
            .unwrap(),
        "--sample-count",
        "2",
        "--vector-mode",
        "auto",
        "--output",
        results_path.to_str().unwrap(),
        "--artifact-dir",
        artifact_dir.path().to_str().unwrap(),
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let file_output = std::fs::read_to_string(results_path).unwrap();
    assert_eq!(stdout.trim(), file_output);

    let summary: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(summary["dataset_id"], "knowledge-small-clean-current");
    assert_eq!(
        summary["query_set"].as_str().unwrap(),
        dataset_dir
            .path()
            .join("queries/core.jsonl")
            .to_str()
            .unwrap()
    );
    assert_eq!(summary["sample_count"], 2);
    assert_eq!(summary["concurrency"], 1);
    assert_eq!(summary["scale_label"], "knowledge-small-clean-current");
    assert_eq!(summary["query_count"], 5);
    assert_eq!(summary["total_searches"], 20);
    assert_eq!(summary["filter_query_count"], 0);
    assert_eq!(summary["supports_metadata_filters"], false);
    assert_eq!(summary["vector_mode"], "auto");
    assert!(summary["rss_kb_before"].as_u64().is_some());
    assert!(summary["rss_kb_after"].as_u64().is_some());
    assert!(summary["rss_kb_delta"].as_i64().is_some());
    assert!(summary["store_build_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["total_elapsed_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["total_search_only_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["request_build_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["qps"].as_f64().unwrap() >= 0.0);
    assert_eq!(summary["qps"], summary["qps_end_to_end"]);
    assert!(summary["qps_end_to_end"].as_f64().unwrap() >= 0.0);
    assert!(summary["qps_search_only"].as_f64().unwrap() >= 0.0);
    assert_eq!(summary["warm_concurrent_elapsed_ms"], 0.0);
    assert_eq!(summary["qps_warm_concurrent"], 0.0);
    assert!(summary["p50_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["p95_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["p99_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["p50_cold_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["p95_cold_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["p99_cold_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["p50_warm_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["p95_warm_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["p99_warm_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert_eq!(summary["p50_warm_concurrent_query_latency_ms"], 0.0);
    assert_eq!(summary["p95_warm_concurrent_query_latency_ms"], 0.0);
    assert_eq!(summary["p99_warm_concurrent_query_latency_ms"], 0.0);
    assert!(summary["min_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["max_query_latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["total_hit_count"].as_u64().unwrap() > 0);
    assert_eq!(summary["per_query"].as_array().unwrap().len(), 20);
    assert_eq!(summary["per_query"][0]["query_id"], "q-101");
    assert_eq!(summary["per_query"][0]["query_class"], "keyword");
    assert_eq!(summary["per_query"][0]["mode"], "text");
    assert_eq!(summary["per_query"][0]["include_preview"], true);
    assert_eq!(summary["per_query"][0]["sample_index"], 0);
    assert_eq!(summary["per_query"][0]["phase"], "cold_query");
    assert!(summary["per_query"][0]["worker_index"].is_null());
    assert!(summary["per_query"][0]["latency_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["per_query"][0]["hit_count"].as_u64().unwrap() > 0);
    assert_eq!(summary["per_query"][1]["query_class"], "hybrid");
    assert_eq!(summary["per_query"][1]["mode"], "hybrid");
    assert_eq!(summary["per_query"][5]["phase"], "warm_steady");
    assert_eq!(summary["per_query"][10]["sample_index"], 1);
    assert_eq!(summary["per_query"][10]["phase"], "cold_query");

    let summary_artifact: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(artifact_dir.path().join("summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(summary_artifact, summary);

    let samples = std::fs::read_to_string(artifact_dir.path().join("samples.ndjson")).unwrap();
    let sample_lines = samples
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(sample_lines.len(), 20);
    assert_eq!(sample_lines[0]["phase"], "cold_query");
    assert!(sample_lines[0]["worker_index"].is_null());
    assert_eq!(sample_lines[5]["phase"], "warm_steady");

    let ranked_results: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(artifact_dir.path().join("ranked-results.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(ranked_results.as_array().unwrap().len(), 5);
    assert_eq!(ranked_results[0]["query_id"], "q-101");
    assert!(ranked_results[0]["hits"].as_array().unwrap().len() > 0);
    assert!(ranked_results[0]["hits"][0]["doc_id"].as_str().is_some());

    let quality: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(artifact_dir.path().join("quality.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quality["query_count"], 5);
    assert!(quality["ndcg_at_10"].as_f64().unwrap() >= 0.0);
}

#[test]
fn local_e2e_smoke_search_bench_records_warm_concurrent_samples() {
    let dataset_dir = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let output = cargo_output([
        "run",
        "-p",
        "rax-bench-cli",
        "--",
        "search-bench",
        "--dataset",
        dataset_dir.path().to_str().unwrap(),
        "--query-set",
        dataset_dir
            .path()
            .join("queries/core.jsonl")
            .to_str()
            .unwrap(),
        "--sample-count",
        "1",
        "--vector-mode",
        "auto",
        "--concurrency",
        "2",
        "--scale-label",
        "smoke-c2",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(summary["concurrency"], 2);
    assert_eq!(summary["scale_label"], "smoke-c2");
    assert_eq!(summary["query_count"], 3);
    assert_eq!(summary["total_searches"], 12);
    assert!(
        summary["p50_warm_concurrent_query_latency_ms"]
            .as_f64()
            .unwrap()
            >= 0.0
    );
    assert!(
        summary["p95_warm_concurrent_query_latency_ms"]
            .as_f64()
            .unwrap()
            >= 0.0
    );
    assert!(
        summary["p99_warm_concurrent_query_latency_ms"]
            .as_f64()
            .unwrap()
            >= 0.0
    );
    assert!(summary["warm_concurrent_elapsed_ms"].as_f64().unwrap() >= 0.0);
    assert!(summary["qps_warm_concurrent"].as_f64().unwrap() >= 0.0);

    let samples = summary["per_query"].as_array().unwrap();
    let concurrent_samples = samples
        .iter()
        .filter(|sample| sample["phase"] == "warm_concurrent")
        .collect::<Vec<_>>();
    assert_eq!(concurrent_samples.len(), 6);
    assert!(samples
        .iter()
        .filter(|sample| sample["phase"] != "warm_concurrent")
        .all(|sample| sample["worker_index"].is_null()));
    assert!(concurrent_samples
        .iter()
        .any(|sample| sample["worker_index"] == 0));
    assert!(concurrent_samples
        .iter()
        .any(|sample| sample["worker_index"] == 1));
}

#[test]
fn local_e2e_smoke_search_bench_rejects_zero_concurrency() {
    let dataset_dir = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let output = cargo_output([
        "run",
        "-p",
        "rax-bench-cli",
        "--",
        "search-bench",
        "--dataset",
        dataset_dir.path().to_str().unwrap(),
        "--query-set",
        dataset_dir
            .path()
            .join("queries/core.jsonl")
            .to_str()
            .unwrap(),
        "--sample-count",
        "1",
        "--vector-mode",
        "auto",
        "--concurrency",
        "0",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("concurrency must be greater than zero"));
}

#[test]
fn local_e2e_smoke_search_bench_rejects_non_auto_runtime_vector_mode() {
    let dataset_dir = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let output = cargo_output([
        "run",
        "-p",
        "rax-bench-cli",
        "--",
        "search-bench",
        "--dataset",
        dataset_dir.path().to_str().unwrap(),
        "--query-set",
        dataset_dir
            .path()
            .join("queries/core.jsonl")
            .to_str()
            .unwrap(),
        "--sample-count",
        "1",
        "--vector-mode",
        "exact_flat",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("runtime search-bench currently supports only --vector-mode auto"));
}

#[test]
fn local_e2e_smoke_search_bench_rejects_empty_query_set() {
    let dataset_dir = tempdir().unwrap();
    let query_dir = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let empty_query_set = query_dir.path().join("empty.jsonl");
    std::fs::write(&empty_query_set, "").unwrap();

    let output = cargo_output([
        "run",
        "-p",
        "rax-bench-cli",
        "--",
        "search-bench",
        "--dataset",
        dataset_dir.path().to_str().unwrap(),
        "--query-set",
        empty_query_set.to_str().unwrap(),
        "--sample-count",
        "1",
        "--vector-mode",
        "auto",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("query_set must contain at least one query"));
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

    let pack = cargo_status([
        "run",
        "-p",
        "rax-bench-cli",
        "--",
        "pack-adhoc",
        "--docs",
        source_dir.path().join("docs.ndjson").to_str().unwrap(),
        "--out",
        dataset_dir.path().to_str().unwrap(),
        "--tier",
        "small",
    ]);
    assert!(pack.success());

    let run = cargo_status_with_env(
        [
            "run",
            "-p",
            "rax-bench-cli",
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
        ],
        [("RAX_BENCH_TEST_MODE", "1")],
    );
    assert!(run.success());

    let output = cargo_output([
        "run",
        "-p",
        "rax-bench-cli",
        "--",
        "query",
        "--dataset",
        dataset_dir.path().to_str().unwrap(),
        "--text",
        "rust vector",
        "--top-k",
        "1",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"doc_id\": \"real-001\""));
    assert!(stdout.contains("\"workspace_id\": \"prod\""));
}
