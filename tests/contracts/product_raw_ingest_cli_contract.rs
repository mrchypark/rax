use std::fs;
use std::path::PathBuf;
use std::process::Command;

use tempfile::tempdir;
use wax_bench_model::embed_text;
use wax_bench_packer::{pack_dataset, PackRequest};

#[test]
fn product_cli_ingests_documents_and_vectors_through_explicit_raw_commands() {
    let dataset_dir = tempdir().unwrap();
    let fixture_root =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/bench/source/minimal");
    let manifest = pack_dataset(&PackRequest::new(
        &fixture_root,
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let docs_jsonl = dataset_dir.path().join("raw-docs.jsonl");
    fs::write(
        &docs_jsonl,
        concat!(
            "{\"doc_id\":\"doc-001\",\"text\":\"rust benchmark guide\",\"metadata\":{\"kind\":\"guide\",\"workspace\":\"prod\"}}\n",
            "{\"doc_id\":\"doc-002\",\"text\":\"semantic latency checklist\",\"metadata\":{\"kind\":\"checklist\",\"workspace\":\"prod\"}}\n",
            "{\"doc_id\":\"doc-003\",\"text\":\"hybrid search tuning notes\",\"metadata\":{\"kind\":\"notes\",\"workspace\":\"prod\"}}\n",
        ),
    )
    .unwrap();
    let vectors_jsonl = dataset_dir.path().join("raw-vectors.jsonl");
    fs::write(
        &vectors_jsonl,
        format!(
            "{}\n{}\n{}\n",
            serde_json::json!({
                "doc_id": "doc-001",
                "values": embed_text("rust benchmark guide", 384),
            }),
            serde_json::json!({
                "doc_id": "doc-002",
                "values": embed_text("semantic latency checklist", 384),
            }),
            serde_json::json!({
                "doc_id": "doc-003",
                "values": embed_text("hybrid search tuning notes", 384),
            }),
        ),
    )
    .unwrap();

    run_wax(&["create", "--root", dataset_dir.path().to_str().unwrap()]);
    run_wax(&[
        "ingest",
        "docs",
        "--root",
        dataset_dir.path().to_str().unwrap(),
        "--input",
        docs_jsonl.to_str().unwrap(),
    ]);
    run_wax(&[
        "ingest",
        "vectors",
        "--root",
        dataset_dir.path().to_str().unwrap(),
        "--input",
        vectors_jsonl.to_str().unwrap(),
    ]);

    for kind in [
        "documents",
        "document_offsets",
        "text_postings",
        "document_ids",
        "document_vectors",
        "document_vectors_preview_q8",
        "query_vectors",
    ] {
        for file in manifest.files.iter().filter(|file| file.kind == kind) {
            fs::remove_file(dataset_dir.path().join(&file.path)).unwrap();
        }
    }

    let search = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-cli",
            "--",
            "search",
            "--root",
            dataset_dir.path().to_str().unwrap(),
            "--text",
            "rust benchmark",
            "--top-k",
            "2",
            "--preview",
        ])
        .output()
        .unwrap();

    assert!(
        search.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&search.stdout),
        String::from_utf8_lossy(&search.stderr)
    );
    let stdout = String::from_utf8(search.stdout).unwrap();
    assert!(stdout.contains("\"doc_id\": \"doc-001\""));
    assert!(stdout.contains("\"preview\": \"rust benchmark guide\""));
}

fn run_wax(args: &[&str]) {
    let output = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(["run", "-p", "wax-cli", "--"])
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
