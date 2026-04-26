use std::fs;
use std::path::PathBuf;
use std::process::Command;

use serde_json::json;
use tempfile::tempdir;
use wax_bench_packer::{pack_dataset, PackRequest};
use wax_v2_core::open_store;
use wax_v2_runtime::{NewDocument, RuntimeStore};

#[test]
fn product_cli_creates_store_when_create_targets_dataset_root() {
    let dataset_dir = tempdir().unwrap();
    let fixture_root =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/bench/source/minimal");
    pack_dataset(&PackRequest::new(
        &fixture_root,
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
            "wax-cli",
            "--",
            "create",
            "--root",
            dataset_dir.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let store_path = dataset_dir.path().join("store.wax");
    assert!(store_path.exists());
    let opened = open_store(&store_path).unwrap();
    assert_eq!(opened.manifest.generation, 0);
}

#[test]
fn product_cli_imports_compatibility_snapshot_before_text_search() {
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

    let create = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-cli",
            "--",
            "create",
            "--root",
            dataset_dir.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        create.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&create.stdout),
        String::from_utf8_lossy(&create.stderr)
    );

    let import = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-cli",
            "--",
            "import-compat",
            "--root",
            dataset_dir.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        import.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&import.stdout),
        String::from_utf8_lossy(&import.stderr)
    );

    for kind in [
        "documents",
        "document_offsets",
        "text_postings",
        "document_ids",
        "document_vectors",
        "document_vectors_preview_q8",
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

#[test]
fn product_cli_searches_text_from_raw_prepared_store_after_sidecar_removal() {
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

    let create = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-cli",
            "--",
            "create",
            "--root",
            dataset_dir.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        create.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&create.stdout),
        String::from_utf8_lossy(&create.stderr)
    );

    let mut runtime = RuntimeStore::open(dataset_dir.path()).unwrap();
    runtime
        .writer()
        .unwrap()
        .publish_raw_documents(vec![
            NewDocument::new("doc-001", "rust benchmark guide")
                .with_metadata(json!({"kind":"guide","workspace":"prod"})),
            NewDocument::new("doc-002", "semantic latency checklist")
                .with_metadata(json!({"kind":"checklist","workspace":"prod"})),
        ])
        .unwrap();
    runtime.close().unwrap();

    for kind in ["documents", "document_offsets", "text_postings"] {
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
