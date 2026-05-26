use std::fs;
use std::process::Command;

use tempfile::tempdir;
use wax_bench_model::embed_text;
use wax_bench_model::ManifestFile;
use wax_bench_packer::{pack_adhoc_dataset, AdhocPackRequest};
use wax_v2_docstore::Docstore;

#[test]
fn product_cli_direct_store_raw_documents_round_trip_without_dataset_manifest() {
    let store_dir = tempdir().unwrap();
    let source_dir = tempdir().unwrap();
    let manifest_dir = tempdir().unwrap();
    let docs_path = source_dir.path().join("docs.ndjson");
    fs::write(&docs_path, "{\"doc_id\":\"seed-001\",\"text\":\"seed\"}\n").unwrap();
    let mut manifest = pack_adhoc_dataset(&AdhocPackRequest::new(
        &docs_path,
        manifest_dir.path(),
        "small",
    ))
    .unwrap();

    let store_path = store_dir.path().join("projection.wax");
    let docs_jsonl = store_dir.path().join("projection-docs.jsonl");
    fs::write(
        &docs_jsonl,
        concat!(
            "{\"doc_id\":\"external-proj-42\",\"text\":\"projected alpha raw document\",",
            "\"metadata\":{\"tenant\":\"external\",\"kind\":\"projection\"},",
            "\"timestamp_ms\":1712345678000,\"source\":\"projection-store\"}\n",
        ),
    )
    .unwrap();

    run_wax(&["create", "--store", store_path.to_str().unwrap()]);
    run_wax(&[
        "ingest",
        "docs",
        "--store",
        store_path.to_str().unwrap(),
        "--input",
        docs_jsonl.to_str().unwrap(),
    ]);

    manifest.files = vec![ManifestFile {
        path: "projection.wax".to_owned(),
        kind: "store".to_owned(),
        format: "wax".to_owned(),
        record_count: 1,
        checksum: "runtime".to_owned(),
    }];
    let docstore =
        Docstore::open_with_store_path(store_dir.path(), &manifest, &store_path).unwrap();
    let documents = docstore
        .load_documents_by_id(&["external-proj-42".to_owned()])
        .unwrap();
    let document = documents.get("external-proj-42").unwrap();
    assert_eq!(
        document.get("metadata"),
        Some(&serde_json::json!({"tenant":"external","kind":"projection"}))
    );
    assert_eq!(
        document.get("timestamp_ms"),
        Some(&serde_json::json!(1712345678000_u64))
    );
    assert_eq!(
        document.get("source"),
        Some(&serde_json::json!("projection-store"))
    );

    let search = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-cli",
            "--",
            "search",
            "--store",
            store_path.to_str().unwrap(),
            "--text",
            "projected alpha",
            "--top-k",
            "1",
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
    assert!(stdout.contains("\"doc_id\": \"external-proj-42\""));
    assert!(stdout.contains("\"preview\": \"projected alpha raw document\""));
}

#[test]
fn product_cli_direct_store_docs_ingest_creates_missing_store() {
    let store_dir = tempdir().unwrap();
    let store_path = store_dir.path().join("projection-create-on-ingest.wax");
    let docs_jsonl = store_dir.path().join("projection-docs-create.jsonl");
    fs::write(
        &docs_jsonl,
        "{\"doc_id\":\"fact:fact-index\",\"text\":\"recent decisions runtime projection\"}\n",
    )
    .unwrap();

    run_wax(&[
        "ingest",
        "docs",
        "--store",
        store_path.to_str().unwrap(),
        "--input",
        docs_jsonl.to_str().unwrap(),
    ]);

    let search = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-cli",
            "--",
            "search",
            "--store",
            store_path.to_str().unwrap(),
            "--text",
            "recent decisions",
            "--top-k",
            "1",
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
    assert!(stdout.contains("\"doc_id\": \"fact:fact-index\""));
    assert!(stdout.contains("\"preview\": \"recent decisions runtime projection\""));
}

#[test]
fn product_cli_direct_store_searches_with_external_query_vector() {
    let store_dir = tempdir().unwrap();
    let store_path = store_dir.path().join("projection-vector.wax");
    let docs_jsonl = store_dir.path().join("projection-vector-docs.jsonl");
    fs::write(
        &docs_jsonl,
        concat!(
            "{\"doc_id\":\"vec-doc-1\",\"text\":\"vector search rust guide\"}\n",
            "{\"doc_id\":\"vec-doc-2\",\"text\":\"semantic latency checklist\"}\n",
        ),
    )
    .unwrap();
    let vectors_jsonl = store_dir.path().join("projection-vectors.jsonl");
    fs::write(
        &vectors_jsonl,
        format!(
            "{}\n{}\n",
            serde_json::json!({
                "doc_id": "vec-doc-1",
                "values": embed_text("vector search rust guide", 384),
            }),
            serde_json::json!({
                "doc_id": "vec-doc-2",
                "values": embed_text("semantic latency checklist", 384),
            }),
        ),
    )
    .unwrap();
    let query_vector = store_dir.path().join("query-vector.json");
    fs::write(
        &query_vector,
        serde_json::json!({
            "values": embed_text("semantic latency checklist", 384),
        })
        .to_string(),
    )
    .unwrap();

    run_wax(&[
        "ingest",
        "docs",
        "--store",
        store_path.to_str().unwrap(),
        "--input",
        docs_jsonl.to_str().unwrap(),
    ]);
    run_wax(&[
        "ingest",
        "vectors",
        "--store",
        store_path.to_str().unwrap(),
        "--input",
        vectors_jsonl.to_str().unwrap(),
    ]);

    let search = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-cli",
            "--",
            "search",
            "--store",
            store_path.to_str().unwrap(),
            "--mode",
            "vector",
            "--vector-input",
            query_vector.to_str().unwrap(),
            "--top-k",
            "1",
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
    assert!(stdout.contains("\"doc_id\": \"vec-doc-2\""));
    assert!(stdout.contains("\"preview\": \"semantic latency checklist\""));

    let hybrid_search = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-cli",
            "--",
            "search",
            "--store",
            store_path.to_str().unwrap(),
            "--mode",
            "hybrid",
            "--text",
            "semantic latency",
            "--vector-input",
            query_vector.to_str().unwrap(),
            "--top-k",
            "1",
            "--preview",
        ])
        .output()
        .unwrap();

    assert!(
        hybrid_search.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&hybrid_search.stdout),
        String::from_utf8_lossy(&hybrid_search.stderr)
    );
    let stdout = String::from_utf8(hybrid_search.stdout).unwrap();
    assert!(stdout.contains("\"doc_id\": \"vec-doc-2\""));
    assert!(stdout.contains("\"preview\": \"semantic latency checklist\""));
}

#[test]
fn product_cli_docs_ingest_does_not_create_store_when_input_is_missing() {
    let store_dir = tempdir().unwrap();
    let store_path = store_dir.path().join("side-effect.wax");
    let missing_input = store_dir.path().join("missing-docs.jsonl");

    let output = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "-p",
            "wax-cli",
            "--",
            "ingest",
            "docs",
            "--store",
            store_path.to_str().unwrap(),
            "--input",
            missing_input.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        !store_path.exists(),
        "failed docs ingest should not create {}",
        store_path.display()
    );
}

#[test]
fn product_cli_search_rejects_ignored_mode_inputs() {
    let store_dir = tempdir().unwrap();
    let store_path = store_dir.path().join("projection-search-args.wax");
    let docs_jsonl = store_dir.path().join("projection-docs.jsonl");
    fs::write(
        &docs_jsonl,
        "{\"doc_id\":\"arg-doc-1\",\"text\":\"argument validation target\"}\n",
    )
    .unwrap();
    let query_vector = store_dir.path().join("query-vector.json");
    fs::write(
        &query_vector,
        serde_json::json!({
            "values": embed_text("argument validation target", 384),
        })
        .to_string(),
    )
    .unwrap();

    run_wax(&[
        "ingest",
        "docs",
        "--store",
        store_path.to_str().unwrap(),
        "--input",
        docs_jsonl.to_str().unwrap(),
    ]);

    let text_with_vector = wax_output(&[
        "search",
        "--store",
        store_path.to_str().unwrap(),
        "--mode",
        "text",
        "--text",
        "argument validation",
        "--vector-input",
        query_vector.to_str().unwrap(),
    ]);
    assert!(!text_with_vector.status.success());
    assert!(String::from_utf8_lossy(&text_with_vector.stderr)
        .contains("search --mode text does not accept --vector-input"));

    let vector_with_text = wax_output(&[
        "search",
        "--store",
        store_path.to_str().unwrap(),
        "--mode",
        "vector",
        "--text",
        "argument validation",
        "--vector-input",
        query_vector.to_str().unwrap(),
    ]);
    assert!(!vector_with_text.status.success());
    assert!(String::from_utf8_lossy(&vector_with_text.stderr)
        .contains("search --mode vector does not accept --text"));
}

#[test]
fn product_cli_query_vector_object_rejects_unknown_fields() {
    let store_dir = tempdir().unwrap();
    let store_path = store_dir.path().join("projection-strict-query.wax");
    let docs_jsonl = store_dir.path().join("projection-docs.jsonl");
    fs::write(
        &docs_jsonl,
        "{\"doc_id\":\"strict-doc-1\",\"text\":\"strict query target\"}\n",
    )
    .unwrap();
    let vectors_jsonl = store_dir.path().join("projection-vectors.jsonl");
    fs::write(
        &vectors_jsonl,
        format!(
            "{}\n",
            serde_json::json!({
                "doc_id": "strict-doc-1",
                "values": embed_text("strict query target", 384),
            }),
        ),
    )
    .unwrap();
    let ambiguous_query_vector = store_dir.path().join("ambiguous-query-vector.json");
    fs::write(
        &ambiguous_query_vector,
        serde_json::json!({
            "doc_id": "strict-doc-1",
            "values": embed_text("strict query target", 384),
        })
        .to_string(),
    )
    .unwrap();

    run_wax(&[
        "ingest",
        "docs",
        "--store",
        store_path.to_str().unwrap(),
        "--input",
        docs_jsonl.to_str().unwrap(),
    ]);
    run_wax(&[
        "ingest",
        "vectors",
        "--store",
        store_path.to_str().unwrap(),
        "--input",
        vectors_jsonl.to_str().unwrap(),
    ]);

    let output = wax_output(&[
        "search",
        "--store",
        store_path.to_str().unwrap(),
        "--mode",
        "vector",
        "--vector-input",
        ambiguous_query_vector.to_str().unwrap(),
    ]);
    assert!(!output.status.success());
}

fn run_wax(args: &[&str]) {
    let output = wax_output(args);
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn wax_output(args: &[&str]) -> std::process::Output {
    Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(["run", "-p", "wax-cli", "--"])
        .args(args)
        .output()
        .unwrap()
}
