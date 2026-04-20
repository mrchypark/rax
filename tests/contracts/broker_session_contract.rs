use std::fs;
use std::path::PathBuf;

use serde_json::json;
use tempfile::tempdir;
use wax_bench_packer::{pack_dataset, PackRequest};
use wax_v2_runtime::{NewDocument, RuntimeStore};

use wax_v2_broker::{SessionSearchRequest, WaxBroker};

#[test]
fn broker_session_reuses_open_store_across_multiple_text_searches() {
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

    let mut broker = WaxBroker::default();
    let session_id = broker.open_session(dataset_dir.path()).unwrap();

    let first = broker
        .search(
            session_id,
            SessionSearchRequest::text("rust benchmark")
                .with_top_k(2)
                .with_preview(true),
        )
        .unwrap();
    let second = broker
        .search(
            session_id,
            SessionSearchRequest::text("semantic latency").with_top_k(2),
        )
        .unwrap();

    assert_eq!(first.hits[0].doc_id, "doc-001");
    assert_eq!(
        first.hits[0].preview.as_deref(),
        Some("rust benchmark guide")
    );
    assert_eq!(second.hits[0].doc_id, "doc-002");

    broker.close_session(session_id).unwrap();
}

#[test]
fn broker_session_imports_compatibility_snapshot_then_searches_without_sidecars() {
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

    let mut broker = WaxBroker::default();
    let session_id = broker.open_session(dataset_dir.path()).unwrap();
    let report = broker.import_compatibility_snapshot(session_id).unwrap();
    assert_eq!(report.generation, 1);

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

    let response = broker
        .search(
            session_id,
            SessionSearchRequest::text("rust benchmark")
                .with_top_k(2)
                .with_preview(true),
        )
        .unwrap();
    assert_eq!(response.hits[0].doc_id, "doc-001");
    assert_eq!(
        response.hits[0].preview.as_deref(),
        Some("rust benchmark guide")
    );

    broker.close_session(session_id).unwrap();
}

#[test]
fn broker_session_searches_raw_prepared_store_without_sidecars() {
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

    let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
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

    let mut broker = WaxBroker::default();
    let session_id = broker.open_session(dataset_dir.path()).unwrap();
    let response = broker
        .search(
            session_id,
            SessionSearchRequest::text("rust benchmark")
                .with_top_k(2)
                .with_preview(true),
        )
        .unwrap();
    assert_eq!(response.hits[0].doc_id, "doc-001");
    assert_eq!(
        response.hits[0].preview.as_deref(),
        Some("rust benchmark guide")
    );

    broker.close_session(session_id).unwrap();
}
