use std::fs;

use serde_json::json;
use tempfile::tempdir;
use wax_bench_packer::{pack_adhoc_dataset, AdhocPackRequest};
use wax_v2_runtime::{NewDocument, RuntimeSearchMode, RuntimeSearchRequest, RuntimeStore};

#[test]
fn raw_document_publish_supports_reopen_search_without_dataset_sidecars() {
    let source_dir = tempdir().unwrap();
    let dataset_dir = tempdir().unwrap();
    let docs_path = source_dir.path().join("docs.ndjson");
    fs::write(&docs_path, "{\"doc_id\":\"seed-001\",\"text\":\"seed\"}\n").unwrap();
    let manifest = pack_adhoc_dataset(&AdhocPackRequest::new(
        &docs_path,
        dataset_dir.path(),
        "small",
    ))
    .unwrap();

    let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();

    for kind in ["documents", "document_offsets", "text_postings"] {
        for file in manifest.files.iter().filter(|file| file.kind == kind) {
            fs::remove_file(dataset_dir.path().join(&file.path)).unwrap();
        }
    }

    let report = runtime
        .writer()
        .unwrap()
        .publish_raw_documents(vec![
            NewDocument::new("doc-001", "rust benchmark guide")
                .with_metadata(json!({"kind":"guide","workspace":"prod"})),
            NewDocument::new("doc-002", "semantic latency checklist")
                .with_metadata(json!({"kind":"checklist","workspace":"prod"})),
        ])
        .unwrap();
    assert_eq!(report.generation, 1);
    assert_eq!(
        report.published_families,
        vec![
            wax_v2_runtime::RuntimePublishFamily::Doc,
            wax_v2_runtime::RuntimePublishFamily::Text,
        ]
    );
    runtime.close().unwrap();

    let mut reopened = RuntimeStore::open(dataset_dir.path()).unwrap();
    let response = reopened
        .search(RuntimeSearchRequest {
            mode: RuntimeSearchMode::Text,
            text_query: Some("rust benchmark".to_owned()),
            vector_query: None,
            top_k: 2,
            include_preview: true,
        })
        .unwrap();
    assert_eq!(response.hits[0].doc_id, "doc-001");
    assert_eq!(
        response.hits[0].preview.as_deref(),
        Some("rust benchmark guide")
    );
}
