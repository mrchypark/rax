use std::fs;

use serde_json::json;
use tempfile::tempdir;
use wax_bench_model::embed_text;
use wax_bench_packer::{pack_adhoc_dataset, AdhocPackRequest};
use wax_v2_runtime::{
    NewDocument, RuntimePublishFamily, RuntimeSearchMode, RuntimeSearchRequest, RuntimeStore,
};

#[test]
fn runtime_store_preserves_text_and_serves_vector_search_when_raw_vectors_are_published_after_raw_documents(
) {
    let source_dir = tempdir().unwrap();
    let dataset_dir = tempdir().unwrap();
    let docs_path = source_dir.path().join("docs.ndjson");
    fs::write(
        &docs_path,
        "{\"doc_id\":\"seed-001\",\"text\":\"seed\"}\n",
    )
    .unwrap();
    let manifest = pack_adhoc_dataset(&AdhocPackRequest::new(
        &docs_path,
        dataset_dir.path(),
        "small",
    ))
    .unwrap();

    let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
    let raw_doc_report = runtime
        .writer()
        .unwrap()
        .publish_raw_documents(vec![
            NewDocument::new("doc-001", "rust benchmark guide")
                .with_metadata(json!({"kind":"guide","workspace":"prod"})),
            NewDocument::new("doc-002", "semantic latency checklist")
                .with_metadata(json!({"kind":"checklist","workspace":"prod"})),
        ])
        .unwrap();
    assert_eq!(raw_doc_report.generation, 1);
    assert_eq!(
        raw_doc_report.published_families,
        vec![RuntimePublishFamily::Doc, RuntimePublishFamily::Text]
    );

    let raw_vector_report = runtime
        .writer()
        .unwrap()
        .publish_raw_vectors(vec![
            wax_v2_runtime::NewDocumentVector::new(
                "doc-001",
                embed_text("rust benchmark guide", 384),
            ),
            wax_v2_runtime::NewDocumentVector::new(
                "doc-002",
                embed_text("semantic latency checklist", 384),
            ),
        ])
        .unwrap();
    assert_eq!(raw_vector_report.generation, 2);
    assert_eq!(raw_vector_report.published_families, vec![RuntimePublishFamily::Vector]);
    runtime.close().unwrap();

    for kind in [
        "documents",
        "document_offsets",
        "text_postings",
        "document_ids",
        "document_vectors",
        "document_vectors_preview_q8",
        "query_vectors",
        "vector_lane_skeleton",
        "vector_hnsw_graph",
        "vector_hnsw_data",
    ] {
        for file in manifest.files.iter().filter(|file| file.kind == kind) {
            fs::remove_file(dataset_dir.path().join(&file.path)).unwrap();
        }
    }

    let mut reopened = RuntimeStore::open(dataset_dir.path()).unwrap();
    let text_response = reopened
        .search(RuntimeSearchRequest {
            mode: RuntimeSearchMode::Text,
            text_query: Some("rust benchmark".to_owned()),
            vector_query: None,
            top_k: 2,
            include_preview: true,
        })
        .unwrap();
    assert_eq!(text_response.hits[0].doc_id, "doc-001");
    assert_eq!(
        text_response.hits[0].preview.as_deref(),
        Some("rust benchmark guide")
    );

    let vector_response = reopened
        .search(RuntimeSearchRequest {
            mode: RuntimeSearchMode::Vector,
            text_query: None,
            vector_query: Some(embed_text("semantic latency checklist", 384)),
            top_k: 2,
            include_preview: false,
        })
        .unwrap();
    assert_eq!(vector_response.hits[0].doc_id, "doc-002");
}
