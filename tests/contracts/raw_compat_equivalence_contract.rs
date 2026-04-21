use std::fs;

use serde_json::json;
use tempfile::tempdir;
use wax_bench_model::embed_text;
use wax_bench_packer::{pack_adhoc_dataset, AdhocPackRequest};
use wax_v2_docstore::Docstore;
use wax_v2_runtime::{
    NewDocument, NewDocumentVector, RuntimeSearchMode, RuntimeSearchRequest, RuntimeStore,
};

#[test]
fn raw_and_compatibility_ingest_produce_equivalent_runtime_results_for_the_same_corpus() {
    let source_dir = tempdir().unwrap();
    let compat_root = tempdir().unwrap();
    let raw_root = tempdir().unwrap();
    let docs_path = source_dir.path().join("docs.ndjson");
    let docs = vec![
        (
            "doc-001",
            "rust benchmark guide",
            json!({"kind":"guide","workspace":"prod"}),
        ),
        (
            "doc-002",
            "semantic latency checklist",
            json!({"kind":"checklist","workspace":"prod"}),
        ),
        (
            "doc-003",
            "hybrid search tuning notes",
            json!({"kind":"notes","workspace":"prod"}),
        ),
    ];
    fs::write(
        &docs_path,
        docs.iter()
            .map(|(doc_id, text, metadata)| {
                serde_json::json!({
                    "doc_id": doc_id,
                    "text": text,
                    "metadata": metadata,
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();

    let compat_manifest = pack_adhoc_dataset(&AdhocPackRequest::new(
        &docs_path,
        compat_root.path(),
        "small",
    ))
    .unwrap();
    let raw_manifest =
        pack_adhoc_dataset(&AdhocPackRequest::new(&docs_path, raw_root.path(), "small")).unwrap();

    let mut compat_runtime = RuntimeStore::create(compat_root.path()).unwrap();
    compat_runtime
        .writer()
        .unwrap()
        .import_compatibility_snapshot()
        .unwrap();
    compat_runtime.close().unwrap();

    let mut raw_runtime = RuntimeStore::create(raw_root.path()).unwrap();
    raw_runtime
        .writer()
        .unwrap()
        .publish_raw_documents(vec![
            NewDocument::new("doc-001", "rust benchmark guide")
                .with_metadata(json!({"kind":"guide","workspace":"prod"})),
            NewDocument::new("doc-002", "semantic latency checklist")
                .with_metadata(json!({"kind":"checklist","workspace":"prod"})),
            NewDocument::new("doc-003", "hybrid search tuning notes")
                .with_metadata(json!({"kind":"notes","workspace":"prod"})),
        ])
        .unwrap();
    raw_runtime
        .writer()
        .unwrap()
        .publish_raw_vectors(vec![
            NewDocumentVector::new("doc-001", embed_text("rust benchmark guide", 384)),
            NewDocumentVector::new("doc-002", embed_text("semantic latency checklist", 384)),
            NewDocumentVector::new("doc-003", embed_text("hybrid search tuning notes", 384)),
        ])
        .unwrap();
    raw_runtime.close().unwrap();

    for root in [compat_root.path(), raw_root.path()] {
        remove_sidecars(
            root,
            if root == compat_root.path() {
                &compat_manifest
            } else {
                &raw_manifest
            },
        );
    }

    let compat_docstore = Docstore::open(compat_root.path(), &compat_manifest).unwrap();
    let raw_docstore = Docstore::open(raw_root.path(), &raw_manifest).unwrap();
    assert_eq!(
        compat_docstore.build_doc_id_map().unwrap(),
        raw_docstore.build_doc_id_map().unwrap()
    );

    let mut compat_runtime = RuntimeStore::open(compat_root.path()).unwrap();
    let mut raw_runtime = RuntimeStore::open(raw_root.path()).unwrap();

    let text_request = RuntimeSearchRequest {
        mode: RuntimeSearchMode::Text,
        text_query: Some("rust benchmark".to_owned()),
        vector_query: None,
        top_k: 3,
        include_preview: true,
    };
    let vector_request = RuntimeSearchRequest {
        mode: RuntimeSearchMode::Vector,
        text_query: None,
        vector_query: Some(embed_text("semantic latency checklist", 384)),
        top_k: 3,
        include_preview: true,
    };
    let hybrid_request = RuntimeSearchRequest {
        mode: RuntimeSearchMode::Hybrid,
        text_query: Some("hybrid search".to_owned()),
        vector_query: Some(embed_text("hybrid search tuning notes", 384)),
        top_k: 3,
        include_preview: true,
    };

    assert_eq!(
        compat_runtime.search(text_request.clone()).unwrap(),
        raw_runtime.search(text_request).unwrap()
    );
    assert_eq!(
        compat_runtime.search(vector_request.clone()).unwrap(),
        raw_runtime.search(vector_request).unwrap()
    );
    assert_eq!(
        compat_runtime.search(hybrid_request.clone()).unwrap(),
        raw_runtime.search(hybrid_request).unwrap()
    );
}

fn remove_sidecars(root: &std::path::Path, manifest: &wax_bench_model::DatasetPackManifest) {
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
            let path = root.join(&file.path);
            if path.exists() {
                fs::remove_file(path).unwrap();
            }
        }
    }
}
