use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tempfile::tempdir;
use wax_bench_model::embed_text;
use wax_bench_model::ManifestFile;
use wax_bench_packer::{pack_adhoc_dataset, AdhocPackRequest};
use wax_v2_docstore::Docstore;

use wax_v2_mcp::{McpRequest, McpResponse, WaxMcpSurface};

#[test]
fn mcp_surface_ingests_documents_and_vectors_through_explicit_raw_requests() {
    let store_dir = tempdir().unwrap();
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(store_dir.path()).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(store_dir.path(), permissions).unwrap();
    }
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
    let mut mcp = WaxMcpSurface::with_allowed_root_and_store_sessions(store_dir.path()).unwrap();
    let open = mcp
        .handle(McpRequest::OpenStoreSession {
            store: store_path.display().to_string(),
        })
        .unwrap();
    let session_id = match open {
        McpResponse::SessionOpened { session_id } => session_id,
        other => panic!("unexpected open response: {other:?}"),
    };

    let doc_ingest = mcp
        .handle(McpRequest::IngestDocuments {
            session_id,
            documents: vec![
                wax_v2_mcp::McpNewDocument {
                    doc_id: "doc-001".to_owned(),
                    text: "rust benchmark guide".to_owned(),
                    metadata: serde_json::json!({"kind":"guide","workspace":"prod"}),
                    timestamp_ms: None,
                    extra_fields: [("priority".to_owned(), serde_json::json!("p0"))]
                        .into_iter()
                        .collect(),
                },
                wax_v2_mcp::McpNewDocument {
                    doc_id: "doc-002".to_owned(),
                    text: "semantic latency checklist".to_owned(),
                    metadata: serde_json::json!({"kind":"checklist","workspace":"prod"}),
                    timestamp_ms: None,
                    extra_fields: Default::default(),
                },
                wax_v2_mcp::McpNewDocument {
                    doc_id: "doc-003".to_owned(),
                    text: "hybrid search tuning notes".to_owned(),
                    metadata: serde_json::json!({"kind":"notes","workspace":"prod"}),
                    timestamp_ms: None,
                    extra_fields: Default::default(),
                },
            ],
        })
        .unwrap();
    match doc_ingest {
        McpResponse::RawIngested {
            generation,
            published_families,
        } => {
            assert_eq!(generation, 1);
            assert_eq!(
                published_families,
                vec!["doc".to_owned(), "text".to_owned()]
            );
        }
        other => panic!("unexpected doc ingest response: {other:?}"),
    }
    manifest.files = vec![ManifestFile {
        path: "projection.wax".to_owned(),
        kind: "store".to_owned(),
        format: "wax".to_owned(),
        record_count: 3,
        checksum: "runtime".to_owned(),
    }];
    let docstore =
        Docstore::open_with_store_path(store_dir.path(), &manifest, &store_path).unwrap();
    let documents = docstore
        .load_documents_by_id(&["doc-001".to_owned()])
        .unwrap();
    assert_eq!(
        documents
            .get("doc-001")
            .and_then(|document| document.get("priority")),
        Some(&serde_json::json!("p0"))
    );

    let vector_ingest = mcp
        .handle(McpRequest::IngestVectors {
            session_id,
            vectors: vec![
                wax_v2_mcp::McpNewDocumentVector {
                    doc_id: "doc-001".to_owned(),
                    values: embed_text("rust benchmark guide", 384),
                },
                wax_v2_mcp::McpNewDocumentVector {
                    doc_id: "doc-002".to_owned(),
                    values: embed_text("semantic latency checklist", 384),
                },
                wax_v2_mcp::McpNewDocumentVector {
                    doc_id: "doc-003".to_owned(),
                    values: embed_text("hybrid search tuning notes", 384),
                },
            ],
        })
        .unwrap();
    match vector_ingest {
        McpResponse::RawIngested {
            generation,
            published_families,
        } => {
            assert_eq!(generation, 2);
            assert_eq!(published_families, vec!["vector".to_owned()]);
        }
        other => panic!("unexpected vector ingest response: {other:?}"),
    }

    let search = mcp
        .handle(McpRequest::SearchText {
            session_id,
            query: "rust benchmark".to_owned(),
            top_k: 2,
            include_preview: true,
        })
        .unwrap();
    match search {
        McpResponse::SearchResults { hits } => {
            assert_eq!(hits[0].doc_id, "doc-001");
            assert_eq!(hits[0].preview.as_deref(), Some("rust benchmark guide"));
        }
        other => panic!("unexpected search response: {other:?}"),
    }
}
