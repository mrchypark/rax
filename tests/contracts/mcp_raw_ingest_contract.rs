use std::fs;
use std::path::PathBuf;

use tempfile::tempdir;
use wax_bench_model::embed_text;
use wax_bench_packer::{pack_dataset, PackRequest};
use wax_v2_docstore::Docstore;
use wax_v2_runtime::RuntimeStore;

use wax_v2_mcp::{McpRequest, McpResponse, WaxMcpSurface};

#[test]
fn mcp_surface_ingests_documents_and_vectors_through_explicit_raw_requests() {
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
    runtime.close().unwrap();

    let mut mcp = WaxMcpSurface::with_allowed_root_and_raw_sessions(dataset_dir.path()).unwrap();
    let open = mcp
        .handle(McpRequest::OpenSession {
            root: dataset_dir.path().display().to_string(),
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
    let docstore = Docstore::open(dataset_dir.path(), &manifest).unwrap();
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
