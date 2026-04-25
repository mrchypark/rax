use std::fs;
use std::path::PathBuf;

use serde_json::json;
use tempfile::tempdir;
use wax_bench_packer::{pack_dataset, PackRequest};
use wax_v2_runtime::{NewDocument, RuntimeStore};

use wax_v2_mcp::{McpErrorCode, McpRequest, McpResponse, WaxMcpSurface};

#[test]
fn mcp_surface_opens_session_and_searches_text_through_tool_boundary() {
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

    let mut mcp = WaxMcpSurface::with_allowed_root(dataset_dir.path()).unwrap();
    let open = mcp
        .handle(McpRequest::OpenSession {
            root: dataset_dir.path().display().to_string(),
        })
        .unwrap();
    let session_id = match open {
        McpResponse::SessionOpened { session_id } => session_id,
        other => panic!("unexpected open response: {other:?}"),
    };

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

    let close = mcp.handle(McpRequest::CloseSession { session_id }).unwrap();
    assert!(matches!(
        close,
        McpResponse::SessionClosed { session_id: _ }
    ));
}

#[test]
fn mcp_surface_rejects_session_roots_outside_allowed_root() {
    let allowed_dir = tempdir().unwrap();
    let outside_dir = tempdir().unwrap();

    let mut mcp = WaxMcpSurface::with_allowed_root(allowed_dir.path()).unwrap();
    let error = mcp
        .handle(McpRequest::OpenSession {
            root: outside_dir.path().display().to_string(),
        })
        .unwrap_err();

    assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
    assert!(error.message().contains("outside allowed root"));
}

#[test]
fn mcp_surface_imports_compatibility_snapshot_then_searches_without_sidecars() {
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

    let mut mcp = WaxMcpSurface::with_allowed_root(dataset_dir.path()).unwrap();
    let open = mcp
        .handle(McpRequest::OpenSession {
            root: dataset_dir.path().display().to_string(),
        })
        .unwrap();
    let session_id = match open {
        McpResponse::SessionOpened { session_id } => session_id,
        other => panic!("unexpected open response: {other:?}"),
    };

    let import = mcp
        .handle(McpRequest::ImportCompatibilitySnapshot { session_id })
        .unwrap();
    match import {
        McpResponse::CompatibilitySnapshotImported {
            generation,
            published_families,
        } => {
            assert_eq!(generation, 1);
            assert_eq!(
                published_families,
                vec!["doc".to_owned(), "text".to_owned(), "vector".to_owned()]
            );
        }
        other => panic!("unexpected import response: {other:?}"),
    }

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

#[test]
fn mcp_surface_searches_raw_prepared_store_without_sidecars() {
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

    let mut mcp = WaxMcpSurface::with_allowed_root(dataset_dir.path()).unwrap();
    let open = mcp
        .handle(McpRequest::OpenSession {
            root: dataset_dir.path().display().to_string(),
        })
        .unwrap();
    let session_id = match open {
        McpResponse::SessionOpened { session_id } => session_id,
        other => panic!("unexpected open response: {other:?}"),
    };

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
