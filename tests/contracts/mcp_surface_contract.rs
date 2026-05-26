use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tempfile::tempdir;

use wax_v2_mcp::{McpErrorCode, McpNewDocument, McpRequest, McpResponse, WaxMcpSurface};

#[cfg(unix)]
fn make_private_dir(path: &std::path::Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions).unwrap();
}

#[cfg(not(unix))]
fn make_private_dir(_path: &std::path::Path) {}

#[test]
fn mcp_surface_opens_store_session_ingests_and_searches_text() {
    let root = tempdir().unwrap();
    make_private_dir(root.path());
    let store_path = root.path().join("projection.wax");

    let mut mcp = WaxMcpSurface::with_allowed_root_and_store_sessions(root.path()).unwrap();
    let open = mcp
        .handle(McpRequest::OpenStoreSession {
            store: store_path.display().to_string(),
        })
        .unwrap();
    let session_id = match open {
        McpResponse::SessionOpened { session_id } => session_id,
        other => panic!("unexpected open response: {other:?}"),
    };

    mcp.handle(McpRequest::IngestDocuments {
        session_id,
        documents: vec![
            McpNewDocument {
                doc_id: "doc-001".to_owned(),
                text: "rust benchmark guide".to_owned(),
                metadata: serde_json::json!({"kind":"guide"}),
                timestamp_ms: None,
                extra_fields: Default::default(),
            },
            McpNewDocument {
                doc_id: "doc-002".to_owned(),
                text: "semantic latency checklist".to_owned(),
                metadata: serde_json::json!({"kind":"checklist"}),
                timestamp_ms: None,
                extra_fields: Default::default(),
            },
        ],
    })
    .unwrap();

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
fn mcp_surface_rejects_store_session_outside_allowed_root() {
    let allowed_dir = tempdir().unwrap();
    let outside_dir = tempdir().unwrap();
    make_private_dir(allowed_dir.path());
    let outside_store = outside_dir.path().join("projection.wax");

    let mut mcp = WaxMcpSurface::with_allowed_root_and_store_sessions(allowed_dir.path()).unwrap();
    let error = mcp
        .handle(McpRequest::OpenStoreSession {
            store: outside_store.display().to_string(),
        })
        .unwrap_err();

    assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
    assert!(error
        .message()
        .contains("must be directly under allowed root"));
}

#[test]
fn mcp_surface_disables_raw_store_sessions_by_default() {
    let root = tempdir().unwrap();
    make_private_dir(root.path());
    let store_path = root.path().join("projection.wax");
    let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

    let error = surface
        .handle(McpRequest::OpenStoreSession {
            store: store_path.display().to_string(),
        })
        .unwrap_err();

    assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
    assert!(error.message().contains("raw store session requests"));
}
