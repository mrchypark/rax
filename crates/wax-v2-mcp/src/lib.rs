use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use wax_v2_broker::{
    SessionNewDocument, SessionNewDocumentVector, SessionSearchRequest, WaxBroker,
};
use wax_v2_runtime::{Memory, MemorySearchOptions, RuntimeSearchMode};

pub const MAX_MCP_SEARCH_TOP_K: usize = 100;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpNewDocument {
    pub doc_id: String,
    pub text: String,
    #[serde(default = "empty_metadata_object")]
    pub metadata: serde_json::Value,
    pub timestamp_ms: Option<u64>,
    #[serde(default, flatten)]
    pub extra_fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpNewDocumentVector {
    pub doc_id: String,
    pub values: Vec<f32>,
}

fn empty_metadata_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "tool", rename_all = "snake_case")]
pub enum McpRequest {
    Remember {
        store: String,
        content: String,
        #[serde(default = "empty_metadata_object")]
        metadata: serde_json::Value,
    },
    Recall {
        store: String,
        query: String,
        top_k: usize,
        include_preview: bool,
    },
    Search {
        store: String,
        query: String,
        #[serde(default = "default_search_mode")]
        mode: String,
        top_k: usize,
        include_preview: bool,
    },
    OpenSession {
        root: String,
    },
    SearchText {
        session_id: u64,
        query: String,
        top_k: usize,
        include_preview: bool,
    },
    ImportCompatibilitySnapshot {
        session_id: u64,
    },
    IngestDocuments {
        session_id: u64,
        documents: Vec<McpNewDocument>,
    },
    IngestVectors {
        session_id: u64,
        vectors: Vec<McpNewDocumentVector>,
    },
    CloseSession {
        session_id: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum McpResponse {
    Remembered {
        doc_id: String,
    },
    SessionOpened {
        session_id: u64,
    },
    SearchResults {
        hits: Vec<McpSearchHit>,
    },
    CompatibilitySnapshotImported {
        generation: u64,
        published_families: Vec<String>,
    },
    RawIngested {
        generation: u64,
        published_families: Vec<String>,
    },
    SessionClosed {
        session_id: u64,
    },
}

fn default_search_mode() -> String {
    "hybrid".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpSearchHit {
    pub doc_id: String,
    pub preview: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpErrorCode {
    InvalidRequest,
    Storage,
    SessionNotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpError {
    code: McpErrorCode,
    message: String,
}

impl McpError {
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: McpErrorCode::InvalidRequest,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &McpErrorCode {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for McpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

pub struct WaxMcpSurface {
    broker: WaxBroker,
    allowed_root: Option<PathBuf>,
    allow_raw_sessions: bool,
}

impl Default for WaxMcpSurface {
    fn default() -> Self {
        Self {
            broker: WaxBroker::default(),
            allowed_root: default_allowed_root(),
            allow_raw_sessions: false,
        }
    }
}

impl WaxMcpSurface {
    /// Creates the untrusted MCP product-memory surface.
    ///
    /// Store paths are intentionally limited to a direct child file of this root. The caller must
    /// provide a trusted, stable root directory; the MCP layer rejects leaf symlinks and core opens
    /// use no-follow for the final store file, but this API is not a sandbox for attacker-controlled
    /// replacement of the allowed-root directory itself.
    pub fn with_allowed_root(root: &Path) -> Result<Self, McpError> {
        let allowed_root = root.canonicalize().map_err(|error| McpError {
            code: McpErrorCode::InvalidRequest,
            message: error.to_string(),
        })?;
        Ok(Self {
            broker: WaxBroker::default(),
            allowed_root: Some(allowed_root),
            allow_raw_sessions: false,
        })
    }

    /// Enables the legacy raw dataset-session request variants for trusted in-process callers.
    ///
    /// The stdio server does not expose these variants as tools. They are kept for benchmark
    /// contract tests and internal integration surfaces that already trust the dataset root.
    pub fn with_allowed_root_and_raw_sessions(root: &Path) -> Result<Self, McpError> {
        let mut surface = Self::with_allowed_root(root)?;
        surface.allow_raw_sessions = true;
        Ok(surface)
    }

    pub fn handle(&mut self, request: McpRequest) -> Result<McpResponse, McpError> {
        match request {
            McpRequest::Remember {
                store,
                content,
                metadata,
            } => {
                let store = self.authorized_path(&store)?;
                let mut memory = Memory::open(&store).map_err(runtime_error)?;
                let doc_id = memory.save(content, metadata).map_err(runtime_error)?;
                memory.close().map_err(runtime_error)?;
                Ok(McpResponse::Remembered { doc_id })
            }
            McpRequest::Recall {
                store,
                query,
                top_k,
                include_preview,
            } => {
                validate_top_k(top_k)?;
                let store = self.authorized_path(&store)?;
                let mut memory = Memory::open_existing(&store).map_err(runtime_error)?;
                let response = memory
                    .search_with_options(
                        query,
                        MemorySearchOptions {
                            mode: RuntimeSearchMode::Hybrid,
                            top_k,
                            include_preview,
                        },
                    )
                    .map_err(runtime_error)?;
                memory.close().map_err(runtime_error)?;
                Ok(McpResponse::SearchResults {
                    hits: map_runtime_hits(response.hits),
                })
            }
            McpRequest::Search {
                store,
                query,
                mode,
                top_k,
                include_preview,
            } => {
                validate_top_k(top_k)?;
                let store = self.authorized_path(&store)?;
                let mode = parse_search_mode(&mode)?;
                let mut memory = Memory::open_existing(&store).map_err(runtime_error)?;
                let response = memory
                    .search_with_options(
                        query,
                        MemorySearchOptions {
                            mode,
                            top_k,
                            include_preview,
                        },
                    )
                    .map_err(runtime_error)?;
                memory.close().map_err(runtime_error)?;
                Ok(McpResponse::SearchResults {
                    hits: map_runtime_hits(response.hits),
                })
            }
            McpRequest::OpenSession { root } => {
                self.require_raw_sessions()?;
                let root = self.authorized_root(&root)?;
                let session_id = self.broker.open_session(&root).map_err(broker_error)?;
                Ok(McpResponse::SessionOpened {
                    session_id: session_id.as_u64(),
                })
            }
            McpRequest::SearchText {
                session_id,
                query,
                top_k,
                include_preview,
            } => {
                self.require_raw_sessions()?;
                validate_top_k(top_k)?;
                let response = self
                    .broker
                    .search(
                        wax_v2_broker::SessionId::from_u64(session_id),
                        SessionSearchRequest::text(query)
                            .with_top_k(top_k)
                            .with_preview(include_preview),
                    )
                    .map_err(broker_error)?;
                Ok(McpResponse::SearchResults {
                    hits: map_runtime_hits(response.hits),
                })
            }
            McpRequest::ImportCompatibilitySnapshot { session_id } => {
                self.require_raw_sessions()?;
                let report = self
                    .broker
                    .import_compatibility_snapshot(wax_v2_broker::SessionId::from_u64(session_id))
                    .map_err(broker_error)?;
                Ok(McpResponse::CompatibilitySnapshotImported {
                    generation: report.generation,
                    published_families: report
                        .published_families
                        .into_iter()
                        .map(|family| match family {
                            wax_v2_broker::BrokerPublishFamily::Doc => "doc".to_owned(),
                            wax_v2_broker::BrokerPublishFamily::Text => "text".to_owned(),
                            wax_v2_broker::BrokerPublishFamily::Vector => "vector".to_owned(),
                        })
                        .collect(),
                })
            }
            McpRequest::IngestDocuments {
                session_id,
                documents,
            } => {
                self.require_raw_sessions()?;
                let report = self
                    .broker
                    .ingest_documents(
                        wax_v2_broker::SessionId::from_u64(session_id),
                        documents
                            .into_iter()
                            .map(|document| SessionNewDocument {
                                doc_id: document.doc_id,
                                text: document.text,
                                metadata: document.metadata,
                                timestamp_ms: document.timestamp_ms,
                                extra_fields: document.extra_fields,
                            })
                            .collect(),
                    )
                    .map_err(broker_error)?;
                Ok(McpResponse::RawIngested {
                    generation: report.generation,
                    published_families: report
                        .published_families
                        .into_iter()
                        .map(|family| match family {
                            wax_v2_broker::BrokerPublishFamily::Doc => "doc".to_owned(),
                            wax_v2_broker::BrokerPublishFamily::Text => "text".to_owned(),
                            wax_v2_broker::BrokerPublishFamily::Vector => "vector".to_owned(),
                        })
                        .collect(),
                })
            }
            McpRequest::IngestVectors {
                session_id,
                vectors,
            } => {
                self.require_raw_sessions()?;
                let report = self
                    .broker
                    .ingest_vectors(
                        wax_v2_broker::SessionId::from_u64(session_id),
                        vectors
                            .into_iter()
                            .map(|vector| SessionNewDocumentVector {
                                doc_id: vector.doc_id,
                                values: vector.values,
                            })
                            .collect(),
                    )
                    .map_err(broker_error)?;
                Ok(McpResponse::RawIngested {
                    generation: report.generation,
                    published_families: report
                        .published_families
                        .into_iter()
                        .map(|family| match family {
                            wax_v2_broker::BrokerPublishFamily::Doc => "doc".to_owned(),
                            wax_v2_broker::BrokerPublishFamily::Text => "text".to_owned(),
                            wax_v2_broker::BrokerPublishFamily::Vector => "vector".to_owned(),
                        })
                        .collect(),
                })
            }
            McpRequest::CloseSession { session_id } => {
                self.require_raw_sessions()?;
                self.broker
                    .close_session(wax_v2_broker::SessionId::from_u64(session_id))
                    .map_err(broker_error)?;
                Ok(McpResponse::SessionClosed { session_id })
            }
        }
    }

    fn require_raw_sessions(&self) -> Result<(), McpError> {
        if self.allow_raw_sessions {
            return Ok(());
        }
        Err(McpError {
            code: McpErrorCode::InvalidRequest,
            message: "raw dataset session requests are disabled on the untrusted MCP surface"
                .to_owned(),
        })
    }

    fn authorized_root(&self, root: &str) -> Result<PathBuf, McpError> {
        let root = Path::new(root).canonicalize().map_err(|error| McpError {
            code: McpErrorCode::InvalidRequest,
            message: error.to_string(),
        })?;
        let Some(allowed_root) = &self.allowed_root else {
            return Err(McpError {
                code: McpErrorCode::InvalidRequest,
                message: "MCP surface has no allowed root".to_owned(),
            });
        };
        if !root.starts_with(allowed_root) {
            return Err(McpError {
                code: McpErrorCode::InvalidRequest,
                message: format!(
                    "session root {} is outside allowed root {}",
                    root.display(),
                    allowed_root.display()
                ),
            });
        }
        Ok(root)
    }

    fn authorized_path(&self, path: &str) -> Result<PathBuf, McpError> {
        let path = Path::new(path);
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let parent = parent.canonicalize().map_err(|error| McpError {
            code: McpErrorCode::InvalidRequest,
            message: error.to_string(),
        })?;
        let Some(allowed_root) = &self.allowed_root else {
            return Err(McpError {
                code: McpErrorCode::InvalidRequest,
                message: "MCP surface has no allowed root".to_owned(),
            });
        };
        if parent != *allowed_root {
            return Err(McpError {
                code: McpErrorCode::InvalidRequest,
                message: format!(
                    "store path {} must be directly under allowed root {}",
                    path.display(),
                    allowed_root.display()
                ),
            });
        }
        let candidate = parent.join(path.file_name().ok_or_else(|| McpError {
            code: McpErrorCode::InvalidRequest,
            message: "store path must include a file name".to_owned(),
        })?);
        if candidate.exists() {
            let metadata = fs::symlink_metadata(&candidate).map_err(|error| McpError {
                code: McpErrorCode::InvalidRequest,
                message: error.to_string(),
            })?;
            if metadata.file_type().is_symlink() {
                return Err(McpError {
                    code: McpErrorCode::InvalidRequest,
                    message: format!("store path {} must not be a symlink", candidate.display()),
                });
            }
            let canonical = candidate.canonicalize().map_err(|error| McpError {
                code: McpErrorCode::InvalidRequest,
                message: error.to_string(),
            })?;
            if !canonical.starts_with(allowed_root) {
                return Err(McpError {
                    code: McpErrorCode::InvalidRequest,
                    message: format!(
                        "store path {} is outside allowed root {}",
                        canonical.display(),
                        allowed_root.display()
                    ),
                });
            }
            return Ok(canonical);
        }
        Ok(candidate)
    }
}

fn default_allowed_root() -> Option<PathBuf> {
    std::env::current_dir()
        .ok()
        .and_then(|path| path.canonicalize().ok())
}

fn broker_error(error: wax_v2_broker::BrokerError) -> McpError {
    match error {
        wax_v2_broker::BrokerError::InvalidRequest(message) => McpError {
            code: McpErrorCode::InvalidRequest,
            message,
        },
        wax_v2_broker::BrokerError::Storage(message) => McpError {
            code: McpErrorCode::Storage,
            message,
        },
        wax_v2_broker::BrokerError::SessionNotFound(session_id) => McpError {
            code: McpErrorCode::SessionNotFound,
            message: format!("session {} is not open", session_id.as_u64()),
        },
        wax_v2_broker::BrokerError::SessionLimitExceeded { max_sessions } => McpError {
            code: McpErrorCode::InvalidRequest,
            message: format!("broker session limit exceeded: {max_sessions}"),
        },
    }
}

fn runtime_error(error: wax_v2_runtime::RuntimeError) -> McpError {
    match error {
        wax_v2_runtime::RuntimeError::InvalidRequest(message) => McpError {
            code: McpErrorCode::InvalidRequest,
            message,
        },
        wax_v2_runtime::RuntimeError::Storage(message) => McpError {
            code: McpErrorCode::Storage,
            message,
        },
    }
}

fn parse_search_mode(mode: &str) -> Result<RuntimeSearchMode, McpError> {
    match mode {
        "text" => Ok(RuntimeSearchMode::Text),
        "vector" => Ok(RuntimeSearchMode::Vector),
        "hybrid" => Ok(RuntimeSearchMode::Hybrid),
        other => Err(McpError {
            code: McpErrorCode::InvalidRequest,
            message: format!("unsupported search mode: {other}"),
        }),
    }
}

fn validate_top_k(top_k: usize) -> Result<(), McpError> {
    if top_k > MAX_MCP_SEARCH_TOP_K {
        return Err(McpError {
            code: McpErrorCode::InvalidRequest,
            message: format!("top_k must be <= {MAX_MCP_SEARCH_TOP_K}"),
        });
    }
    Ok(())
}

fn map_runtime_hits(hits: Vec<wax_v2_runtime::RuntimeSearchHit>) -> Vec<McpSearchHit> {
    hits.into_iter()
        .map(|hit| McpSearchHit {
            doc_id: hit.doc_id,
            preview: hit.preview,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        McpError, McpErrorCode, McpNewDocument, McpRequest, McpResponse, WaxMcpSurface,
        MAX_MCP_SEARCH_TOP_K,
    };
    use tempfile::tempdir;
    use wax_v2_broker::WaxBroker;

    #[test]
    fn mcp_request_round_trips_as_transport_ready_json() {
        let request = McpRequest::SearchText {
            session_id: 7,
            query: "rust benchmark".to_owned(),
            top_k: 2,
            include_preview: true,
        };

        let encoded = serde_json::to_string(&request).unwrap();
        let decoded: McpRequest = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn mcp_response_round_trips_as_transport_ready_json() {
        let response = McpResponse::CompatibilitySnapshotImported {
            generation: 3,
            published_families: vec!["doc".to_owned(), "text".to_owned(), "vector".to_owned()],
        };

        let encoded = serde_json::to_string(&response).unwrap();
        let decoded: McpResponse = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn mcp_error_round_trips_as_transport_ready_json() {
        let error = McpError {
            code: McpErrorCode::InvalidRequest,
            message: "duplicate doc_ids".to_owned(),
        };

        let encoded = serde_json::to_string(&error).unwrap();
        let decoded: McpError = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, error);
    }

    #[test]
    fn mcp_new_document_defaults_missing_metadata_to_empty_object() {
        let document: McpNewDocument =
            serde_json::from_str(r#"{"doc_id":"doc-001","text":"hello"}"#).unwrap();

        assert_eq!(document.metadata, serde_json::json!({}));
        assert!(document.extra_fields.is_empty());
    }

    #[test]
    fn mcp_surface_without_allowed_root_rejects_open_session() {
        let mut surface = WaxMcpSurface {
            broker: WaxBroker::default(),
            allowed_root: None,
            allow_raw_sessions: true,
        };

        let error = surface
            .handle(McpRequest::OpenSession {
                root: std::env::current_dir()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert_eq!(error.message(), "MCP surface has no allowed root");
    }

    #[test]
    fn mcp_surface_disables_raw_sessions_by_default() {
        let root = tempdir().unwrap();
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let error = surface
            .handle(McpRequest::OpenSession {
                root: root.path().to_string_lossy().into_owned(),
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("raw dataset session requests"));
    }

    #[test]
    fn mcp_surface_remembers_and_recalls_wax_style_store_file() {
        let root = tempdir().unwrap();
        let store = root.path().join("agent.wax");
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let remembered = surface
            .handle(McpRequest::Remember {
                store: store.to_string_lossy().into_owned(),
                content: "The user is building a habit tracker in Rust".to_owned(),
                metadata: serde_json::json!({"source":"test"}),
            })
            .unwrap();
        assert_eq!(
            remembered,
            McpResponse::Remembered {
                doc_id: "mem-0000000000000001".to_owned()
            }
        );

        let recalled = surface
            .handle(McpRequest::Recall {
                store: store.to_string_lossy().into_owned(),
                query: "What is the user building?".to_owned(),
                top_k: 3,
                include_preview: true,
            })
            .unwrap();

        let McpResponse::SearchResults { hits } = recalled else {
            panic!("expected search results");
        };
        assert_eq!(hits[0].doc_id, "mem-0000000000000001");
        assert_eq!(
            hits[0].preview.as_deref(),
            Some("The user is building a habit tracker in Rust")
        );
    }

    #[test]
    fn mcp_surface_recall_does_not_create_missing_store() {
        let root = tempdir().unwrap();
        let store = root.path().join("missing.wax");
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let error = surface
            .handle(McpRequest::Recall {
                store: store.to_string_lossy().into_owned(),
                query: "anything".to_owned(),
                top_k: 1,
                include_preview: false,
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(!store.exists());
    }

    #[test]
    fn mcp_surface_rejects_unbounded_top_k() {
        let root = tempdir().unwrap();
        let store = root.path().join("agent.wax");
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();
        surface
            .handle(McpRequest::Remember {
                store: store.to_string_lossy().into_owned(),
                content: "bounded search".to_owned(),
                metadata: serde_json::json!({}),
            })
            .unwrap();

        let error = surface
            .handle(McpRequest::Search {
                store: store.to_string_lossy().into_owned(),
                query: "search".to_owned(),
                mode: "hybrid".to_owned(),
                top_k: MAX_MCP_SEARCH_TOP_K + 1,
                include_preview: false,
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("top_k must be <="));
    }

    #[test]
    fn mcp_surface_rejects_nested_store_paths_under_allowed_root() {
        let root = tempdir().unwrap();
        let nested = root.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        let store = nested.join("agent.wax");
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let error = surface
            .handle(McpRequest::Remember {
                store: store.to_string_lossy().into_owned(),
                content: "nested".to_owned(),
                metadata: serde_json::json!({}),
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("directly under allowed root"));
    }

    #[cfg(unix)]
    #[test]
    fn mcp_surface_rejects_store_file_symlink_under_allowed_root() {
        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let external_store = outside.path().join("external.wax");
        std::fs::write(&external_store, b"outside store").unwrap();
        let link_store = root.path().join("link.wax");
        std::os::unix::fs::symlink(&external_store, &link_store).unwrap();
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let error = surface
            .handle(McpRequest::Recall {
                store: link_store.to_string_lossy().into_owned(),
                query: "anything".to_owned(),
                top_k: 1,
                include_preview: false,
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("must not be a symlink"));
    }
}
