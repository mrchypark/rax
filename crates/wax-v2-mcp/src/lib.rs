#[cfg(not(unix))]
use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use fs2::FileExt;
use std::ffi::{OsStr, OsString};
use std::fs::File;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
use std::path::Component;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use wax_v2_broker::{
    SessionNewDocument, SessionNewDocumentVector, SessionSearchRequest, WaxBroker,
};
use wax_v2_runtime::{Memory, MemorySearchOptions, RuntimeSearchMode};
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
};

pub const MAX_MCP_SEARCH_TOP_K: usize = 100;
pub const MAX_MCP_PREVIEW_BYTES: usize = 1024;
const MAX_MCP_REMEMBER_CONTENT_BYTES: usize = 64 * 1024;
const MAX_MCP_STORE_BYTES: u64 = 64 * 1024 * 1024;
const MCP_PUBLISH_FIXED_BUDGET_BYTES: u64 = 1024 * 1024;
const STORE_LOCK_RETRY_ATTEMPTS: usize = 8;
const STORE_LOCK_RETRY_DELAY_MS: u64 = 10;
#[cfg(unix)]
static MCP_TEMP_STORE_COUNTER: AtomicU64 = AtomicU64::new(0);
#[cfg(all(test, target_os = "linux"))]
type BeforeRuntimeOpenHook = Box<dyn FnOnce() + Send>;
#[cfg(all(test, target_os = "linux"))]
thread_local! {
    static BEFORE_RUNTIME_OPEN_HOOK: std::cell::RefCell<Option<BeforeRuntimeOpenHook>> =
        std::cell::RefCell::new(None);
}

#[cfg(unix)]
type AllowedRootDir = File;
#[cfg(not(unix))]
type AllowedRootDir = cap_std::fs::Dir;

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
    OpenStoreSession {
        store: String,
    },
    SearchText {
        session_id: u64,
        query: String,
        top_k: usize,
        include_preview: bool,
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
    allowed_root_dir: Option<AllowedRootDir>,
    allow_store_sessions: bool,
}

impl Default for WaxMcpSurface {
    fn default() -> Self {
        Self {
            broker: WaxBroker::default(),
            allowed_root: default_allowed_root(),
            allowed_root_dir: default_allowed_root()
                .as_deref()
                .and_then(|root| open_allowed_root_dir(root).ok())
                .filter(|dir| validate_allowed_root_security(dir).is_ok()),
            allow_store_sessions: false,
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
        let allowed_root_dir = open_allowed_root_dir(root).map_err(|error| McpError {
            code: McpErrorCode::InvalidRequest,
            message: error.to_string(),
        })?;
        validate_allowed_root_security(&allowed_root_dir).map_err(|error| McpError {
            code: McpErrorCode::InvalidRequest,
            message: error,
        })?;
        let allowed_root = root.canonicalize().map_err(|error| McpError {
            code: McpErrorCode::InvalidRequest,
            message: error.to_string(),
        })?;
        Ok(Self {
            broker: WaxBroker::default(),
            allowed_root_dir: Some(allowed_root_dir),
            allowed_root: Some(allowed_root),
            allow_store_sessions: false,
        })
    }

    /// Enables direct store-session request variants for trusted in-process callers.
    ///
    /// The stdio server does not expose these variants as tools. They are kept for benchmark
    /// contract tests and internal integration surfaces that already trust the store root.
    pub fn with_allowed_root_and_store_sessions(root: &Path) -> Result<Self, McpError> {
        let mut surface = Self::with_allowed_root(root)?;
        surface.allow_store_sessions = true;
        Ok(surface)
    }

    pub fn handle(&mut self, request: McpRequest) -> Result<McpResponse, McpError> {
        match request {
            McpRequest::Remember {
                store,
                content,
                metadata,
            } => {
                validate_remember_content(&content)?;
                ensure_secure_store_file_supported()?;
                let allowed_root_dir = self.allowed_root_dir.as_ref().ok_or_else(|| {
                    McpError::invalid_request("WAX_MCP_ALLOWED_ROOT is required for store files")
                })?;
                let allowed_root = self.allowed_root.as_ref().ok_or_else(|| {
                    McpError::invalid_request("WAX_MCP_ALLOWED_ROOT is required for store files")
                })?;
                ensure_allowed_root_path_matches_dir(allowed_root, allowed_root_dir)
                    .map_err(storage_io_error)?;
                let store =
                    self.authorized_store_with_retry(&store, true, AuthorizedStoreLock::Exclusive)?;
                let store_path = store.store_path();
                store.verify_runtime_path_identity()?;
                run_before_runtime_open_hook();
                store.verify_runtime_path_identity()?;
                let mut memory = Memory::open_existing(&store_path).map_err(runtime_error)?;
                store.verify_runtime_path_identity()?;
                drop(store);
                ensure_allowed_root_path_matches_dir(allowed_root, allowed_root_dir)
                    .map_err(storage_io_error)?;
                let doc_id = memory
                    .save_with_store_size_limit(
                        content,
                        metadata,
                        MAX_MCP_STORE_BYTES,
                        MCP_PUBLISH_FIXED_BUDGET_BYTES,
                    )
                    .map_err(runtime_error)?;
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
                ensure_secure_store_file_supported()?;
                let allowed_root_dir = self.allowed_root_dir.as_ref().ok_or_else(|| {
                    McpError::invalid_request("WAX_MCP_ALLOWED_ROOT is required for store files")
                })?;
                let allowed_root = self.allowed_root.as_ref().ok_or_else(|| {
                    McpError::invalid_request("WAX_MCP_ALLOWED_ROOT is required for store files")
                })?;
                ensure_allowed_root_path_matches_dir(allowed_root, allowed_root_dir)
                    .map_err(storage_io_error)?;
                let store =
                    self.authorized_store_with_retry(&store, false, AuthorizedStoreLock::Shared)?;
                let store_path = store.store_path();
                store.verify_runtime_path_identity()?;
                run_before_runtime_open_hook();
                store.verify_runtime_path_identity()?;
                ensure_allowed_root_path_matches_dir(allowed_root, allowed_root_dir)
                    .map_err(storage_io_error)?;
                let mut memory =
                    Memory::open_existing_read_only(&store_path).map_err(runtime_error)?;
                store.verify_runtime_path_identity()?;
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
                drop(store);
                ensure_allowed_root_path_matches_dir(allowed_root, allowed_root_dir)
                    .map_err(storage_io_error)?;
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
                let mode = parse_search_mode(&mode)?;
                ensure_secure_store_file_supported()?;
                let allowed_root_dir = self.allowed_root_dir.as_ref().ok_or_else(|| {
                    McpError::invalid_request("WAX_MCP_ALLOWED_ROOT is required for store files")
                })?;
                let allowed_root = self.allowed_root.as_ref().ok_or_else(|| {
                    McpError::invalid_request("WAX_MCP_ALLOWED_ROOT is required for store files")
                })?;
                ensure_allowed_root_path_matches_dir(allowed_root, allowed_root_dir)
                    .map_err(storage_io_error)?;
                let store =
                    self.authorized_store_with_retry(&store, false, AuthorizedStoreLock::Shared)?;
                let store_path = store.store_path();
                store.verify_runtime_path_identity()?;
                run_before_runtime_open_hook();
                store.verify_runtime_path_identity()?;
                ensure_allowed_root_path_matches_dir(allowed_root, allowed_root_dir)
                    .map_err(storage_io_error)?;
                let mut memory =
                    Memory::open_existing_read_only(&store_path).map_err(runtime_error)?;
                store.verify_runtime_path_identity()?;
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
                drop(store);
                ensure_allowed_root_path_matches_dir(allowed_root, allowed_root_dir)
                    .map_err(storage_io_error)?;
                Ok(McpResponse::SearchResults {
                    hits: map_runtime_hits(response.hits),
                })
            }
            McpRequest::OpenStoreSession { store } => {
                self.require_store_sessions()?;
                let store =
                    self.authorized_store_with_retry(&store, true, AuthorizedStoreLock::Exclusive)?;
                let store_path = store.store_path();
                store.verify_runtime_path_identity()?;
                run_before_runtime_open_hook();
                store.verify_runtime_path_identity()?;
                let session_id = match self.broker.open_store_session(&store_path) {
                    Ok(session_id) => session_id,
                    Err(error) => return Err(broker_error(error)),
                };
                if let Err(error) = store.verify_runtime_path_identity() {
                    let _ = self.broker.close_session(session_id);
                    return Err(error);
                }
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
                self.require_store_sessions()?;
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
            McpRequest::IngestDocuments {
                session_id,
                documents,
            } => {
                self.require_store_sessions()?;
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
                self.require_store_sessions()?;
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
                self.require_store_sessions()?;
                self.broker
                    .close_session(wax_v2_broker::SessionId::from_u64(session_id))
                    .map_err(broker_error)?;
                Ok(McpResponse::SessionClosed { session_id })
            }
        }
    }

    fn require_store_sessions(&self) -> Result<(), McpError> {
        if self.allow_store_sessions {
            return Ok(());
        }
        Err(McpError {
            code: McpErrorCode::InvalidRequest,
            message: "raw store session requests are disabled on the untrusted MCP surface"
                .to_owned(),
        })
    }

    fn authorized_store(
        &self,
        path: &str,
        create_missing: bool,
        lock_mode: AuthorizedStoreLock,
    ) -> Result<AuthorizedStore, McpError> {
        let path = Path::new(path);
        let Some(allowed_root) = &self.allowed_root else {
            return Err(McpError {
                code: McpErrorCode::InvalidRequest,
                message: "MCP surface has no allowed root".to_owned(),
            });
        };
        let Some(allowed_root_dir) = &self.allowed_root_dir else {
            return Err(McpError {
                code: McpErrorCode::InvalidRequest,
                message: "MCP surface has no opened allowed root".to_owned(),
            });
        };
        if path.is_absolute() {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .ok_or_else(|| McpError::invalid_request("store path must include a parent"))?
                .canonicalize()
                .map_err(|error| McpError {
                    code: McpErrorCode::InvalidRequest,
                    message: error.to_string(),
                })?;
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
        } else if path.components().count() != 1 {
            return Err(McpError {
                code: McpErrorCode::InvalidRequest,
                message: format!(
                    "store path {} must be directly under allowed root {}",
                    path.display(),
                    allowed_root.display()
                ),
            });
        }
        let file_name = path.file_name().ok_or_else(|| McpError {
            code: McpErrorCode::InvalidRequest,
            message: "store path must include a file name".to_owned(),
        })?;
        validate_store_file_name(file_name)?;
        open_authorized_store(
            allowed_root,
            allowed_root_dir,
            file_name,
            create_missing,
            lock_mode,
        )
    }

    fn authorized_store_with_retry(
        &self,
        path: &str,
        create_missing: bool,
        lock_mode: AuthorizedStoreLock,
    ) -> Result<AuthorizedStore, McpError> {
        for attempt in 0..STORE_LOCK_RETRY_ATTEMPTS {
            match self.authorized_store(path, create_missing, lock_mode) {
                Err(error)
                    if is_store_lock_busy_error(&error)
                        && attempt + 1 < STORE_LOCK_RETRY_ATTEMPTS =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(
                        STORE_LOCK_RETRY_DELAY_MS * (attempt as u64 + 1),
                    ));
                }
                result => return result,
            }
        }
        unreachable!("STORE_LOCK_RETRY_ATTEMPTS loop always returns");
    }
}

#[derive(Clone, Copy)]
enum AuthorizedStoreLock {
    Shared,
    Exclusive,
}

struct AuthorizedStore {
    #[cfg_attr(not(test), allow(dead_code))]
    authorized_path: PathBuf,
    runtime_path: PathBuf,
    #[cfg(unix)]
    identity: AuthorizedStoreIdentity,
    _file: File,
    _lock_file: File,
}

impl AuthorizedStore {
    #[cfg(test)]
    fn authorized_path(&self) -> &Path {
        &self.authorized_path
    }

    fn store_path(&self) -> PathBuf {
        self.runtime_path.clone()
    }

    #[cfg(unix)]
    fn verify_runtime_path_identity(&self) -> Result<(), McpError> {
        let metadata = std::fs::metadata(&self.runtime_path).map_err(storage_io_error)?;
        if self.identity.matches_metadata(&metadata) {
            return Ok(());
        }
        Err(McpError::invalid_request(
            "authorized MCP store file changed before runtime open",
        ))
    }

    #[cfg(not(unix))]
    fn verify_runtime_path_identity(&self) -> Result<(), McpError> {
        Ok(())
    }
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct AuthorizedStoreIdentity {
    dev: u64,
    ino: u64,
}

#[cfg(unix)]
impl AuthorizedStoreIdentity {
    fn from_file(file: &File) -> Result<Self, McpError> {
        let metadata = file.metadata().map_err(storage_io_error)?;
        Ok(Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }

    fn matches_metadata(&self, metadata: &std::fs::Metadata) -> bool {
        self.dev == metadata.dev() && self.ino == metadata.ino()
    }
}

#[cfg(all(test, target_os = "linux"))]
fn set_before_runtime_open_hook(hook: impl FnOnce() + Send + 'static) {
    BEFORE_RUNTIME_OPEN_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(all(test, target_os = "linux"))]
fn run_before_runtime_open_hook() {
    let hook = BEFORE_RUNTIME_OPEN_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(not(all(test, target_os = "linux")))]
fn run_before_runtime_open_hook() {}

fn lock_authorized_store(file: &File, lock_mode: AuthorizedStoreLock) -> Result<(), McpError> {
    let result = match lock_mode {
        AuthorizedStoreLock::Shared => FileExt::try_lock_shared(file),
        AuthorizedStoreLock::Exclusive => FileExt::try_lock_exclusive(file),
    };
    result.map_err(|error| {
        let message = if error.kind() == std::io::ErrorKind::WouldBlock {
            "authorized store lock is busy; retry".to_owned()
        } else {
            error.to_string()
        };
        McpError {
            code: McpErrorCode::Storage,
            message,
        }
    })
}

fn is_store_lock_busy_error(error: &McpError) -> bool {
    error.code == McpErrorCode::Storage && error.message == "authorized store lock is busy; retry"
}

fn validate_remember_content(content: &str) -> Result<(), McpError> {
    if content.len() > MAX_MCP_REMEMBER_CONTENT_BYTES {
        return Err(McpError::invalid_request(format!(
            "content must be <= {MAX_MCP_REMEMBER_CONTENT_BYTES} bytes"
        )));
    }
    Ok(())
}

#[cfg(all(unix, target_os = "linux"))]
fn ensure_secure_store_file_supported() -> Result<(), McpError> {
    Ok(())
}

#[cfg(not(all(unix, target_os = "linux")))]
fn ensure_secure_store_file_supported() -> Result<(), McpError> {
    Err(McpError::invalid_request(
        "secure MCP store-file operations require Linux fd-relative filesystem support",
    ))
}

#[cfg(unix)]
fn ensure_allowed_root_path_matches_dir(
    allowed_root: &Path,
    allowed_root_dir: &AllowedRootDir,
) -> Result<(), std::io::Error> {
    let path_metadata = std::fs::metadata(allowed_root)?;
    let dir_metadata = allowed_root_dir.metadata()?;
    if path_metadata.dev() == dir_metadata.dev() && path_metadata.ino() == dir_metadata.ino() {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "allowed root path no longer matches opened directory",
        ))
    }
}

#[cfg(not(unix))]
fn ensure_allowed_root_path_matches_dir(
    _allowed_root: &Path,
    _allowed_root_dir: &AllowedRootDir,
) -> Result<(), std::io::Error> {
    Ok(())
}

fn lock_file_name_for(file_name: &OsStr) -> OsString {
    let mut hasher = Sha256::new();
    #[cfg(unix)]
    hasher.update(file_name.as_bytes().to_ascii_lowercase());
    #[cfg(not(unix))]
    hasher.update(file_name.to_string_lossy().to_lowercase().as_bytes());
    OsString::from(format!(".wax-mcp-lock-{:x}", hasher.finalize()))
}

fn validate_store_file_name(file_name: &OsStr) -> Result<(), McpError> {
    let file_path = Path::new(file_name);
    if file_path.components().count() != 1 {
        return Err(McpError::invalid_request(
            "store path must be a direct child file",
        ));
    }
    if file_path.extension() != Some(OsStr::new("wax")) {
        return Err(McpError::invalid_request(
            "store file name must use the .wax extension",
        ));
    }
    if file_name
        .to_string_lossy()
        .to_ascii_lowercase()
        .starts_with(".wax-mcp-")
    {
        return Err(McpError::invalid_request(
            "store file name must not use the reserved .wax-mcp- prefix",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_allowed_root_security(allowed_root_dir: &AllowedRootDir) -> Result<(), String> {
    let metadata = allowed_root_dir
        .metadata()
        .map_err(|error| error.to_string())?;
    let effective_uid = unsafe { libc::geteuid() };
    if metadata.uid() != effective_uid {
        return Err("WAX_MCP_ALLOWED_ROOT must be owned by the server user".to_owned());
    }
    if metadata.mode() & 0o077 != 0 {
        return Err(
            "WAX_MCP_ALLOWED_ROOT must be a private directory with mode 0700 or stricter"
                .to_owned(),
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_allowed_root_security(_allowed_root_dir: &AllowedRootDir) -> Result<(), String> {
    Ok(())
}

#[cfg(unix)]
fn validate_private_regular_file_metadata(metadata: &std::fs::Metadata) -> Result<(), McpError> {
    let effective_uid = unsafe { libc::geteuid() };
    if metadata.uid() != effective_uid {
        return Err(McpError::invalid_request(
            "MCP store files and lock files must be owned by the server user",
        ));
    }
    if metadata.mode() & 0o077 != 0 {
        return Err(McpError::invalid_request(
            "MCP store files and lock files must not be readable or writable by group/other",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn open_authorized_store(
    _allowed_root: &Path,
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
    create_missing: bool,
    lock_mode: AuthorizedStoreLock,
) -> Result<AuthorizedStore, McpError> {
    if file_name.as_bytes().contains(&0) || Path::new(file_name).components().count() != 1 {
        return Err(McpError::invalid_request(
            "store path must be a direct child file",
        ));
    }
    if !create_missing {
        let file = open_child_no_follow(allowed_root_dir, file_name, AuthorizedOpenMode::ReadOnly)
            .map_err(|error| McpError {
                code: McpErrorCode::InvalidRequest,
                message: error.to_string(),
            })?;
        reject_hard_linked_store_file(allowed_root_dir, file_name, &file)?;
        let lock_file = open_store_lock_file(allowed_root_dir, file_name)?;
        reject_hard_linked_regular_file(&lock_file)?;
        lock_authorized_store(&lock_file, lock_mode)?;
        lock_authorized_store(&file, lock_mode)?;
        let authorized_path = _allowed_root.join(file_name);
        return Ok(AuthorizedStore {
            runtime_path: authorized_store_runtime_path(
                allowed_root_dir,
                file_name,
                &authorized_path,
            ),
            authorized_path,
            identity: AuthorizedStoreIdentity::from_file(&file)?,
            _file: file,
            _lock_file: lock_file,
        });
    }
    let lock_file = open_store_lock_file(allowed_root_dir, file_name)?;
    reject_hard_linked_regular_file(&lock_file)?;
    lock_authorized_store(&lock_file, lock_mode)?;
    let existing_mode = if create_missing {
        AuthorizedOpenMode::ReadWrite
    } else {
        AuthorizedOpenMode::ReadOnly
    };
    let file = match open_child_no_follow(allowed_root_dir, file_name, existing_mode) {
        Ok(file) => file,
        Err(error) if create_missing && error.kind() == std::io::ErrorKind::NotFound => {
            let temp_file_name = temporary_store_file_name(file_name);
            let mut file = open_child_no_follow(
                allowed_root_dir,
                &temp_file_name,
                AuthorizedOpenMode::CreateNew,
            )
            .map_err(|error| McpError {
                code: McpErrorCode::InvalidRequest,
                message: error.to_string(),
            })?;
            if let Err(error) = wax_v2_core::create_empty_store_from_file(&mut file) {
                unlink_child_if_same_file(allowed_root_dir, &temp_file_name, &file)
                    .map_err(storage_io_error)?;
                return Err(McpError {
                    code: McpErrorCode::Storage,
                    message: error.to_string(),
                });
            }
            if let Err(error) =
                publish_temporary_store_file(allowed_root_dir, &temp_file_name, file_name, &file)
            {
                unlink_child_if_same_file(allowed_root_dir, &temp_file_name, &file)
                    .map_err(storage_io_error)?;
                return Err(McpError {
                    code: McpErrorCode::InvalidRequest,
                    message: error.to_string(),
                });
            }
            file
        }
        Err(error) => {
            return Err(McpError {
                code: McpErrorCode::InvalidRequest,
                message: error.to_string(),
            });
        }
    };
    reject_hard_linked_store_file(allowed_root_dir, file_name, &file)?;
    lock_authorized_store(&file, lock_mode)?;
    let authorized_path = _allowed_root.join(file_name);
    Ok(AuthorizedStore {
        runtime_path: authorized_store_runtime_path(allowed_root_dir, file_name, &authorized_path),
        authorized_path,
        identity: AuthorizedStoreIdentity::from_file(&file)?,
        _file: file,
        _lock_file: lock_file,
    })
}

#[cfg(all(unix, target_os = "linux"))]
fn authorized_store_runtime_path(
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
    _authorized_path: &Path,
) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", allowed_root_dir.as_raw_fd())).join(file_name)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn authorized_store_runtime_path(
    _allowed_root_dir: &AllowedRootDir,
    _file_name: &OsStr,
    authorized_path: &Path,
) -> PathBuf {
    authorized_path.to_path_buf()
}

#[cfg(unix)]
fn reject_hard_linked_regular_file(file: &File) -> Result<(), McpError> {
    let metadata = file.metadata().map_err(storage_io_error)?;
    if metadata.nlink() != 1 {
        return Err(McpError::invalid_request(
            "MCP store files and lock files must not be hard-linked",
        ));
    }
    validate_private_regular_file_metadata(&metadata)?;
    Ok(())
}

#[cfg(unix)]
fn reject_hard_linked_store_file(
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
    file: &File,
) -> Result<(), McpError> {
    cleanup_temporary_store_links_for_file(allowed_root_dir, file_name, file)
        .map_err(storage_io_error)?;
    reject_hard_linked_regular_file(file)
}

#[cfg(unix)]
fn cleanup_temporary_store_links_for_file(
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
    file: &File,
) -> Result<(), std::io::Error> {
    let metadata = file.metadata()?;
    if metadata.nlink() <= 1 {
        return Ok(());
    }
    let mcp_suffix = temporary_store_file_suffix(file_name);
    let allow_ascii_case_match =
        !directory_contains_exact_store_entry(allowed_root_dir, file_name, &metadata)?;
    let dir_fd = open_directory_cursor_fd(allowed_root_dir)?;
    let dir = unsafe { libc::fdopendir(dir_fd) };
    if dir.is_null() {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(dir_fd);
        }
        return Err(error);
    }
    let mut removed_any = false;
    loop {
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
            break;
        }
        let entry_name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if !is_recoverable_temporary_store_link(
            entry_name,
            file_name.as_bytes(),
            &mcp_suffix,
            allow_ascii_case_match,
        ) {
            continue;
        }
        let mut entry_name_nul = entry_name.to_vec();
        entry_name_nul.push(0);
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let stat_result = unsafe {
            libc::fstatat(
                allowed_root_dir.as_raw_fd(),
                entry_name_nul.as_ptr().cast(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if stat_result != 0 {
            continue;
        }
        let stat = unsafe { stat.assume_init() };
        if stat_dev_matches(stat.st_dev, metadata.dev()) && stat.st_ino == metadata.ino() {
            let unlink_result = unsafe {
                libc::unlinkat(
                    allowed_root_dir.as_raw_fd(),
                    entry_name_nul.as_ptr().cast(),
                    0,
                )
            };
            if unlink_result == 0 {
                removed_any = true;
            }
        }
    }
    let close_result = unsafe { libc::closedir(dir) };
    if close_result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    if removed_any {
        allowed_root_dir.sync_all()?;
    }
    Ok(())
}

#[cfg(unix)]
fn is_recoverable_temporary_store_link(
    entry_name: &[u8],
    target_name: &[u8],
    mcp_suffix: &[u8],
    allow_ascii_case_match: bool,
) -> bool {
    entry_name != target_name
        && (is_mcp_temporary_store_link(entry_name, mcp_suffix)
            || is_core_temporary_store_link(entry_name, target_name, allow_ascii_case_match))
}

#[cfg(unix)]
fn is_core_temporary_store_link(
    entry_name: &[u8],
    target_name: &[u8],
    allow_ascii_case_match: bool,
) -> bool {
    let Some(rest) = entry_name.strip_prefix(b".") else {
        return false;
    };
    let Some(rest) = rest.strip_suffix(b".tmp") else {
        return false;
    };
    if rest.len() <= target_name.len() + b".create-".len() {
        return false;
    }
    let candidate_target = &rest[..target_name.len()];
    let target_matches = candidate_target == target_name
        || (allow_ascii_case_match && candidate_target.eq_ignore_ascii_case(target_name));
    if !target_matches {
        return false;
    }
    let Some(suffix) = rest[target_name.len()..].strip_prefix(b".create-") else {
        return false;
    };
    is_pid_counter_suffix(suffix)
}

#[cfg(unix)]
fn is_mcp_temporary_store_link(entry_name: &[u8], mcp_suffix: &[u8]) -> bool {
    let Some(rest) = entry_name.strip_prefix(b".wax-mcp-create-") else {
        return false;
    };
    let Some(middle) = rest.strip_suffix(mcp_suffix) else {
        return false;
    };
    is_pid_counter_suffix(middle)
}

#[cfg(unix)]
fn is_pid_counter_suffix(suffix: &[u8]) -> bool {
    let Some(separator_index) = suffix.iter().position(|byte| *byte == b'-') else {
        return false;
    };
    let pid = &suffix[..separator_index];
    let counter = &suffix[separator_index + 1..];
    !pid.is_empty()
        && !counter.is_empty()
        && pid.iter().all(u8::is_ascii_digit)
        && counter.iter().all(u8::is_ascii_digit)
}

#[cfg(unix)]
fn directory_contains_exact_store_entry(
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
    metadata: &std::fs::Metadata,
) -> Result<bool, std::io::Error> {
    let target_name = file_name.as_bytes();
    let dir_fd = open_directory_cursor_fd(allowed_root_dir)?;
    let dir = unsafe { libc::fdopendir(dir_fd) };
    if dir.is_null() {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(dir_fd);
        }
        return Err(error);
    }
    let mut found = false;
    loop {
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
            break;
        }
        let entry_name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if entry_name != target_name {
            continue;
        }
        let mut entry_name_nul = entry_name.to_vec();
        entry_name_nul.push(0);
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let stat_result = unsafe {
            libc::fstatat(
                allowed_root_dir.as_raw_fd(),
                entry_name_nul.as_ptr().cast(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if stat_result != 0 {
            continue;
        }
        let stat = unsafe { stat.assume_init() };
        found = stat_dev_matches(stat.st_dev, metadata.dev())
            && stat.st_ino == metadata.ino()
            && (stat.st_mode & libc::S_IFMT) == libc::S_IFREG;
        break;
    }
    let close_result = unsafe { libc::closedir(dir) };
    if close_result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(found)
}

#[cfg(unix)]
fn open_directory_cursor_fd(
    allowed_root_dir: &AllowedRootDir,
) -> Result<libc::c_int, std::io::Error> {
    let dot = b".\0";
    let fd = unsafe {
        libc::openat(
            allowed_root_dir.as_raw_fd(),
            dot.as_ptr().cast(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fd)
}

#[cfg(unix)]
fn temporary_store_file_name(file_name: &OsStr) -> OsString {
    let mut hasher = Sha256::new();
    hasher.update(file_name.as_bytes().to_ascii_lowercase());
    OsString::from(format!(
        ".wax-mcp-create-{}-{}-{:x}.tmp",
        std::process::id(),
        MCP_TEMP_STORE_COUNTER.fetch_add(1, Ordering::Relaxed),
        hasher.finalize()
    ))
}

#[cfg(unix)]
fn temporary_store_file_suffix(file_name: &OsStr) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(file_name.as_bytes().to_ascii_lowercase());
    format!("-{:x}.tmp", hasher.finalize()).into_bytes()
}

#[cfg(unix)]
fn publish_temporary_store_file(
    allowed_root_dir: &AllowedRootDir,
    temp_file_name: &OsStr,
    file_name: &OsStr,
    expected_file: &File,
) -> Result<(), std::io::Error> {
    verify_child_matches_file(allowed_root_dir, temp_file_name, expected_file)?;
    #[cfg(target_os = "linux")]
    {
        rename_child_no_replace(allowed_root_dir, temp_file_name, file_name)?;
        allowed_root_dir.sync_all()?;
        verify_child_matches_file(allowed_root_dir, file_name, expected_file)
    }
    #[cfg(not(target_os = "linux"))]
    {
        link_child_no_replace(allowed_root_dir, temp_file_name, file_name)?;
        unlink_child(allowed_root_dir, temp_file_name)?;
        allowed_root_dir.sync_all()?;
        verify_child_matches_file(allowed_root_dir, file_name, expected_file)
    }
}

#[cfg(unix)]
fn verify_child_matches_file(
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
    expected_file: &File,
) -> Result<(), std::io::Error> {
    let expected = expected_file.metadata()?;
    let bytes = nul_terminated_child_name(file_name)?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            allowed_root_dir.as_raw_fd(),
            bytes.as_ptr().cast(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    if stat_dev_matches(stat.st_dev, expected.dev()) && stat.st_ino == expected.ino() {
        Ok(())
    } else {
        Err(std::io::Error::other(
            "published store file identity did not match initialized temp file",
        ))
    }
}

#[cfg(all(unix, target_os = "linux"))]
fn rename_child_no_replace(
    allowed_root_dir: &AllowedRootDir,
    old_name: &OsStr,
    new_name: &OsStr,
) -> Result<(), std::io::Error> {
    const RENAME_NOREPLACE: libc::c_uint = 1;
    let old_name = nul_terminated_child_name(old_name)?;
    let new_name = nul_terminated_child_name(new_name)?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            allowed_root_dir.as_raw_fd(),
            old_name.as_ptr().cast::<libc::c_char>(),
            allowed_root_dir.as_raw_fd(),
            new_name.as_ptr().cast::<libc::c_char>(),
            RENAME_NOREPLACE,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "linux")))]
fn link_child_no_replace(
    allowed_root_dir: &AllowedRootDir,
    old_name: &OsStr,
    new_name: &OsStr,
) -> Result<(), std::io::Error> {
    let old_name = nul_terminated_child_name(old_name)?;
    let new_name = nul_terminated_child_name(new_name)?;
    let result = unsafe {
        libc::linkat(
            allowed_root_dir.as_raw_fd(),
            old_name.as_ptr().cast(),
            allowed_root_dir.as_raw_fd(),
            new_name.as_ptr().cast(),
            0,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn open_store_lock_file(
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
) -> Result<File, McpError> {
    let lock_file_name = lock_file_name_for(file_name);
    match open_child_no_follow(
        allowed_root_dir,
        &lock_file_name,
        AuthorizedOpenMode::ReadWrite,
    ) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match open_child_no_follow(
                allowed_root_dir,
                &lock_file_name,
                AuthorizedOpenMode::CreateNew,
            ) {
                Ok(file) => Ok(file),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    open_child_no_follow(
                        allowed_root_dir,
                        &lock_file_name,
                        AuthorizedOpenMode::ReadWrite,
                    )
                    .map_err(|error| McpError {
                        code: McpErrorCode::InvalidRequest,
                        message: error.to_string(),
                    })
                }
                Err(error) => Err(McpError {
                    code: McpErrorCode::InvalidRequest,
                    message: error.to_string(),
                }),
            }
        }
        Err(error) => Err(McpError {
            code: McpErrorCode::InvalidRequest,
            message: error.to_string(),
        }),
    }
}

#[cfg(unix)]
#[derive(Clone, Copy)]
enum AuthorizedOpenMode {
    ReadOnly,
    ReadWrite,
    CreateNew,
}

#[cfg(unix)]
fn unlink_child(
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
) -> Result<(), std::io::Error> {
    let bytes = nul_terminated_child_name(file_name)?;
    let result = unsafe { libc::unlinkat(allowed_root_dir.as_raw_fd(), bytes.as_ptr().cast(), 0) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(error);
        }
    }
    allowed_root_dir.sync_all()
}

#[cfg(unix)]
fn unlink_child_if_same_file(
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
    expected_file: &File,
) -> Result<(), std::io::Error> {
    let expected = expected_file.metadata()?;
    let bytes = nul_terminated_child_name(file_name)?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            allowed_root_dir.as_raw_fd(),
            bytes.as_ptr().cast(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        return if error.kind() == std::io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(error)
        };
    }
    let stat = unsafe { stat.assume_init() };
    let actual_ino = stat.st_ino;
    if !stat_dev_matches(stat.st_dev, expected.dev()) || actual_ino != expected.ino() {
        return Ok(());
    }
    unlink_child(allowed_root_dir, file_name)
}

#[cfg(unix)]
#[cfg(any(target_os = "linux", target_os = "android"))]
fn stat_dev_matches(stat_dev: libc::dev_t, metadata_dev: u64) -> bool {
    stat_dev == metadata_dev
}

#[cfg(unix)]
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn stat_dev_matches(stat_dev: libc::dev_t, metadata_dev: u64) -> bool {
    u64::try_from(stat_dev).is_ok_and(|actual| actual == metadata_dev)
}

#[cfg(unix)]
fn nul_terminated_child_name(file_name: &OsStr) -> Result<Vec<u8>, std::io::Error> {
    let mut bytes = file_name.as_bytes().to_vec();
    if bytes.is_empty() || bytes.contains(&0) || Path::new(file_name).components().count() != 1 {
        return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
    }
    bytes.push(0);
    Ok(bytes)
}

#[cfg(unix)]
fn open_child_no_follow(
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
    mode: AuthorizedOpenMode,
) -> Result<File, std::io::Error> {
    let mut bytes = file_name.as_bytes().to_vec();
    bytes.push(0);
    let flags = match mode {
        AuthorizedOpenMode::ReadOnly => {
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK
        }
        AuthorizedOpenMode::ReadWrite => {
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK
        }
        AuthorizedOpenMode::CreateNew => {
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK
        }
    };
    let fd = unsafe {
        libc::openat(
            allowed_root_dir.as_raw_fd(),
            bytes.as_ptr().cast(),
            flags,
            0o600,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(error);
    }
    let stat = unsafe { stat.assume_init() };
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
        unsafe {
            libc::close(fd);
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "store path must resolve to a regular file",
        ));
    }
    let file = unsafe { File::from_raw_fd(fd) };
    if matches!(mode, AuthorizedOpenMode::CreateNew) {
        allowed_root_dir.sync_all()?;
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_authorized_store(
    _allowed_root: &Path,
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
    create_missing: bool,
    lock_mode: AuthorizedStoreLock,
) -> Result<AuthorizedStore, McpError> {
    let lock_file = open_store_lock_file(allowed_root_dir, file_name)?;
    reject_non_unix_hard_linked_file(&lock_file)?;
    lock_authorized_store(&lock_file, lock_mode)?;
    let mut options = cap_std::fs::OpenOptions::new();
    options.read(true).write(create_missing);
    options.follow(FollowSymlinks::No);
    let mut created = false;
    if create_missing {
        options.create_new(true);
    }
    let mut file = match allowed_root_dir.open_with(Path::new(file_name), &options) {
        Ok(file) => {
            created = create_missing;
            let file = file.into_std();
            reject_non_unix_hard_linked_file(&file)?;
            file
        }
        Err(error) if create_missing && error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut existing_options = cap_std::fs::OpenOptions::new();
            existing_options
                .read(true)
                .write(true)
                .follow(FollowSymlinks::No);
            let file = allowed_root_dir
                .open_with(Path::new(file_name), &existing_options)
                .map(cap_std::fs::File::into_std)
                .map_err(|error| McpError {
                    code: McpErrorCode::InvalidRequest,
                    message: error.to_string(),
                })?;
            reject_non_unix_hard_linked_file(&file)?;
            file
        }
        Err(error) => {
            return Err(McpError {
                code: McpErrorCode::InvalidRequest,
                message: error.to_string(),
            });
        }
    };
    lock_authorized_store(&file, lock_mode)?;
    if !file
        .metadata()
        .map_err(|error| McpError {
            code: McpErrorCode::InvalidRequest,
            message: error.to_string(),
        })?
        .is_file()
    {
        return Err(McpError::invalid_request(
            "store path must resolve to a regular file",
        ));
    }
    if created {
        if let Err(error) = wax_v2_core::create_empty_store_from_file(&mut file) {
            allowed_root_dir
                .remove_file(Path::new(file_name))
                .map_err(storage_io_error)?;
            sync_allowed_root_dir(allowed_root_dir).map_err(storage_io_error)?;
            return Err(McpError {
                code: McpErrorCode::Storage,
                message: error.to_string(),
            });
        }
        sync_allowed_root_dir(allowed_root_dir).map_err(storage_io_error)?;
    }
    let authorized_path = _allowed_root.join(file_name);
    Ok(AuthorizedStore {
        runtime_path: authorized_path.clone(),
        authorized_path,
        _file: file,
        _lock_file: lock_file,
    })
}

#[cfg(not(unix))]
fn open_store_lock_file(
    allowed_root_dir: &AllowedRootDir,
    file_name: &OsStr,
) -> Result<File, McpError> {
    let lock_file_name = lock_file_name_for(file_name);
    let mut options = cap_std::fs::OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
    match allowed_root_dir.open_with(Path::new(&lock_file_name), &options) {
        Ok(file) => {
            sync_allowed_root_dir(allowed_root_dir).map_err(storage_io_error)?;
            let file = file.into_std();
            reject_non_unix_hard_linked_file(&file)?;
            Ok(file)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut existing_options = cap_std::fs::OpenOptions::new();
            existing_options
                .read(true)
                .write(true)
                .follow(FollowSymlinks::No);
            let file = allowed_root_dir
                .open_with(Path::new(&lock_file_name), &existing_options)
                .map(cap_std::fs::File::into_std)
                .map_err(|error| McpError {
                    code: McpErrorCode::InvalidRequest,
                    message: error.to_string(),
                })?;
            reject_non_unix_hard_linked_file(&file)?;
            Ok(file)
        }
        Err(error) => Err(McpError {
            code: McpErrorCode::InvalidRequest,
            message: error.to_string(),
        }),
    }
}

#[cfg(all(not(unix), windows))]
fn reject_non_unix_hard_linked_file(file: &File) -> Result<(), McpError> {
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    let result =
        unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), info.as_mut_ptr()) };
    if result == 0 {
        return Err(McpError {
            code: McpErrorCode::InvalidRequest,
            message: std::io::Error::last_os_error().to_string(),
        });
    }
    if unsafe { info.assume_init() }.nNumberOfLinks != 1 {
        return Err(McpError::invalid_request(
            "MCP store files and lock files must not be hard-linked",
        ));
    }
    Ok(())
}

#[cfg(all(not(unix), not(windows)))]
fn reject_non_unix_hard_linked_file(_file: &File) -> Result<(), McpError> {
    Err(McpError::invalid_request(
        "MCP hard-link validation is unsupported on this platform",
    ))
}

#[cfg(not(unix))]
fn sync_allowed_root_dir(allowed_root_dir: &AllowedRootDir) -> Result<(), std::io::Error> {
    allowed_root_dir.try_clone()?.into_std_file().sync_all()
}

fn default_allowed_root() -> Option<PathBuf> {
    std::env::current_dir()
        .ok()
        .and_then(|path| path.canonicalize().ok())
}

#[cfg(unix)]
fn open_allowed_root_dir(root: &Path) -> Result<AllowedRootDir, std::io::Error> {
    #[cfg(target_os = "macos")]
    let macos_private_alias_path = macos_private_alias_path(root);
    #[cfg(target_os = "macos")]
    let root = macos_private_alias_path.as_deref().unwrap_or(root);

    let mut components = root.components();
    let mut dir = if root.is_absolute() {
        match components.next() {
            Some(Component::RootDir) => open_absolute_root_dir()?,
            _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
        }
    } else {
        open_current_root_dir()?
    };

    for component in components {
        match component {
            Component::Normal(name) => {
                dir = open_allowed_root_component_no_follow(&dir, name)?;
            }
            Component::CurDir => {}
            _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
        }
    }

    Ok(dir)
}

#[cfg(all(unix, target_os = "macos"))]
fn macos_private_alias_path(path: &Path) -> Option<PathBuf> {
    if let Ok(rest) = path.strip_prefix("/tmp") {
        Some(Path::new("/private/tmp").join(rest))
    } else if let Ok(rest) = path.strip_prefix("/var") {
        Some(Path::new("/private/var").join(rest))
    } else {
        None
    }
}

#[cfg(unix)]
fn open_absolute_root_dir() -> Result<AllowedRootDir, std::io::Error> {
    let fd = unsafe {
        libc::open(
            c"/".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    file_from_dir_fd(fd)
}

#[cfg(unix)]
fn open_current_root_dir() -> Result<AllowedRootDir, std::io::Error> {
    let fd = unsafe {
        libc::open(
            c".".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    file_from_dir_fd(fd)
}

#[cfg(unix)]
fn open_allowed_root_component_no_follow(
    parent: &AllowedRootDir,
    name: &OsStr,
) -> Result<AllowedRootDir, std::io::Error> {
    let mut bytes = name.as_bytes().to_vec();
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
    }
    bytes.push(0);
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            bytes.as_ptr().cast(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    file_from_dir_fd(fd)
}

#[cfg(unix)]
fn file_from_dir_fd(fd: i32) -> Result<AllowedRootDir, std::io::Error> {
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(error);
    }
    let stat = unsafe { stat.assume_init() };
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFDIR {
        unsafe {
            libc::close(fd);
        }
        return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(not(unix))]
fn open_allowed_root_dir(root: &Path) -> Result<AllowedRootDir, std::io::Error> {
    let mut components = root.components();
    let mut dir = match components.next() {
        Some(Component::Prefix(prefix)) => {
            let mut base = PathBuf::new();
            base.push(prefix.as_os_str());
            if matches!(components.clone().next(), Some(Component::RootDir)) {
                components.next();
                base.push(Component::RootDir.as_os_str());
            }
            cap_std::fs::Dir::open_ambient_dir(&base, cap_std::ambient_authority())?
        }
        Some(Component::RootDir) => cap_std::fs::Dir::open_ambient_dir(
            Component::RootDir.as_os_str(),
            cap_std::ambient_authority(),
        )?,
        Some(Component::CurDir) | None => {
            cap_std::fs::Dir::open_ambient_dir(".", cap_std::ambient_authority())?
        }
        Some(Component::ParentDir) => {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
        }
        Some(Component::Normal(name)) => {
            let current = cap_std::fs::Dir::open_ambient_dir(".", cap_std::ambient_authority())?;
            current.open_dir_nofollow(Path::new(name))?
        }
    };

    for component in components {
        match component {
            Component::Normal(name) => {
                dir = dir.open_dir_nofollow(Path::new(name))?;
            }
            Component::CurDir => {}
            _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
        }
    }

    Ok(dir)
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

fn storage_io_error(error: std::io::Error) -> McpError {
    McpError {
        code: McpErrorCode::Storage,
        message: error.to_string(),
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
            preview: hit
                .preview
                .map(|preview| truncate_utf8_bytes(&preview, MAX_MCP_PREVIEW_BYTES)),
        })
        .collect()
}

fn truncate_utf8_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    #[cfg(unix)]
    use std::ffi::OsString;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use super::{
        validate_store_file_name, McpError, McpErrorCode, McpNewDocument, McpRequest, McpResponse,
        WaxMcpSurface, MAX_MCP_SEARCH_TOP_K,
    };
    use tempfile::tempdir;
    use tempfile::TempDir;
    use wax_v2_broker::WaxBroker;
    #[cfg(target_os = "linux")]
    use wax_v2_runtime::Memory;

    fn private_tempdir() -> TempDir {
        let root = tempdir().unwrap();
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(root.path()).unwrap().permissions();
            permissions.set_mode(0o700);
            fs::set_permissions(root.path(), permissions).unwrap();
        }
        root
    }

    #[cfg(target_os = "linux")]
    fn create_private_memory_store(path: &std::path::Path, content: &str) {
        let mut memory = Memory::open(path).unwrap();
        memory.remember(content).unwrap();
        memory.close().unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(path, permissions).unwrap();
    }

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
        let response = McpResponse::RawIngested {
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
    fn mcp_store_file_names_must_be_explicit_wax_files() {
        assert!(validate_store_file_name(OsStr::new("agent.wax")).is_ok());

        let missing_extension = validate_store_file_name(OsStr::new("agent")).unwrap_err();
        assert_eq!(missing_extension.code(), &McpErrorCode::InvalidRequest);
        assert!(missing_extension.message().contains(".wax extension"));

        let wrong_extension = validate_store_file_name(OsStr::new("agent.txt")).unwrap_err();
        assert_eq!(wrong_extension.code(), &McpErrorCode::InvalidRequest);
        assert!(wrong_extension.message().contains(".wax extension"));

        let reserved =
            validate_store_file_name(OsStr::new(".wax-mcp-lock-deadbeef.wax")).unwrap_err();
        assert_eq!(reserved.code(), &McpErrorCode::InvalidRequest);
        assert!(reserved.message().contains("reserved .wax-mcp- prefix"));
    }

    #[cfg(unix)]
    #[test]
    fn mcp_create_failure_cleanup_does_not_remove_replacement_file() {
        let root = private_tempdir();
        let allowed_root_dir = fs::File::open(root.path()).unwrap();
        let file_name = OsStr::new("agent.wax");
        let created_file = super::open_child_no_follow(
            &allowed_root_dir,
            file_name,
            super::AuthorizedOpenMode::CreateNew,
        )
        .unwrap();
        fs::remove_file(root.path().join(file_name)).unwrap();
        fs::write(root.path().join(file_name), b"replacement").unwrap();

        super::unlink_child_if_same_file(&allowed_root_dir, file_name, &created_file).unwrap();

        assert_eq!(
            fs::read(root.path().join(file_name)).unwrap(),
            b"replacement"
        );
    }

    #[cfg(unix)]
    #[test]
    fn mcp_create_publishes_initialized_temp_store_to_final_name() {
        let root = private_tempdir();
        let allowed_root_dir = fs::File::open(root.path()).unwrap();
        let file_name = OsStr::new("agent.wax");

        let store = super::open_authorized_store(
            root.path(),
            &allowed_root_dir,
            file_name,
            true,
            super::AuthorizedStoreLock::Exclusive,
        )
        .unwrap();
        drop(store);

        wax_v2_core::open_store(&root.path().join(file_name)).unwrap();
        let temp_files = fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".wax-mcp-create-"))
            .collect::<Vec<_>>();
        assert!(temp_files.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn mcp_create_rejects_temp_path_swap_before_publish() {
        let root = private_tempdir();
        let allowed_root_dir = fs::File::open(root.path()).unwrap();
        let file_name = OsStr::new("agent.wax");
        let temp_file_name = super::temporary_store_file_name(file_name);
        let mut file = super::open_child_no_follow(
            &allowed_root_dir,
            &temp_file_name,
            super::AuthorizedOpenMode::CreateNew,
        )
        .unwrap();
        wax_v2_core::create_empty_store_from_file(&mut file).unwrap();
        fs::remove_file(root.path().join(&temp_file_name)).unwrap();
        fs::write(root.path().join(&temp_file_name), b"replacement").unwrap();

        let error = super::publish_temporary_store_file(
            &allowed_root_dir,
            &temp_file_name,
            file_name,
            &file,
        )
        .unwrap_err();

        assert!(error.to_string().contains("identity"));
        assert!(!root.path().join(file_name).exists());
    }

    #[cfg(unix)]
    #[test]
    fn mcp_surface_rejects_world_writable_allowed_root() {
        let root = private_tempdir();
        let mut permissions = fs::metadata(root.path()).unwrap().permissions();
        permissions.set_mode(0o777);
        fs::set_permissions(root.path(), permissions).unwrap();

        let error = match WaxMcpSurface::with_allowed_root(root.path()) {
            Ok(_) => panic!("world-writable allowed root should fail"),
            Err(error) => error,
        };

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("private directory"));
    }

    #[cfg(unix)]
    #[test]
    fn mcp_surface_rejects_group_or_world_readable_existing_store() {
        let root = private_tempdir();
        let store = root.path().join("agent.wax");
        wax_v2_core::create_empty_store(&store).unwrap();
        let mut permissions = fs::metadata(&store).unwrap().permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&store, permissions).unwrap();
        let surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let error = match surface.authorized_store(
            "agent.wax",
            false,
            crate::AuthorizedStoreLock::Shared,
        ) {
            Ok(_) => panic!("world-readable store should fail"),
            Err(error) => error,
        };

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("must not be readable or writable"));
    }

    #[test]
    fn mcp_surface_rejects_oversized_remember_content() {
        let root = private_tempdir();
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let error = surface
            .handle(McpRequest::Remember {
                store: root.path().join("agent.wax").to_string_lossy().into_owned(),
                content: "x".repeat(crate::MAX_MCP_REMEMBER_CONTENT_BYTES + 1),
                metadata: serde_json::json!({}),
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("content must be <="));
    }

    #[test]
    fn mcp_search_hit_previews_are_truncated() {
        let hits = crate::map_runtime_hits(vec![wax_v2_runtime::RuntimeSearchHit {
            doc_id: "doc-1".to_owned(),
            preview: Some("x".repeat(crate::MAX_MCP_PREVIEW_BYTES + 32)),
        }]);

        assert_eq!(
            hits[0].preview.as_ref().unwrap().len(),
            crate::MAX_MCP_PREVIEW_BYTES
        );
    }

    #[cfg(unix)]
    #[test]
    fn mcp_existing_store_recovers_leftover_create_temp_hard_link() {
        let root = private_tempdir();
        let allowed_root_dir = fs::File::open(root.path()).unwrap();
        let file_name = OsStr::new("agent.wax");
        let store_path = root.path().join(file_name);
        wax_v2_core::create_empty_store(&store_path).unwrap();
        let temp_file_name = super::temporary_store_file_name(file_name);
        fs::hard_link(&store_path, root.path().join(&temp_file_name)).unwrap();

        let store = super::open_authorized_store(
            root.path(),
            &allowed_root_dir,
            file_name,
            false,
            super::AuthorizedStoreLock::Shared,
        )
        .unwrap();
        drop(store);

        wax_v2_core::open_store(&store_path).unwrap();
        assert!(!root.path().join(temp_file_name).exists());
    }

    #[cfg(unix)]
    #[test]
    fn mcp_existing_store_recovers_leftover_core_create_temp_hard_link() {
        let root = private_tempdir();
        let allowed_root_dir = fs::File::open(root.path()).unwrap();
        let file_name = OsStr::new("agent.wax");
        let store_path = root.path().join(file_name);
        wax_v2_core::create_empty_store(&store_path).unwrap();
        let temp_file_name = OsStr::new(".agent.wax.create-123-456.tmp");
        fs::hard_link(&store_path, root.path().join(temp_file_name)).unwrap();

        let store = super::open_authorized_store(
            root.path(),
            &allowed_root_dir,
            file_name,
            false,
            super::AuthorizedStoreLock::Shared,
        )
        .unwrap();
        drop(store);

        wax_v2_core::open_store(&store_path).unwrap();
        assert!(!root.path().join(temp_file_name).exists());
    }

    #[cfg(unix)]
    #[test]
    fn mcp_existing_store_recovers_core_temp_for_target_containing_create_marker() {
        let root = private_tempdir();
        let allowed_root_dir = fs::File::open(root.path()).unwrap();
        let file_name = OsStr::new("agent.create-prod.wax");
        let store_path = root.path().join(file_name);
        wax_v2_core::create_empty_store(&store_path).unwrap();
        let temp_file_name = OsStr::new(".agent.create-prod.wax.create-123-456.tmp");
        fs::hard_link(&store_path, root.path().join(temp_file_name)).unwrap();

        let store = super::open_authorized_store(
            root.path(),
            &allowed_root_dir,
            file_name,
            false,
            super::AuthorizedStoreLock::Shared,
        )
        .unwrap();
        drop(store);

        wax_v2_core::open_store(&store_path).unwrap();
        assert!(!root.path().join(temp_file_name).exists());
    }

    #[cfg(unix)]
    #[test]
    fn mcp_existing_store_recovers_case_mismatched_core_temp_hard_link_on_casefolded_volume() {
        let root = private_tempdir();
        let allowed_root_dir = fs::File::open(root.path()).unwrap();
        let actual_file_name = OsStr::new("Agent.wax");
        let requested_file_name = OsStr::new("agent.wax");
        let actual_store_path = root.path().join(actual_file_name);
        let requested_store_path = root.path().join(requested_file_name);
        wax_v2_core::create_empty_store(&actual_store_path).unwrap();
        if !requested_store_path.exists() {
            return;
        }
        let temp_file_name = OsStr::new(".Agent.wax.create-123-456.tmp");
        fs::hard_link(&actual_store_path, root.path().join(temp_file_name)).unwrap();

        let store = super::open_authorized_store(
            root.path(),
            &allowed_root_dir,
            requested_file_name,
            false,
            super::AuthorizedStoreLock::Shared,
        )
        .unwrap();
        drop(store);

        wax_v2_core::open_store(&actual_store_path).unwrap();
        assert!(!root.path().join(temp_file_name).exists());
    }

    #[cfg(unix)]
    #[test]
    fn mcp_existing_store_rejects_case_mismatched_core_temp_on_case_sensitive_volume() {
        let root = private_tempdir();
        let allowed_root_dir = fs::File::open(root.path()).unwrap();
        let file_name = OsStr::new("agent.wax");
        let store_path = root.path().join(file_name);
        wax_v2_core::create_empty_store(&store_path).unwrap();
        let temp_file_name = OsStr::new(".Agent.wax.create-123-456.tmp");
        fs::hard_link(&store_path, root.path().join(temp_file_name)).unwrap();

        let error = match super::open_authorized_store(
            root.path(),
            &allowed_root_dir,
            file_name,
            false,
            super::AuthorizedStoreLock::Shared,
        ) {
            Ok(_) => panic!("case-mismatched hard link should be rejected"),
            Err(error) => error,
        };

        assert_eq!(
            error.code(),
            &McpErrorCode::InvalidRequest,
            "unexpected error: {error}"
        );
        assert_eq!(
            error.message(),
            "MCP store files and lock files must not be hard-linked"
        );
        assert!(root.path().join(temp_file_name).exists());
    }

    #[cfg(unix)]
    #[test]
    fn mcp_existing_store_rejects_broad_core_like_hard_link() {
        let root = private_tempdir();
        let allowed_root_dir = fs::File::open(root.path()).unwrap();
        let file_name = OsStr::new("agent.wax");
        let store_path = root.path().join(file_name);
        wax_v2_core::create_empty_store(&store_path).unwrap();
        let non_generated_link = OsStr::new(".agent.wax.create-manual-stale.tmp");
        fs::hard_link(&store_path, root.path().join(non_generated_link)).unwrap();

        let error = match super::open_authorized_store(
            root.path(),
            &allowed_root_dir,
            file_name,
            false,
            super::AuthorizedStoreLock::Shared,
        ) {
            Ok(_) => panic!("non-generated hard link should be rejected"),
            Err(error) => error,
        };

        assert_eq!(
            error.code(),
            &McpErrorCode::InvalidRequest,
            "unexpected error: {error}"
        );
        assert_eq!(
            error.message(),
            "MCP store files and lock files must not be hard-linked"
        );
        assert!(root.path().join(non_generated_link).exists());
    }

    #[cfg(unix)]
    #[test]
    fn mcp_existing_store_rejects_nongenerated_mcp_create_hard_link() {
        let root = private_tempdir();
        let allowed_root_dir = fs::File::open(root.path()).unwrap();
        let file_name = OsStr::new("agent.wax");
        let store_path = root.path().join(file_name);
        wax_v2_core::create_empty_store(&store_path).unwrap();
        let suffix = String::from_utf8(super::temporary_store_file_suffix(file_name)).unwrap();
        let non_generated_link = OsString::from(format!(".wax-mcp-create-manual{suffix}"));
        fs::hard_link(&store_path, root.path().join(&non_generated_link)).unwrap();

        let error = match super::open_authorized_store(
            root.path(),
            &allowed_root_dir,
            file_name,
            false,
            super::AuthorizedStoreLock::Shared,
        ) {
            Ok(_) => panic!("non-generated MCP hard link should be rejected"),
            Err(error) => error,
        };

        assert_eq!(
            error.code(),
            &McpErrorCode::InvalidRequest,
            "unexpected error: {error}"
        );
        assert_eq!(
            error.message(),
            "MCP store files and lock files must not be hard-linked"
        );
        assert!(root.path().join(non_generated_link).exists());
    }

    #[test]
    fn mcp_surface_without_allowed_root_rejects_open_store_session() {
        let mut surface = WaxMcpSurface {
            broker: WaxBroker::default(),
            allowed_root: None,
            allowed_root_dir: None,
            allow_store_sessions: true,
        };

        let error = surface
            .handle(McpRequest::OpenStoreSession {
                store: std::env::current_dir()
                    .unwrap()
                    .join("projection.wax")
                    .to_string_lossy()
                    .into_owned(),
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert_eq!(error.message(), "MCP surface has no allowed root");
    }

    #[test]
    fn mcp_surface_disables_store_sessions_by_default() {
        let root = private_tempdir();
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let error = surface
            .handle(McpRequest::OpenStoreSession {
                store: root
                    .path()
                    .join("projection.wax")
                    .to_string_lossy()
                    .into_owned(),
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("raw store session requests"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mcp_surface_remembers_and_recalls_wax_style_store_file() {
        let root = private_tempdir();
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

        let hidden_files = fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".wax-mcp-"))
            .collect::<Vec<_>>();
        assert!(hidden_files
            .iter()
            .all(|name| !name.starts_with(".wax-mcp-scratch-")));
        assert!(hidden_files
            .iter()
            .all(|name| !name.starts_with(".wax-mcp-replace-")));
        assert_eq!(
            hidden_files
                .iter()
                .filter(|name| name.starts_with(".wax-mcp-lock-"))
                .count(),
            1
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mcp_surface_recall_rejects_leaf_replacement_after_authorization() {
        let root = private_tempdir();
        let store = root.path().join("agent.wax");
        let original = root.path().join("original-authorized.wax");
        let replacement = root.path().join("replacement.wax");
        create_private_memory_store(&store, "authorized original memory");
        create_private_memory_store(&replacement, "replacement memory");
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        super::set_before_runtime_open_hook({
            let store = store.clone();
            let original = original.clone();
            let replacement = replacement.clone();
            move || {
                fs::rename(&store, &original).unwrap();
                fs::rename(&replacement, &store).unwrap();
            }
        });

        let error = surface
            .handle(McpRequest::Recall {
                store: store.to_string_lossy().into_owned(),
                query: "replacement".to_owned(),
                top_k: 3,
                include_preview: true,
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error
            .message()
            .contains("authorized MCP store file changed"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mcp_surface_remember_rejects_leaf_replacement_after_authorization() {
        let root = private_tempdir();
        let store = root.path().join("agent.wax");
        let authorized = root.path().join("authorized-created.wax");
        let replacement = root.path().join("replacement.wax");
        create_private_memory_store(&replacement, "replacement baseline memory");
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        super::set_before_runtime_open_hook({
            let store = store.clone();
            let authorized = authorized.clone();
            let replacement = replacement.clone();
            move || {
                fs::rename(&store, &authorized).unwrap();
                fs::rename(&replacement, &store).unwrap();
            }
        });

        let error = surface
            .handle(McpRequest::Remember {
                store: store.to_string_lossy().into_owned(),
                content: "must not land in replacement".to_owned(),
                metadata: serde_json::json!({}),
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error
            .message()
            .contains("authorized MCP store file changed"));
        let mut replacement_memory = Memory::open_existing_read_only(&store).unwrap();
        let response = replacement_memory
            .search_with_options(
                "must not land",
                wax_v2_runtime::MemorySearchOptions {
                    mode: wax_v2_runtime::RuntimeSearchMode::Hybrid,
                    top_k: 3,
                    include_preview: true,
                },
            )
            .unwrap();
        replacement_memory.close().unwrap();
        assert!(response.hits.is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mcp_surface_open_store_session_rejects_leaf_replacement_after_authorization() {
        let root = private_tempdir();
        let store = root.path().join("projection.wax");
        let original = root.path().join("original-authorized.wax");
        let replacement = root.path().join("replacement.wax");
        wax_v2_core::create_empty_store(&store).unwrap();
        wax_v2_core::create_empty_store(&replacement).unwrap();
        let mut surface = WaxMcpSurface::with_allowed_root_and_store_sessions(root.path()).unwrap();

        super::set_before_runtime_open_hook({
            let store = store.clone();
            let original = original.clone();
            let replacement = replacement.clone();
            move || {
                fs::rename(&store, &original).unwrap();
                fs::rename(&replacement, &store).unwrap();
            }
        });

        let error = surface
            .handle(McpRequest::OpenStoreSession {
                store: store.to_string_lossy().into_owned(),
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error
            .message()
            .contains("authorized MCP store file changed"));
    }

    #[test]
    fn mcp_surface_authorizes_simple_relative_store_path_under_current_allowed_root() {
        let root = private_tempdir();
        let store = root.path().join("agent.wax");
        wax_v2_core::create_empty_store(&store).unwrap();
        let surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let authorized = surface
            .authorized_store("agent.wax", false, crate::AuthorizedStoreLock::Shared)
            .unwrap();

        assert_eq!(authorized.authorized_path(), store.canonicalize().unwrap());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn mcp_surface_store_file_tools_fail_closed_without_fd_relative_paths() {
        let root = private_tempdir();
        let store = root.path().join("agent.wax");
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let error = surface
            .handle(McpRequest::Remember {
                store: store.to_string_lossy().into_owned(),
                content: "unsupported platform".to_owned(),
                metadata: serde_json::json!({}),
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("fd-relative filesystem support"));
        assert!(!store.exists());
        let hidden_files = fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".wax-mcp-"))
            .collect::<Vec<_>>();
        assert!(hidden_files.is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mcp_surface_recall_does_not_create_missing_store() {
        let root = private_tempdir();
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
        let hidden_files = fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".wax-mcp-"))
            .collect::<Vec<_>>();
        assert!(hidden_files.is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mcp_surface_rejects_non_wax_store_file_without_creating_files() {
        let root = private_tempdir();
        let store = root.path().join("agent");
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let error = surface
            .handle(McpRequest::Remember {
                store: store.to_string_lossy().into_owned(),
                content: "must not create".to_owned(),
                metadata: serde_json::json!({}),
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains(".wax extension"));
        assert!(!store.exists());
        let hidden_files = fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".wax-mcp-"))
            .collect::<Vec<_>>();
        assert!(hidden_files.is_empty());
    }

    #[test]
    fn mcp_surface_rejects_unbounded_top_k() {
        let root = private_tempdir();
        let store = root.path().join("agent.wax");
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

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

    #[cfg(target_os = "linux")]
    #[test]
    fn mcp_surface_rejects_nested_store_paths_under_allowed_root() {
        let root = private_tempdir();
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

    #[cfg(target_os = "linux")]
    #[test]
    fn mcp_surface_rejects_store_file_symlink_under_allowed_root() {
        let root = private_tempdir();
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
        assert!(
            error.message().contains("symlink")
                || error
                    .message()
                    .contains("Too many levels of symbolic links")
        );
    }

    #[cfg(unix)]
    #[test]
    fn mcp_surface_rejects_symlink_allowed_root() {
        let target = tempdir().unwrap();
        let parent = tempdir().unwrap();
        let link_root = parent.path().join("allowed-link");
        std::os::unix::fs::symlink(target.path(), &link_root).unwrap();

        let Err(error) = WaxMcpSurface::with_allowed_root(&link_root) else {
            panic!("symlink allowed root should be rejected");
        };

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(!error.message().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn mcp_surface_rejects_symlink_component_in_allowed_root() {
        let target_parent = tempdir().unwrap();
        let allowed = target_parent.path().join("allowed");
        std::fs::create_dir(&allowed).unwrap();
        let parent = tempdir().unwrap();
        let link_parent = parent.path().join("link-parent");
        std::os::unix::fs::symlink(target_parent.path(), &link_parent).unwrap();

        let Err(error) = WaxMcpSurface::with_allowed_root(&link_parent.join("allowed")) else {
            panic!("symlink component in allowed root should be rejected");
        };

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(!error.message().is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mcp_surface_recall_and_search_accept_read_only_store_file() {
        use std::os::unix::fs::PermissionsExt;

        let root = private_tempdir();
        let store = root.path().join("agent.wax");
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();
        surface
            .handle(McpRequest::Remember {
                store: store.to_string_lossy().into_owned(),
                content: "read only memory".to_owned(),
                metadata: serde_json::json!({}),
            })
            .unwrap();
        let mut permissions = std::fs::metadata(&store).unwrap().permissions();
        permissions.set_mode(0o400);
        std::fs::set_permissions(&store, permissions).unwrap();

        let recalled = surface
            .handle(McpRequest::Recall {
                store: store.to_string_lossy().into_owned(),
                query: "memory".to_owned(),
                top_k: 1,
                include_preview: true,
            })
            .unwrap();

        let McpResponse::SearchResults { hits } = recalled else {
            panic!("expected search results");
        };
        assert_eq!(hits[0].doc_id, "mem-0000000000000001");

        let searched = surface
            .handle(McpRequest::Search {
                store: store.to_string_lossy().into_owned(),
                query: "memory".to_owned(),
                mode: "hybrid".to_owned(),
                top_k: 1,
                include_preview: true,
            })
            .unwrap();

        let McpResponse::SearchResults { hits } = searched else {
            panic!("expected search results");
        };
        assert_eq!(hits[0].doc_id, "mem-0000000000000001");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mcp_surface_rejects_fifo_store_file_without_blocking() {
        use std::os::unix::ffi::OsStrExt;

        let root = private_tempdir();
        let fifo = root.path().join("fifo.wax");
        let mut bytes = fifo.as_os_str().as_bytes().to_vec();
        bytes.push(0);
        let result = unsafe { libc::mkfifo(bytes.as_ptr().cast(), 0o600) };
        assert_eq!(result, 0);
        let mut surface = WaxMcpSurface::with_allowed_root(root.path()).unwrap();

        let error = surface
            .handle(McpRequest::Recall {
                store: fifo.to_string_lossy().into_owned(),
                query: "anything".to_owned(),
                top_k: 1,
                include_preview: false,
            })
            .unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("regular file"));
    }
}
