#[cfg(test)]
use std::cell::Cell;
use std::collections::HashMap;
#[cfg(target_os = "macos")]
use std::ffi::CStr;
#[cfg(unix)]
use std::ffi::CString;
#[cfg(target_os = "linux")]
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io::{BufRead, BufReader};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::fd::FromRawFd;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
use std::path::{Component, Path, PathBuf};
use std::time::Instant;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
};

use rax_bench_model::{
    CorpusProfile, DatasetIdentity, DatasetPackManifest, DirtyProfile, EnvironmentConstraints,
    LengthBuckets, ManifestChecksums, ManifestFile, ManifestGenerator, MetadataProfile,
    QueryVectorProfile, SelectivityExemplars, TextProfile, VectorProfile,
};
use rax_docstore::DocIdMap;
use rax_docstore::Docstore;
use rax_search::hybrid_search_with_diagnostics;
use rax_text::TextLane;
use rax_vector::VectorLane;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeSearchMode {
    Text,
    Vector,
    Hybrid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSearchRequest {
    pub mode: RuntimeSearchMode,
    pub text_query: Option<String>,
    pub vector_query: Option<Vec<f32>>,
    pub top_k: usize,
    pub include_preview: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSearchHit {
    pub doc_id: String,
    pub preview: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSearchResponse {
    pub hits: Vec<RuntimeSearchHit>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSearchProfile {
    pub attempts: u32,
    pub refresh_ms: f64,
    pub live_doc_count_ms: f64,
    pub lane_load_ms: f64,
    pub rank_ms: f64,
    pub generation_check_ms: f64,
    pub total_ms: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSearchDocIdsResponse {
    pub doc_ids: Vec<String>,
    pub profile: RuntimeSearchProfile,
}

pub type MemorySearchResponse = RuntimeSearchResponse;
pub type MemorySearchHit = RuntimeSearchHit;

const DEFAULT_PRODUCT_EMBEDDING_DIMENSIONS: usize = 384;
const MEMORY_SAVE_MAX_ATTEMPTS: usize = 8;
const MEMORY_SAVE_RETRY_DELAY_MS: u64 = 10;
const MEMORY_OPEN_CREATE_RACE_ATTEMPTS: usize = 8;
const MEMORY_OPEN_CREATE_RACE_RETRY_DELAY_MS: u64 = 10;
const STORE_GENERATION_CHANGED_MESSAGE: &str =
    "publish_raw_snapshot store generation changed before publish; retry with latest documents";
const STORE_PUBLISH_LOCK_BUSY_MESSAGE: &str = "store publish lock is busy; retry";

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn validate_search_request(request: &RuntimeSearchRequest) -> Result<(), RuntimeError> {
    match &request.mode {
        RuntimeSearchMode::Text if request.text_query.is_none() => Err(
            RuntimeError::InvalidRequest("text_query is required for text search".to_owned()),
        ),
        RuntimeSearchMode::Vector if request.vector_query.is_none() => Err(
            RuntimeError::InvalidRequest("vector_query is required for vector search".to_owned()),
        ),
        RuntimeSearchMode::Hybrid if request.text_query.is_none() => Err(
            RuntimeError::InvalidRequest("text_query is required for hybrid search".to_owned()),
        ),
        RuntimeSearchMode::Hybrid if request.vector_query.is_none() => Err(
            RuntimeError::InvalidRequest("vector_query is required for hybrid search".to_owned()),
        ),
        _ => Ok(()),
    }
}

#[cfg(test)]
type SearchGenerationRaceHook = Box<dyn FnOnce() + Send>;
#[cfg(test)]
thread_local! {
    static SEARCH_GENERATION_RACE_HOOK: std::cell::RefCell<Option<SearchGenerationRaceHook>> =
        std::cell::RefCell::new(None);
}
#[cfg(test)]
thread_local! {
    static SEARCH_POST_HYDRATE_RACE_HOOK: std::cell::RefCell<Option<SearchGenerationRaceHook>> =
        std::cell::RefCell::new(None);
}
#[cfg(test)]
type FirstCreatePostCreateHook = Box<dyn FnOnce() + Send>;
#[cfg(test)]
thread_local! {
    static FIRST_CREATE_POST_CREATE_HOOK: std::cell::RefCell<Option<FirstCreatePostCreateHook>> =
        std::cell::RefCell::new(None);
}
#[cfg(test)]
thread_local! {
    static FILE_IDENTITY_FROM_FILE_SHOULD_FAIL: Cell<bool> = const { Cell::new(false) };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimePublishFamily {
    Doc,
    Text,
    Vector,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePublishReport {
    pub generation: u64,
    pub published_families: Vec<RuntimePublishFamily>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewDocument {
    pub doc_id: String,
    pub text: String,
    pub metadata: serde_json::Value,
    pub timestamp_ms: Option<u64>,
    pub extra_fields: serde_json::Map<String, serde_json::Value>,
}

impl NewDocument {
    pub fn new(doc_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            doc_id: doc_id.into(),
            text: text.into(),
            metadata: serde_json::json!({}),
            timestamp_ms: None,
            extra_fields: serde_json::Map::new(),
        }
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_timestamp_ms(mut self, timestamp_ms: u64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }

    pub fn with_extra_field(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.extra_fields.insert(key.into(), value);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewDocumentVector {
    pub doc_id: String,
    pub values: Vec<f32>,
}

impl NewDocumentVector {
    pub fn new(doc_id: impl Into<String>, values: Vec<f32>) -> Self {
        Self {
            doc_id: doc_id.into(),
            values,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimePlatformAccelerationFamily {
    Apple,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeAccelerationAvailability {
    Available,
    BackendNotCompiled,
    UnsupportedPlatform,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeAccelerationCapability {
    pub family: RuntimePlatformAccelerationFamily,
    pub availability: RuntimeAccelerationAvailability,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    pub platform_acceleration: Vec<RuntimeAccelerationCapability>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeAccelerationPreference {
    Default,
    PreferPlatform,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeExecutionBackend {
    RustDefault,
    PlatformAcceleration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeAccelerationSelection {
    pub preference: RuntimeAccelerationPreference,
    pub requested_family: Option<RuntimePlatformAccelerationFamily>,
    pub chosen_backend: RuntimeExecutionBackend,
    pub fallback_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    InvalidRequest(String),
    Storage(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(message) | Self::Storage(message) => write!(f, "{message}"),
        }
    }
}

pub struct RuntimeStore {
    root: PathBuf,
    #[cfg(unix)]
    _root_handle: Option<fs::File>,
    #[cfg(unix)]
    _store_handle: Option<fs::File>,
    store_path: PathBuf,
    store_identity: Option<FileIdentity>,
    manifest: DatasetPackManifest,
    docstore: Docstore,
    text_lane: Option<TextLane>,
    vector_lane: Option<VectorLane>,
    store_generation: Option<u64>,
    store_open_mode: ProductStoreOpenMode,
    closed: bool,
}

pub struct RuntimeStoreWriter<'a> {
    store: &'a mut RuntimeStore,
}

pub struct Memory {
    runtime: RuntimeStore,
    embedding_dimensions: usize,
}

struct LoadedRuntimeDocuments {
    store_generation: u64,
    documents: Vec<NewDocument>,
}

#[derive(Clone, Copy)]
struct MemorySaveBudget {
    max_store_bytes: u64,
    fixed_budget_bytes: u64,
}

struct StableRuntimeRoot {
    path: PathBuf,
    #[cfg(unix)]
    handle: Option<fs::File>,
}

#[cfg(unix)]
type StableStoreHandle = Option<fs::File>;
#[cfg(not(unix))]
type StableStoreHandle = Option<()>;

struct StableProductStorePath {
    path: PathBuf,
    handle: StableStoreHandle,
    identity: Option<FileIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySearchOptions {
    pub mode: RuntimeSearchMode,
    pub top_k: usize,
    pub include_preview: bool,
}

impl Default for MemorySearchOptions {
    fn default() -> Self {
        Self {
            mode: RuntimeSearchMode::Hybrid,
            top_k: 5,
            include_preview: true,
        }
    }
}

impl RuntimeStore {
    pub fn capabilities() -> RuntimeCapabilities {
        RuntimeCapabilities {
            platform_acceleration: vec![apple_acceleration_capability()],
        }
    }

    pub fn resolve_acceleration(
        preference: RuntimeAccelerationPreference,
    ) -> RuntimeAccelerationSelection {
        match preference {
            RuntimeAccelerationPreference::Default => RuntimeAccelerationSelection {
                preference,
                requested_family: None,
                chosen_backend: RuntimeExecutionBackend::RustDefault,
                fallback_reason: None,
            },
            RuntimeAccelerationPreference::PreferPlatform => {
                let capability = apple_acceleration_capability();
                let fallback_reason = match capability.availability {
                    RuntimeAccelerationAvailability::Available => None,
                    RuntimeAccelerationAvailability::BackendNotCompiled
                    | RuntimeAccelerationAvailability::UnsupportedPlatform => {
                        capability.detail.clone()
                    }
                };
                RuntimeAccelerationSelection {
                    preference,
                    requested_family: Some(RuntimePlatformAccelerationFamily::Apple),
                    chosen_backend: if fallback_reason.is_some() {
                        RuntimeExecutionBackend::RustDefault
                    } else {
                        RuntimeExecutionBackend::PlatformAcceleration
                    },
                    fallback_reason,
                }
            }
        }
    }

    pub fn create(root: &Path) -> Result<Self, RuntimeError> {
        ensure_stable_store_paths_supported()?;
        let root = stable_runtime_root(root)?;
        let manifest = read_manifest(&root.path)?;
        let store_path = writable_store_path_from_manifest(&root.path, &manifest)?;
        if store_path.exists() {
            return Err(RuntimeError::InvalidRequest(format!(
                "store already exists at {}",
                store_path.display()
            )));
        }
        let created_file = rax_core::create_empty_store_and_open(&store_path)
            .map_err(|error| RuntimeError::Storage(error.to_string()))?;
        let created_identity = match runtime_file_identity_from_file(&created_file) {
            Ok(identity) => identity,
            Err(error) => {
                drop(created_file);
                return Err(error);
            }
        };
        run_first_create_post_create_hook();
        let stable_store =
            match stable_created_store_path(created_file, &store_path, Some(&created_identity)) {
                Ok(stable_store) => stable_store,
                Err(error) => {
                    cleanup_created_empty_store_file(&store_path, Some(&created_identity));
                    return Err(error);
                }
            };
        let (stable_store_path, stable_store_handle, stable_store_identity) = stable_store;
        match Self::open_created_store(
            root,
            manifest,
            &stable_store_path,
            stable_store_handle,
            stable_store_identity,
        ) {
            Ok(runtime) => Ok(runtime),
            Err(error) => {
                cleanup_created_empty_store_file(&store_path, Some(&created_identity));
                Err(error)
            }
        }
    }

    pub fn create_at(path: &Path) -> Result<Self, RuntimeError> {
        ensure_stable_store_paths_supported()?;
        let input_root = product_store_root(path)?;
        let store_path = product_store_path_under_root(&input_root, path)?;
        let created_file = match rax_core::create_empty_store_and_open(&store_path) {
            Ok(file) => file,
            Err(error) if error.is_already_exists() => {
                if store_manifest_generation_if_present(&store_path)?.is_none() {
                    return Err(RuntimeError::Storage(error.to_string()));
                }
                return Self::open_at_after_create_conflict(path);
            }
            Err(error) => return Err(RuntimeError::Storage(error.to_string())),
        };
        let created_identity = match runtime_file_identity_from_file(&created_file) {
            Ok(identity) => identity,
            Err(error) => {
                drop(created_file);
                return Err(error);
            }
        };
        let root = match stable_runtime_root(&input_root) {
            Ok(root) => root,
            Err(error) => {
                drop(created_file);
                cleanup_created_empty_store_file(&store_path, Some(&created_identity));
                return Err(error);
            }
        };
        let store_path = product_store_path_under_root(&root.path, path)?;
        let manifest = product_manifest(&root.path, &store_path)?;
        run_first_create_post_create_hook();
        let stable_store =
            match stable_created_store_path(created_file, &store_path, Some(&created_identity)) {
                Ok(stable_store) => stable_store,
                Err(error) => {
                    cleanup_created_empty_store_file(&store_path, Some(&created_identity));
                    return Err(error);
                }
            };
        let (stable_store_path, stable_store_handle, stable_store_identity) = stable_store;
        match Self::open_created_store(
            root,
            manifest,
            &stable_store_path,
            stable_store_handle,
            stable_store_identity,
        ) {
            Ok(runtime) => Ok(runtime),
            Err(error) => {
                cleanup_created_empty_store_file(&store_path, Some(&created_identity));
                Err(error)
            }
        }
    }

    pub fn open_or_create_at(path: &Path) -> Result<Self, RuntimeError> {
        if path.exists() {
            Self::open_existing_at(path)
        } else {
            Self::create_at(path)
        }
    }

    fn open_at_after_create_conflict(path: &Path) -> Result<Self, RuntimeError> {
        let mut last_error = None;
        for attempt in 0..MEMORY_OPEN_CREATE_RACE_ATTEMPTS {
            match Self::open_at(path) {
                Ok(runtime) => return Ok(runtime),
                Err(error) if is_retryable_first_create_open_error(&error) => {
                    last_error = Some(error);
                    if attempt + 1 < MEMORY_OPEN_CREATE_RACE_ATTEMPTS {
                        std::thread::sleep(std::time::Duration::from_millis(
                            MEMORY_OPEN_CREATE_RACE_RETRY_DELAY_MS * (attempt as u64 + 1),
                        ));
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            RuntimeError::Storage(
                "memory store create race did not publish a valid store".to_owned(),
            )
        }))
    }

    pub fn open(root: &Path) -> Result<Self, RuntimeError> {
        Self::open_with_mode(root, ProductStoreOpenMode::ReadWrite)
    }

    pub fn open_read_only(root: &Path) -> Result<Self, RuntimeError> {
        Self::open_with_mode(root, ProductStoreOpenMode::ReadOnly)
    }

    fn open_with_mode(
        root: &Path,
        store_open_mode: ProductStoreOpenMode,
    ) -> Result<Self, RuntimeError> {
        ensure_stable_store_paths_supported()?;
        let root = stable_runtime_root(root)?;
        let manifest = read_manifest(&root.path)?;
        Self::open_from_manifest(root, manifest, store_open_mode)
    }

    pub fn open_at(path: &Path) -> Result<Self, RuntimeError> {
        Self::open_at_with_mode(path, ProductStoreOpenMode::ReadWrite)
    }

    fn open_at_with_mode(
        path: &Path,
        store_open_mode: ProductStoreOpenMode,
    ) -> Result<Self, RuntimeError> {
        ensure_stable_store_paths_supported()?;
        let stable_store = stable_product_store_path(path, store_open_mode)?;
        let input_root = product_store_root(&stable_store.path)?;
        let root = stable_runtime_root(&input_root)?;
        let store_path = product_store_path_under_root(&root.path, &stable_store.path)?;
        let manifest = product_manifest(&root.path, &store_path)?;
        Self::open_from_manifest_with_store_path(
            root,
            manifest,
            store_path,
            stable_store.handle,
            stable_store.identity,
            store_open_mode,
        )
    }

    pub fn open_existing_at(path: &Path) -> Result<Self, RuntimeError> {
        if !path.exists() {
            return Err(RuntimeError::InvalidRequest(format!(
                "memory store does not exist at {}",
                path.display()
            )));
        }
        Self::open_at(path)
    }

    pub fn open_existing_read_only_at(path: &Path) -> Result<Self, RuntimeError> {
        if !path.exists() {
            return Err(RuntimeError::InvalidRequest(format!(
                "memory store does not exist at {}",
                path.display()
            )));
        }
        Self::open_at_with_mode(path, ProductStoreOpenMode::ReadOnly)
    }

    pub fn writer(&mut self) -> Result<RuntimeStoreWriter<'_>, RuntimeError> {
        if self.closed {
            return Err(RuntimeError::InvalidRequest(
                "runtime store is already closed".to_owned(),
            ));
        }
        if self.store_open_mode == ProductStoreOpenMode::ReadOnly {
            return Err(RuntimeError::InvalidRequest(
                "runtime store was opened read-only".to_owned(),
            ));
        }
        Ok(RuntimeStoreWriter { store: self })
    }

    pub fn store_path(&self) -> PathBuf {
        #[cfg(target_os = "macos")]
        if let Some(path) =
            macos_store_path_from_parent_handle(&self.store_path, &self._store_handle)
        {
            return path;
        }
        self.store_path.clone()
    }

    fn root_path(&self) -> PathBuf {
        #[cfg(target_os = "macos")]
        if let Some(handle) = self._root_handle.as_ref() {
            if let Ok(path) = macos_path_from_file(handle) {
                return path;
            }
        }
        self.root.clone()
    }

    fn open_from_manifest(
        root: StableRuntimeRoot,
        manifest: DatasetPackManifest,
        store_open_mode: ProductStoreOpenMode,
    ) -> Result<Self, RuntimeError> {
        let store_path = store_path_from_manifest(&root.path, &manifest)?;
        let stable_store = stable_store_path_if_present(&store_path, store_open_mode)?;
        Self::open_from_manifest_with_store_path(
            root,
            manifest,
            stable_store.path,
            stable_store.handle,
            stable_store.identity,
            store_open_mode,
        )
    }

    fn open_from_manifest_with_store_path(
        root: StableRuntimeRoot,
        manifest: DatasetPackManifest,
        store_path: PathBuf,
        store_handle: StableStoreHandle,
        store_identity: Option<FileIdentity>,
        store_open_mode: ProductStoreOpenMode,
    ) -> Result<Self, RuntimeError> {
        #[cfg(not(unix))]
        let _ = store_handle;
        ensure_store_identity_matches(&store_path, store_identity.as_ref())?;
        let store_generation = loaded_store_manifest_generation_if_present(&store_path)?;
        ensure_store_identity_matches(&store_path, store_identity.as_ref())?;
        let root_path = stable_runtime_root_current_path(&root);
        validate_prebuilt_store_segments_against_dataset_pack(&root_path, &manifest, &store_path)?;
        let docstore = Docstore::open_with_store_path(&root_path, &manifest, &store_path)
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;

        Ok(Self {
            root: root_path,
            #[cfg(unix)]
            _root_handle: root.handle,
            #[cfg(unix)]
            _store_handle: store_handle,
            store_path,
            store_identity,
            manifest,
            docstore,
            text_lane: None,
            vector_lane: None,
            store_generation,
            store_open_mode,
            closed: false,
        })
    }

    fn open_created_store(
        root: StableRuntimeRoot,
        manifest: DatasetPackManifest,
        _store_path: &Path,
        store_handle: StableStoreHandle,
        store_identity: Option<FileIdentity>,
    ) -> Result<Self, RuntimeError> {
        let store_path = _store_path.to_path_buf();
        Self::open_from_manifest_with_store_path(
            root,
            manifest,
            store_path,
            store_handle,
            store_identity,
            ProductStoreOpenMode::ReadWrite,
        )
    }

    pub fn search(
        &mut self,
        request: RuntimeSearchRequest,
    ) -> Result<RuntimeSearchResponse, RuntimeError> {
        if self.closed {
            return Err(RuntimeError::InvalidRequest(
                "runtime store is already closed".to_owned(),
            ));
        }
        match &request.mode {
            RuntimeSearchMode::Text if request.text_query.is_none() => {
                return Err(RuntimeError::InvalidRequest(
                    "text_query is required for text search".to_owned(),
                ));
            }
            RuntimeSearchMode::Vector if request.vector_query.is_none() => {
                return Err(RuntimeError::InvalidRequest(
                    "vector_query is required for vector search".to_owned(),
                ));
            }
            RuntimeSearchMode::Hybrid if request.text_query.is_none() => {
                return Err(RuntimeError::InvalidRequest(
                    "text_query is required for hybrid search".to_owned(),
                ));
            }
            RuntimeSearchMode::Hybrid if request.vector_query.is_none() => {
                return Err(RuntimeError::InvalidRequest(
                    "vector_query is required for hybrid search".to_owned(),
                ));
            }
            _ => {}
        }
        if request.top_k == 0 {
            return Ok(RuntimeSearchResponse { hits: Vec::new() });
        }
        for attempt in 0..2 {
            self.refresh_read_state_if_store_generation_changed()?;
            let snapshot_generation = self.store_generation;
            #[cfg(test)]
            run_search_generation_race_hook();

            let live_doc_count = self.live_doc_count()?;
            if live_doc_count == 0 {
                if self.search_generation_changed_since(snapshot_generation)? {
                    self.invalidate_read_cache();
                    if attempt == 1 {
                        break;
                    }
                    continue;
                }
                return Ok(RuntimeSearchResponse { hits: Vec::new() });
            }
            let top_k = request.top_k.min(live_doc_count);

            let doc_ids_result = match request.mode {
                RuntimeSearchMode::Text => (|| {
                    let text_query = request.text_query.as_deref().ok_or_else(|| {
                        RuntimeError::InvalidRequest(
                            "text_query is required for text search".to_owned(),
                        )
                    })?;
                    Ok(self
                        .ensure_text_lane()?
                        .search_with_limit(text_query, top_k))
                })(),
                RuntimeSearchMode::Vector => (|| {
                    let vector_query = request.vector_query.as_deref().ok_or_else(|| {
                        RuntimeError::InvalidRequest(
                            "vector_query is required for vector search".to_owned(),
                        )
                    })?;
                    self.ensure_vector_lane()?
                        .search_with_query(
                            vector_query,
                            top_k,
                            rax_bench_model::VectorQueryMode::Auto,
                            false,
                        )
                        .map_err(RuntimeError::Storage)
                })(),
                RuntimeSearchMode::Hybrid => (|| {
                    let text_query = request.text_query.as_deref().ok_or_else(|| {
                        RuntimeError::InvalidRequest(
                            "text_query is required for hybrid search".to_owned(),
                        )
                    })?;
                    let vector_query = request.vector_query.as_deref().ok_or_else(|| {
                        RuntimeError::InvalidRequest(
                            "vector_query is required for hybrid search".to_owned(),
                        )
                    })?;
                    let text_limit = hybrid_text_candidate_limit(top_k, live_doc_count);
                    let text_hits = self
                        .ensure_text_lane()?
                        .search_with_limit(text_query, text_limit);
                    let report = hybrid_search_with_diagnostics(
                        &text_hits,
                        self.ensure_vector_lane()?,
                        vector_query,
                        top_k,
                        rax_bench_model::VectorQueryMode::Auto,
                        false,
                    )
                    .map_err(RuntimeError::Storage)?;
                    Ok(report.fused_hits)
                })(),
            };
            let doc_ids = match doc_ids_result {
                Ok(doc_ids) => doc_ids,
                Err(error) => {
                    if self.search_generation_changed_since(snapshot_generation)? {
                        self.invalidate_read_cache();
                        if attempt == 1 {
                            break;
                        }
                        continue;
                    }
                    return Err(error);
                }
            };

            if self.search_generation_changed_since(snapshot_generation)? {
                self.invalidate_read_cache();
                if attempt == 1 {
                    break;
                }
                continue;
            }

            let hits = match self.hydrate_hits(&doc_ids, request.include_preview) {
                Ok(hits) => hits,
                Err(error) => {
                    if self.search_generation_changed_since(snapshot_generation)? {
                        self.invalidate_read_cache();
                        if attempt == 1 {
                            break;
                        }
                        continue;
                    }
                    return Err(error);
                }
            };
            #[cfg(test)]
            run_search_post_hydrate_race_hook();

            if self.search_generation_changed_since(snapshot_generation)? {
                self.invalidate_read_cache();
                if attempt == 1 {
                    break;
                }
                continue;
            }
            return Ok(RuntimeSearchResponse { hits });
        }
        Err(RuntimeError::Storage(
            "store generation changed during search; retry".to_owned(),
        ))
    }

    pub fn search_doc_ids(
        &mut self,
        request: RuntimeSearchRequest,
    ) -> Result<Vec<String>, RuntimeError> {
        self.search_doc_ids_profiled(request)
            .map(|response| response.doc_ids)
    }

    pub fn search_doc_ids_snapshot(
        &mut self,
        request: RuntimeSearchRequest,
    ) -> Result<Vec<String>, RuntimeError> {
        self.search_doc_ids_snapshot_profiled(request)
            .map(|response| response.doc_ids)
    }

    pub fn search_doc_ids_profiled(
        &mut self,
        request: RuntimeSearchRequest,
    ) -> Result<RuntimeSearchDocIdsResponse, RuntimeError> {
        let total_start = Instant::now();
        let mut profile = RuntimeSearchProfile {
            attempts: 0,
            refresh_ms: 0.0,
            live_doc_count_ms: 0.0,
            lane_load_ms: 0.0,
            rank_ms: 0.0,
            generation_check_ms: 0.0,
            total_ms: 0.0,
        };
        if self.closed {
            return Err(RuntimeError::InvalidRequest(
                "runtime store is already closed".to_owned(),
            ));
        }
        validate_search_request(&request)?;
        if request.top_k == 0 {
            profile.total_ms = elapsed_ms(total_start);
            return Ok(RuntimeSearchDocIdsResponse {
                doc_ids: Vec::new(),
                profile,
            });
        }
        for attempt in 0..2 {
            profile.attempts += 1;
            let phase_start = Instant::now();
            self.refresh_read_state_if_store_generation_changed()?;
            profile.refresh_ms += elapsed_ms(phase_start);
            let snapshot_generation = self.store_generation;
            #[cfg(test)]
            run_search_generation_race_hook();

            let phase_start = Instant::now();
            let live_doc_count = self.live_doc_count()?;
            profile.live_doc_count_ms += elapsed_ms(phase_start);
            if live_doc_count == 0 {
                let phase_start = Instant::now();
                let generation_changed =
                    self.search_generation_changed_since(snapshot_generation)?;
                profile.generation_check_ms += elapsed_ms(phase_start);
                if generation_changed {
                    self.invalidate_read_cache();
                    if attempt == 1 {
                        break;
                    }
                    continue;
                }
                profile.total_ms = elapsed_ms(total_start);
                return Ok(RuntimeSearchDocIdsResponse {
                    doc_ids: Vec::new(),
                    profile,
                });
            }
            let top_k = request.top_k.min(live_doc_count);

            let doc_ids_result = match request.mode {
                RuntimeSearchMode::Text => (|| {
                    let text_query = request.text_query.as_deref().ok_or_else(|| {
                        RuntimeError::InvalidRequest(
                            "text_query is required for text search".to_owned(),
                        )
                    })?;
                    let phase_start = Instant::now();
                    let text_lane = self.ensure_text_lane()?;
                    profile.lane_load_ms += elapsed_ms(phase_start);
                    let phase_start = Instant::now();
                    let hits = text_lane.search_with_limit(text_query, top_k);
                    profile.rank_ms += elapsed_ms(phase_start);
                    Ok(hits)
                })(),
                RuntimeSearchMode::Vector => (|| {
                    let vector_query = request.vector_query.as_deref().ok_or_else(|| {
                        RuntimeError::InvalidRequest(
                            "vector_query is required for vector search".to_owned(),
                        )
                    })?;
                    let phase_start = Instant::now();
                    let vector_lane = self.ensure_vector_lane()?;
                    profile.lane_load_ms += elapsed_ms(phase_start);
                    let phase_start = Instant::now();
                    let hits = vector_lane
                        .search_with_query(
                            vector_query,
                            top_k,
                            rax_bench_model::VectorQueryMode::Auto,
                            false,
                        )
                        .map_err(RuntimeError::Storage)?;
                    profile.rank_ms += elapsed_ms(phase_start);
                    Ok(hits)
                })(),
                RuntimeSearchMode::Hybrid => (|| {
                    let text_query = request.text_query.as_deref().ok_or_else(|| {
                        RuntimeError::InvalidRequest(
                            "text_query is required for hybrid search".to_owned(),
                        )
                    })?;
                    let vector_query = request.vector_query.as_deref().ok_or_else(|| {
                        RuntimeError::InvalidRequest(
                            "vector_query is required for hybrid search".to_owned(),
                        )
                    })?;
                    let text_limit = hybrid_text_candidate_limit(top_k, live_doc_count);
                    let phase_start = Instant::now();
                    let text_lane = self.ensure_text_lane()?;
                    profile.lane_load_ms += elapsed_ms(phase_start);
                    let phase_start = Instant::now();
                    let text_hits = text_lane.search_with_limit(text_query, text_limit);
                    profile.rank_ms += elapsed_ms(phase_start);
                    let phase_start = Instant::now();
                    let vector_lane = self.ensure_vector_lane()?;
                    profile.lane_load_ms += elapsed_ms(phase_start);
                    let phase_start = Instant::now();
                    let report = hybrid_search_with_diagnostics(
                        &text_hits,
                        vector_lane,
                        vector_query,
                        top_k,
                        rax_bench_model::VectorQueryMode::Auto,
                        false,
                    )
                    .map_err(RuntimeError::Storage)?;
                    profile.rank_ms += elapsed_ms(phase_start);
                    Ok(report.fused_hits)
                })(),
            };
            let doc_ids = match doc_ids_result {
                Ok(doc_ids) => doc_ids,
                Err(error) => {
                    let phase_start = Instant::now();
                    let generation_changed =
                        self.search_generation_changed_since(snapshot_generation)?;
                    profile.generation_check_ms += elapsed_ms(phase_start);
                    if generation_changed {
                        self.invalidate_read_cache();
                        if attempt == 1 {
                            break;
                        }
                        continue;
                    }
                    return Err(error);
                }
            };

            let phase_start = Instant::now();
            let generation_changed = self.search_generation_changed_since(snapshot_generation)?;
            profile.generation_check_ms += elapsed_ms(phase_start);
            if generation_changed {
                self.invalidate_read_cache();
                if attempt == 1 {
                    break;
                }
                continue;
            }
            profile.total_ms = elapsed_ms(total_start);
            return Ok(RuntimeSearchDocIdsResponse { doc_ids, profile });
        }
        Err(RuntimeError::Storage(
            "store generation changed during search; retry".to_owned(),
        ))
    }

    pub fn search_doc_ids_snapshot_profiled(
        &mut self,
        request: RuntimeSearchRequest,
    ) -> Result<RuntimeSearchDocIdsResponse, RuntimeError> {
        let total_start = Instant::now();
        let mut profile = RuntimeSearchProfile {
            attempts: 1,
            refresh_ms: 0.0,
            live_doc_count_ms: 0.0,
            lane_load_ms: 0.0,
            rank_ms: 0.0,
            generation_check_ms: 0.0,
            total_ms: 0.0,
        };
        if self.closed {
            return Err(RuntimeError::InvalidRequest(
                "runtime store is already closed".to_owned(),
            ));
        }
        validate_search_request(&request)?;
        if request.top_k == 0 {
            profile.total_ms = elapsed_ms(total_start);
            return Ok(RuntimeSearchDocIdsResponse {
                doc_ids: Vec::new(),
                profile,
            });
        }

        let phase_start = Instant::now();
        let live_doc_count = self.live_doc_count()?;
        profile.live_doc_count_ms += elapsed_ms(phase_start);
        if live_doc_count == 0 {
            profile.total_ms = elapsed_ms(total_start);
            return Ok(RuntimeSearchDocIdsResponse {
                doc_ids: Vec::new(),
                profile,
            });
        }
        let top_k = request.top_k.min(live_doc_count);

        let doc_ids = match request.mode {
            RuntimeSearchMode::Text => {
                let text_query = request.text_query.as_deref().ok_or_else(|| {
                    RuntimeError::InvalidRequest(
                        "text_query is required for text search".to_owned(),
                    )
                })?;
                let phase_start = Instant::now();
                let text_lane = self.ensure_text_lane()?;
                profile.lane_load_ms += elapsed_ms(phase_start);
                let phase_start = Instant::now();
                let hits = text_lane.search_with_limit(text_query, top_k);
                profile.rank_ms += elapsed_ms(phase_start);
                hits
            }
            RuntimeSearchMode::Vector => {
                let vector_query = request.vector_query.as_deref().ok_or_else(|| {
                    RuntimeError::InvalidRequest(
                        "vector_query is required for vector search".to_owned(),
                    )
                })?;
                let phase_start = Instant::now();
                let vector_lane = self.ensure_vector_lane()?;
                profile.lane_load_ms += elapsed_ms(phase_start);
                let phase_start = Instant::now();
                let hits = vector_lane
                    .search_with_query(
                        vector_query,
                        top_k,
                        rax_bench_model::VectorQueryMode::Auto,
                        false,
                    )
                    .map_err(RuntimeError::Storage)?;
                profile.rank_ms += elapsed_ms(phase_start);
                hits
            }
            RuntimeSearchMode::Hybrid => {
                let text_query = request.text_query.as_deref().ok_or_else(|| {
                    RuntimeError::InvalidRequest(
                        "text_query is required for hybrid search".to_owned(),
                    )
                })?;
                let vector_query = request.vector_query.as_deref().ok_or_else(|| {
                    RuntimeError::InvalidRequest(
                        "vector_query is required for hybrid search".to_owned(),
                    )
                })?;
                let text_limit = hybrid_text_candidate_limit(top_k, live_doc_count);
                let phase_start = Instant::now();
                let text_lane = self.ensure_text_lane()?;
                profile.lane_load_ms += elapsed_ms(phase_start);
                let phase_start = Instant::now();
                let text_hits = text_lane.search_with_limit(text_query, text_limit);
                profile.rank_ms += elapsed_ms(phase_start);
                let phase_start = Instant::now();
                let vector_lane = self.ensure_vector_lane()?;
                profile.lane_load_ms += elapsed_ms(phase_start);
                let phase_start = Instant::now();
                let report = hybrid_search_with_diagnostics(
                    &text_hits,
                    vector_lane,
                    vector_query,
                    top_k,
                    rax_bench_model::VectorQueryMode::Auto,
                    false,
                )
                .map_err(RuntimeError::Storage)?;
                profile.rank_ms += elapsed_ms(phase_start);
                report.fused_hits
            }
        };

        profile.total_ms = elapsed_ms(total_start);
        Ok(RuntimeSearchDocIdsResponse { doc_ids, profile })
    }

    pub fn close(&mut self) -> Result<(), RuntimeError> {
        self.closed = true;
        Ok(())
    }

    fn refresh_read_state(&mut self) -> Result<(), RuntimeError> {
        let store_path = self.store_path();
        self.ensure_store_identity()?;
        let store_generation = loaded_store_manifest_generation_if_present(&store_path)?;
        self.ensure_store_identity()?;
        let root_path = self.root_path();
        let docstore = Docstore::open_with_store_path(&root_path, &self.manifest, &store_path)
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
        self.store_generation = store_generation;
        self.docstore = docstore;
        self.text_lane = None;
        self.vector_lane = None;
        Ok(())
    }

    fn refresh_read_state_after_committed_publish(&mut self, _generation: u64) {
        if self.refresh_read_state().is_err() {
            self.store_generation = None;
            self.text_lane = None;
            self.vector_lane = None;
        }
    }

    fn refresh_read_state_if_store_generation_changed(&mut self) -> Result<(), RuntimeError> {
        let store_path = self.store_path();
        self.ensure_store_identity()?;
        let current_generation = store_manifest_generation_if_present(&store_path)?;
        self.ensure_store_identity()?;
        if self.store_generation != current_generation {
            self.refresh_read_state()?;
        }
        Ok(())
    }

    fn search_generation_changed_since(
        &self,
        snapshot_generation: Option<u64>,
    ) -> Result<bool, RuntimeError> {
        self.ensure_store_identity()?;
        let current_generation = store_manifest_generation_if_present(&self.store_path())?;
        self.ensure_store_identity()?;
        Ok(current_generation != snapshot_generation)
    }

    fn ensure_store_identity(&self) -> Result<(), RuntimeError> {
        ensure_store_identity_matches(&self.store_path(), self.store_identity.as_ref())
    }

    fn invalidate_read_cache(&mut self) {
        self.store_generation = None;
        self.text_lane = None;
        self.vector_lane = None;
    }

    fn ensure_text_lane(&mut self) -> Result<&TextLane, RuntimeError> {
        if self.text_lane.is_none() {
            let root_path = self.root_path();
            let store_path = self.store_path();
            self.text_lane = Some(
                TextLane::load_runtime_with_store_path(&root_path, &self.manifest, &store_path)
                    .map_err(RuntimeError::Storage)?,
            );
        }
        self.text_lane
            .as_ref()
            .ok_or_else(|| RuntimeError::Storage("text lane not materialized".to_owned()))
    }

    fn ensure_vector_lane(&mut self) -> Result<&mut VectorLane, RuntimeError> {
        if self.vector_lane.is_none() {
            let root_path = self.root_path();
            let store_path = self.store_path();
            self.vector_lane = Some(
                VectorLane::load_runtime_with_store_path(
                    &root_path,
                    &self.manifest,
                    &store_path,
                    rax_bench_model::VectorQueryMode::Auto,
                )
                .map_err(RuntimeError::Storage)?,
            );
        }
        self.vector_lane
            .as_mut()
            .ok_or_else(|| RuntimeError::Storage("vector lane not materialized".to_owned()))
    }

    fn live_doc_count(&self) -> Result<usize, RuntimeError> {
        self.docstore
            .document_count()
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))
    }

    fn hydrate_hits(
        &self,
        doc_ids: &[String],
        include_preview: bool,
    ) -> Result<Vec<RuntimeSearchHit>, RuntimeError> {
        if !include_preview {
            return Ok(doc_ids
                .iter()
                .map(|doc_id| RuntimeSearchHit {
                    doc_id: doc_id.clone(),
                    preview: None,
                })
                .collect());
        }

        let documents = self
            .docstore
            .load_documents_by_id(doc_ids)
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
        doc_ids
            .iter()
            .map(|doc_id| {
                let document = documents.get(doc_id).ok_or_else(|| {
                    RuntimeError::Storage(format!(
                        "search hit {doc_id} has no loadable document payload"
                    ))
                })?;
                let preview = document
                    .get("text")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        RuntimeError::Storage(format!(
                            "search hit {doc_id} document payload is missing text"
                        ))
                    })?
                    .to_owned();
                Ok(RuntimeSearchHit {
                    doc_id: doc_id.clone(),
                    preview: Some(preview),
                })
            })
            .collect()
    }
}

impl RuntimeStoreWriter<'_> {
    pub fn publish_raw_documents(
        mut self,
        documents: Vec<NewDocument>,
    ) -> Result<RuntimePublishReport, RuntimeError> {
        let store_path = self.require_existing_store()?;
        if documents.is_empty() {
            return Err(RuntimeError::InvalidRequest(
                "publish_raw_documents requires at least one document".to_owned(),
            ));
        }
        reject_duplicate_doc_ids(
            documents.iter().map(|document| document.doc_id.as_str()),
            "publish_raw_documents",
        )?;

        self.store.ensure_store_identity()?;
        let expected_generation = store_manifest_generation_from_store(&store_path)?;
        self.store.ensure_store_identity()?;
        let documents = self.merged_raw_documents(&store_path, expected_generation, documents)?;
        self.publish_raw_snapshot_with_expected_generation(
            store_path,
            expected_generation,
            documents,
            None,
        )
    }

    pub fn publish_raw_snapshot(
        self,
        documents: Vec<NewDocument>,
        vectors: Option<Vec<NewDocumentVector>>,
    ) -> Result<RuntimePublishReport, RuntimeError> {
        let store_path = self.require_existing_store()?;
        self.store.ensure_store_identity()?;
        let expected_generation = store_manifest_generation_from_store(&store_path)?;
        self.store.ensure_store_identity()?;
        self.publish_raw_snapshot_with_expected_generation(
            store_path,
            expected_generation,
            documents,
            vectors,
        )
    }

    fn publish_raw_snapshot_with_expected_generation(
        self,
        store_path: PathBuf,
        expected_generation: u64,
        documents: Vec<NewDocument>,
        vectors: Option<Vec<NewDocumentVector>>,
    ) -> Result<RuntimePublishReport, RuntimeError> {
        if documents.is_empty() {
            return Err(RuntimeError::InvalidRequest(
                "publish_raw_snapshot requires at least one document".to_owned(),
            ));
        }
        reject_duplicate_doc_ids(
            documents.iter().map(|document| document.doc_id.as_str()),
            "publish_raw_snapshot documents",
        )?;
        self.store.refresh_read_state()?;
        self.store.ensure_store_identity()?;
        ensure_store_generation_unchanged_from_store(&store_path, expected_generation)?;
        self.store.ensure_store_identity()?;
        let remove_existing_vector_segment =
            vectors.is_none() && store_has_vector_segment(&store_path, expected_generation)?;

        let ordered_documents = raw_ordered_documents(&documents);
        let doc_pending =
            rax_docstore::prepare_raw_documents_segment(&store_path, ordered_documents)
                .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
        let mut text_pending = rax_text::prepare_text_segment_from_document_refs(
            documents
                .iter()
                .map(|document| (document.doc_id.as_str(), document.text.as_str())),
        )
        .map_err(RuntimeError::Storage)?;
        text_pending.descriptor.doc_id_start = doc_pending.descriptor.doc_id_start;
        text_pending.descriptor.doc_id_end_exclusive = doc_pending.descriptor.doc_id_end_exclusive;
        let active_doc_id_range =
            doc_pending.descriptor.doc_id_start..doc_pending.descriptor.doc_id_end_exclusive;
        let mut pending_segments = vec![doc_pending, text_pending];
        let mut published_families = vec![RuntimePublishFamily::Doc, RuntimePublishFamily::Text];

        if let Some(vectors) = vectors {
            if vectors.is_empty() {
                return Err(RuntimeError::InvalidRequest(
                    "publish_raw_snapshot vectors must be non-empty when provided".to_owned(),
                ));
            }
            if vectors.len() != documents.len() {
                return Err(RuntimeError::InvalidRequest(format!(
                    "publish_raw_snapshot requires {} vectors to match the provided document set",
                    documents.len()
                )));
            }
            reject_duplicate_doc_ids(
                vectors.iter().map(|vector| vector.doc_id.as_str()),
                "publish_raw_snapshot vectors",
            )?;

            let document_ids = documents
                .iter()
                .map(|document| document.doc_id.clone())
                .collect::<Vec<_>>();
            let document_id_set = document_ids
                .iter()
                .map(String::as_str)
                .collect::<std::collections::HashSet<_>>();
            let missing = vectors
                .iter()
                .filter(|vector| !document_id_set.contains(vector.doc_id.as_str()))
                .map(|vector| vector.doc_id.clone())
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                return Err(RuntimeError::InvalidRequest(format!(
                    "publish_raw_snapshot vectors require matching documents for all doc_ids; missing: {}",
                    summarize_doc_ids(&missing)
                )));
            }

            let active_document_ids =
                rax_docstore::order_document_ids_for_store(&store_path, &document_ids)
                    .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
            let mut vectors_by_doc_id = vectors
                .into_iter()
                .map(|vector| (vector.doc_id, vector.values))
                .collect::<HashMap<_, _>>();
            let vector_inputs = active_document_ids
                .into_iter()
                .map(|doc_id| {
                    let values = vectors_by_doc_id.remove(&doc_id).ok_or_else(|| {
                        RuntimeError::InvalidRequest(format!(
                            "publish_raw_snapshot vectors require matching documents for all doc_ids; missing: {doc_id}"
                        ))
                    })?;
                    Ok((doc_id, values))
                })
                .collect::<Result<Vec<_>, RuntimeError>>()?;
            let mut vector_pending = rax_vector::prepare_raw_vector_segment(
                self.store.manifest.vector_profile.embedding_dimensions as usize,
                &vector_inputs,
            )
            .map_err(RuntimeError::Storage)?;
            vector_pending.descriptor.doc_id_start = active_doc_id_range.start;
            vector_pending.descriptor.doc_id_end_exclusive = active_doc_id_range.end;
            pending_segments.push(vector_pending);
            published_families.push(RuntimePublishFamily::Vector);
        }

        let expected_identity = self.store.store_identity;
        let opened = if remove_existing_vector_segment {
            rax_core::publish_segments_replacing_families_with_precondition(
                &store_path,
                pending_segments,
                &[rax_core::SegmentKind::Vec],
                |manifest| {
                    ensure_store_identity_unchanged_for_publish(
                        &store_path,
                        expected_identity.as_ref(),
                    )?;
                    ensure_store_generation_unchanged(manifest, expected_generation)
                },
            )
        } else {
            rax_core::publish_segments_with_precondition(
                &store_path,
                pending_segments,
                |manifest| {
                    ensure_store_identity_unchanged_for_publish(
                        &store_path,
                        expected_identity.as_ref(),
                    )?;
                    ensure_store_generation_unchanged(manifest, expected_generation)
                },
            )
        }
        .map_err(runtime_core_error)?;

        self.store
            .refresh_read_state_after_committed_publish(opened.manifest.generation);
        Ok(RuntimePublishReport {
            generation: opened.manifest.generation,
            published_families,
        })
    }

    fn merged_raw_documents(
        &mut self,
        store_path: &Path,
        expected_generation: u64,
        documents: Vec<NewDocument>,
    ) -> Result<Vec<NewDocument>, RuntimeError> {
        let mut incoming_order = Vec::with_capacity(documents.len());
        let mut incoming_by_doc_id = std::collections::HashMap::with_capacity(documents.len());
        for document in documents {
            incoming_order.push(document.doc_id.clone());
            incoming_by_doc_id.insert(document.doc_id.clone(), document);
        }
        self.store.ensure_store_identity()?;
        let opened = rax_core::open_store(store_path).map_err(runtime_core_error)?;
        self.store.ensure_store_identity()?;
        ensure_store_generation_unchanged(&opened.manifest, expected_generation)
            .map_err(runtime_core_error)?;
        if latest_doc_segment_identity(&opened.manifest).is_none() {
            return Ok(incoming_order
                .into_iter()
                .filter_map(|doc_id| incoming_by_doc_id.remove(&doc_id))
                .collect());
        }

        self.store.refresh_read_state()?;
        self.store.ensure_store_identity()?;
        ensure_store_generation_unchanged_from_store(store_path, expected_generation)?;
        self.store.ensure_store_identity()?;
        let current_doc_ids = self
            .store
            .docstore
            .load_document_ids()
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
        let retained_doc_ids = current_doc_ids
            .iter()
            .filter(|doc_id| !incoming_by_doc_id.contains_key(doc_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let current_documents = self
            .store
            .docstore
            .load_documents_by_id(&retained_doc_ids)
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
        let mut merged = Vec::with_capacity(current_doc_ids.len() + incoming_by_doc_id.len());
        for doc_id in current_doc_ids {
            if let Some(document) = incoming_by_doc_id.remove(&doc_id) {
                merged.push(document);
            } else if let Some(value) = current_documents.get(&doc_id) {
                merged.push(new_document_from_value(value)?);
            } else {
                return Err(RuntimeError::Storage(format!(
                    "stored document id {doc_id} was listed but could not be loaded"
                )));
            }
        }
        for doc_id in incoming_order {
            if let Some(document) = incoming_by_doc_id.remove(&doc_id) {
                merged.push(document);
            }
        }
        Ok(merged)
    }

    pub fn publish_staged_compatibility_snapshot(
        self,
    ) -> Result<RuntimePublishReport, RuntimeError> {
        let root_path = self.store.root_path();
        let documents = load_compatibility_raw_documents(&root_path, &self.store.manifest)?;
        let vectors = rax_vector::load_compatibility_raw_vectors(&root_path, &self.store.manifest)
            .map_err(RuntimeError::Storage)?
            .into_iter()
            .map(|(doc_id, values)| NewDocumentVector::new(doc_id, values))
            .collect::<Vec<_>>();
        self.publish_raw_snapshot(documents, Some(vectors))
    }

    pub fn import_compatibility_snapshot(self) -> Result<RuntimePublishReport, RuntimeError> {
        self.publish_staged_compatibility_snapshot()
    }

    pub fn publish_raw_vectors(
        self,
        vectors: Vec<NewDocumentVector>,
    ) -> Result<RuntimePublishReport, RuntimeError> {
        let store_path = self.require_existing_store()?;
        if vectors.is_empty() {
            return Err(RuntimeError::InvalidRequest(
                "publish_raw_vectors requires at least one vector".to_owned(),
            ));
        }
        reject_duplicate_doc_ids(
            vectors.iter().map(|vector| vector.doc_id.as_str()),
            "publish_raw_vectors",
        )?;
        self.store.ensure_store_identity()?;
        let opened = rax_core::open_store(&store_path).map_err(runtime_core_error)?;
        self.store.ensure_store_identity()?;
        let expected_generation = opened.manifest.generation;
        let validated_doc_segment = latest_doc_segment_identity(&opened.manifest);
        if validated_doc_segment.is_none() {
            return Err(RuntimeError::InvalidRequest(
                "publish_raw_vectors requires an active document segment; publish documents first"
                    .to_owned(),
            ));
        }
        self.store.refresh_read_state()?;
        self.store.ensure_store_identity()?;
        ensure_store_generation_unchanged_from_store(&store_path, expected_generation)?;
        ensure_doc_segment_unchanged_from_store(&store_path, validated_doc_segment.as_ref())?;
        self.store.ensure_store_identity()?;

        let doc_ids = vectors
            .iter()
            .map(|vector| vector.doc_id.clone())
            .collect::<Vec<_>>();
        let known_documents = self
            .store
            .docstore
            .load_documents_by_id(&doc_ids)
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
        let known_document_count = self
            .store
            .docstore
            .load_document_ids()
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?
            .len();
        if known_documents.len() != vectors.len() {
            let missing = doc_ids
                .into_iter()
                .filter(|doc_id| !known_documents.contains_key(doc_id))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(RuntimeError::InvalidRequest(format!(
                "publish_raw_vectors requires existing documents for all doc_ids; missing: {missing}"
            )));
        }
        if vectors.len() != known_document_count {
            return Err(RuntimeError::InvalidRequest(format!(
                "publish_raw_vectors currently requires {} vectors to match the current document set",
                known_document_count
            )));
        }

        let doc_id_map = self
            .store
            .docstore
            .build_doc_id_map()
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
        let (doc_id_start, doc_id_end_exclusive, vector_inputs) =
            vector_inputs_sorted_by_rax_doc_id(vectors, &doc_id_map)?;

        let mut pending_segment = rax_vector::prepare_raw_vector_segment(
            self.store.manifest.vector_profile.embedding_dimensions as usize,
            &vector_inputs,
        )
        .map_err(RuntimeError::Storage)?;
        pending_segment.descriptor.doc_id_start = doc_id_start;
        pending_segment.descriptor.doc_id_end_exclusive = doc_id_end_exclusive;
        let expected_identity = self.store.store_identity;
        let opened = rax_core::publish_segments_with_precondition(
            &store_path,
            vec![pending_segment],
            |manifest| {
                ensure_store_identity_unchanged_for_publish(
                    &store_path,
                    expected_identity.as_ref(),
                )?;
                ensure_store_generation_unchanged(manifest, expected_generation)?;
                ensure_doc_segment_unchanged(manifest, validated_doc_segment.as_ref())
            },
        )
        .map_err(runtime_core_error)?;

        self.store
            .refresh_read_state_after_committed_publish(opened.manifest.generation);
        Ok(RuntimePublishReport {
            generation: opened.manifest.generation,
            published_families: vec![RuntimePublishFamily::Vector],
        })
    }

    fn require_existing_store(&self) -> Result<PathBuf, RuntimeError> {
        let store_path = self.store.store_path();
        self.store.ensure_store_identity()?;
        if store_manifest_generation_if_present(&store_path)?.is_none() {
            return Err(RuntimeError::InvalidRequest(
                "store.rax is missing; call RuntimeStore::create first".to_owned(),
            ));
        }
        self.store.ensure_store_identity()?;
        Ok(store_path)
    }
}

impl Memory {
    pub fn open(path: &Path) -> Result<Self, RuntimeError> {
        let runtime = RuntimeStore::create_at(path)?;
        Ok(Self {
            runtime,
            embedding_dimensions: DEFAULT_PRODUCT_EMBEDDING_DIMENSIONS,
        })
    }

    pub fn open_existing(path: &Path) -> Result<Self, RuntimeError> {
        Ok(Self {
            runtime: RuntimeStore::open_existing_at(path)?,
            embedding_dimensions: DEFAULT_PRODUCT_EMBEDDING_DIMENSIONS,
        })
    }

    pub fn open_existing_read_only(path: &Path) -> Result<Self, RuntimeError> {
        Ok(Self {
            runtime: RuntimeStore::open_existing_read_only_at(path)?,
            embedding_dimensions: DEFAULT_PRODUCT_EMBEDDING_DIMENSIONS,
        })
    }

    pub fn remember(&mut self, text: impl Into<String>) -> Result<String, RuntimeError> {
        self.save(text, serde_json::json!({}))
    }

    pub fn save(
        &mut self,
        text: impl Into<String>,
        metadata: serde_json::Value,
    ) -> Result<String, RuntimeError> {
        self.save_with_optional_store_size_limit(text, metadata, None)
    }

    pub fn save_with_store_size_limit(
        &mut self,
        text: impl Into<String>,
        metadata: serde_json::Value,
        max_store_bytes: u64,
        fixed_budget_bytes: u64,
    ) -> Result<String, RuntimeError> {
        self.save_with_optional_store_size_limit(
            text,
            metadata,
            Some(MemorySaveBudget {
                max_store_bytes,
                fixed_budget_bytes,
            }),
        )
    }

    fn save_with_optional_store_size_limit(
        &mut self,
        text: impl Into<String>,
        metadata: serde_json::Value,
        budget: Option<MemorySaveBudget>,
    ) -> Result<String, RuntimeError> {
        let text = text.into();
        for attempt in 0..MEMORY_SAVE_MAX_ATTEMPTS {
            match self.save_once(text.clone(), metadata.clone(), budget) {
                Err(error)
                    if is_retryable_memory_save_error(&error)
                        && attempt + 1 < MEMORY_SAVE_MAX_ATTEMPTS =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(
                        MEMORY_SAVE_RETRY_DELAY_MS * (attempt as u64 + 1),
                    ));
                    continue;
                }
                result => return result,
            }
        }
        unreachable!("MEMORY_SAVE_MAX_ATTEMPTS loop always returns");
    }

    fn save_once(
        &mut self,
        text: String,
        metadata: serde_json::Value,
        budget: Option<MemorySaveBudget>,
    ) -> Result<String, RuntimeError> {
        let loaded = load_all_runtime_documents(&mut self.runtime)?;
        let mut documents = loaded.documents;
        let doc_id = next_memory_doc_id(&documents);
        let store_path = self.runtime.store_path();
        self.runtime.ensure_store_identity()?;
        ensure_store_generation_unchanged_from_store(&store_path, loaded.store_generation)?;
        self.runtime.ensure_store_identity()?;
        let mut vectors = match load_memory_vectors(&self.runtime, &documents) {
            Ok(vectors) => vectors,
            Err(_error)
                if store_generation_has_changed(&store_path, loaded.store_generation)
                    .unwrap_or(false) =>
            {
                return Err(RuntimeError::InvalidRequest(
                    STORE_GENERATION_CHANGED_MESSAGE.to_owned(),
                ));
            }
            Err(error) => return Err(error),
        };
        self.runtime.ensure_store_identity()?;
        ensure_store_generation_unchanged_from_store(&store_path, loaded.store_generation)?;
        self.runtime.ensure_store_identity()?;
        let new_document = NewDocument::new(doc_id.clone(), text).with_metadata(metadata);
        if let Some(budget) = budget {
            let payload_bytes = serialized_document_payload_len(&new_document)?;
            enforce_memory_save_store_size_budget(&store_path, budget, payload_bytes)?;
            self.runtime.ensure_store_identity()?;
            ensure_store_generation_unchanged_from_store(&store_path, loaded.store_generation)?;
            self.runtime.ensure_store_identity()?;
        }
        documents.push(new_document);
        let new_document = documents
            .last()
            .expect("new memory document was just appended");
        vectors.push(NewDocumentVector::new(
            new_document.doc_id.clone(),
            rax_bench_model::embed_text(&new_document.text, self.embedding_dimensions as u32),
        ));
        self.runtime
            .writer()?
            .publish_raw_snapshot_with_expected_generation(
                store_path,
                loaded.store_generation,
                documents,
                Some(vectors),
            )?;
        Ok(doc_id)
    }

    pub fn search(
        &mut self,
        query: impl Into<String>,
    ) -> Result<MemorySearchResponse, RuntimeError> {
        self.search_with_options(query, MemorySearchOptions::default())
    }

    pub fn recall(
        &mut self,
        query: impl Into<String>,
    ) -> Result<MemorySearchResponse, RuntimeError> {
        self.search(query)
    }

    pub fn search_with_options(
        &mut self,
        query: impl Into<String>,
        options: MemorySearchOptions,
    ) -> Result<MemorySearchResponse, RuntimeError> {
        let query = query.into();
        let vector_query = matches!(
            options.mode,
            RuntimeSearchMode::Vector | RuntimeSearchMode::Hybrid
        )
        .then(|| rax_bench_model::embed_text(&query, self.embedding_dimensions as u32));
        self.runtime.search(RuntimeSearchRequest {
            mode: options.mode,
            text_query: Some(query),
            vector_query,
            top_k: options.top_k,
            include_preview: options.include_preview,
        })
    }

    pub fn close(&mut self) -> Result<(), RuntimeError> {
        self.runtime.close()
    }
}

fn raw_ordered_documents(documents: &[NewDocument]) -> Vec<(String, serde_json::Value)> {
    documents
        .iter()
        .map(|document| {
            let mut object = document.extra_fields.clone();
            object.insert(
                "doc_id".to_owned(),
                serde_json::Value::String(document.doc_id.clone()),
            );
            object.insert(
                "text".to_owned(),
                serde_json::Value::String(document.text.clone()),
            );
            object.insert("metadata".to_owned(), document.metadata.clone());
            if let Some(timestamp_ms) = document.timestamp_ms {
                object.insert(
                    "timestamp_ms".to_owned(),
                    serde_json::Value::Number(timestamp_ms.into()),
                );
            }
            (document.doc_id.clone(), serde_json::Value::Object(object))
        })
        .collect()
}

fn serialized_document_payload_len(document: &NewDocument) -> Result<u64, RuntimeError> {
    let payload = raw_ordered_documents(std::slice::from_ref(document))
        .into_iter()
        .next()
        .expect("single document payload is always present")
        .1;
    let bytes =
        serde_json::to_vec(&payload).map_err(|error| RuntimeError::Storage(error.to_string()))?;
    Ok(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
}

fn load_all_runtime_documents(
    store: &mut RuntimeStore,
) -> Result<LoadedRuntimeDocuments, RuntimeError> {
    store.refresh_read_state_if_store_generation_changed()?;
    let store_generation = store.store_generation.ok_or_else(|| {
        RuntimeError::InvalidRequest("memory store is missing; reopen the memory store".to_owned())
    })?;
    let doc_ids = store
        .docstore
        .load_document_ids()
        .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
    let documents = store
        .docstore
        .load_documents_by_id(&doc_ids)
        .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
    let documents = doc_ids
        .iter()
        .map(|doc_id| {
            documents.get(doc_id).ok_or_else(|| {
                RuntimeError::Storage(format!(
                    "stored document id {doc_id} was listed but could not be loaded"
                ))
            })
        })
        .map(|document| document.and_then(new_document_from_value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LoadedRuntimeDocuments {
        store_generation,
        documents,
    })
}

fn load_memory_vectors(
    store: &RuntimeStore,
    documents: &[NewDocument],
) -> Result<Vec<NewDocumentVector>, RuntimeError> {
    if documents.is_empty() {
        return Ok(Vec::new());
    }
    let raw_vectors = rax_vector::load_runtime_raw_vectors_with_store_path(
        &store.root_path(),
        &store.manifest,
        &store.store_path(),
    )
    .map_err(RuntimeError::Storage)?;
    if raw_vectors.len() != documents.len() {
        return Err(RuntimeError::Storage(format!(
            "persisted vector row count {} does not match document count {}",
            raw_vectors.len(),
            documents.len()
        )));
    }
    let mut vectors_by_doc_id = std::collections::HashMap::with_capacity(raw_vectors.len());
    for (doc_id, values) in raw_vectors {
        if vectors_by_doc_id.insert(doc_id.clone(), values).is_some() {
            return Err(RuntimeError::Storage(format!(
                "persisted vector doc_id {doc_id} appears more than once"
            )));
        }
    }
    let vectors = documents
        .iter()
        .map(|document| {
            vectors_by_doc_id
                .remove(&document.doc_id)
                .map(|values| NewDocumentVector::new(document.doc_id.clone(), values))
                .ok_or_else(|| {
                    RuntimeError::Storage(format!(
                        "stored document id {} has no persisted vector payload",
                        document.doc_id
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !vectors_by_doc_id.is_empty() {
        let mut extra = vectors_by_doc_id.into_keys().collect::<Vec<_>>();
        extra.sort();
        return Err(RuntimeError::Storage(format!(
            "persisted vectors contain doc_ids not present in documents: {}",
            summarize_doc_ids(&extra)
        )));
    }
    Ok(vectors)
}

fn next_memory_doc_id(documents: &[NewDocument]) -> String {
    let existing = documents
        .iter()
        .map(|document| document.doc_id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let mut index = documents.len() + 1;
    loop {
        let doc_id = format!("mem-{index:016}");
        if !existing.contains(doc_id.as_str()) {
            return doc_id;
        }
        index += 1;
    }
}

fn new_document_from_value(value: &serde_json::Value) -> Result<NewDocument, RuntimeError> {
    let object = value.as_object().ok_or_else(|| {
        RuntimeError::Storage("stored document payload must be a json object".to_owned())
    })?;
    let doc_id = object
        .get("doc_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            RuntimeError::Storage("stored document payload missing doc_id".to_owned())
        })?;
    let text = object
        .get("text")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| RuntimeError::Storage("stored document payload missing text".to_owned()))?;
    let mut document = NewDocument::new(doc_id, text).with_metadata(
        object
            .get("metadata")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
    );
    if let Some(timestamp_ms) = object
        .get("timestamp_ms")
        .and_then(serde_json::Value::as_u64)
    {
        document = document.with_timestamp_ms(timestamp_ms);
    }
    for (key, value) in object {
        if !matches!(
            key.as_str(),
            "doc_id" | "text" | "metadata" | "timestamp_ms"
        ) {
            document = document.with_extra_field(key.clone(), value.clone());
        }
    }
    Ok(document)
}

type SortedVectorInputs = (u64, u64, Vec<(String, Vec<f32>)>);

fn vector_inputs_sorted_by_rax_doc_id(
    vectors: Vec<NewDocumentVector>,
    doc_id_map: &DocIdMap,
) -> Result<SortedVectorInputs, RuntimeError> {
    let mut vector_inputs = vectors
        .into_iter()
        .map(|vector| {
            let rax_doc_id = doc_id_map.rax_doc_id(&vector.doc_id).ok_or_else(|| {
                RuntimeError::Storage(format!("missing rax doc id binding for {}", vector.doc_id))
            })?;
            Ok((rax_doc_id, vector.doc_id, vector.values))
        })
        .collect::<Result<Vec<_>, _>>()?;
    vector_inputs.sort_by_key(|(rax_doc_id, _, _)| *rax_doc_id);
    let doc_id_start = vector_inputs
        .first()
        .map(|(rax_doc_id, _, _)| *rax_doc_id)
        .unwrap_or(0);
    let doc_id_end_exclusive = vector_inputs
        .last()
        .map(|(rax_doc_id, _, _)| {
            rax_doc_id
                .checked_add(1)
                .ok_or_else(|| RuntimeError::Storage("rax_doc_id space exhausted".to_owned()))
        })
        .transpose()?
        .unwrap_or(doc_id_start);
    let vector_inputs = vector_inputs
        .into_iter()
        .map(|(_, doc_id, values)| (doc_id, values))
        .collect();
    Ok((doc_id_start, doc_id_end_exclusive, vector_inputs))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DocSegmentIdentity {
    segment_generation: u64,
    object_offset: u64,
    object_length: u64,
    object_checksum: [u8; 32],
    doc_id_start: u64,
    doc_id_end_exclusive: u64,
}

impl From<&rax_core::SegmentDescriptor> for DocSegmentIdentity {
    fn from(segment: &rax_core::SegmentDescriptor) -> Self {
        Self {
            segment_generation: segment.segment_generation,
            object_offset: segment.object_offset,
            object_length: segment.object_length,
            object_checksum: segment.object_checksum,
            doc_id_start: segment.doc_id_start,
            doc_id_end_exclusive: segment.doc_id_end_exclusive,
        }
    }
}

fn store_has_vector_segment(
    store_path: &Path,
    expected_generation: u64,
) -> Result<bool, RuntimeError> {
    let opened = rax_core::open_store(store_path).map_err(runtime_core_error)?;
    ensure_store_generation_unchanged(&opened.manifest, expected_generation)
        .map_err(runtime_core_error)?;
    Ok(opened
        .manifest
        .segments
        .iter()
        .any(|segment| segment.family == rax_core::SegmentKind::Vec))
}

fn store_manifest_generation_from_store(store_path: &Path) -> Result<u64, RuntimeError> {
    let opened = rax_core::open_store(store_path).map_err(runtime_core_error)?;
    Ok(opened.manifest.generation)
}

fn loaded_store_manifest_generation_from_store(store_path: &Path) -> Result<u64, RuntimeError> {
    store_manifest_generation_from_store(store_path)
}

fn store_manifest_generation_if_present(store_path: &Path) -> Result<Option<u64>, RuntimeError> {
    match std::fs::symlink_metadata(store_path) {
        Ok(_) => store_manifest_generation_from_store(store_path).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(RuntimeError::Storage(error.to_string())),
    }
}

fn loaded_store_manifest_generation_if_present(
    store_path: &Path,
) -> Result<Option<u64>, RuntimeError> {
    match std::fs::symlink_metadata(store_path) {
        Ok(_) => loaded_store_manifest_generation_from_store(store_path).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(RuntimeError::Storage(error.to_string())),
    }
}

#[cfg(target_os = "linux")]
fn stable_created_store_path(
    file: fs::File,
    store_path: &Path,
    expected_identity: Option<&FileIdentity>,
) -> Result<(PathBuf, StableStoreHandle, Option<FileIdentity>), RuntimeError> {
    let identity = expected_identity
        .copied()
        .map(Ok)
        .unwrap_or_else(|| runtime_file_identity_from_file(&file))?;
    drop(file);
    stable_unix_store_path_from_parent(store_path, Some(identity))
}

#[cfg(target_os = "macos")]
fn stable_created_store_path(
    file: fs::File,
    _store_path: &Path,
    expected_identity: Option<&FileIdentity>,
) -> Result<(PathBuf, StableStoreHandle, Option<FileIdentity>), RuntimeError> {
    let identity = expected_identity
        .copied()
        .map(Ok)
        .unwrap_or_else(|| runtime_file_identity_from_file(&file))?;
    stable_macos_store_path_from_open_file(file, Some(identity))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn stable_created_store_path(
    file: fs::File,
    store_path: &Path,
    expected_identity: Option<&FileIdentity>,
) -> Result<(PathBuf, StableStoreHandle, Option<FileIdentity>), RuntimeError> {
    let identity = expected_identity
        .copied()
        .map(Ok)
        .unwrap_or_else(|| runtime_file_identity_from_file(&file))?;
    drop(file);
    Ok((
        store_path.to_path_buf(),
        no_stable_store_handle(),
        Some(identity),
    ))
}

#[cfg(not(unix))]
fn stable_created_store_path(
    file: fs::File,
    store_path: &Path,
    expected_identity: Option<&FileIdentity>,
) -> Result<(PathBuf, StableStoreHandle, Option<FileIdentity>), RuntimeError> {
    let identity = expected_identity
        .copied()
        .map(Ok)
        .unwrap_or_else(|| runtime_file_identity_from_file(&file))?;
    drop(file);
    Ok((
        store_path.to_path_buf(),
        no_stable_store_handle(),
        Some(identity),
    ))
}

fn is_retryable_first_create_open_error(error: &RuntimeError) -> bool {
    matches!(
        error,
        RuntimeError::Storage(message)
            if message.contains("NoValidSuperblock")
                || message.contains("UnexpectedLength")
                || message.contains("failed to fill whole buffer")
    )
}

fn cleanup_created_empty_store_file(path: &Path, expected_identity: Option<&FileIdentity>) {
    let Some(expected_identity) = expected_identity else {
        return;
    };
    if !path_identity_matches(path, expected_identity) {
        return;
    }
    let Ok(opened) = rax_core::open_store(path) else {
        return;
    };
    if opened.manifest.generation == 0 && opened.manifest.segments.is_empty() {
        let _ = fs::remove_file(path);
    }
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct FileIdentity {
    dev: u64,
    ino: u64,
}

#[cfg(windows)]
#[derive(Clone, Copy)]
struct FileIdentity {
    volume_serial_number: u32,
    file_index: u64,
}

#[cfg(not(any(unix, windows)))]
#[derive(Clone, Copy)]
struct FileIdentity;

#[cfg(unix)]
fn file_identity_from_path(path: &Path) -> Result<FileIdentity, std::io::Error> {
    let metadata = fs::metadata(path)?;
    Ok(FileIdentity {
        dev: metadata.dev(),
        ino: metadata.ino(),
    })
}

#[cfg(unix)]
fn file_identity_from_file(file: &fs::File) -> Result<FileIdentity, std::io::Error> {
    let metadata = file.metadata()?;
    Ok(FileIdentity {
        dev: metadata.dev(),
        ino: metadata.ino(),
    })
}

#[cfg(windows)]
fn file_identity_from_path(path: &Path) -> Result<FileIdentity, std::io::Error> {
    let file = fs::File::open(path)?;
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    let result =
        unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), info.as_mut_ptr()) };
    if result == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let info = unsafe { info.assume_init() };
    Ok(FileIdentity {
        volume_serial_number: info.dwVolumeSerialNumber,
        file_index: ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64,
    })
}

#[cfg(windows)]
fn file_identity_from_file(file: &fs::File) -> Result<FileIdentity, std::io::Error> {
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    let result =
        unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), info.as_mut_ptr()) };
    if result == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let info = unsafe { info.assume_init() };
    Ok(FileIdentity {
        volume_serial_number: info.dwVolumeSerialNumber,
        file_index: ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64,
    })
}

#[cfg(not(any(unix, windows)))]
fn file_identity_from_path(_path: &Path) -> Result<FileIdentity, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "stable file identity is unsupported on this platform",
    ))
}

#[cfg(not(any(unix, windows)))]
fn file_identity_from_file(_file: &fs::File) -> Result<FileIdentity, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "stable file identity is unsupported on this platform",
    ))
}

fn runtime_file_identity_from_file(file: &fs::File) -> Result<FileIdentity, RuntimeError> {
    #[cfg(test)]
    if FILE_IDENTITY_FROM_FILE_SHOULD_FAIL.with(Cell::get) {
        return Err(RuntimeError::Storage(
            "injected file identity failure".to_owned(),
        ));
    }
    file_identity_from_file(file).map_err(|error| RuntimeError::Storage(error.to_string()))
}

#[cfg(unix)]
fn path_identity_matches(path: &Path, expected: &FileIdentity) -> bool {
    file_identity_from_path(path)
        .map(|current| current.dev == expected.dev && current.ino == expected.ino)
        .unwrap_or(false)
}

#[cfg(windows)]
fn path_identity_matches(path: &Path, expected: &FileIdentity) -> bool {
    file_identity_from_path(path)
        .map(|current| {
            current.volume_serial_number == expected.volume_serial_number
                && current.file_index == expected.file_index
        })
        .unwrap_or(false)
}

#[cfg(not(any(unix, windows)))]
fn path_identity_matches(path: &Path, expected: &FileIdentity) -> bool {
    let _ = (path, expected);
    false
}

fn ensure_store_identity_matches(
    path: &Path,
    expected_identity: Option<&FileIdentity>,
) -> Result<(), RuntimeError> {
    match expected_identity {
        Some(expected) => {
            if !path_identity_matches(path, expected) {
                return Err(RuntimeError::Storage(format!(
                    "store file identity changed after open: {}",
                    path.display()
                )));
            }
        }
        None => match fs::symlink_metadata(path) {
            Ok(_) => {
                return Err(RuntimeError::Storage(format!(
                    "store file appeared without a pinned identity; reopen before using {}",
                    path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(RuntimeError::Storage(error.to_string())),
        },
    }
    Ok(())
}

fn ensure_store_identity_unchanged_for_publish(
    path: &Path,
    expected_identity: Option<&FileIdentity>,
) -> Result<(), rax_core::CoreError> {
    let Some(expected_identity) = expected_identity else {
        return Err(rax_core::CoreError::PublishPreconditionFailed(
            "store file identity was not pinned before publish; reopen before writing".to_owned(),
        ));
    };
    if !path_identity_matches(path, expected_identity) {
        return Err(rax_core::CoreError::PublishPreconditionFailed(
            "store file identity changed before publish".to_owned(),
        ));
    }
    Ok(())
}

fn latest_doc_segment_identity(manifest: &rax_core::ActiveManifest) -> Option<DocSegmentIdentity> {
    manifest
        .segments
        .iter()
        .filter(|segment| segment.family == rax_core::SegmentKind::Doc)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
        .map(DocSegmentIdentity::from)
}

fn ensure_doc_segment_unchanged(
    manifest: &rax_core::ActiveManifest,
    expected: Option<&DocSegmentIdentity>,
) -> Result<(), rax_core::CoreError> {
    let current = latest_doc_segment_identity(manifest);
    if current.as_ref() == expected {
        return Ok(());
    }

    Err(rax_core::CoreError::PublishPreconditionFailed(
        "publish_raw_vectors document generation changed before vector publish; retry with latest documents"
            .to_owned(),
    ))
}

fn ensure_doc_segment_unchanged_from_store(
    store_path: &Path,
    expected: Option<&DocSegmentIdentity>,
) -> Result<(), RuntimeError> {
    let opened = rax_core::open_store(store_path).map_err(runtime_core_error)?;
    ensure_doc_segment_unchanged(&opened.manifest, expected).map_err(runtime_core_error)
}

fn ensure_store_generation_unchanged_from_store(
    store_path: &Path,
    expected: u64,
) -> Result<(), RuntimeError> {
    let opened = rax_core::open_store(store_path).map_err(runtime_core_error)?;
    ensure_store_generation_unchanged(&opened.manifest, expected).map_err(runtime_core_error)
}

fn store_generation_has_changed(store_path: &Path, expected: u64) -> Result<bool, RuntimeError> {
    let opened = rax_core::open_store(store_path).map_err(runtime_core_error)?;
    Ok(opened.manifest.generation != expected)
}

fn enforce_memory_save_store_size_budget(
    store_path: &Path,
    budget: MemorySaveBudget,
    payload_bytes: u64,
) -> Result<(), RuntimeError> {
    let current_len = fs::metadata(store_path)
        .map_err(|error| RuntimeError::Storage(error.to_string()))?
        .len();
    let projected_len = current_len
        .saturating_mul(2)
        .saturating_add(payload_bytes.saturating_mul(8))
        .saturating_add(budget.fixed_budget_bytes);
    if current_len > budget.max_store_bytes || projected_len > budget.max_store_bytes {
        return Err(RuntimeError::InvalidRequest(format!(
            "memory store write would exceed {} bytes",
            budget.max_store_bytes
        )));
    }
    Ok(())
}

fn ensure_store_generation_unchanged(
    manifest: &rax_core::ActiveManifest,
    expected: u64,
) -> Result<(), rax_core::CoreError> {
    if manifest.generation == expected {
        return Ok(());
    }

    Err(rax_core::CoreError::PublishPreconditionFailed(
        STORE_GENERATION_CHANGED_MESSAGE.to_owned(),
    ))
}

fn runtime_core_error(error: rax_core::CoreError) -> RuntimeError {
    match error {
        rax_core::CoreError::PublishPreconditionFailed(message) => {
            RuntimeError::InvalidRequest(message)
        }
        other => RuntimeError::Storage(other.to_string()),
    }
}

fn is_retryable_memory_save_error(error: &RuntimeError) -> bool {
    matches!(
        error,
        RuntimeError::InvalidRequest(message)
            if message == STORE_GENERATION_CHANGED_MESSAGE
                || message == STORE_PUBLISH_LOCK_BUSY_MESSAGE
    )
}

fn hybrid_text_candidate_limit(top_k: usize, live_doc_count: usize) -> usize {
    if top_k == 0 || live_doc_count == 0 {
        return 0;
    }
    live_doc_count.min(top_k.saturating_mul(10).max(100))
}

fn reject_duplicate_doc_ids<'a>(
    doc_ids: impl IntoIterator<Item = &'a str>,
    context: &str,
) -> Result<(), RuntimeError> {
    let mut seen = std::collections::HashSet::new();
    let mut duplicates = std::collections::BTreeSet::new();
    for doc_id in doc_ids {
        if !seen.insert(doc_id) {
            duplicates.insert(doc_id.to_owned());
        }
    }
    if duplicates.is_empty() {
        Ok(())
    } else {
        Err(RuntimeError::InvalidRequest(format!(
            "{context} received duplicate doc_ids: {}",
            duplicates.into_iter().collect::<Vec<_>>().join(", ")
        )))
    }
}

fn summarize_doc_ids(doc_ids: &[String]) -> String {
    const MAX_SHOWN: usize = 5;

    let shown = doc_ids
        .iter()
        .take(MAX_SHOWN)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let remaining = doc_ids.len().saturating_sub(MAX_SHOWN);
    if remaining == 0 {
        shown
    } else {
        format!("{shown} (+{remaining} more)")
    }
}

fn load_compatibility_raw_documents(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<Vec<NewDocument>, RuntimeError> {
    let documents_path = manifest
        .files
        .iter()
        .find(|file| file.kind == "documents")
        .map(|file| manifest_file_path(mount_root, file))
        .transpose()?
        .ok_or_else(|| RuntimeError::Storage("documents file missing from manifest".to_owned()))?;
    BufReader::new(
        rax_core::open_file_read_no_symlinks(&documents_path).map_err(runtime_core_error)?,
    )
    .lines()
    .filter_map(|line| match line {
        Ok(line) if line.trim().is_empty() => None,
        other => Some(other),
    })
    .map(|line| {
        let line = line.map_err(|error| RuntimeError::Storage(error.to_string()))?;
        let value: serde_json::Value = serde_json::from_str(&line)
            .map_err(|error| RuntimeError::Storage(error.to_string()))?;
        let object = value.as_object().ok_or_else(|| {
            RuntimeError::Storage("document line must be a json object".to_owned())
        })?;
        let mut document = NewDocument::new(
            object
                .get("doc_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| RuntimeError::Storage("document line missing doc_id".to_owned()))?,
            object
                .get("text")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| RuntimeError::Storage("document line missing text".to_owned()))?,
        )
        .with_metadata(
            object
                .get("metadata")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
        );
        if let Some(timestamp_ms) = object
            .get("timestamp_ms")
            .and_then(serde_json::Value::as_u64)
        {
            document = document.with_timestamp_ms(timestamp_ms);
        }
        for (key, value) in object {
            if !matches!(
                key.as_str(),
                "doc_id" | "text" | "metadata" | "timestamp_ms"
            ) {
                document = document.with_extra_field(key.clone(), value.clone());
            }
        }
        Ok(document)
    })
    .collect()
}

fn apple_acceleration_capability() -> RuntimeAccelerationCapability {
    if cfg!(target_os = "macos") || cfg!(target_os = "ios") {
        RuntimeAccelerationCapability {
            family: RuntimePlatformAccelerationFamily::Apple,
            availability: RuntimeAccelerationAvailability::BackendNotCompiled,
            detail: Some("apple acceleration backend is not linked in this build".to_owned()),
        }
    } else {
        RuntimeAccelerationCapability {
            family: RuntimePlatformAccelerationFamily::Apple,
            availability: RuntimeAccelerationAvailability::UnsupportedPlatform,
            detail: Some("apple acceleration requires an Apple platform runtime".to_owned()),
        }
    }
}

fn read_manifest(root: &Path) -> Result<DatasetPackManifest, RuntimeError> {
    let manifest_bytes =
        rax_core::read_file_no_symlinks(&root.join("manifest.json")).map_err(runtime_core_error)?;
    let manifest_text = String::from_utf8(manifest_bytes)
        .map_err(|error| RuntimeError::Storage(error.to_string()))?;
    serde_json_fallback_parse_manifest(&manifest_text)
}

fn validate_prebuilt_store_segments_against_dataset_pack(
    root: &Path,
    manifest: &DatasetPackManifest,
    store_path: &Path,
) -> Result<(), RuntimeError> {
    if manifest_file_by_kind(manifest, "prebuilt_store").is_none()
        || manifest_file_by_kind(manifest, "store").is_some()
    {
        return Ok(());
    }
    if store_manifest_generation_if_present(store_path)?.is_none() {
        return Err(RuntimeError::Storage(format!(
            "manifest prebuilt_store {} is missing or unreadable",
            store_path.display()
        )));
    }
    rax_docstore::validate_store_segment_against_dataset_pack_with_store_path(
        store_path, root, manifest,
    )
    .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
    rax_text::validate_store_segment_against_dataset_pack_with_store_path(
        store_path, root, manifest,
    )
    .map_err(RuntimeError::Storage)?;
    rax_vector::validate_store_segment_against_dataset_pack_with_store_path(
        store_path, root, manifest,
    )
    .map_err(RuntimeError::Storage)?;
    Ok(())
}

#[cfg(any(unix, windows))]
fn ensure_stable_store_paths_supported() -> Result<(), RuntimeError> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn ensure_stable_store_paths_supported() -> Result<(), RuntimeError> {
    Err(RuntimeError::Storage(
        "runtime store paths require stable file identity support on this platform".to_owned(),
    ))
}

fn product_store_root(path: &Path) -> Result<PathBuf, RuntimeError> {
    let root = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    #[cfg(target_os = "macos")]
    if let Some(root) = macos_private_alias_path(&root) {
        return Ok(root);
    }
    Ok(root)
}

fn product_store_path_under_root(root: &Path, path: &Path) -> Result<PathBuf, RuntimeError> {
    let file_name = path.file_name().ok_or_else(|| {
        RuntimeError::InvalidRequest("memory store path must include a file name".to_owned())
    })?;
    if root == Path::new(".") {
        return Ok(PathBuf::from(file_name));
    }
    Ok(root.join(Path::new(file_name)))
}

#[cfg(unix)]
fn stable_unix_store_path_from_parent(
    path: &Path,
    identity: Option<FileIdentity>,
) -> Result<(PathBuf, StableStoreHandle, Option<FileIdentity>), RuntimeError> {
    #[cfg(target_os = "linux")]
    let file_name = path.file_name().ok_or_else(|| {
        RuntimeError::InvalidRequest("memory store path must include a file name".to_owned())
    })?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let handle = open_unix_directory_no_follow(parent)?;
    #[cfg(target_os = "linux")]
    let stable_path = stable_fd_directory_path(handle.as_raw_fd()).join(file_name);
    #[cfg(not(target_os = "linux"))]
    let stable_path = path.to_path_buf();
    if identity
        .as_ref()
        .is_some_and(|expected| !path_identity_matches(&stable_path, expected))
    {
        return Err(RuntimeError::Storage(format!(
            "store file identity changed after opening parent directory: {}",
            path.display()
        )));
    }
    Ok((stable_path, Some(handle), identity))
}

#[cfg(target_os = "macos")]
fn stable_macos_store_path_from_open_file(
    file: fs::File,
    identity: Option<FileIdentity>,
) -> Result<(PathBuf, StableStoreHandle, Option<FileIdentity>), RuntimeError> {
    let path = macos_path_from_file(&file)?;
    drop(file);
    stable_unix_store_path_from_parent(&path, identity)
}

#[cfg(target_os = "macos")]
fn macos_store_path_from_parent_handle(
    current_path: &Path,
    handle: &StableStoreHandle,
) -> Option<PathBuf> {
    let handle = handle.as_ref()?;
    let file_name = current_path.file_name()?;
    macos_path_from_file(handle)
        .ok()
        .map(|parent| parent.join(file_name))
}

#[cfg(target_os = "macos")]
fn macos_path_from_file(file: &fs::File) -> Result<PathBuf, RuntimeError> {
    let mut buffer = [0 as libc::c_char; libc::PATH_MAX as usize];
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETPATH, buffer.as_mut_ptr()) };
    if result != 0 {
        return Err(RuntimeError::Storage(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    let bytes = unsafe { CStr::from_ptr(buffer.as_ptr()) }
        .to_bytes()
        .to_vec();
    Ok(PathBuf::from(std::ffi::OsString::from_vec(bytes)))
}

#[cfg(unix)]
fn open_unix_directory_no_follow(path: &Path) -> Result<fs::File, RuntimeError> {
    let bytes = path.as_os_str().as_bytes();
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(RuntimeError::InvalidRequest(
            "store parent directory path must be non-empty and must not contain NUL".to_owned(),
        ));
    }
    let c_path =
        CString::new(bytes).map_err(|error| RuntimeError::InvalidRequest(error.to_string()))?;
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(RuntimeError::Storage(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    Ok(unsafe { fs::File::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn stable_fd_directory_path(fd: std::os::fd::RawFd) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{fd}"))
}

#[cfg(unix)]
fn is_direct_stable_store_fd_path(path: &Path) -> bool {
    path.parent().is_some_and(is_stable_fd_table_root)
        && path.file_name().is_some_and(|file_name| {
            file_name.to_str().is_some_and(|value| {
                !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
            })
        })
}

#[cfg(not(unix))]
fn is_direct_stable_store_fd_path(_path: &Path) -> bool {
    false
}

#[cfg(unix)]
fn no_stable_store_handle() -> StableStoreHandle {
    None
}

#[cfg(not(unix))]
fn no_stable_store_handle() -> StableStoreHandle {
    None
}

#[cfg(test)]
fn set_search_generation_race_hook(hook: impl FnOnce() + Send + 'static) {
    SEARCH_GENERATION_RACE_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
fn run_search_generation_race_hook() {
    let hook = SEARCH_GENERATION_RACE_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn set_search_post_hydrate_race_hook(hook: impl FnOnce() + Send + 'static) {
    SEARCH_POST_HYDRATE_RACE_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
fn run_search_post_hydrate_race_hook() {
    let hook = SEARCH_POST_HYDRATE_RACE_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn set_first_create_post_create_hook(hook: impl FnOnce() + Send + 'static) {
    FIRST_CREATE_POST_CREATE_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
fn run_first_create_post_create_hook() {
    let hook = FIRST_CREATE_POST_CREATE_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(not(test))]
fn run_first_create_post_create_hook() {}

#[cfg(test)]
struct FileIdentityFromFileFailureGuard {
    previous: bool,
}

#[cfg(test)]
impl Drop for FileIdentityFromFileFailureGuard {
    fn drop(&mut self) {
        FILE_IDENTITY_FROM_FILE_SHOULD_FAIL.with(|flag| flag.set(self.previous));
    }
}

#[cfg(test)]
fn file_identity_from_file_failure_guard() -> FileIdentityFromFileFailureGuard {
    let previous = FILE_IDENTITY_FROM_FILE_SHOULD_FAIL.with(|flag| {
        let previous = flag.get();
        flag.set(true);
        previous
    });
    FileIdentityFromFileFailureGuard { previous }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProductStoreOpenMode {
    ReadOnly,
    ReadWrite,
}

fn stable_product_store_path(
    path: &Path,
    mode: ProductStoreOpenMode,
) -> Result<StableProductStorePath, RuntimeError> {
    stable_existing_store_path(path, mode)
}

fn stable_store_path_if_present(
    path: &Path,
    mode: ProductStoreOpenMode,
) -> Result<StableProductStorePath, RuntimeError> {
    match fs::symlink_metadata(path) {
        Ok(_) => stable_existing_store_path(path, mode),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(StableProductStorePath {
            path: path.to_path_buf(),
            handle: no_stable_store_handle(),
            identity: None,
        }),
        Err(error) => Err(RuntimeError::Storage(error.to_string())),
    }
}

fn stable_existing_store_path(
    path: &Path,
    mode: ProductStoreOpenMode,
) -> Result<StableProductStorePath, RuntimeError> {
    if is_direct_stable_store_fd_path(path) {
        return Err(RuntimeError::InvalidRequest(
            "direct file-descriptor store paths are unsupported; open the containing fd directory plus file name instead".to_owned(),
        ));
    }
    #[cfg(target_os = "linux")]
    if let Some(stable_path) = stable_linux_proc_fd_child_store_path(path, mode)? {
        return Ok(stable_path);
    }
    let file = match mode {
        ProductStoreOpenMode::ReadOnly => {
            rax_core::open_file_read_no_symlinks(path).map_err(runtime_core_error)?
        }
        ProductStoreOpenMode::ReadWrite => {
            rax_core::open_file_readwrite_no_symlinks(path).map_err(runtime_core_error)?
        }
    };
    #[cfg(target_os = "linux")]
    {
        let identity = runtime_file_identity_from_file(&file)?;
        drop(file);
        let (path, handle, identity) = stable_unix_store_path_from_parent(path, Some(identity))?;
        Ok(StableProductStorePath {
            path,
            handle,
            identity,
        })
    }
    #[cfg(target_os = "macos")]
    {
        let identity = runtime_file_identity_from_file(&file)?;
        let (path, handle, identity) =
            stable_macos_store_path_from_open_file(file, Some(identity))?;
        Ok(StableProductStorePath {
            path,
            handle,
            identity,
        })
    }
    #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
    {
        let identity = runtime_file_identity_from_file(&file)?;
        drop(file);
        Ok(StableProductStorePath {
            path: path.to_path_buf(),
            handle: no_stable_store_handle(),
            identity: Some(identity),
        })
    }
    #[cfg(not(unix))]
    {
        let identity = runtime_file_identity_from_file(&file)?;
        drop(file);
        Ok(StableProductStorePath {
            path: path.to_path_buf(),
            handle: no_stable_store_handle(),
            identity: Some(identity),
        })
    }
}

#[cfg(target_os = "linux")]
fn stable_linux_proc_fd_child_store_path(
    path: &Path,
    mode: ProductStoreOpenMode,
) -> Result<Option<StableProductStorePath>, RuntimeError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let Some(handle) = duplicate_linux_proc_self_fd_root(parent)? else {
        return Ok(None);
    };
    let file_name = path.file_name().ok_or_else(|| {
        RuntimeError::InvalidRequest("memory store path must include a file name".to_owned())
    })?;
    let stable_path = stable_fd_directory_path(handle.as_raw_fd()).join(file_name);
    let file = match mode {
        ProductStoreOpenMode::ReadOnly => {
            rax_core::open_file_read_no_symlinks(&stable_path).map_err(runtime_core_error)?
        }
        ProductStoreOpenMode::ReadWrite => {
            rax_core::open_file_readwrite_no_symlinks(&stable_path).map_err(runtime_core_error)?
        }
    };
    let identity = runtime_file_identity_from_file(&file)?;
    drop(file);
    Ok(Some(StableProductStorePath {
        path: stable_path,
        handle: Some(handle),
        identity: Some(identity),
    }))
}

fn stable_runtime_root(root: &Path) -> Result<StableRuntimeRoot, RuntimeError> {
    #[cfg(target_os = "macos")]
    let macos_private_alias_path = macos_private_alias_path(root);
    #[cfg(target_os = "macos")]
    let root = macos_private_alias_path.as_deref().unwrap_or(root);
    #[cfg(unix)]
    {
        #[cfg(target_os = "linux")]
        if let Some(handle) = duplicate_linux_proc_self_fd_root(root)? {
            let path = stable_fd_directory_path(handle.as_raw_fd());
            return Ok(StableRuntimeRoot {
                path,
                handle: Some(handle),
            });
        }
        let handle = open_unix_directory_no_follow(root)?;
        #[cfg(target_os = "linux")]
        let path = stable_fd_directory_path(handle.as_raw_fd());
        #[cfg(target_os = "macos")]
        let path = macos_path_from_file(&handle)?;
        #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
        let path = root.to_path_buf();
        Ok(StableRuntimeRoot {
            path,
            handle: Some(handle),
        })
    }
    #[cfg(not(unix))]
    Ok(StableRuntimeRoot {
        path: root.to_path_buf(),
        #[cfg(unix)]
        handle: None,
    })
}

#[cfg(target_os = "macos")]
fn macos_private_alias_path(path: &Path) -> Option<PathBuf> {
    if let Ok(rest) = path.strip_prefix("/tmp") {
        Some(Path::new("/private/tmp").join(rest))
    } else if let Ok(rest) = path.strip_prefix("/var") {
        Some(Path::new("/private/var").join(rest))
    } else {
        None
    }
}

fn stable_runtime_root_current_path(root: &StableRuntimeRoot) -> PathBuf {
    #[cfg(target_os = "macos")]
    if let Some(handle) = root.handle.as_ref() {
        if let Ok(path) = macos_path_from_file(handle) {
            return path;
        }
    }
    root.path.clone()
}

#[cfg(target_os = "linux")]
fn duplicate_linux_proc_self_fd_root(root: &Path) -> Result<Option<fs::File>, RuntimeError> {
    let Some((fd, tail_components)) = linux_proc_self_fd_number_and_tail(root)? else {
        return Ok(None);
    };
    let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicated < 0 {
        return Err(RuntimeError::InvalidRequest(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    let mut dir = file_from_linux_directory_fd(duplicated)?;
    for component in tail_components {
        dir = open_linux_child_dir_no_follow(&dir, component)?;
    }
    Ok(Some(dir))
}

#[cfg(target_os = "linux")]
fn linux_proc_self_fd_number_and_tail(
    path: &Path,
) -> Result<Option<(libc::c_int, Vec<&OsStr>)>, RuntimeError> {
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::RootDir)) {
        return Ok(None);
    }
    for expected in ["proc", "self", "fd"] {
        match components.next() {
            Some(Component::Normal(component)) if component == OsStr::new(expected) => {}
            _ => return Ok(None),
        }
    }
    let Some(Component::Normal(fd)) = components.next() else {
        return Ok(None);
    };
    let Some(fd) = fd.to_str() else {
        return Err(RuntimeError::InvalidRequest(
            "/proc/self/fd component must be valid UTF-8".to_owned(),
        ));
    };
    if fd.is_empty() || !fd.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RuntimeError::InvalidRequest(
            "/proc/self/fd component must be a non-negative file descriptor".to_owned(),
        ));
    }
    let fd = fd.parse::<libc::c_int>().map_err(|_| {
        RuntimeError::InvalidRequest("/proc/self/fd component is out of range".to_owned())
    })?;
    let mut tail = Vec::new();
    for component in components {
        match component {
            Component::Normal(component) => tail.push(component),
            Component::CurDir => {}
            _ => {
                return Err(RuntimeError::InvalidRequest(
                    "/proc/self/fd root tail must not contain parent or prefix components"
                        .to_owned(),
                ));
            }
        }
    }
    Ok(Some((fd, tail)))
}

#[cfg(target_os = "linux")]
fn open_linux_child_dir_no_follow(
    parent: &fs::File,
    name: &OsStr,
) -> Result<fs::File, RuntimeError> {
    let mut bytes = name.as_bytes().to_vec();
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(RuntimeError::InvalidRequest(
            "/proc/self/fd root tail contains an invalid path component".to_owned(),
        ));
    }
    bytes.push(0);
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            bytes.as_ptr().cast(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    file_from_linux_directory_fd(fd)
}

#[cfg(target_os = "linux")]
fn file_from_linux_directory_fd(fd: libc::c_int) -> Result<fs::File, RuntimeError> {
    if fd < 0 {
        return Err(RuntimeError::InvalidRequest(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(RuntimeError::InvalidRequest(error.to_string()));
    }
    let stat = unsafe { stat.assume_init() };
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFDIR {
        unsafe {
            libc::close(fd);
        }
        return Err(RuntimeError::InvalidRequest(
            "/proc/self/fd root must resolve to a directory".to_owned(),
        ));
    }
    Ok(unsafe { fs::File::from_raw_fd(fd) })
}

fn product_manifest(root: &Path, store_path: &Path) -> Result<DatasetPackManifest, RuntimeError> {
    let store_file = store_path.strip_prefix(root).unwrap_or(store_path);
    Ok(DatasetPackManifest {
        schema_version: "rax-product".to_owned(),
        generated_at: "product-runtime".to_owned(),
        generator: ManifestGenerator {
            name: "rax".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
        },
        identity: DatasetIdentity {
            dataset_id: store_path.display().to_string(),
            dataset_version: "current".to_owned(),
            dataset_family: "memory".to_owned(),
            dataset_tier: "product".to_owned(),
            variant_id: "live".to_owned(),
            embedding_spec_id: "rax-deterministic".to_owned(),
            embedding_model_version: "1".to_owned(),
            embedding_model_hash: "runtime".to_owned(),
            corpus_checksum: "runtime".to_owned(),
            query_checksum: "runtime".to_owned(),
        },
        environment_constraints: EnvironmentConstraints {
            min_ram_gb: 1,
            recommended_ram_gb: 1,
            notes: Some("product memory store".to_owned()),
        },
        corpus: CorpusProfile {
            doc_count: 0,
            vector_count: 0,
            total_text_bytes: 0,
            avg_doc_length: 0.0,
            median_doc_length: 0,
            p95_doc_length: 0,
            max_doc_length: 0,
            languages: Vec::new(),
        },
        text_profile: TextProfile {
            length_buckets: LengthBuckets {
                short_ratio: 0.0,
                medium_ratio: 0.0,
                long_ratio: 0.0,
            },
            tokenization_notes: Some("runtime text segment".to_owned()),
        },
        metadata_profile: MetadataProfile {
            facets: Vec::new(),
            selectivity_exemplars: SelectivityExemplars {
                broad: String::new(),
                medium: String::new(),
                narrow: String::new(),
                zero_hit: String::new(),
            },
        },
        vector_profile: VectorProfile {
            enabled: true,
            embedding_dimensions: DEFAULT_PRODUCT_EMBEDDING_DIMENSIONS as u32,
            embedding_dtype: "f32".to_owned(),
            distance_metric: "cosine".to_owned(),
            ann_index_backend: None,
            ann_sidecar_reproducibility: None,
            query_vectors: QueryVectorProfile {
                precomputed_available: false,
                runtime_embedding_supported: true,
            },
        },
        dirty_profile: DirtyProfile {
            profile: "clean".to_owned(),
            base_dataset_id: None,
            seed: 0,
            delete_ratio: 0.0,
            update_ratio: 0.0,
            append_ratio: 0.0,
            target_segment_count_range: [1, 1],
            target_segment_topology: Vec::new(),
            target_tombstone_ratio: 0.0,
            compaction_state: "none".to_owned(),
        },
        files: vec![ManifestFile {
            path: store_file.display().to_string(),
            kind: "store".to_owned(),
            format: "rax".to_owned(),
            record_count: 1,
            checksum: "runtime".to_owned(),
        }],
        query_sets: Vec::new(),
        checksums: ManifestChecksums {
            manifest_payload_checksum: "runtime".to_owned(),
            logical_documents_checksum: "runtime".to_owned(),
            logical_metadata_checksum: "runtime".to_owned(),
            logical_query_definitions_checksum: "runtime".to_owned(),
            logical_vector_payload_checksum: None,
            fairness_fingerprint: "runtime".to_owned(),
        },
    })
}

fn store_path_from_manifest(
    root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<PathBuf, RuntimeError> {
    manifest_file_by_kind(manifest, "store")
        .or_else(|| manifest_file_by_kind(manifest, "prebuilt_store"))
        .map(|file| manifest_file_path(root, file))
        .unwrap_or_else(|| Ok(root.join("store.rax")))
}

fn writable_store_path_from_manifest(
    root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<PathBuf, RuntimeError> {
    if let Some(file) = manifest.files.iter().find(|file| file.kind == "store") {
        return manifest_file_path(root, file);
    }
    if let Some(file) = manifest
        .files
        .iter()
        .find(|file| file.kind == "prebuilt_store")
    {
        manifest_file_path(root, file)?;
        return Err(RuntimeError::InvalidRequest(format!(
            "manifest prebuilt_store {} is read-only; declare a store file before creating a writable runtime store",
            root.join(&file.path).display()
        )));
    }
    Ok(root.join("store.rax"))
}

fn manifest_file_path(
    root: &Path,
    file: &rax_bench_model::ManifestFile,
) -> Result<PathBuf, RuntimeError> {
    let path = Path::new(&file.path);
    if !is_pack_relative_path(path) {
        return Err(RuntimeError::InvalidRequest(format!(
            "manifest {} path {} must stay within dataset root",
            file.kind, file.path
        )));
    }
    root_confined_path(root, path, &file.kind)
}

fn is_pack_relative_path(path: &Path) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn root_confined_path(root: &Path, path: &Path, kind: &str) -> Result<PathBuf, RuntimeError> {
    if matches!(kind, "store" | "prebuilt_store")
        && is_stable_fd_table_root(root)
        && is_single_numeric_path_component(path)
    {
        return Ok(root.join(path));
    }
    if is_linux_proc_self_fd_root(root) {
        reject_symlink_components(root, path, kind)?;
        return Ok(root.join(path));
    }
    let Ok(root) = root.canonicalize() else {
        return Ok(root.join(path));
    };
    reject_symlink_components(&root, path, kind)?;
    let candidate = root.join(path);
    if candidate.exists() {
        let canonical = candidate
            .canonicalize()
            .map_err(|error| RuntimeError::InvalidRequest(error.to_string()))?;
        if !canonical.starts_with(&root) {
            return Err(RuntimeError::InvalidRequest(format!(
                "manifest {kind} path {} resolves outside dataset root {}",
                candidate.display(),
                root.display()
            )));
        }
        return Ok(canonical);
    }
    let mut ancestor = candidate.parent();
    while let Some(path) = ancestor {
        if path.exists() {
            let canonical = path
                .canonicalize()
                .map_err(|error| RuntimeError::InvalidRequest(error.to_string()))?;
            if !canonical.starts_with(&root) {
                return Err(RuntimeError::InvalidRequest(format!(
                    "manifest {kind} ancestor {} resolves outside dataset root {}",
                    path.display(),
                    root.display()
                )));
            }
            return Ok(candidate);
        }
        ancestor = path.parent();
    }
    if !candidate.starts_with(&root) {
        return Err(RuntimeError::InvalidRequest(format!(
            "manifest {kind} path {} is outside dataset root {}",
            candidate.display(),
            root.display()
        )));
    }
    Ok(candidate)
}

#[cfg(unix)]
fn is_stable_fd_table_root(path: &Path) -> bool {
    path_has_absolute_normal_components(path, stable_fd_table_components())
}

#[cfg(not(unix))]
fn is_stable_fd_table_root(_path: &Path) -> bool {
    false
}

#[cfg(target_os = "linux")]
fn stable_fd_table_components() -> &'static [&'static str] {
    &["proc", "self", "fd"]
}

#[cfg(all(unix, not(target_os = "linux")))]
fn stable_fd_table_components() -> &'static [&'static str] {
    &["dev", "fd"]
}

#[cfg(unix)]
fn path_has_absolute_normal_components(path: &Path, expected_components: &[&str]) -> bool {
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::RootDir)) {
        return false;
    }
    for expected in expected_components {
        match components.next() {
            Some(Component::Normal(component)) if component == *expected => {}
            _ => return false,
        }
    }
    components.next().is_none()
}

fn is_single_numeric_path_component(path: &Path) -> bool {
    let mut components = path.components();
    let is_numeric = match components.next() {
        Some(Component::Normal(component)) => component.to_str().is_some_and(|value| {
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
        }),
        _ => false,
    };
    is_numeric && components.next().is_none()
}

#[cfg(target_os = "linux")]
fn is_linux_proc_self_fd_root(path: &Path) -> bool {
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::RootDir)) {
        return false;
    }
    for expected in ["proc", "self", "fd"] {
        match components.next() {
            Some(Component::Normal(component)) if component == expected => {}
            _ => return false,
        }
    }
    let has_valid_fd = match components.next() {
        Some(Component::Normal(fd)) => fd
            .to_str()
            .is_some_and(|fd| !fd.is_empty() && fd.bytes().all(|byte| byte.is_ascii_digit())),
        _ => false,
    };
    has_valid_fd && components.next().is_none()
}

#[cfg(not(target_os = "linux"))]
fn is_linux_proc_self_fd_root(_path: &Path) -> bool {
    false
}

fn reject_symlink_components(root: &Path, path: &Path, kind: &str) -> Result<(), RuntimeError> {
    let mut current = root.to_path_buf();
    for component in path.components() {
        let Component::Normal(component) = component else {
            continue;
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(RuntimeError::InvalidRequest(format!(
                    "manifest {kind} path {} contains a symlink component",
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(RuntimeError::InvalidRequest(error.to_string())),
        }
    }
    Ok(())
}

fn manifest_file_by_kind<'a>(
    manifest: &'a DatasetPackManifest,
    kind: &str,
) -> Option<&'a rax_bench_model::ManifestFile> {
    manifest.files.iter().find(|file| file.kind == kind)
}

fn serde_json_fallback_parse_manifest(text: &str) -> Result<DatasetPackManifest, RuntimeError> {
    serde_json::from_str(text).map_err(|error| RuntimeError::Storage(error.to_string()))
}

fn docstore_error(error: rax_docstore::DocstoreError) -> String {
    match error {
        rax_docstore::DocstoreError::Io(message)
        | rax_docstore::DocstoreError::Json(message)
        | rax_docstore::DocstoreError::InvalidDocument(message) => message,
        rax_docstore::DocstoreError::MissingDocumentsFile => {
            "dataset pack missing documents file".to_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use rax_bench_model::embed_text;
    use rax_bench_packer::{pack_adhoc_dataset, pack_dataset, AdhocPackRequest, PackRequest};
    use rax_core::{
        create_empty_store, map_segment_object, open_store, open_store_shallow, publish_segment,
        publish_segments_with_precondition, PendingSegmentDescriptor, PendingSegmentWrite,
        SegmentKind,
    };
    use rax_docstore::{prepare_raw_documents_segment, Docstore};
    use rax_text::{prepare_text_segment_from_documents, publish_compatibility_text_segment};
    use rax_vector::publish_compatibility_vector_segment;
    use serde_json::json;
    use tempfile::tempdir;

    use crate::{
        file_identity_from_file_failure_guard, load_all_runtime_documents, no_stable_store_handle,
        product_manifest, read_manifest, set_search_generation_race_hook,
        set_search_post_hydrate_race_hook, stable_runtime_root, Memory, NewDocument,
        NewDocumentVector, RuntimeAccelerationAvailability, RuntimeAccelerationPreference,
        RuntimeError, RuntimeExecutionBackend, RuntimePlatformAccelerationFamily,
        RuntimePublishFamily, RuntimeSearchMode, RuntimeSearchRequest, RuntimeStore,
    };

    #[test]
    fn memory_facade_opens_single_file_remembers_and_recalls_hybrid_results() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");

        let mut memory = Memory::open(&store_path).unwrap();
        let doc_id = memory
            .remember("The user is building a habit tracker in Rust")
            .unwrap();
        let results = memory.recall("What is the user building?").unwrap();
        memory.close().unwrap();

        assert_eq!(doc_id, "mem-0000000000000001");
        assert!(store_path.exists());
        assert_eq!(results.hits[0].doc_id, doc_id);
        assert_eq!(
            results.hits[0].preview.as_deref(),
            Some("The user is building a habit tracker in Rust")
        );

        let mut reopened = Memory::open(&store_path).unwrap();
        let reopened_results = reopened.search("habit tracker").unwrap();
        assert_eq!(reopened_results.hits[0].doc_id, "mem-0000000000000001");
    }

    #[test]
    fn memory_save_with_store_size_limit_rechecks_latest_store_size() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut memory = Memory::open(&store_path).unwrap();

        let error = memory
            .save_with_store_size_limit("alpha memory", serde_json::json!({}), 1, 0)
            .unwrap_err();

        assert!(
            matches!(error, RuntimeError::InvalidRequest(message) if message.contains("memory store write would exceed"))
        );
    }

    #[test]
    fn memory_save_with_store_size_limit_counts_metadata_payload() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut memory = Memory::open(&store_path).unwrap();
        let current_len = fs::metadata(&store_path).unwrap().len();
        let text = "tiny";
        let text_only_budget = current_len
            .saturating_mul(2)
            .saturating_add((text.len() as u64).saturating_mul(8))
            .saturating_add(128);
        let metadata = serde_json::json!({ "blob": "x".repeat(4096) });

        let error = memory
            .save_with_store_size_limit(text, metadata, text_only_budget, 0)
            .unwrap_err();

        assert!(
            matches!(error, RuntimeError::InvalidRequest(message) if message.contains("memory store write would exceed"))
        );
        assert_eq!(
            rax_core::open_store(&store_path)
                .unwrap()
                .manifest
                .generation,
            0
        );
    }

    #[test]
    fn runtime_store_identity_failure_fails_open_and_create_closed() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let new_store_path = temp_dir.path().join("new-agent.rax");
        Memory::open(&store_path).unwrap().close().unwrap();

        let (open_error, read_only_error, create_error) = {
            let _identity_failure = file_identity_from_file_failure_guard();
            let open_error = match RuntimeStore::open_at(&store_path) {
                Ok(_) => panic!("identity failure must reject read-write open"),
                Err(error) => error,
            };
            let read_only_error = match RuntimeStore::open_existing_read_only_at(&store_path) {
                Ok(_) => panic!("identity failure must reject read-only open"),
                Err(error) => error,
            };
            let create_error = match RuntimeStore::create_at(&new_store_path) {
                Ok(_) => panic!("identity failure must reject create"),
                Err(error) => error,
            };
            (open_error, read_only_error, create_error)
        };

        assert!(
            matches!(open_error, RuntimeError::Storage(message) if message.contains("identity"))
        );
        assert!(
            matches!(read_only_error, RuntimeError::Storage(message) if message.contains("identity"))
        );
        assert!(
            matches!(create_error, RuntimeError::Storage(message) if message.contains("identity"))
        );
        assert!(open_store(&new_store_path).is_ok());
        fs::remove_file(&new_store_path).unwrap();
    }

    #[test]
    fn runtime_search_retries_when_generation_changes_during_lane_load() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut memory = Memory::open(&store_path).unwrap();
        memory.remember("alpha memory").unwrap();
        memory.close().unwrap();

        let mut reader = RuntimeStore::open_at(&store_path).unwrap();
        let writer_store_path = store_path.clone();
        set_search_generation_race_hook(move || {
            let mut writer = Memory::open(&writer_store_path).unwrap();
            writer.remember("beta memory").unwrap();
            writer.close().unwrap();
        });

        let results = reader
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Text,
                text_query: Some("beta".to_owned()),
                vector_query: None,
                top_k: 1,
                include_preview: true,
            })
            .unwrap();

        assert_eq!(results.hits[0].doc_id, "mem-0000000000000002");
        assert_eq!(results.hits[0].preview.as_deref(), Some("beta memory"));
    }

    #[test]
    fn runtime_search_retries_when_generation_changes_after_hydration() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut memory = Memory::open(&store_path).unwrap();
        memory.remember("alpha memory").unwrap();
        memory.close().unwrap();

        let mut reader = RuntimeStore::open_at(&store_path).unwrap();
        let writer_store_path = store_path.clone();
        set_search_post_hydrate_race_hook(move || {
            let mut writer = Memory::open(&writer_store_path).unwrap();
            writer.remember("beta memory").unwrap();
            writer.close().unwrap();
        });

        let results = reader
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Text,
                text_query: Some("beta".to_owned()),
                vector_query: None,
                top_k: 1,
                include_preview: true,
            })
            .unwrap();

        assert_eq!(results.hits[0].doc_id, "mem-0000000000000002");
        assert_eq!(results.hits[0].preview.as_deref(), Some("beta memory"));
    }

    #[test]
    fn runtime_search_retries_empty_snapshot_when_generation_changes() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut reader = Memory::open(&store_path).unwrap();

        let writer_store_path = store_path.clone();
        set_search_generation_race_hook(move || {
            let mut writer = Memory::open(&writer_store_path).unwrap();
            writer.remember("beta memory").unwrap();
            writer.close().unwrap();
        });

        let results = reader.search("beta").unwrap();

        assert_eq!(results.hits[0].doc_id, "mem-0000000000000001");
        assert_eq!(results.hits[0].preview.as_deref(), Some("beta memory"));
    }

    #[test]
    fn memory_open_existing_rejects_missing_store_without_creating_it() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("missing.rax");

        let error = match Memory::open_existing(&store_path) {
            Ok(_) => panic!("missing store should not open"),
            Err(error) => error,
        };

        assert!(matches!(error, crate::RuntimeError::InvalidRequest(_)));
        assert!(!store_path.exists());
    }

    #[test]
    fn memory_open_creates_missing_parent_directories() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("nested").join("agent.rax");

        let mut memory = Memory::open(&store_path).unwrap();
        memory.remember("created under a missing parent").unwrap();
        memory.close().unwrap();

        assert!(store_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn memory_open_does_not_create_parent_dirs_through_symlink_component() {
        let temp_dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let link = temp_dir.path().join("link");
        std::os::unix::fs::symlink(outside.path(), &link).unwrap();
        let store_path = link.join("nested").join("agent.rax");

        let error = match Memory::open(&store_path) {
            Ok(_) => panic!("symlink parent should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, crate::RuntimeError::Storage(_)));
        assert!(!outside.path().join("nested").exists());
    }

    #[cfg(unix)]
    #[test]
    fn memory_open_keeps_created_store_bound_after_parent_path_swap() {
        let temp_dir = tempdir().unwrap();
        let parent = temp_dir.path().join("parent");
        let moved_parent = temp_dir.path().join("moved-parent");
        let replacement_parent = temp_dir.path().join("replacement-parent");
        fs::create_dir(&parent).unwrap();
        fs::create_dir(&replacement_parent).unwrap();
        let store_path = parent.join("agent.rax");
        let hook_parent = parent.clone();
        let hook_moved_parent = moved_parent.clone();
        let hook_replacement_parent = replacement_parent.clone();
        crate::set_first_create_post_create_hook(move || {
            fs::rename(&hook_parent, &hook_moved_parent).unwrap();
            fs::rename(&hook_replacement_parent, &hook_parent).unwrap();
            create_empty_store(&hook_parent.join("agent.rax")).unwrap();
        });

        let mut memory = Memory::open(&store_path).unwrap();
        memory.remember("original store memory").unwrap();
        let same_handle_results = memory.search("original").unwrap();
        assert_eq!(same_handle_results.hits.len(), 1);
        assert_eq!(
            same_handle_results.hits[0].preview.as_deref(),
            Some("original store memory")
        );
        memory.close().unwrap();

        let mut original = Memory::open_existing(&moved_parent.join("agent.rax")).unwrap();
        let original_results = original.search("original").unwrap();
        assert_eq!(original_results.hits.len(), 1);
        assert_eq!(
            original_results.hits[0].preview.as_deref(),
            Some("original store memory")
        );
        let mut replacement = Memory::open_existing(&parent.join("agent.rax")).unwrap();
        let replacement_results = replacement.search("original").unwrap();
        assert!(replacement_results.hits.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn memory_open_existing_keeps_store_bound_after_parent_path_swap() {
        let temp_dir = tempdir().unwrap();
        let parent = temp_dir.path().join("parent");
        let moved_parent = temp_dir.path().join("moved-parent");
        fs::create_dir(&parent).unwrap();
        let store_path = parent.join("agent.rax");
        let mut initial = Memory::open(&store_path).unwrap();
        initial.remember("original existing store memory").unwrap();
        initial.close().unwrap();

        let mut memory = Memory::open_existing(&store_path).unwrap();
        fs::rename(&parent, &moved_parent).unwrap();
        fs::create_dir(&parent).unwrap();
        create_empty_store(&parent.join("agent.rax")).unwrap();

        memory.remember("secret after path swap").unwrap();
        memory.close().unwrap();

        let mut original = Memory::open_existing(&moved_parent.join("agent.rax")).unwrap();
        let original_results = original.search("secret").unwrap();
        assert!(original_results
            .hits
            .iter()
            .any(|hit| hit.preview.as_deref() == Some("secret after path swap")));

        let mut replacement = Memory::open_existing(&parent.join("agent.rax")).unwrap();
        let replacement_results = replacement.search("secret").unwrap();
        assert!(replacement_results.hits.is_empty());
    }

    #[test]
    fn memory_open_existing_read_only_rejects_writes_even_when_file_is_writable() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut writable = Memory::open(&store_path).unwrap();
        writable.remember("read-only baseline memory").unwrap();
        writable.close().unwrap();

        let mut read_only = Memory::open_existing_read_only(&store_path).unwrap();
        let error = read_only
            .remember("this write must be rejected")
            .expect_err("read-only memory handle should reject writes");

        assert!(matches!(
            error,
            RuntimeError::InvalidRequest(message) if message.contains("read-only")
        ));
        let mut reopened = Memory::open_existing(&store_path).unwrap();
        let response = reopened.search("rejected").unwrap();
        assert!(!response
            .hits
            .iter()
            .any(|hit| hit.preview.as_deref() == Some("this write must be rejected")));
    }

    #[test]
    fn loaded_store_generation_uses_generation_that_full_open_accepts() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        create_empty_store(&store_path).unwrap();
        publish_segment(
            &store_path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"generation-one",
        )
        .unwrap();
        let generation_two = publish_segments_with_precondition(
            &store_path,
            vec![PendingSegmentWrite {
                descriptor: PendingSegmentDescriptor {
                    family: SegmentKind::Doc,
                    family_version: 1,
                    flags: 0,
                    doc_id_start: 1,
                    doc_id_end_exclusive: 2,
                    min_timestamp_ms: 0,
                    max_timestamp_ms: 0,
                    live_items: 1,
                    tombstoned_items: 0,
                    backend_id: 0,
                    backend_aux: 0,
                },
                object_bytes: b"generation-two".to_vec(),
            }],
            |_| Ok(()),
        )
        .unwrap();
        let latest_descriptor = generation_two
            .manifest
            .segments
            .iter()
            .find(|segment| segment.family == SegmentKind::Doc)
            .unwrap();
        let mut bytes = fs::read(&store_path).unwrap();
        let payload_start = latest_descriptor.object_offset as usize + 64;
        bytes[payload_start] ^= 0xFF;
        fs::write(&store_path, bytes).unwrap();

        assert_eq!(
            open_store_shallow(&store_path).unwrap().manifest.generation,
            2
        );
        assert_eq!(
            super::store_manifest_generation_if_present(&store_path).unwrap(),
            Some(1)
        );
        assert_eq!(
            super::loaded_store_manifest_generation_if_present(&store_path).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn memory_search_serves_full_open_fallback_generation_after_corrupt_latest_segment() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut memory = Memory::open(&store_path).unwrap();
        memory
            .remember("alpha stable fallback memory")
            .expect("first generation");
        memory
            .remember("beta corrupt latest memory")
            .expect("second generation");
        memory.close().unwrap();
        corrupt_latest_segment_payload(&store_path, SegmentKind::Doc);

        let mut reopened = Memory::open_existing_read_only(&store_path).unwrap();
        let response = reopened.search("stable fallback").unwrap();

        assert!(response
            .hits
            .iter()
            .any(|hit| hit.preview.as_deref() == Some("alpha stable fallback memory")));
        assert!(!response
            .hits
            .iter()
            .any(|hit| hit.preview.as_deref() == Some("beta corrupt latest memory")));
    }

    #[test]
    fn runtime_store_create_at_opens_store_created_by_racing_writer() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        create_empty_store(&store_path).unwrap();

        let runtime = RuntimeStore::create_at(&store_path).unwrap();

        assert_eq!(
            runtime.store_path().canonicalize().unwrap(),
            store_path.canonicalize().unwrap()
        );
    }

    #[test]
    fn runtime_store_create_at_rejects_existing_invalid_store_file() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        fs::write(&store_path, b"not a rax store").unwrap();

        let error = match RuntimeStore::create_at(&store_path) {
            Ok(_) => panic!("invalid store should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, crate::RuntimeError::Storage(_)));
    }

    #[test]
    fn memory_search_refreshes_empty_handle_after_concurrent_remember() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut reader = Memory::open(&store_path).unwrap();

        let mut writer = Memory::open(&store_path).unwrap();
        writer
            .remember("concurrent memory from another handle")
            .unwrap();
        writer.close().unwrap();

        let results = reader.search("concurrent memory").unwrap();

        assert_eq!(results.hits[0].doc_id, "mem-0000000000000001");
        assert_eq!(
            results.hits[0].preview.as_deref(),
            Some("concurrent memory from another handle")
        );
    }

    #[test]
    fn memory_save_reuses_existing_vectors_when_appending() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut memory = Memory::open(&store_path).unwrap();

        let first_doc_id = memory.remember("alpha first").unwrap();
        memory
            .runtime
            .writer()
            .unwrap()
            .publish_raw_vectors(vec![NewDocumentVector::new(
                first_doc_id.clone(),
                test_vector(42.0),
            )])
            .unwrap();
        let second_doc_id = memory.remember("beta second").unwrap();

        let vectors =
            rax_vector::load_runtime_raw_vectors(&memory.runtime.root, &memory.runtime.manifest)
                .unwrap()
                .into_iter()
                .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(vectors[&first_doc_id][0], 42.0);
        assert_ne!(vectors[&second_doc_id][0], 42.0);
    }

    #[test]
    fn memory_save_rejects_existing_documents_without_vectors() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut memory = Memory::open(&store_path).unwrap();
        memory
            .runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(vec![NewDocument::new("doc-001", "doc only")], None)
            .unwrap();

        let error = memory
            .remember("new memory")
            .expect_err("doc-only stores must not be silently re-vectorized");

        assert!(error.to_string().contains("matching vector segment"));
    }

    #[test]
    fn product_store_root_defaults_simple_relative_paths_to_current_directory() {
        assert_eq!(
            super::product_store_root(Path::new("agent.rax")).unwrap(),
            PathBuf::from(".")
        );
        assert_eq!(
            super::product_store_path_under_root(Path::new("."), Path::new("agent.rax")).unwrap(),
            PathBuf::from("agent.rax")
        );
        assert_eq!(
            super::product_store_path_under_root(Path::new("."), Path::new("./agent.rax")).unwrap(),
            PathBuf::from("agent.rax")
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn product_memory_create_accepts_top_level_tmp_alias() {
        let store_path = Path::new("/tmp").join(format!(
            "rax-runtime-alias-{}-{}.rax",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_file(&store_path);

        let mut memory = Memory::open(&store_path).unwrap();
        memory.remember("created through tmp alias").unwrap();
        memory.close().unwrap();

        assert!(store_path.exists());
        fs::remove_file(&store_path).unwrap();
    }

    #[test]
    fn runtime_handle_opened_without_store_identity_rejects_later_store_publish() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(&docs_path, "{\"doc_id\":\"seed-001\",\"text\":\"seed\"}\n").unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::open(dataset_dir.path()).unwrap();
        let store_path = runtime.store_path();
        create_empty_store(&store_path).unwrap();

        let error = runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(vec![NewDocument::new("doc-001", "alpha")], None)
            .unwrap_err();

        assert!(
            matches!(error, RuntimeError::Storage(message) if message.contains("pinned identity"))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn runtime_store_open_existing_preserves_proc_self_fd_store_path() {
        use std::os::fd::AsRawFd;

        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        create_empty_store(&store_path).unwrap();
        let root_dir = fs::File::open(temp_dir.path()).unwrap();
        let fd_store_path =
            PathBuf::from(format!("/proc/self/fd/{}/agent.rax", root_dir.as_raw_fd()));

        let runtime = RuntimeStore::open_existing_at(&fd_store_path).unwrap();

        assert!(runtime.store_path().starts_with(Path::new("/proc/self/fd")));
        let stable_store_path = runtime.store_path();
        drop(root_dir);
        let opened = rax_core::open_store(&stable_store_path).unwrap();
        assert_eq!(opened.manifest.generation, 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn runtime_store_open_pins_proc_self_fd_root_with_tail_component() {
        use std::os::fd::AsRawFd;

        let parent_dir = tempdir().unwrap();
        let dataset_dir = parent_dir.path().join("dataset");
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(&docs_path, "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n").unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(&docs_path, &dataset_dir, "small")).unwrap();
        let _created = RuntimeStore::create(&dataset_dir).unwrap();
        let parent = fs::File::open(parent_dir.path()).unwrap();
        let fd_dataset_root =
            PathBuf::from(format!("/proc/self/fd/{}/dataset", parent.as_raw_fd()));

        let runtime = RuntimeStore::open(&fd_dataset_root).unwrap();
        let stable_store_path = runtime.store_path();
        drop(parent);

        assert!(stable_store_path.starts_with(Path::new("/proc/self/fd")));
        assert!(open_store(&stable_store_path).is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn memory_open_existing_rejects_direct_proc_self_fd_store_path() {
        use std::os::fd::AsRawFd;

        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        create_empty_store(&store_path).unwrap();
        let store_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&store_path)
            .unwrap();
        let fd_store_path = PathBuf::from(format!("/proc/self/fd/{}", store_file.as_raw_fd()));

        let error = Memory::open_existing(&fd_store_path).unwrap_err();
        assert!(error
            .to_string()
            .contains("direct file-descriptor store paths are unsupported"));

        let error = Memory::open_existing_read_only(&fd_store_path).unwrap_err();
        assert!(error
            .to_string()
            .contains("direct file-descriptor store paths are unsupported"));
    }

    #[test]
    fn runtime_store_create_honors_manifest_store_path() {
        let dataset_dir = tempdir().unwrap();
        let store_path = dataset_dir.path().join("nested").join("custom.rax");
        let manifest = crate::product_manifest(dataset_dir.path(), &store_path).unwrap();
        fs::write(
            dataset_dir.path().join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let runtime = RuntimeStore::create(dataset_dir.path()).unwrap();

        assert_eq!(
            runtime.store_path().canonicalize().unwrap(),
            store_path.canonicalize().unwrap()
        );
        assert!(store_path.exists());
        assert!(!dataset_dir.path().join("store.rax").exists());
    }

    #[cfg(unix)]
    #[test]
    fn runtime_store_create_removes_empty_store_after_post_create_validation_failure() {
        let dataset_dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let outside_docs = outside.path().join("docs.ndjson");
        fs::write(
            &outside_docs,
            "{\"doc_id\":\"doc-001\",\"text\":\"outside\"}\n",
        )
        .unwrap();
        std::os::unix::fs::symlink(&outside_docs, dataset_dir.path().join("docs.ndjson")).unwrap();
        let store_path = dataset_dir.path().join("store.rax");
        let mut manifest = product_manifest(dataset_dir.path(), &store_path).unwrap();
        manifest.files.push(rax_bench_model::ManifestFile {
            path: "docs.ndjson".to_owned(),
            kind: "documents".to_owned(),
            format: "ndjson".to_owned(),
            record_count: 1,
            checksum: "outside".to_owned(),
        });
        fs::write(
            dataset_dir.path().join("manifest.json"),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();

        let error = match RuntimeStore::create(dataset_dir.path()) {
            Ok(_) => panic!("post-create validation failure should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            crate::RuntimeError::Storage(_) | crate::RuntimeError::InvalidRequest(_)
        ));
        assert!(!store_path.exists());
    }

    #[test]
    fn runtime_store_create_rejects_manifest_store_path_outside_root() {
        let dataset_dir = tempdir().unwrap();
        let outside_store = dataset_dir.path().join("..").join("outside.rax");
        let mut manifest = product_manifest(
            dataset_dir.path(),
            &dataset_dir.path().join("placeholder.rax"),
        )
        .unwrap();
        manifest.files[0].path = "../outside.rax".to_owned();
        fs::write(
            dataset_dir.path().join("manifest.json"),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();

        let error = match RuntimeStore::create(dataset_dir.path()) {
            Ok(_) => panic!("escaping manifest store path should be rejected"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("must stay within dataset root"));
        assert!(!outside_store.exists());
    }

    #[test]
    fn runtime_store_open_rejects_manifest_store_path_outside_root() {
        let dataset_dir = tempdir().unwrap();
        let mut manifest = product_manifest(
            dataset_dir.path(),
            &dataset_dir.path().join("placeholder.rax"),
        )
        .unwrap();
        manifest.files[0].path = "../outside.rax".to_owned();
        fs::write(
            dataset_dir.path().join("manifest.json"),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();

        let error = match RuntimeStore::open(dataset_dir.path()) {
            Ok(_) => panic!("escaping manifest store path should be rejected"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("must stay within dataset root"));
    }

    #[test]
    fn runtime_store_create_prefers_store_over_prebuilt_store() {
        let dataset_dir = tempdir().unwrap();
        let writable_store = dataset_dir.path().join("writable.rax");
        let prebuilt_store = dataset_dir.path().join("prebuilt.rax");
        create_empty_store(&prebuilt_store).unwrap();
        let mut manifest = crate::product_manifest(dataset_dir.path(), &writable_store).unwrap();
        let mut prebuilt_file = manifest.files[0].clone();
        prebuilt_file.path = "prebuilt.rax".to_owned();
        prebuilt_file.kind = "prebuilt_store".to_owned();
        manifest.files.insert(0, prebuilt_file);
        fs::write(
            dataset_dir.path().join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let runtime = RuntimeStore::create(dataset_dir.path()).unwrap();

        assert_eq!(runtime.store_path(), writable_store.canonicalize().unwrap());
        assert!(writable_store.exists());
    }

    #[test]
    fn memory_save_rejects_stale_snapshot_after_concurrent_publish() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut memory = Memory::open(&store_path).unwrap();
        memory.remember("first memory").unwrap();

        let mut stale_runtime = RuntimeStore::open_at(&store_path).unwrap();
        let loaded = load_all_runtime_documents(&mut stale_runtime).unwrap();

        let mut concurrent_memory = Memory::open(&store_path).unwrap();
        concurrent_memory.remember("second memory").unwrap();

        let mut stale_documents = loaded.documents;
        stale_documents.push(NewDocument::new("mem-0000000000000003", "third memory"));
        let vectors = stale_documents
            .iter()
            .map(|document| {
                NewDocumentVector::new(document.doc_id.clone(), embed_text(&document.text, 384))
            })
            .collect::<Vec<_>>();
        let runtime_store_path = stale_runtime.store_path();
        let error = stale_runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot_with_expected_generation(
                runtime_store_path,
                loaded.store_generation,
                stale_documents,
                Some(vectors),
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "publish_raw_snapshot store generation changed before publish; retry with latest documents"
        );
    }

    #[test]
    fn runtime_store_open_searches_and_closes_without_benchmark_workload_names() {
        let dataset_dir = tempdir().unwrap();
        let fixture_root =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/bench/source/minimal");
        let manifest = pack_dataset(&PackRequest::new(
            &fixture_root,
            dataset_dir.path(),
            "small",
            "clean",
        ))
        .unwrap();
        let store_path = dataset_dir.path().join("store.rax");
        create_empty_store(&store_path).unwrap();
        let dataset_docstore = Docstore::open_dataset_pack(dataset_dir.path(), &manifest).unwrap();
        dataset_docstore.publish_to_store(&store_path).unwrap();
        publish_compatibility_text_segment(dataset_dir.path(), &manifest, &store_path).unwrap();
        publish_compatibility_vector_segment(dataset_dir.path(), &manifest, &store_path).unwrap();

        let mut runtime = RuntimeStore::open(dataset_dir.path()).unwrap();

        let text = runtime
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Text,
                text_query: Some("rust benchmark".to_owned()),
                vector_query: None,
                top_k: 1,
                include_preview: true,
            })
            .unwrap();
        assert_eq!(text.hits.len(), 1);
        assert_eq!(text.hits[0].doc_id, "doc-001");
        assert_eq!(
            text.hits[0].preview.as_deref(),
            Some("rust benchmark guide")
        );

        let hybrid = runtime
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Hybrid,
                text_query: Some("semantic latency".to_owned()),
                vector_query: Some(embed_text("semantic latency", 384)),
                top_k: 1,
                include_preview: false,
            })
            .unwrap();
        assert_eq!(hybrid.hits.len(), 1);
        assert_eq!(hybrid.hits[0].doc_id, "doc-002");
        assert_eq!(hybrid.hits[0].preview, None);

        let doc_ids = runtime
            .search_doc_ids(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Text,
                text_query: Some("rust benchmark".to_owned()),
                vector_query: None,
                top_k: 1,
                include_preview: false,
            })
            .unwrap();
        assert_eq!(doc_ids, vec!["doc-001"]);

        runtime.close().unwrap();
    }

    #[test]
    fn runtime_search_without_preview_does_not_hydrate_document_payloads() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.rax");
        create_empty_store(&store_path).unwrap();
        let doc_pending = prepare_raw_documents_segment(
            &store_path,
            vec![(
                "doc-1".to_owned(),
                json!({"doc_id":"doc-1","text":"payload"}),
            )],
        )
        .unwrap();
        let text_pending =
            prepare_text_segment_from_documents(&[("ghost".to_owned(), "alpha".to_owned())])
                .unwrap();
        publish_segments_with_precondition(
            &store_path,
            vec![doc_pending, text_pending],
            |_| Ok(()),
        )
        .unwrap();
        let mut runtime = RuntimeStore::open_at(&store_path).unwrap();

        let response = runtime
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Text,
                text_query: Some("alpha".to_owned()),
                vector_query: None,
                top_k: 1,
                include_preview: false,
            })
            .unwrap();

        assert_eq!(response.hits[0].doc_id, "ghost");
        assert_eq!(response.hits[0].preview, None);
    }

    #[test]
    fn runtime_hybrid_search_overfetches_text_candidates_before_rrf() {
        let dataset_dir = tempdir().unwrap();
        let fixture_root =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/bench/source/minimal");
        pack_dataset(&PackRequest::new(
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
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-001", "alpha"),
                    NewDocument::new("doc-002", "beta"),
                    NewDocument::new("doc-003", "alpha"),
                ],
                Some(vec![
                    NewDocumentVector::new("doc-001", embed_text("other", 384)),
                    NewDocumentVector::new("doc-002", embed_text("different", 384)),
                    NewDocumentVector::new("doc-003", embed_text("alpha target", 384)),
                ]),
            )
            .unwrap();

        let response = runtime
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Hybrid,
                text_query: Some("alpha".to_owned()),
                vector_query: Some(embed_text("alpha target", 384)),
                top_k: 1,
                include_preview: false,
            })
            .unwrap();

        assert_eq!(response.hits.len(), 1);
        assert_eq!(response.hits[0].doc_id, "doc-003");
    }

    #[test]
    fn runtime_hybrid_search_handles_top_k_larger_than_corpus() {
        let dataset_dir = tempdir().unwrap();
        let fixture_root =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/bench/source/minimal");
        pack_dataset(&PackRequest::new(
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
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-001", "alpha note"),
                    NewDocument::new("doc-002", "beta note"),
                    NewDocument::new("doc-003", "gamma note"),
                ],
                Some(vec![
                    NewDocumentVector::new("doc-001", embed_text("alpha note", 384)),
                    NewDocumentVector::new("doc-002", embed_text("beta note", 384)),
                    NewDocumentVector::new("doc-003", embed_text("gamma note", 384)),
                ]),
            )
            .unwrap();

        let response = runtime
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Hybrid,
                text_query: Some("alpha".to_owned()),
                vector_query: Some(embed_text("alpha note", 384)),
                top_k: 5,
                include_preview: false,
            })
            .unwrap();

        assert!(!response.hits.is_empty());
        assert!(response.hits.len() <= 3);
        assert_eq!(response.hits[0].doc_id, "doc-001");
    }

    #[test]
    fn runtime_search_validates_mode_inputs_before_zero_top_k_short_circuit() {
        let dataset_dir = tempdir().unwrap();
        let fixture_root =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/bench/source/minimal");
        pack_dataset(&PackRequest::new(
            &fixture_root,
            dataset_dir.path(),
            "small",
            "clean",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();

        let error = runtime
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Hybrid,
                text_query: Some("alpha".to_owned()),
                vector_query: None,
                top_k: 0,
                include_preview: false,
            })
            .unwrap_err();

        assert!(
            matches!(error, crate::RuntimeError::InvalidRequest(message) if message.contains("vector_query is required for hybrid search"))
        );
    }

    #[test]
    fn runtime_hybrid_search_uses_live_doc_count_for_raw_publish_overfetch() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(&docs_path, "{\"doc_id\":\"seed-001\",\"text\":\"seed\"}\n").unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-001", "alpha"),
                    NewDocument::new("doc-002", "alpha"),
                ],
                Some(vec![
                    NewDocumentVector::new("doc-001", test_vector(0.0)),
                    NewDocumentVector::new("doc-002", test_vector(1.0)),
                ]),
            )
            .unwrap();

        let response = runtime
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Hybrid,
                text_query: Some("alpha".to_owned()),
                vector_query: Some(test_vector(1.0)),
                top_k: 1,
                include_preview: false,
            })
            .unwrap();

        assert_eq!(response.hits[0].doc_id, "doc-002");
    }

    #[test]
    fn compatibility_import_preserves_extra_document_payload_fields() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\",\"metadata\":{\"kind\":\"note\"},",
                "\"workspace_id\":\"workspace-a\",\"tags\":[\"one\",\"two\"]}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        runtime
            .writer()
            .unwrap()
            .import_compatibility_snapshot()
            .unwrap();

        let loaded = runtime
            .docstore
            .load_documents_by_id(&["doc-001".to_owned()])
            .unwrap();
        assert_eq!(
            loaded.get("doc-001").unwrap().get("workspace_id"),
            Some(&json!("workspace-a"))
        );
        assert_eq!(
            loaded.get("doc-001").unwrap().get("tags"),
            Some(&json!(["one", "two"]))
        );
        assert_eq!(
            loaded
                .get("doc-001")
                .unwrap()
                .get("metadata")
                .and_then(|metadata| metadata.get("kind")),
            Some(&json!("note"))
        );
    }

    #[test]
    fn runtime_store_creates_and_publishes_compatibility_segments_for_reopen_search() {
        let dataset_dir = tempdir().unwrap();
        let fixture_root =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/bench/source/minimal");
        let manifest = pack_dataset(&PackRequest::new(
            &fixture_root,
            dataset_dir.path(),
            "small",
            "clean",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        let publish_report = runtime
            .writer()
            .unwrap()
            .import_compatibility_snapshot()
            .unwrap();
        assert_eq!(publish_report.generation, 1);
        assert_eq!(
            publish_report.published_families,
            vec![
                RuntimePublishFamily::Doc,
                RuntimePublishFamily::Text,
                RuntimePublishFamily::Vector,
            ]
        );
        runtime.close().unwrap();

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
                vector_query: Some(embed_text("semantic latency", 384)),
                top_k: 2,
                include_preview: false,
            })
            .unwrap();
        assert_eq!(vector_response.hits[0].doc_id, "doc-002");
    }

    #[test]
    fn runtime_store_create_leaves_failed_store_file_for_manual_recovery() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(&docs_path, "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n").unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();
        let manifest = read_manifest(dataset_dir.path()).unwrap();
        let store_path = dataset_dir.path().join("store.rax");
        fs::write(&store_path, b"not-a-valid-store").unwrap();

        let root = stable_runtime_root(dataset_dir.path()).unwrap();
        let error = match RuntimeStore::open_created_store(
            root,
            manifest,
            &store_path,
            no_stable_store_handle(),
            None,
        ) {
            Ok(_) => {
                panic!("create cleanup path should fail when reopen cannot validate store")
            }
            Err(error) => error,
        };

        assert!(matches!(error, crate::RuntimeError::Storage(_)));
        assert!(store_path.exists());
    }

    #[test]
    fn publish_raw_snapshot_rejects_duplicate_document_doc_ids() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        let error = runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-001", "alpha"),
                    NewDocument::new("doc-001", "beta"),
                ],
                None,
            )
            .unwrap_err();

        assert!(
            matches!(error, crate::RuntimeError::InvalidRequest(message) if message.contains("duplicate doc_ids"))
        );
    }

    #[test]
    fn publish_raw_snapshot_rejects_duplicate_vector_doc_ids() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        let error = runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-001", "alpha"),
                    NewDocument::new("doc-002", "beta"),
                ],
                Some(vec![
                    NewDocumentVector::new("doc-001", embed_text("alpha", 384)),
                    NewDocumentVector::new("doc-001", embed_text("beta", 384)),
                ]),
            )
            .unwrap_err();

        assert!(
            matches!(error, crate::RuntimeError::InvalidRequest(message) if message.contains("duplicate doc_ids"))
        );
    }

    #[test]
    fn publish_raw_snapshot_truncates_missing_vector_doc_ids_in_error_message() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\"}\n",
                "{\"doc_id\":\"doc-003\",\"text\":\"gamma\"}\n",
                "{\"doc_id\":\"doc-004\",\"text\":\"delta\"}\n",
                "{\"doc_id\":\"doc-005\",\"text\":\"epsilon\"}\n",
                "{\"doc_id\":\"doc-006\",\"text\":\"zeta\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        let error = runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-001", "alpha"),
                    NewDocument::new("doc-002", "beta"),
                    NewDocument::new("doc-003", "gamma"),
                    NewDocument::new("doc-004", "delta"),
                    NewDocument::new("doc-005", "epsilon"),
                    NewDocument::new("doc-006", "zeta"),
                ],
                Some(vec![
                    NewDocumentVector::new("missing-001", embed_text("alpha", 384)),
                    NewDocumentVector::new("missing-002", embed_text("beta", 384)),
                    NewDocumentVector::new("missing-003", embed_text("gamma", 384)),
                    NewDocumentVector::new("missing-004", embed_text("delta", 384)),
                    NewDocumentVector::new("missing-005", embed_text("epsilon", 384)),
                    NewDocumentVector::new("missing-006", embed_text("zeta", 384)),
                ]),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            crate::RuntimeError::InvalidRequest(message)
                if message.contains("missing-001")
                    && message.contains("missing-005")
                    && message.contains("(+1 more)")
                    && !message.contains("missing-006")
        ));
    }

    #[test]
    fn publish_raw_vectors_rejects_store_without_active_documents() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(&docs_path, "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n").unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        let error = runtime
            .writer()
            .unwrap()
            .publish_raw_vectors(vec![NewDocumentVector::new(
                "doc-001",
                embed_text("alpha", 384),
            )])
            .unwrap_err();

        assert!(matches!(
            error,
            crate::RuntimeError::InvalidRequest(message)
                if message.contains("active document segment")
        ));
    }

    #[test]
    fn publish_raw_vectors_counts_only_active_documents() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-001", "alpha"),
                    NewDocument::new("doc-002", "beta"),
                ],
                Some(vec![
                    NewDocumentVector::new("doc-001", embed_text("alpha", 384)),
                    NewDocumentVector::new("doc-002", embed_text("beta", 384)),
                ]),
            )
            .unwrap();

        runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(vec![NewDocument::new("doc-001", "alpha only")], None)
            .unwrap();

        let report = runtime
            .writer()
            .unwrap()
            .publish_raw_vectors(vec![NewDocumentVector::new(
                "doc-001",
                embed_text("alpha only", 384),
            )])
            .unwrap();

        assert_eq!(
            report.published_families,
            vec![RuntimePublishFamily::Vector]
        );
    }

    #[test]
    fn publish_raw_documents_merges_with_existing_active_documents() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\",\"metadata\":{\"workspace\":\"old\"},\"priority\":\"keep\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\",\"metadata\":{\"workspace\":\"old\"}}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        runtime
            .writer()
            .unwrap()
            .publish_raw_documents(vec![
                NewDocument::new("doc-001", "alpha")
                    .with_metadata(serde_json::json!({"workspace":"old"}))
                    .with_extra_field("priority", serde_json::json!("keep")),
                NewDocument::new("doc-002", "beta")
                    .with_metadata(serde_json::json!({"workspace":"old"})),
            ])
            .unwrap();
        runtime
            .writer()
            .unwrap()
            .publish_raw_documents(vec![NewDocument::new("doc-003", "gamma")])
            .unwrap();

        let reopened = RuntimeStore::open(dataset_dir.path()).unwrap();
        let doc_ids = reopened.docstore.load_document_ids().unwrap();
        let documents = reopened.docstore.load_documents_by_id(&doc_ids).unwrap();

        assert_eq!(
            doc_ids,
            vec![
                "doc-001".to_owned(),
                "doc-002".to_owned(),
                "doc-003".to_owned()
            ]
        );
        assert_eq!(
            documents
                .get("doc-001")
                .and_then(|document| document.get("priority"))
                .and_then(serde_json::Value::as_str),
            Some("keep")
        );
        assert_eq!(
            documents
                .get("doc-003")
                .and_then(|document| document.get("text"))
                .and_then(serde_json::Value::as_str),
            Some("gamma")
        );
    }

    #[test]
    fn publish_raw_documents_repairs_from_full_open_fallback_generation() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("agent.rax");
        let mut memory = Memory::open(&store_path).unwrap();
        memory
            .remember("alpha recoverable baseline")
            .expect("first generation");
        memory
            .remember("beta corrupt latest")
            .expect("second generation");
        memory.close().unwrap();
        corrupt_latest_segment_payload(&store_path, SegmentKind::Doc);

        let mut runtime = RuntimeStore::open_at(&store_path).unwrap();
        let report = runtime
            .writer()
            .unwrap()
            .publish_raw_documents(vec![NewDocument::new("doc-003", "gamma repair")])
            .unwrap();
        runtime.close().unwrap();

        assert!(report.generation > 2);
        let mut reopened = Memory::open_existing_read_only(&store_path).unwrap();
        let response = reopened
            .search_with_options(
                "repair",
                crate::MemorySearchOptions {
                    mode: RuntimeSearchMode::Text,
                    top_k: 5,
                    include_preview: true,
                },
            )
            .unwrap();
        assert!(response
            .hits
            .iter()
            .any(|hit| hit.preview.as_deref() == Some("gamma repair")));
    }

    #[test]
    fn runtime_search_refreshes_when_another_handle_publishes_documents() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha original\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta original\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut reader = RuntimeStore::create(dataset_dir.path()).unwrap();
        let mut writer = RuntimeStore::open(dataset_dir.path()).unwrap();
        let first = reader
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Text,
                text_query: Some("alpha".to_owned()),
                vector_query: None,
                top_k: 1,
                include_preview: false,
            })
            .unwrap();
        assert_eq!(first.hits[0].doc_id, "doc-001");

        writer
            .writer()
            .unwrap()
            .publish_raw_documents(vec![NewDocument::new("doc-003", "fresh remote token")])
            .unwrap();

        let refreshed = reader
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Text,
                text_query: Some("fresh remote token".to_owned()),
                vector_query: None,
                top_k: 1,
                include_preview: true,
            })
            .unwrap();

        assert_eq!(refreshed.hits[0].doc_id, "doc-003");
        assert_eq!(
            refreshed.hits[0].preview.as_deref(),
            Some("fresh remote token")
        );
    }

    #[test]
    fn runtime_search_invalidates_cached_lanes_when_store_file_is_removed() {
        let dataset_dir = tempdir().unwrap();
        let fixture_root =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/bench/source/minimal");
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
            .import_compatibility_snapshot()
            .unwrap();
        let first = runtime
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Text,
                text_query: Some("rust benchmark".to_owned()),
                vector_query: None,
                top_k: 1,
                include_preview: false,
            })
            .unwrap();
        assert_eq!(first.hits[0].doc_id, "doc-001");

        fs::remove_file(dataset_dir.path().join("store.rax")).unwrap();
        for kind in [
            "documents",
            "document_offsets",
            "text_postings",
            "document_ids",
        ] {
            for file in manifest.files.iter().filter(|file| file.kind == kind) {
                let path = dataset_dir.path().join(&file.path);
                if path.exists() {
                    fs::remove_file(path).unwrap();
                }
            }
        }

        let error = runtime
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Text,
                text_query: Some("rust benchmark".to_owned()),
                vector_query: None,
                top_k: 1,
                include_preview: false,
            })
            .unwrap_err();

        assert!(matches!(error, crate::RuntimeError::Storage(_)));
    }

    #[test]
    fn publish_raw_vectors_refreshes_documents_published_by_another_handle() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut stale_runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        let mut writer_runtime = RuntimeStore::open(dataset_dir.path()).unwrap();
        writer_runtime
            .writer()
            .unwrap()
            .publish_raw_documents(vec![
                NewDocument::new("doc-001", "alpha refreshed"),
                NewDocument::new("doc-002", "beta"),
            ])
            .unwrap();

        let report = stale_runtime
            .writer()
            .unwrap()
            .publish_raw_vectors(vec![
                NewDocumentVector::new("doc-001", embed_text("alpha refreshed", 384)),
                NewDocumentVector::new("doc-002", embed_text("beta", 384)),
            ])
            .unwrap();

        assert_eq!(
            report.published_families,
            vec![RuntimePublishFamily::Vector]
        );
    }

    #[test]
    fn doc_generation_revalidation_rejects_changed_manifest_before_vector_publish() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        runtime
            .writer()
            .unwrap()
            .publish_raw_documents(vec![NewDocument::new("doc-001", "alpha")])
            .unwrap();
        let store_path = dataset_dir.path().join("store.rax");
        let validated_doc_segment =
            super::latest_doc_segment_identity(&open_store(&store_path).unwrap().manifest);

        runtime
            .writer()
            .unwrap()
            .publish_raw_documents(vec![
                NewDocument::new("doc-001", "alpha"),
                NewDocument::new("doc-002", "beta"),
            ])
            .unwrap();

        let opened = open_store(&store_path).unwrap();
        let error =
            super::ensure_doc_segment_unchanged(&opened.manifest, validated_doc_segment.as_ref())
                .unwrap_err();

        assert!(matches!(
            error,
            rax_core::CoreError::PublishPreconditionFailed(message)
                if message.contains("document generation changed")
        ));
    }

    #[test]
    fn publish_raw_snapshot_vectors_use_latest_store_doc_id_map() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut stale_runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        let mut writer_runtime = RuntimeStore::open(dataset_dir.path()).unwrap();
        writer_runtime
            .writer()
            .unwrap()
            .publish_raw_documents(vec![NewDocument::new("doc-002", "beta first")])
            .unwrap();

        stale_runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-002", "beta first"),
                    NewDocument::new("doc-001", "alpha second"),
                ],
                Some(vec![
                    NewDocumentVector::new("doc-002", embed_text("beta first", 384)),
                    NewDocumentVector::new("doc-001", embed_text("alpha second", 384)),
                ]),
            )
            .unwrap();

        let opened = open_store(&dataset_dir.path().join("store.rax")).unwrap();
        let vector_segment = opened
            .manifest
            .segments
            .iter()
            .rfind(|segment| segment.family == SegmentKind::Vec)
            .unwrap();
        let bytes =
            map_segment_object(&dataset_dir.path().join("store.rax"), vector_segment).unwrap();

        assert_eq!(
            read_vector_segment_doc_ids(&bytes),
            vec!["doc-002".to_owned(), "doc-001".to_owned()]
        );
    }

    #[test]
    fn publish_raw_snapshot_removes_vectors_for_doc_only_replace() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-001", "alpha"),
                    NewDocument::new("doc-002", "beta"),
                ],
                Some(vec![
                    NewDocumentVector::new("doc-001", embed_text("alpha", 384)),
                    NewDocumentVector::new("doc-002", embed_text("beta", 384)),
                ]),
            )
            .unwrap();

        runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(vec![NewDocument::new("doc-003", "gamma")], None)
            .unwrap();

        let opened = open_store(&dataset_dir.path().join("store.rax")).unwrap();
        assert!(opened
            .manifest
            .segments
            .iter()
            .all(|segment| segment.family != SegmentKind::Vec));
    }

    #[test]
    fn publish_raw_vectors_persists_rows_in_rax_doc_id_order() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\"}\n",
                "{\"doc_id\":\"doc-003\",\"text\":\"gamma\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-001", "alpha"),
                    NewDocument::new("doc-002", "beta"),
                    NewDocument::new("doc-003", "gamma"),
                ],
                Some(vec![
                    NewDocumentVector::new("doc-001", embed_text("alpha", 384)),
                    NewDocumentVector::new("doc-002", embed_text("beta", 384)),
                    NewDocumentVector::new("doc-003", embed_text("gamma", 384)),
                ]),
            )
            .unwrap();
        runtime
            .writer()
            .unwrap()
            .publish_raw_vectors(vec![
                NewDocumentVector::new("doc-003", embed_text("gamma updated", 384)),
                NewDocumentVector::new("doc-001", embed_text("alpha updated", 384)),
                NewDocumentVector::new("doc-002", embed_text("beta updated", 384)),
            ])
            .unwrap();

        let opened = open_store(&dataset_dir.path().join("store.rax")).unwrap();
        let vector_segment = opened
            .manifest
            .segments
            .iter()
            .rfind(|segment| segment.family == SegmentKind::Vec)
            .unwrap();
        let bytes =
            map_segment_object(&dataset_dir.path().join("store.rax"), vector_segment).unwrap();
        assert_eq!(
            read_vector_segment_doc_ids(&bytes),
            vec![
                "doc-001".to_owned(),
                "doc-002".to_owned(),
                "doc-003".to_owned()
            ]
        );
    }

    #[test]
    fn publish_raw_snapshot_replaces_family_segments_and_preserves_doc_id_ranges() {
        let dataset_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let docs_path = source_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-001\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-002\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        pack_adhoc_dataset(&AdhocPackRequest::new(
            &docs_path,
            dataset_dir.path(),
            "small",
        ))
        .unwrap();

        let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
        runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(
                vec![
                    NewDocument::new("doc-001", "alpha"),
                    NewDocument::new("doc-002", "beta"),
                ],
                Some(vec![
                    NewDocumentVector::new("doc-001", embed_text("alpha", 384)),
                    NewDocumentVector::new("doc-002", embed_text("beta", 384)),
                ]),
            )
            .unwrap();

        runtime
            .writer()
            .unwrap()
            .publish_raw_snapshot(
                vec![NewDocument::new("doc-003", "gamma")],
                Some(vec![NewDocumentVector::new(
                    "doc-003",
                    embed_text("gamma", 384),
                )]),
            )
            .unwrap();

        let opened = open_store(&dataset_dir.path().join("store.rax")).unwrap();
        let doc_segments = opened
            .manifest
            .segments
            .iter()
            .filter(|segment| segment.family == SegmentKind::Doc)
            .collect::<Vec<_>>();
        let text_segments = opened
            .manifest
            .segments
            .iter()
            .filter(|segment| segment.family == SegmentKind::Txt)
            .collect::<Vec<_>>();
        let vector_segments = opened
            .manifest
            .segments
            .iter()
            .filter(|segment| segment.family == SegmentKind::Vec)
            .collect::<Vec<_>>();

        assert_eq!(doc_segments.len(), 1);
        assert_eq!(text_segments.len(), 1);
        assert_eq!(vector_segments.len(), 1);
        assert_eq!(doc_segments[0].doc_id_start, 2);
        assert_eq!(doc_segments[0].doc_id_end_exclusive, 3);
        assert_eq!(text_segments[0].doc_id_start, 2);
        assert_eq!(text_segments[0].doc_id_end_exclusive, 3);
        assert_eq!(vector_segments[0].doc_id_start, 2);
        assert_eq!(vector_segments[0].doc_id_end_exclusive, 3);
    }

    #[test]
    fn runtime_reports_apple_acceleration_capability_explicitly() {
        let capabilities = RuntimeStore::capabilities();
        let apple = capabilities
            .platform_acceleration
            .iter()
            .find(|capability| capability.family == RuntimePlatformAccelerationFamily::Apple)
            .unwrap();

        if cfg!(target_os = "macos") || cfg!(target_os = "ios") {
            assert_eq!(
                apple.availability,
                RuntimeAccelerationAvailability::BackendNotCompiled
            );
        } else {
            assert_eq!(
                apple.availability,
                RuntimeAccelerationAvailability::UnsupportedPlatform
            );
        }
        assert!(!apple.detail.as_deref().unwrap_or("").is_empty());
    }

    #[test]
    fn runtime_resolves_platform_preference_without_changing_default_backend() {
        let selection =
            RuntimeStore::resolve_acceleration(RuntimeAccelerationPreference::PreferPlatform);

        assert_eq!(
            selection.preference,
            RuntimeAccelerationPreference::PreferPlatform
        );
        assert_eq!(
            selection.requested_family,
            Some(RuntimePlatformAccelerationFamily::Apple)
        );
        assert_eq!(
            selection.chosen_backend,
            RuntimeExecutionBackend::RustDefault
        );
        assert!(!selection
            .fallback_reason
            .as_deref()
            .unwrap_or("")
            .is_empty());
    }

    fn test_vector(first_value: f32) -> Vec<f32> {
        let mut vector = vec![0.0; 384];
        vector[0] = first_value;
        vector
    }

    fn corrupt_latest_segment_payload(store_path: &Path, family: SegmentKind) {
        let opened = open_store(store_path).unwrap();
        let descriptor = opened
            .manifest
            .segments
            .iter()
            .find(|segment| segment.family == family)
            .unwrap();
        let mut bytes = fs::read(store_path).unwrap();
        let payload_start = descriptor.object_offset as usize + 64;
        bytes[payload_start] ^= 0xFF;
        fs::write(store_path, bytes).unwrap();
    }

    fn read_vector_segment_doc_ids(bytes: &[u8]) -> Vec<String> {
        let doc_count = read_u64_at(bytes, 16) as usize;
        let doc_ids_offset = read_u64_at(bytes, 24) as usize;
        let exact_vectors_offset = read_u64_at(bytes, 32) as usize;
        let mut cursor = doc_ids_offset;
        let mut doc_ids = Vec::new();
        for _ in 0..doc_count {
            let length = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            doc_ids.push(String::from_utf8(bytes[cursor..cursor + length].to_vec()).unwrap());
            cursor += length;
        }
        assert!(bytes[cursor..exact_vectors_offset]
            .iter()
            .all(|byte| *byte == 0));
        doc_ids
    }

    fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
    }
}
