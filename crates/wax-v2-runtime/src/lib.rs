use std::fmt;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use wax_bench_model::DatasetPackManifest;
use wax_v2_docstore::DocIdMap;
use wax_v2_docstore::Docstore;
use wax_v2_search::hybrid_search_with_diagnostics;
use wax_v2_text::TextLane;
use wax_v2_vector::VectorLane;

#[derive(Debug, Clone, PartialEq, Eq)]
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
    manifest: DatasetPackManifest,
    docstore: Docstore,
    text_lane: Option<TextLane>,
    vector_lane: Option<VectorLane>,
    closed: bool,
}

pub struct RuntimeStoreWriter<'a> {
    store: &'a mut RuntimeStore,
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
        let manifest = read_manifest(root)?;
        let store_path = root.join("store.wax");
        if store_path.exists() {
            return Err(RuntimeError::InvalidRequest(format!(
                "store already exists at {}",
                store_path.display()
            )));
        }
        wax_v2_core::create_empty_store(&store_path)
            .map_err(|error| RuntimeError::Storage(error.to_string()))?;
        Self::open_created_store(root, manifest, &store_path)
    }

    pub fn open(root: &Path) -> Result<Self, RuntimeError> {
        let manifest = read_manifest(root)?;
        Self::open_from_manifest(root, manifest)
    }

    pub fn writer(&mut self) -> Result<RuntimeStoreWriter<'_>, RuntimeError> {
        if self.closed {
            return Err(RuntimeError::InvalidRequest(
                "runtime store is already closed".to_owned(),
            ));
        }
        Ok(RuntimeStoreWriter { store: self })
    }

    pub fn store_path(&self) -> PathBuf {
        self.root.join("store.wax")
    }

    fn open_from_manifest(
        root: &Path,
        manifest: DatasetPackManifest,
    ) -> Result<Self, RuntimeError> {
        let store_path = root.join("store.wax");
        if store_path.exists() {
            wax_v2_core::open_store(&store_path)
                .map_err(|error| RuntimeError::Storage(error.to_string()))?;
        }
        let docstore = Docstore::open(root, &manifest)
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;

        Ok(Self {
            root: root.to_path_buf(),
            manifest,
            docstore,
            text_lane: None,
            vector_lane: None,
            closed: false,
        })
    }

    fn open_created_store(
        root: &Path,
        manifest: DatasetPackManifest,
        store_path: &Path,
    ) -> Result<Self, RuntimeError> {
        match Self::open_from_manifest(root, manifest) {
            Ok(store) => Ok(store),
            Err(error) => {
                let _ = fs::remove_file(store_path);
                Err(error)
            }
        }
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
        if request.top_k == 0 {
            return Ok(RuntimeSearchResponse { hits: Vec::new() });
        }

        let doc_ids = match request.mode {
            RuntimeSearchMode::Text => {
                let text_query = request.text_query.as_deref().ok_or_else(|| {
                    RuntimeError::InvalidRequest(
                        "text_query is required for text search".to_owned(),
                    )
                })?;
                self.ensure_text_lane()?
                    .search_with_limit(text_query, request.top_k)
            }
            RuntimeSearchMode::Vector => {
                let vector_query = request.vector_query.as_deref().ok_or_else(|| {
                    RuntimeError::InvalidRequest(
                        "vector_query is required for vector search".to_owned(),
                    )
                })?;
                self.ensure_vector_lane()?
                    .search_with_query(
                        vector_query,
                        request.top_k,
                        wax_bench_model::VectorQueryMode::Auto,
                        false,
                    )
                    .map_err(RuntimeError::Storage)?
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
                let live_doc_count = self.live_doc_count()?;
                let text_limit = hybrid_text_candidate_limit(request.top_k, live_doc_count);
                let text_hits = self
                    .ensure_text_lane()?
                    .search_with_limit(text_query, text_limit);
                let report = hybrid_search_with_diagnostics(
                    &text_hits,
                    self.ensure_vector_lane()?,
                    vector_query,
                    request.top_k,
                    wax_bench_model::VectorQueryMode::Auto,
                    false,
                )
                .map_err(RuntimeError::Storage)?;
                report.fused_hits
            }
        };

        Ok(RuntimeSearchResponse {
            hits: self.hydrate_hits(&doc_ids, request.include_preview)?,
        })
    }

    pub fn close(&mut self) -> Result<(), RuntimeError> {
        self.closed = true;
        Ok(())
    }

    fn refresh_read_state(&mut self) -> Result<(), RuntimeError> {
        self.docstore = Docstore::open(&self.root, &self.manifest)
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
        self.text_lane = None;
        self.vector_lane = None;
        Ok(())
    }

    fn ensure_text_lane(&mut self) -> Result<&TextLane, RuntimeError> {
        if self.text_lane.is_none() {
            self.text_lane =
                Some(TextLane::load(&self.root, &self.manifest).map_err(RuntimeError::Storage)?);
        }
        self.text_lane
            .as_ref()
            .ok_or_else(|| RuntimeError::Storage("text lane not materialized".to_owned()))
    }

    fn ensure_vector_lane(&mut self) -> Result<&mut VectorLane, RuntimeError> {
        if self.vector_lane.is_none() {
            self.vector_lane = Some(
                VectorLane::load_runtime(
                    &self.root,
                    &self.manifest,
                    wax_bench_model::VectorQueryMode::Auto,
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
            .load_document_ids()
            .map(|doc_ids| doc_ids.len())
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
                .cloned()
                .map(|doc_id| RuntimeSearchHit {
                    doc_id,
                    preview: None,
                })
                .collect());
        }

        let documents = self
            .docstore
            .load_documents_by_id(doc_ids)
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
        Ok(doc_ids
            .iter()
            .cloned()
            .map(|doc_id| RuntimeSearchHit {
                preview: documents
                    .get(&doc_id)
                    .and_then(|document| document.get("text"))
                    .and_then(|value| value.as_str())
                    .map(ToOwned::to_owned),
                doc_id,
            })
            .collect())
    }
}

impl RuntimeStoreWriter<'_> {
    pub fn publish_raw_documents(
        self,
        documents: Vec<NewDocument>,
    ) -> Result<RuntimePublishReport, RuntimeError> {
        self.publish_raw_snapshot(documents, None)
    }

    pub fn publish_raw_snapshot(
        self,
        documents: Vec<NewDocument>,
        vectors: Option<Vec<NewDocumentVector>>,
    ) -> Result<RuntimePublishReport, RuntimeError> {
        let store_path = self.require_existing_store()?;
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

        let ordered_documents = raw_ordered_documents(&documents);
        let text_inputs = raw_text_inputs(&documents);
        let doc_pending =
            wax_v2_docstore::prepare_raw_documents_segment(&store_path, ordered_documents)
                .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
        let mut text_pending = wax_v2_text::prepare_text_segment_from_documents(&text_inputs)
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

            let doc_id_map = self
                .store
                .docstore
                .build_doc_id_map()
                .map_err(|error| RuntimeError::Storage(docstore_error(error)))?
                .extend_to_cover_document_order(&document_ids)
                .map_err(|error| RuntimeError::Storage(docstore_error(error)))?;
            let (_, _, vector_inputs) = vector_inputs_sorted_by_wax_doc_id(vectors, &doc_id_map)?;
            let mut vector_pending = wax_v2_vector::prepare_raw_vector_segment(
                self.store.manifest.vector_profile.embedding_dimensions as usize,
                &vector_inputs,
            )
            .map_err(RuntimeError::Storage)?;
            vector_pending.descriptor.doc_id_start = active_doc_id_range.start;
            vector_pending.descriptor.doc_id_end_exclusive = active_doc_id_range.end;
            pending_segments.push(vector_pending);
            published_families.push(RuntimePublishFamily::Vector);
        }

        let opened = wax_v2_core::publish_segments(&store_path, pending_segments)
            .map_err(|error| RuntimeError::Storage(error.to_string()))?;

        self.store.refresh_read_state()?;
        Ok(RuntimePublishReport {
            generation: opened.manifest.generation,
            published_families,
        })
    }

    pub fn publish_staged_compatibility_snapshot(
        self,
    ) -> Result<RuntimePublishReport, RuntimeError> {
        let documents = load_compatibility_raw_documents(&self.store.root, &self.store.manifest)?;
        let vectors =
            wax_v2_vector::load_compatibility_raw_vectors(&self.store.root, &self.store.manifest)
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
        self.store.refresh_read_state()?;
        let validated_doc_segment = latest_doc_segment_identity_from_store(&store_path)?;

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
            vector_inputs_sorted_by_wax_doc_id(vectors, &doc_id_map)?;

        let mut pending_segment = wax_v2_vector::prepare_raw_vector_segment(
            self.store.manifest.vector_profile.embedding_dimensions as usize,
            &vector_inputs,
        )
        .map_err(RuntimeError::Storage)?;
        pending_segment.descriptor.doc_id_start = doc_id_start;
        pending_segment.descriptor.doc_id_end_exclusive = doc_id_end_exclusive;
        let opened = wax_v2_core::publish_segments_with_precondition(
            &store_path,
            vec![pending_segment],
            |manifest| ensure_doc_segment_unchanged(manifest, validated_doc_segment.as_ref()),
        )
        .map_err(runtime_core_error)?;

        self.store.refresh_read_state()?;
        Ok(RuntimePublishReport {
            generation: opened.manifest.generation,
            published_families: vec![RuntimePublishFamily::Vector],
        })
    }

    fn require_existing_store(&self) -> Result<PathBuf, RuntimeError> {
        let store_path = self.store.store_path();
        if !store_path.exists() {
            return Err(RuntimeError::InvalidRequest(
                "store.wax is missing; call RuntimeStore::create first".to_owned(),
            ));
        }
        Ok(store_path)
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

fn vector_inputs_sorted_by_wax_doc_id(
    vectors: Vec<NewDocumentVector>,
    doc_id_map: &DocIdMap,
) -> Result<(u64, u64, Vec<(String, Vec<f32>)>), RuntimeError> {
    let mut vector_inputs = vectors
        .into_iter()
        .map(|vector| {
            let wax_doc_id = doc_id_map.wax_doc_id(&vector.doc_id).ok_or_else(|| {
                RuntimeError::Storage(format!("missing wax doc id binding for {}", vector.doc_id))
            })?;
            Ok((wax_doc_id, vector.doc_id, vector.values))
        })
        .collect::<Result<Vec<_>, _>>()?;
    vector_inputs.sort_by_key(|(wax_doc_id, _, _)| *wax_doc_id);
    let doc_id_start = vector_inputs
        .first()
        .map(|(wax_doc_id, _, _)| *wax_doc_id)
        .unwrap_or(0);
    let doc_id_end_exclusive = vector_inputs
        .last()
        .map(|(wax_doc_id, _, _)| wax_doc_id + 1)
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

impl From<&wax_v2_core::SegmentDescriptor> for DocSegmentIdentity {
    fn from(segment: &wax_v2_core::SegmentDescriptor) -> Self {
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

fn latest_doc_segment_identity_from_store(
    store_path: &Path,
) -> Result<Option<DocSegmentIdentity>, RuntimeError> {
    let opened = wax_v2_core::open_store(store_path).map_err(runtime_core_error)?;
    Ok(latest_doc_segment_identity(&opened.manifest))
}

fn latest_doc_segment_identity(
    manifest: &wax_v2_core::ActiveManifest,
) -> Option<DocSegmentIdentity> {
    manifest
        .segments
        .iter()
        .filter(|segment| segment.family == wax_v2_core::SegmentKind::Doc)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
        .map(DocSegmentIdentity::from)
}

fn ensure_doc_segment_unchanged(
    manifest: &wax_v2_core::ActiveManifest,
    expected: Option<&DocSegmentIdentity>,
) -> Result<(), wax_v2_core::CoreError> {
    let current = latest_doc_segment_identity(manifest);
    if current.as_ref() == expected {
        return Ok(());
    }

    Err(wax_v2_core::CoreError::PublishPreconditionFailed(
        "publish_raw_vectors document generation changed before vector publish; retry with latest documents"
            .to_owned(),
    ))
}

fn runtime_core_error(error: wax_v2_core::CoreError) -> RuntimeError {
    match error {
        wax_v2_core::CoreError::PublishPreconditionFailed(message) => {
            RuntimeError::InvalidRequest(message)
        }
        other => RuntimeError::Storage(other.to_string()),
    }
}

fn hybrid_text_candidate_limit(top_k: usize, live_doc_count: usize) -> usize {
    if top_k == 0 || live_doc_count == 0 {
        return 0;
    }
    live_doc_count.min(top_k.saturating_mul(10).max(100))
}

fn raw_text_inputs(documents: &[NewDocument]) -> Vec<(String, String)> {
    documents
        .iter()
        .map(|document| (document.doc_id.clone(), document.text.clone()))
        .collect()
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
        .map(|file| mount_root.join(&file.path))
        .ok_or_else(|| RuntimeError::Storage("documents file missing from manifest".to_owned()))?;
    BufReader::new(
        File::open(&documents_path).map_err(|error| RuntimeError::Storage(error.to_string()))?,
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
    let manifest_text = fs::read_to_string(root.join("manifest.json"))
        .map_err(|error| RuntimeError::Storage(error.to_string()))?;
    serde_json_fallback_parse_manifest(&manifest_text)
}

fn serde_json_fallback_parse_manifest(text: &str) -> Result<DatasetPackManifest, RuntimeError> {
    serde_json::from_str(text).map_err(|error| RuntimeError::Storage(error.to_string()))
}

fn docstore_error(error: wax_v2_docstore::DocstoreError) -> String {
    match error {
        wax_v2_docstore::DocstoreError::Io(message)
        | wax_v2_docstore::DocstoreError::Json(message)
        | wax_v2_docstore::DocstoreError::InvalidDocument(message) => message,
        wax_v2_docstore::DocstoreError::MissingDocumentsFile => {
            "dataset pack missing documents file".to_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use serde_json::json;
    use tempfile::tempdir;
    use wax_bench_model::embed_text;
    use wax_bench_packer::{pack_adhoc_dataset, pack_dataset, AdhocPackRequest, PackRequest};
    use wax_v2_core::{create_empty_store, map_segment_object, open_store, SegmentKind};
    use wax_v2_docstore::Docstore;
    use wax_v2_text::publish_compatibility_text_segment;
    use wax_v2_vector::publish_compatibility_vector_segment;

    use crate::{
        read_manifest, NewDocument, NewDocumentVector, RuntimeAccelerationAvailability,
        RuntimeAccelerationPreference, RuntimeExecutionBackend, RuntimePlatformAccelerationFamily,
        RuntimePublishFamily, RuntimeSearchMode, RuntimeSearchRequest, RuntimeStore,
    };

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
        let store_path = dataset_dir.path().join("store.wax");
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

        runtime.close().unwrap();
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
    fn runtime_store_create_removes_store_when_initialization_fails() {
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
        let store_path = dataset_dir.path().join("store.wax");
        fs::write(&store_path, b"not-a-valid-store").unwrap();

        let error =
            match RuntimeStore::open_created_store(dataset_dir.path(), manifest, &store_path) {
                Ok(_) => {
                    panic!("create cleanup path should fail when reopen cannot validate store")
                }
                Err(error) => error,
            };

        assert!(matches!(error, crate::RuntimeError::Storage(_)));
        assert!(!store_path.exists());
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
            .publish_raw_documents(vec![NewDocument::new("doc-001", "alpha only")])
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
            .publish_raw_documents(vec![NewDocument::new("doc-001", "alpha refreshed")])
            .unwrap();

        let report = stale_runtime
            .writer()
            .unwrap()
            .publish_raw_vectors(vec![NewDocumentVector::new(
                "doc-001",
                embed_text("alpha refreshed", 384),
            )])
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
        let store_path = dataset_dir.path().join("store.wax");
        let validated_doc_segment =
            super::latest_doc_segment_identity_from_store(&store_path).unwrap();

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
            wax_v2_core::CoreError::PublishPreconditionFailed(message)
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

        let opened = open_store(&dataset_dir.path().join("store.wax")).unwrap();
        let vector_segment = opened
            .manifest
            .segments
            .iter()
            .filter(|segment| segment.family == SegmentKind::Vec)
            .last()
            .unwrap();
        let bytes =
            map_segment_object(&dataset_dir.path().join("store.wax"), vector_segment).unwrap();

        assert_eq!(
            read_vector_segment_doc_ids(&bytes),
            vec!["doc-002".to_owned(), "doc-001".to_owned()]
        );
    }

    #[test]
    fn publish_raw_vectors_persists_rows_in_wax_doc_id_order() {
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

        let opened = open_store(&dataset_dir.path().join("store.wax")).unwrap();
        let vector_segment = opened
            .manifest
            .segments
            .iter()
            .filter(|segment| segment.family == SegmentKind::Vec)
            .last()
            .unwrap();
        let bytes =
            map_segment_object(&dataset_dir.path().join("store.wax"), vector_segment).unwrap();
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

        let opened = open_store(&dataset_dir.path().join("store.wax")).unwrap();
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
        assert!(apple.detail.as_deref().unwrap_or("").len() > 0);
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
        assert!(selection.fallback_reason.as_deref().unwrap_or("").len() > 0);
    }

    fn test_vector(first_value: f32) -> Vec<f32> {
        let mut vector = vec![0.0; 384];
        vector[0] = first_value;
        vector
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
