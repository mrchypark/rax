use std::fmt;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use wax_bench_model::DatasetPackManifest;
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
}

impl NewDocument {
    pub fn new(doc_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            doc_id: doc_id.into(),
            text: text.into(),
            metadata: serde_json::json!({}),
            timestamp_ms: None,
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
        Self::open_from_manifest(root, manifest)
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
                let text_hits = self
                    .ensure_text_lane()?
                    .search_with_limit(text_query, request.top_k);
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

        let ordered_documents = raw_ordered_documents(&documents);
        let text_inputs = raw_text_inputs(&documents);
        let mut pending_segments = vec![
            wax_v2_docstore::prepare_raw_documents_segment(&store_path, ordered_documents)
                .map_err(|error| RuntimeError::Storage(docstore_error(error)))?,
            wax_v2_text::prepare_text_segment_from_documents(&text_inputs)
                .map_err(RuntimeError::Storage)?,
        ];
        let mut published_families =
            vec![RuntimePublishFamily::Doc, RuntimePublishFamily::Text];

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
                .map(|document| document.doc_id.as_str())
                .collect::<std::collections::HashSet<_>>();
            let missing = vectors
                .iter()
                .filter(|vector| !document_ids.contains(vector.doc_id.as_str()))
                .map(|vector| vector.doc_id.clone())
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                return Err(RuntimeError::InvalidRequest(format!(
                    "publish_raw_snapshot vectors require matching documents for all doc_ids; missing: {}",
                    missing.join(", ")
                )));
            }

            let vector_inputs = vectors
                .into_iter()
                .map(|vector| (vector.doc_id, vector.values))
                .collect::<Vec<_>>();
            pending_segments.push(
                wax_v2_vector::prepare_raw_vector_segment(
                    self.store.manifest.vector_profile.embedding_dimensions as usize,
                    &vector_inputs,
                )
                .map_err(RuntimeError::Storage)?,
            );
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

    pub fn publish_staged_compatibility_snapshot(self) -> Result<RuntimePublishReport, RuntimeError> {
        let documents = load_compatibility_raw_documents(&self.store.root, &self.store.manifest)?;
        let vectors = wax_v2_vector::load_compatibility_raw_vectors(
            &self.store.root,
            &self.store.manifest,
        )
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
            .build_doc_id_map()
            .map_err(|error| RuntimeError::Storage(docstore_error(error)))?
            .bindings()
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

        let vector_inputs = vectors
            .into_iter()
            .map(|vector| (vector.doc_id, vector.values))
            .collect::<Vec<_>>();
        let pending_segment = wax_v2_vector::prepare_raw_vector_segment(
            self.store.manifest.vector_profile.embedding_dimensions as usize,
            &vector_inputs,
        )
        .map_err(RuntimeError::Storage)?;
        let opened = wax_v2_core::publish_segments(&store_path, vec![pending_segment])
            .map_err(|error| RuntimeError::Storage(error.to_string()))?;

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
            let mut object = serde_json::Map::new();
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
        File::open(&documents_path)
            .map_err(|error| RuntimeError::Storage(error.to_string()))?,
    )
    .lines()
    .filter_map(|line| match line {
        Ok(line) if line.trim().is_empty() => None,
        other => Some(other),
    })
    .map(|line| {
        let line = line.map_err(|error| RuntimeError::Storage(error.to_string()))?;
        let value: serde_json::Value =
            serde_json::from_str(&line).map_err(|error| RuntimeError::Storage(error.to_string()))?;
        let object = value
            .as_object()
            .ok_or_else(|| RuntimeError::Storage("document line must be a json object".to_owned()))?;
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

    use tempfile::tempdir;
    use wax_bench_model::embed_text;
    use wax_bench_packer::{pack_adhoc_dataset, pack_dataset, AdhocPackRequest, PackRequest};
    use wax_v2_core::create_empty_store;
    use wax_v2_docstore::Docstore;
    use wax_v2_text::publish_compatibility_text_segment;
    use wax_v2_vector::publish_compatibility_vector_segment;

    use crate::{
        NewDocument, NewDocumentVector,
        RuntimeAccelerationAvailability, RuntimeAccelerationPreference, RuntimeExecutionBackend,
        RuntimePlatformAccelerationFamily, RuntimePublishFamily, RuntimeSearchMode,
        RuntimeSearchRequest, RuntimeStore,
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
        pack_adhoc_dataset(&AdhocPackRequest::new(&docs_path, dataset_dir.path(), "small"))
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
        pack_adhoc_dataset(&AdhocPackRequest::new(&docs_path, dataset_dir.path(), "small"))
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

        assert_eq!(selection.preference, RuntimeAccelerationPreference::PreferPlatform);
        assert_eq!(
            selection.requested_family,
            Some(RuntimePlatformAccelerationFamily::Apple)
        );
        assert_eq!(selection.chosen_backend, RuntimeExecutionBackend::RustDefault);
        assert!(selection.fallback_reason.as_deref().unwrap_or("").len() > 0);
    }
}
