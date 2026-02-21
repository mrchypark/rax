use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rax_core::store::durable::DurableLifecycleStore;
use rax_embeddings::TextEmbedder;
use rax_vector_search::engine::VectorSearch;
use rax_vector_search::factory::{create_default_engine, ConfiguredVectorEngine};

use crate::structured_memory::{StructuredEntity, StructuredMemory};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SessionMode {
    ReadOnly,
    #[default]
    ReadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionConfig {
    pub enable_text_search: bool,
    pub enable_vector_search: bool,
    pub enable_structured_memory: bool,
    pub vector_dimensions: Option<usize>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            enable_text_search: true,
            enable_vector_search: true,
            enable_structured_memory: true,
            vector_dimensions: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    ReadOnly,
    TextSearchDisabled,
    VectorSearchDisabled,
    StructuredMemoryDisabled,
    EmptyEmbedding,
    VectorDimensionMismatch { expected: usize, got: usize },
    DurableStoreIo(String),
    EmbeddingProvider(String),
    WriterBusy,
    WriterTimeout,
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly => write!(f, "session is read-only"),
            Self::TextSearchDisabled => write!(f, "text search is disabled"),
            Self::VectorSearchDisabled => write!(f, "vector search is disabled"),
            Self::StructuredMemoryDisabled => write!(f, "structured memory is disabled"),
            Self::EmptyEmbedding => write!(f, "embedding must not be empty"),
            Self::VectorDimensionMismatch { expected, got } => {
                write!(
                    f,
                    "vector dimension mismatch: expected {}, got {}",
                    expected, got
                )
            }
            Self::DurableStoreIo(message) => write!(f, "durable store io error: {message}"),
            Self::EmbeddingProvider(message) => {
                write!(f, "embedding provider error: {message}")
            }
            Self::WriterBusy => write!(f, "writer lease is busy"),
            Self::WriterTimeout => write!(f, "writer lease acquire timed out"),
        }
    }
}

impl std::error::Error for SessionError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionStageReport {
    pub staged_text_entries: usize,
    pub staged_vector_mutations: usize,
    pub staged_vector_entries: usize,
}

pub struct WaxSession {
    mode: SessionMode,
    config: SessionConfig,
    memories: Vec<String>,
    structured: StructuredMemory,
    durable_store_root: PathBuf,
    durable_store: DurableLifecycleStore,
    vector_engine: Option<ConfiguredVectorEngine>,
    vector_memories: HashMap<u64, String>,
    vector_dimensions: Option<usize>,
    pending_text_entries: usize,
    next_timestamp_ms: u64,
}

impl Default for WaxSession {
    fn default() -> Self {
        Self::new()
    }
}

impl WaxSession {
    pub fn new() -> Self {
        Self::open(SessionMode::ReadWrite, SessionConfig::default())
    }

    pub fn open(mode: SessionMode, config: SessionConfig) -> Self {
        let root = default_session_store_root();
        Self::open_with_durable_root(mode, config, &root)
            .expect("open WaxSession with durable store")
    }

    pub fn open_with_durable_root(
        mode: SessionMode,
        config: SessionConfig,
        root: impl AsRef<Path>,
    ) -> Result<Self, SessionError> {
        let durable_store = DurableLifecycleStore::open(root.as_ref())
            .map_err(|err| SessionError::DurableStoreIo(err.to_string()))?;
        let vector_engine = if config.enable_vector_search {
            Some(create_default_engine())
        } else {
            None
        };
        Ok(Self {
            mode,
            config,
            memories: Vec::new(),
            structured: StructuredMemory::new(),
            durable_store_root: root.as_ref().to_path_buf(),
            durable_store,
            vector_engine,
            vector_memories: HashMap::new(),
            vector_dimensions: config.vector_dimensions,
            pending_text_entries: 0,
            next_timestamp_ms: current_time_millis(),
        })
    }

    pub fn remember(&mut self, text: impl Into<String>) {
        let _ = self.try_remember(text);
    }

    pub fn try_remember(&mut self, text: impl Into<String>) -> Result<(), SessionError> {
        self.ensure_writable()?;
        if !self.config.enable_text_search {
            return Err(SessionError::TextSearchDisabled);
        }
        let text = text.into();
        let timestamp = self.next_session_timestamp();
        self.put_frame_internal(text.as_bytes().to_vec(), timestamp)?;
        self.memories.push(text);
        self.pending_text_entries = self.pending_text_entries.saturating_add(1);
        Ok(())
    }

    pub fn remember_with_embedding(&mut self, text: impl Into<String>, embedding: Vec<f32>) {
        let _ = self.try_remember_with_embedding(text, embedding);
    }

    pub fn remember_with_embedder(
        &mut self,
        text: impl Into<String>,
        embedder: &mut dyn TextEmbedder,
    ) {
        let _ = self.try_remember_with_embedder(text, embedder);
    }

    pub fn try_remember_with_embedder(
        &mut self,
        text: impl Into<String>,
        embedder: &mut dyn TextEmbedder,
    ) -> Result<(), SessionError> {
        let text = text.into();
        let embedding = embedder
            .embed(&text)
            .map_err(|err| SessionError::EmbeddingProvider(err.to_string()))?;
        self.try_remember_with_embedding(text, embedding)
    }

    pub fn try_remember_with_embedding(
        &mut self,
        text: impl Into<String>,
        embedding: Vec<f32>,
    ) -> Result<(), SessionError> {
        self.ensure_writable()?;
        if !self.config.enable_vector_search {
            return Err(SessionError::VectorSearchDisabled);
        }
        if embedding.is_empty() {
            return Err(SessionError::EmptyEmbedding);
        }
        self.check_or_init_dimensions(embedding.len())?;

        let text = text.into();
        let timestamp = self.next_session_timestamp();
        let frame_id = self.put_frame_internal(text.as_bytes().to_vec(), timestamp)?;
        self.vector_memories.insert(frame_id, text.clone());
        self.memories.push(text);
        self.pending_text_entries = self.pending_text_entries.saturating_add(1);
        if let Some(engine) = self.vector_engine.as_mut() {
            engine.upsert(frame_id, embedding);
        }
        Ok(())
    }

    pub fn recall(&self, query: &str) -> Vec<String> {
        self.try_recall(query).unwrap_or_default()
    }

    pub fn try_recall(&self, query: &str) -> Result<Vec<String>, SessionError> {
        if !self.config.enable_text_search {
            return Err(SessionError::TextSearchDisabled);
        }
        Ok(self
            .memories
            .iter()
            .filter(|m| m.contains(query))
            .cloned()
            .collect())
    }

    pub fn recall_semantic(&self, query_embedding: &[f32], k: usize) -> Vec<String> {
        self.try_recall_semantic(query_embedding, k)
            .unwrap_or_default()
    }

    pub fn try_recall_semantic(
        &self,
        query_embedding: &[f32],
        k: usize,
    ) -> Result<Vec<String>, SessionError> {
        if !self.config.enable_vector_search {
            return Err(SessionError::VectorSearchDisabled);
        }
        if k == 0 || query_embedding.is_empty() {
            return Ok(Vec::new());
        }

        let expected = self.vector_dimensions.unwrap_or(query_embedding.len());
        if expected != query_embedding.len() {
            return Err(SessionError::VectorDimensionMismatch {
                expected,
                got: query_embedding.len(),
            });
        }

        let Some(engine) = self.vector_engine.as_ref() else {
            return Ok(Vec::new());
        };
        let hits = engine.search(query_embedding, k);
        Ok(hits
            .into_iter()
            .filter_map(|hit| self.vector_memories.get(&hit.id).cloned())
            .collect())
    }

    pub fn flush(&mut self) -> usize {
        self.try_flush().unwrap_or(0)
    }

    pub fn try_flush(&mut self) -> Result<usize, SessionError> {
        let report = self.try_commit(false)?;
        Ok(report.staged_text_entries)
    }

    pub fn stage(&mut self, compact: bool) -> SessionStageReport {
        self.try_stage(compact).unwrap_or_default()
    }

    pub fn try_stage(&mut self, _compact: bool) -> Result<SessionStageReport, SessionError> {
        self.ensure_writable()?;
        let mut report = SessionStageReport {
            staged_text_entries: self.pending_text_entries,
            staged_vector_mutations: 0,
            staged_vector_entries: 0,
        };

        if let Some(engine) = self.vector_engine.as_mut() {
            match engine {
                ConfiguredVectorEngine::USearch(_) => {
                    report.staged_vector_entries = self.vector_memories.len();
                }
            }
        }
        self.pending_text_entries = 0;
        Ok(report)
    }

    pub fn commit(&mut self, compact: bool) -> SessionStageReport {
        self.try_commit(compact).unwrap_or_default()
    }

    pub fn try_commit(&mut self, compact: bool) -> Result<SessionStageReport, SessionError> {
        let report = self.try_stage(compact)?;
        if let Err(err) = self.durable_store.commit() {
            self.pending_text_entries = self
                .pending_text_entries
                .saturating_add(report.staged_text_entries);
            return Err(SessionError::DurableStoreIo(err.to_string()));
        }
        Ok(report)
    }

    pub fn try_put_frame(&mut self, payload: Vec<u8>, timestamp: u64) -> Result<u64, SessionError> {
        self.ensure_writable()?;
        let frame_id = self.put_frame_internal(payload, timestamp)?;
        self.pending_text_entries = self.pending_text_entries.saturating_add(1);
        Ok(frame_id)
    }

    pub fn try_is_frame_visible(&self, frame_id: u64) -> Result<bool, SessionError> {
        Ok(self.durable_store.get_visible_meta(frame_id).is_some())
    }

    pub fn try_durable_wal_entry_count(&self) -> Result<usize, SessionError> {
        self.durable_store
            .wal_entry_count()
            .map_err(|err| SessionError::DurableStoreIo(err.to_string()))
    }

    pub fn durable_store_root(&self) -> &Path {
        &self.durable_store_root
    }

    pub fn try_upsert_entity(
        &mut self,
        id: impl Into<String>,
        attrs: HashMap<String, String>,
    ) -> Result<(), SessionError> {
        self.ensure_writable()?;
        if !self.config.enable_structured_memory {
            return Err(SessionError::StructuredMemoryDisabled);
        }
        self.structured.upsert(id, attrs);
        Ok(())
    }

    pub fn try_get_entity(&self, id: &str) -> Result<Option<StructuredEntity>, SessionError> {
        if !self.config.enable_structured_memory {
            return Err(SessionError::StructuredMemoryDisabled);
        }
        Ok(self.structured.get(id).cloned())
    }

    pub fn mode(&self) -> SessionMode {
        self.mode
    }

    pub fn config(&self) -> SessionConfig {
        self.config
    }

    fn ensure_writable(&self) -> Result<(), SessionError> {
        if self.mode == SessionMode::ReadOnly {
            return Err(SessionError::ReadOnly);
        }
        Ok(())
    }

    fn check_or_init_dimensions(&mut self, got: usize) -> Result<(), SessionError> {
        if let Some(expected) = self.vector_dimensions {
            if expected != got {
                return Err(SessionError::VectorDimensionMismatch { expected, got });
            }
            return Ok(());
        }
        self.vector_dimensions = Some(got);
        Ok(())
    }

    fn put_frame_internal(
        &mut self,
        payload: Vec<u8>,
        timestamp: u64,
    ) -> Result<u64, SessionError> {
        self.durable_store
            .put(payload, timestamp)
            .map_err(|err| SessionError::DurableStoreIo(err.to_string()))
    }

    fn next_session_timestamp(&mut self) -> u64 {
        let ts = self.next_timestamp_ms;
        self.next_timestamp_ms = self.next_timestamp_ms.saturating_add(1);
        ts
    }

    pub fn legacy_recall(&self, query: &str) -> Vec<String> {
        self.memories
            .iter()
            .filter(|m| m.contains(query))
            .cloned()
            .collect()
    }
}

fn current_time_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn default_session_store_root() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nonce = current_time_millis();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "rax-session-store-{}-{nonce}-{seq}",
        std::process::id()
    ))
}
