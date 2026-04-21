use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use wax_v2_runtime::{
    NewDocument, NewDocumentVector, RuntimePublishFamily, RuntimePublishReport, RuntimeSearchMode,
    RuntimeSearchRequest, RuntimeSearchResponse, RuntimeStore,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);

impl SessionId {
    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn from_u64(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerPublishFamily {
    Doc,
    Text,
    Vector,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionImportReport {
    pub generation: u64,
    pub published_families: Vec<BrokerPublishFamily>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionNewDocument {
    pub doc_id: String,
    pub text: String,
    pub metadata: serde_json::Value,
    pub timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionNewDocumentVector {
    pub doc_id: String,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSearchRequest {
    text_query: String,
    top_k: usize,
    include_preview: bool,
}

impl SessionSearchRequest {
    pub fn text(text_query: impl Into<String>) -> Self {
        Self {
            text_query: text_query.into(),
            top_k: 5,
            include_preview: false,
        }
    }

    pub fn with_top_k(mut self, top_k: usize) -> Self {
        self.top_k = top_k;
        self
    }

    pub fn with_preview(mut self, include_preview: bool) -> Self {
        self.include_preview = include_preview;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerError {
    InvalidRequest(String),
    Storage(String),
    SessionNotFound(SessionId),
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(message) | Self::Storage(message) => write!(f, "{message}"),
            Self::SessionNotFound(session_id) => {
                write!(f, "session {} is not open", session_id.as_u64())
            }
        }
    }
}

#[derive(Default)]
pub struct WaxBroker {
    next_session_id: u64,
    sessions: HashMap<SessionId, RuntimeStore>,
}

impl WaxBroker {
    pub fn open_session(&mut self, root: &Path) -> Result<SessionId, BrokerError> {
        let runtime = if root.join("store.wax").exists() {
            RuntimeStore::open(root).map_err(runtime_error)?
        } else {
            RuntimeStore::create(root).map_err(runtime_error)?
        };
        let session_id = SessionId(self.next_session_id);
        self.next_session_id = self
            .next_session_id
            .checked_add(1)
            .ok_or_else(|| BrokerError::Storage("session id overflow".to_owned()))?;
        self.sessions.insert(session_id, runtime);
        Ok(session_id)
    }

    pub fn search(
        &mut self,
        session_id: SessionId,
        request: SessionSearchRequest,
    ) -> Result<RuntimeSearchResponse, BrokerError> {
        let runtime = self
            .sessions
            .get_mut(&session_id)
            .ok_or(BrokerError::SessionNotFound(session_id))?;
        runtime
            .search(RuntimeSearchRequest {
                mode: RuntimeSearchMode::Text,
                text_query: Some(request.text_query),
                vector_query: None,
                top_k: request.top_k,
                include_preview: request.include_preview,
            })
            .map_err(runtime_error)
    }

    pub fn import_compatibility_snapshot(
        &mut self,
        session_id: SessionId,
    ) -> Result<SessionImportReport, BrokerError> {
        let runtime = self
            .sessions
            .get_mut(&session_id)
            .ok_or(BrokerError::SessionNotFound(session_id))?;
        let report = runtime
            .writer()
            .map_err(runtime_error)?
            .import_compatibility_snapshot()
            .map_err(runtime_error)?;
        Ok(map_publish_report(report))
    }

    pub fn ingest_documents(
        &mut self,
        session_id: SessionId,
        documents: Vec<SessionNewDocument>,
    ) -> Result<SessionImportReport, BrokerError> {
        let runtime = self
            .sessions
            .get_mut(&session_id)
            .ok_or(BrokerError::SessionNotFound(session_id))?;
        let report = runtime
            .writer()
            .map_err(runtime_error)?
            .publish_raw_documents(
                documents
                    .into_iter()
                    .map(|document| {
                        let mut runtime_document = NewDocument::new(document.doc_id, document.text)
                            .with_metadata(document.metadata);
                        if let Some(timestamp_ms) = document.timestamp_ms {
                            runtime_document = runtime_document.with_timestamp_ms(timestamp_ms);
                        }
                        runtime_document
                    })
                    .collect(),
            )
            .map_err(runtime_error)?;
        Ok(map_publish_report(report))
    }

    pub fn ingest_vectors(
        &mut self,
        session_id: SessionId,
        vectors: Vec<SessionNewDocumentVector>,
    ) -> Result<SessionImportReport, BrokerError> {
        let runtime = self
            .sessions
            .get_mut(&session_id)
            .ok_or(BrokerError::SessionNotFound(session_id))?;
        let report = runtime
            .writer()
            .map_err(runtime_error)?
            .publish_raw_vectors(
                vectors
                    .into_iter()
                    .map(|vector| NewDocumentVector::new(vector.doc_id, vector.values))
                    .collect(),
            )
            .map_err(runtime_error)?;
        Ok(map_publish_report(report))
    }

    pub fn close_session(&mut self, session_id: SessionId) -> Result<(), BrokerError> {
        let mut runtime = self
            .sessions
            .remove(&session_id)
            .ok_or(BrokerError::SessionNotFound(session_id))?;
        runtime.close().map_err(runtime_error)
    }
}

fn runtime_error(error: wax_v2_runtime::RuntimeError) -> BrokerError {
    match error {
        wax_v2_runtime::RuntimeError::InvalidRequest(message) => {
            BrokerError::InvalidRequest(message)
        }
        wax_v2_runtime::RuntimeError::Storage(message) => BrokerError::Storage(message),
    }
}

fn map_publish_report(report: RuntimePublishReport) -> SessionImportReport {
    SessionImportReport {
        generation: report.generation,
        published_families: report
            .published_families
            .into_iter()
            .map(|family| match family {
                RuntimePublishFamily::Doc => BrokerPublishFamily::Doc,
                RuntimePublishFamily::Text => BrokerPublishFamily::Text,
                RuntimePublishFamily::Vector => BrokerPublishFamily::Vector,
            })
            .collect(),
    }
}
