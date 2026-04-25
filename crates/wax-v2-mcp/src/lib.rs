use serde::{Deserialize, Serialize};
use wax_v2_broker::{
    SessionNewDocument, SessionNewDocumentVector, SessionSearchRequest, WaxBroker,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpNewDocument {
    pub doc_id: String,
    pub text: String,
    pub metadata: serde_json::Value,
    pub timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpNewDocumentVector {
    pub doc_id: String,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "tool", rename_all = "snake_case")]
pub enum McpRequest {
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

#[derive(Default)]
pub struct WaxMcpSurface {
    broker: WaxBroker,
}

impl WaxMcpSurface {
    pub fn handle(&mut self, request: McpRequest) -> Result<McpResponse, McpError> {
        match request {
            McpRequest::OpenSession { root } => {
                let session_id = self
                    .broker
                    .open_session(std::path::Path::new(&root))
                    .map_err(broker_error)?;
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
                    hits: response
                        .hits
                        .into_iter()
                        .map(|hit| McpSearchHit {
                            doc_id: hit.doc_id,
                            preview: hit.preview,
                        })
                        .collect(),
                })
            }
            McpRequest::ImportCompatibilitySnapshot { session_id } => {
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
                self.broker
                    .close_session(wax_v2_broker::SessionId::from_u64(session_id))
                    .map_err(broker_error)?;
                Ok(McpResponse::SessionClosed { session_id })
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{McpError, McpErrorCode, McpRequest, McpResponse};

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
}
