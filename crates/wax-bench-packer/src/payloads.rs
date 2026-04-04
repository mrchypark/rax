use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use wax_bench_model::{embed_text, tokenize};

use crate::PackError;

#[derive(Deserialize)]
pub(crate) struct QueryDefinitionStub {
    pub(crate) query_id: String,
    pub(crate) query_class: String,
    pub(crate) difficulty: String,
}

#[derive(Deserialize)]
pub(crate) struct QueryVectorStub {
    pub(crate) query_id: String,
    pub(crate) query_text: String,
    pub(crate) top_k: u32,
    pub(crate) lane_eligibility: QueryLaneEligibility,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct QueryLaneEligibility {
    pub(crate) text: bool,
    pub(crate) vector: bool,
    pub(crate) hybrid: bool,
}

#[derive(Serialize)]
struct QueryVectorRecord {
    query_id: String,
    query_text: String,
    top_k: u32,
    vector: Vec<f32>,
    lane_eligibility: QueryLaneEligibility,
}

#[derive(Deserialize)]
pub(crate) struct GroundTruthStub {
    pub(crate) query_id: String,
}

#[derive(Deserialize)]
pub(crate) struct DocumentStub {
    pub(crate) doc_id: String,
    pub(crate) text: String,
}

#[derive(Serialize)]
struct DocumentIdRecord<'a> {
    doc_id: &'a str,
}

#[derive(Serialize)]
struct DocumentOffsetRecord<'a> {
    doc_id: &'a str,
    offset: u64,
    length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DocumentOffsetRecordOwned {
    pub(crate) doc_id: String,
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

#[derive(Serialize)]
struct TextPostingRecord {
    token: String,
    doc_ids: Vec<String>,
}

pub(crate) fn build_document_vector_payload(records: &[DocumentStub], dimensions: u32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(records.len() * dimensions as usize * 4);
    for record in records {
        for value in embed_text(&record.text, dimensions) {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    bytes
}

pub(crate) fn build_quantized_vector_preview_payload(
    vector_bytes: &[u8],
) -> Result<Vec<u8>, PackError> {
    if !vector_bytes.len().is_multiple_of(4) {
        return Err(PackError::new(
            "document vector payload must be aligned to f32",
        ));
    }

    let mut out = Vec::with_capacity(vector_bytes.len() / 4);
    for chunk in vector_bytes.chunks_exact(4) {
        let value = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let scaled = (value.clamp(-1.0, 1.0) * 127.0).round() as i8;
        out.push(scaled as u8);
    }
    Ok(out)
}

pub(crate) fn build_adhoc_query_files(
    records: &[DocumentStub],
) -> Result<(Vec<u8>, Vec<u8>), PackError> {
    let first = records
        .first()
        .ok_or_else(|| PackError::new("documents file must contain at least one record"))?;
    let query_text = if first.text.trim().is_empty() {
        first.doc_id.clone()
    } else {
        first.text.clone()
    };
    let top_k = records.len().min(10) as u32;
    let query_line = serde_json::json!({
        "query_id": "adhoc-q-001",
        "query_class": "hybrid",
        "difficulty": "easy",
        "query_text": query_text,
        "top_k": top_k.max(1),
        "filter_spec": {},
        "preview_expected": true,
        "embedding_available": true,
        "lane_eligibility": {
            "text": true,
            "vector": true,
            "hybrid": true,
        }
    });
    let ground_truth_line = serde_json::json!({
        "query_id": "adhoc-q-001",
    });

    let mut query_bytes = serde_json::to_vec(&query_line)
        .map_err(|_| PackError::new("failed to serialize adhoc query"))?;
    query_bytes.push(b'\n');
    let mut ground_truth_bytes = serde_json::to_vec(&ground_truth_line)
        .map_err(|_| PackError::new("failed to serialize adhoc ground truth"))?;
    ground_truth_bytes.push(b'\n');
    Ok((query_bytes, ground_truth_bytes))
}

pub(crate) fn build_query_vector_payload(
    bytes: &[u8],
    dimensions: u32,
) -> Result<Vec<u8>, PackError> {
    let mut out = Vec::new();
    for record in load_query_vector_stubs(bytes)? {
        let payload = QueryVectorRecord {
            query_id: record.query_id,
            top_k: record.top_k,
            vector: embed_text(&record.query_text, dimensions),
            lane_eligibility: record.lane_eligibility,
            query_text: record.query_text,
        };
        let line = serde_json::to_string(&payload)
            .map_err(|_| PackError::new("failed to serialize query vector payload"))?;
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

pub(crate) fn non_empty_line_count(bytes: &[u8]) -> u64 {
    std::str::from_utf8(bytes)
        .ok()
        .map(|text| text.lines().filter(|line| !line.is_empty()).count() as u64)
        .unwrap_or(0)
}

pub(crate) fn load_query_vector_stubs(bytes: &[u8]) -> Result<Vec<QueryVectorStub>, PackError> {
    let text = std::str::from_utf8(bytes).map_err(|_| PackError::new("query set must be utf-8"))?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .map_err(|_| PackError::new("query_set file contains invalid json"))
        })
        .collect()
}

pub(crate) fn build_document_id_payload(
    records: &[DocumentStub],
) -> Result<Vec<u8>, serde_json::Error> {
    let mut out = Vec::new();
    for record in records {
        let line = serde_json::to_string(&DocumentIdRecord {
            doc_id: &record.doc_id,
        })?;
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

pub(crate) fn build_document_offset_payload(
    records: &[DocumentOffsetRecordOwned],
) -> Result<Vec<u8>, serde_json::Error> {
    let mut out = Vec::new();
    for record in records {
        let line = serde_json::to_string(&DocumentOffsetRecord {
            doc_id: &record.doc_id,
            offset: record.offset,
            length: record.length,
        })?;
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

pub(crate) fn build_text_postings_payload(
    records: &[DocumentStub],
) -> Result<Vec<u8>, serde_json::Error> {
    let mut postings: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for record in records {
        let mut seen = HashSet::new();
        for token in tokenize(&record.text) {
            if seen.insert(token.clone()) {
                postings
                    .entry(token)
                    .or_default()
                    .push(record.doc_id.clone());
            }
        }
    }

    let mut out = Vec::new();
    for (token, doc_ids) in postings {
        let line = serde_json::to_string(&TextPostingRecord { token, doc_ids })?;
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}
