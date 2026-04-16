use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use bytemuck::try_cast_slice;
use serde::Deserialize;
use wax_bench_model::{embed_text, VectorQueryMode};

use crate::documents::parse_document_id;
use crate::vector_lane::VectorLane;
use crate::{TextLane, RRF_K};

pub(crate) fn load_first_text_query(paths: &[PathBuf]) -> Result<(String, usize), String> {
    for path in paths {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let query: QueryRecord =
                serde_json::from_str(line).map_err(|error| error.to_string())?;
            if query.lane_eligibility.text {
                return Ok((query.query_text, query.top_k as usize));
            }
        }
    }

    Err("no text-eligible query found".to_owned())
}

pub(crate) fn load_query_records(path: &Path) -> Result<Vec<QueryRecord>, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(|error| error.to_string()))
        .collect()
}

pub(crate) fn load_first_hybrid_text_query(
    paths: &[PathBuf],
) -> Result<Option<FirstTextQuery>, String> {
    for path in paths {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let query: QueryRecord =
                serde_json::from_str(line).map_err(|error| error.to_string())?;
            if query.lane_eligibility.hybrid {
                return Ok(Some(FirstTextQuery {
                    query_text: query.query_text,
                    top_k: query.top_k as usize,
                }));
            }
        }
    }

    Ok(None)
}

pub(crate) fn load_text_postings(path: &Path) -> Result<HashMap<String, Vec<String>>, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let reader = BufReader::new(file);
    let mut postings = HashMap::new();
    for line in reader.lines() {
        let line = line.map_err(|error| error.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let posting: TextPostingRecord =
            serde_json::from_str(&line).map_err(|error| error.to_string())?;
        postings.insert(posting.token, posting.doc_ids);
    }
    Ok(postings)
}

pub(crate) fn load_document_ids(path: &Path) -> Result<Vec<String>, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let reader = BufReader::new(file);
    reader
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some(other),
        })
        .map(|line| {
            let line = line.map_err(|error| error.to_string())?;
            parse_document_id(&line, "document id line").map(Cow::into_owned)
        })
        .collect()
}

pub(crate) fn validate_document_vectors(
    bytes: &[u8],
    dimensions: usize,
    doc_count: usize,
) -> Result<(), String> {
    if dimensions == 0 {
        return Ok(());
    }
    if !bytes.len().is_multiple_of(dimensions * 4) {
        return Err("document vector payload has invalid length".to_owned());
    }
    let values = try_cast_slice::<u8, f32>(bytes)
        .map_err(|_| "document vector payload alignment is invalid".to_owned())?;
    if values.len() != doc_count * dimensions {
        return Err("document vector payload row count does not match manifest".to_owned());
    }
    Ok(())
}

pub(crate) fn validate_preview_vectors(
    bytes: &[u8],
    dimensions: usize,
    doc_count: usize,
) -> Result<(), String> {
    if dimensions == 0 {
        return Ok(());
    }
    if bytes.len() != doc_count * dimensions {
        return Err("preview vector payload row count does not match manifest".to_owned());
    }
    Ok(())
}

pub(crate) fn load_query_vector_records_from_paths(
    paths: &[PathBuf],
) -> Result<Vec<QueryVectorRecord>, String> {
    let mut records = Vec::new();
    for path in paths {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let query: QueryVectorRecord =
                serde_json::from_str(line).map_err(|error| error.to_string())?;
            records.push(query);
        }
    }

    Ok(records)
}

pub(crate) fn load_query_vector_records(
    query_set_path: &Path,
    dimensions: usize,
) -> Result<Vec<QueryVectorRecord>, String> {
    let text = fs::read_to_string(query_set_path).map_err(|error| error.to_string())?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let query: QueryRecord =
                serde_json::from_str(line).map_err(|error| error.to_string())?;
            Ok(QueryVectorRecord {
                query_id: query.query_id,
                top_k: query.top_k,
                vector: embed_text(&query.query_text, dimensions as u32),
                lane_eligibility: query.lane_eligibility,
            })
        })
        .collect()
}

pub(crate) fn first_vector_query_from_records(
    records: &[QueryVectorRecord],
) -> Result<FirstVectorQuery, String> {
    records
        .iter()
        .find(|query| query.lane_eligibility.vector)
        .map(|query| FirstVectorQuery {
            vector: query.vector.clone(),
            top_k: query.top_k as usize,
        })
        .ok_or_else(|| "no vector-eligible query found".to_owned())
}

pub(crate) fn first_hybrid_vector_query_from_records(
    records: &[QueryVectorRecord],
) -> Option<FirstVectorQuery> {
    records
        .iter()
        .find(|query| query.lane_eligibility.hybrid)
        .map(|query| FirstVectorQuery {
            vector: query.vector.clone(),
            top_k: query.top_k as usize,
        })
}

pub(crate) fn search_first_hybrid_query(
    text_lane: &TextLane,
    vector_lane: &mut VectorLane,
    vector_mode: VectorQueryMode,
    auto_force_exact: bool,
) -> Result<Vec<String>, String> {
    let Some(hybrid_text_query) = text_lane.first_hybrid_query.as_ref() else {
        return Ok(Vec::new());
    };
    let Some(hybrid_vector_query) = vector_lane.first_hybrid_query.as_ref() else {
        return Ok(Vec::new());
    };
    let limit = text_lane
        .first_hybrid_top_k
        .max(vector_lane.first_hybrid_top_k)
        .max(1);
    let text_hits = text_lane.search_with_limit(hybrid_text_query, limit);
    let vector_query = hybrid_vector_query.clone();
    let vector_hits =
        vector_lane.search_with_query(&vector_query, limit, vector_mode, auto_force_exact)?;
    Ok(fuse_ranked_hits(&text_hits, &vector_hits, limit))
}

pub(crate) fn search_query_hybrid(
    text_lane: &TextLane,
    vector_lane: &mut VectorLane,
    query_text: &str,
    query_vector: &[f32],
    limit: usize,
    vector_mode: VectorQueryMode,
    auto_force_exact: bool,
) -> Result<Vec<String>, String> {
    let text_hits = text_lane.search_with_limit(query_text, limit);
    let vector_hits =
        vector_lane.search_with_query(query_vector, limit, vector_mode, auto_force_exact)?;
    Ok(fuse_ranked_hits(&text_hits, &vector_hits, limit))
}

pub(crate) fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    let len = left.len().min(right.len());
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;
    let mut index = 0usize;

    while index + 4 <= len {
        sum0 += left[index] * right[index];
        sum1 += left[index + 1] * right[index + 1];
        sum2 += left[index + 2] * right[index + 2];
        sum3 += left[index + 3] * right[index + 3];
        index += 4;
    }

    let mut tail = 0.0f32;
    while index < len {
        tail += left[index] * right[index];
        index += 1;
    }

    sum0 + sum1 + sum2 + sum3 + tail
}

pub(crate) fn dot_product_i8_preview(left: &[f32], right: &[u8]) -> f32 {
    let len = left.len().min(right.len());
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;
    let mut index = 0usize;

    while index + 4 <= len {
        sum0 += left[index] * (right[index] as i8 as f32);
        sum1 += left[index + 1] * (right[index + 1] as i8 as f32);
        sum2 += left[index + 2] * (right[index + 2] as i8 as f32);
        sum3 += left[index + 3] * (right[index + 3] as i8 as f32);
        index += 4;
    }

    let mut tail = 0.0f32;
    while index < len {
        tail += left[index] * (right[index] as i8 as f32);
        index += 1;
    }

    sum0 + sum1 + sum2 + sum3 + tail
}

fn fuse_ranked_hits(text_hits: &[String], vector_hits: &[String], limit: usize) -> Vec<String> {
    let mut scores = HashMap::<String, f64>::new();
    for (rank, doc_id) in text_hits.iter().enumerate() {
        *scores.entry(doc_id.clone()).or_insert(0.0) += 1.0 / (RRF_K + rank as f64 + 1.0);
    }
    for (rank, doc_id) in vector_hits.iter().enumerate() {
        *scores.entry(doc_id.clone()).or_insert(0.0) += 1.0 / (RRF_K + rank as f64 + 1.0);
    }

    let mut fused = scores.into_iter().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap()
            .then_with(|| left.0.cmp(&right.0))
    });
    fused
        .into_iter()
        .take(limit)
        .map(|(doc_id, _)| doc_id)
        .collect()
}

#[derive(Debug, Deserialize)]
pub(crate) struct QueryRecord {
    pub(crate) query_id: String,
    pub(crate) query_text: String,
    pub(crate) top_k: u32,
    pub(crate) lane_eligibility: LaneEligibility,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LaneEligibility {
    pub(crate) text: bool,
    pub(crate) vector: bool,
    pub(crate) hybrid: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct QueryVectorRecord {
    pub(crate) query_id: String,
    pub(crate) top_k: u32,
    pub(crate) vector: Vec<f32>,
    pub(crate) lane_eligibility: LaneEligibility,
}

#[derive(Debug, Deserialize)]
struct TextPostingRecord {
    token: String,
    doc_ids: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct FirstVectorQuery {
    pub(crate) vector: Vec<f32>,
    pub(crate) top_k: usize,
}

#[derive(Debug)]
pub(crate) struct FirstTextQuery {
    pub(crate) query_text: String,
    pub(crate) top_k: usize,
}
