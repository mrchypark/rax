use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::Deserialize;
use wax_bench_model::{EnvironmentConstraints, LanguageShare, LengthBuckets, MetadataProfile};

use crate::payloads::{load_query_vector_stubs, DocumentOffsetRecordOwned, DocumentStub};
use crate::PackError;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct SourceConfig {
    pub(crate) dataset_family: String,
    pub(crate) dataset_version: String,
    pub(crate) generated_at: String,
    pub(crate) embedding_spec_id: String,
    pub(crate) embedding_model_version: String,
    pub(crate) embedding_model_hash: String,
    pub(crate) environment_constraints: EnvironmentConstraints,
    pub(crate) languages: Vec<LanguageShare>,
    pub(crate) metadata_profile: MetadataProfile,
    pub(crate) query_sets: Vec<SourceQuerySet>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct SourceQuerySet {
    pub(crate) name: String,
    pub(crate) path: String,
    pub(crate) ground_truth_path: String,
    #[serde(default)]
    pub(crate) qrels_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DocumentStats {
    pub(crate) doc_count: u64,
    pub(crate) total_text_bytes: u64,
    pub(crate) avg_doc_length: f64,
    pub(crate) median_doc_length: u64,
    pub(crate) p95_doc_length: u64,
    pub(crate) max_doc_length: u64,
    pub(crate) length_buckets: LengthBuckets,
}

pub(crate) fn load_source_config(path: &Path) -> Result<SourceConfig, PackError> {
    let text =
        fs::read_to_string(path).map_err(|_| PackError::new("failed to read source config"))?;
    serde_json::from_str(&text).map_err(|_| PackError::new("failed to parse source config"))
}

pub(crate) fn analyze_documents(records: &[DocumentStub]) -> DocumentStats {
    let mut lengths = Vec::new();
    for record in records {
        lengths.push(record.text.len() as u64);
    }

    lengths.sort_unstable();
    let doc_count = lengths.len() as u64;
    let total_text_bytes = lengths.iter().sum::<u64>();
    let avg_doc_length = if doc_count == 0 {
        0.0
    } else {
        total_text_bytes as f64 / doc_count as f64
    };
    let median_doc_length = percentile_value(&lengths, 0.5);
    let p95_doc_length = percentile_value(&lengths, 0.95);
    let max_doc_length = lengths.last().copied().unwrap_or(0);
    let short_count = lengths.iter().filter(|length| **length <= 64).count() as f64;
    let medium_count = lengths
        .iter()
        .filter(|length| **length > 64 && **length <= 256)
        .count() as f64;
    let long_count = lengths.iter().filter(|length| **length > 256).count() as f64;
    let total_docs = doc_count.max(1) as f64;

    DocumentStats {
        doc_count,
        total_text_bytes,
        avg_doc_length,
        median_doc_length,
        p95_doc_length,
        max_doc_length,
        length_buckets: LengthBuckets {
            short_ratio: short_count / total_docs,
            medium_ratio: medium_count / total_docs,
            long_ratio: long_count / total_docs,
        },
    }
}

pub(crate) fn load_document_records_with_offsets(
    path: &Path,
) -> Result<(Vec<DocumentStub>, Vec<DocumentOffsetRecordOwned>), PackError> {
    let file = File::open(path).map_err(|_| PackError::new("failed to read source file"))?;
    let mut reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut offsets = Vec::new();
    let mut line = String::new();
    let mut offset = 0u64;

    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|_| PackError::new("failed to read source file"))?;
        if read == 0 {
            break;
        }

        let line_offset = offset;
        offset += read as u64;
        if line.trim().is_empty() {
            continue;
        }

        let record: DocumentStub = serde_json::from_str(&line)
            .map_err(|_| PackError::new("documents file contains invalid json"))?;
        offsets.push(DocumentOffsetRecordOwned {
            doc_id: record.doc_id.clone(),
            offset: line_offset,
            length: read as u64,
        });
        records.push(record);
    }

    Ok((records, offsets))
}

pub(crate) fn ensure_vector_query_exists(
    source_dir: &Path,
    query_sets: &[SourceQuerySet],
) -> Result<(), PackError> {
    for query_set in query_sets {
        let bytes = fs::read(source_dir.join(&query_set.path))
            .map_err(|_| PackError::new("failed to read source file"))?;
        if load_query_vector_stubs(&bytes)?
            .into_iter()
            .any(|record| record.lane_eligibility.vector)
        {
            return Ok(());
        }
    }

    Err(PackError::new(
        "vector-enabled datasets require a vector query",
    ))
}

fn percentile_value(lengths: &[u64], percentile: f64) -> u64 {
    if lengths.is_empty() {
        return 0;
    }

    let index = ((lengths.len() as f64 * percentile).ceil() as usize).saturating_sub(1);
    lengths[index.min(lengths.len() - 1)]
}
