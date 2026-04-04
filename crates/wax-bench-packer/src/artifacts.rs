use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use wax_bench_model::{build_vector_lane_skeleton, DifficultyDistribution, ManifestFile};

use crate::payloads::{
    build_document_id_payload, build_document_offset_payload, build_document_vector_payload,
    build_quantized_vector_preview_payload, build_query_vector_payload,
    build_text_postings_payload, non_empty_line_count, DocumentOffsetRecordOwned, DocumentStub,
    QueryDefinitionStub,
};
use crate::{build_hnsw_vector_sidecar, build_manifest_file, PackError};

pub(crate) struct EmittedVectorArtifacts {
    pub(crate) document_vector_bytes: Vec<u8>,
    pub(crate) manifest_files: Vec<ManifestFile>,
}

pub(crate) struct EmittedQueryArtifacts {
    pub(crate) query_vector_bytes: Vec<u8>,
    pub(crate) manifest_files: Vec<ManifestFile>,
    pub(crate) summary: QuerySetSummary,
}

pub(crate) struct QueryArtifactSpec<'a> {
    pub(crate) query_path: &'a str,
    pub(crate) query_bytes: &'a [u8],
    pub(crate) ground_truth_path: &'a str,
    pub(crate) ground_truth_bytes: &'a [u8],
    pub(crate) qrels_path: Option<&'a str>,
    pub(crate) qrels_bytes: Option<&'a [u8]>,
    pub(crate) query_vector_path: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuerySetSummary {
    pub(crate) query_count: u64,
    pub(crate) classes: BTreeSet<String>,
    pub(crate) difficulty_distribution: DifficultyDistribution,
}

pub(crate) fn emit_document_sidecars(
    out_dir: &Path,
    document_records: &[DocumentStub],
    document_offsets: &[DocumentOffsetRecordOwned],
) -> Result<Vec<ManifestFile>, PackError> {
    let document_id_bytes = build_document_id_payload(document_records)
        .map_err(|_| PackError::new("failed to serialize document id payload"))?;
    write_payload(out_dir, "document_ids.jsonl", &document_id_bytes)
        .map_err(|_| PackError::new("failed to write document id payload"))?;

    let text_posting_bytes = build_text_postings_payload(document_records)
        .map_err(|_| PackError::new("failed to serialize text postings payload"))?;
    write_payload(out_dir, "text_postings.jsonl", &text_posting_bytes)
        .map_err(|_| PackError::new("failed to write text postings payload"))?;

    let document_offset_bytes = build_document_offset_payload(document_offsets)
        .map_err(|_| PackError::new("failed to serialize document offset payload"))?;
    write_payload(out_dir, "document_offsets.jsonl", &document_offset_bytes)
        .map_err(|_| PackError::new("failed to write document offset payload"))?;

    Ok(vec![
        build_manifest_file(
            "document_ids.jsonl",
            "document_ids",
            "jsonl",
            document_records.len() as u64,
            &document_id_bytes,
        ),
        build_manifest_file(
            "text_postings.jsonl",
            "text_postings",
            "jsonl",
            non_empty_line_count(&text_posting_bytes),
            &text_posting_bytes,
        ),
        build_manifest_file(
            "document_offsets.jsonl",
            "document_offsets",
            "jsonl",
            document_offsets.len() as u64,
            &document_offset_bytes,
        ),
    ])
}

pub(crate) fn emit_vector_artifacts(
    out_dir: &Path,
    document_records: &[DocumentStub],
    dimensions: u32,
) -> Result<EmittedVectorArtifacts, PackError> {
    let document_vector_bytes = build_document_vector_payload(document_records, dimensions);
    write_payload(out_dir, "document_vectors.f32", &document_vector_bytes)
        .map_err(|_| PackError::new("failed to write document vector payload"))?;

    let (hnsw_graph_path, hnsw_graph_bytes, hnsw_data_path, hnsw_data_bytes) =
        build_hnsw_vector_sidecar(
            out_dir,
            &document_vector_bytes,
            dimensions as usize,
            document_records.len(),
        )?;

    let document_vector_preview_bytes =
        build_quantized_vector_preview_payload(&document_vector_bytes)
            .map_err(|_| PackError::new("failed to build quantized vector preview payload"))?;
    write_payload(
        out_dir,
        "document_vectors.q8",
        &document_vector_preview_bytes,
    )
    .map_err(|_| PackError::new("failed to write quantized vector preview payload"))?;

    let vector_lane_skeleton_bytes = build_vector_lane_skeleton(
        &document_records
            .iter()
            .map(|record| record.doc_id.clone())
            .collect::<Vec<_>>(),
        dimensions,
    );
    write_payload(out_dir, "vector_lane.skel", &vector_lane_skeleton_bytes)
        .map_err(|_| PackError::new("failed to write vector lane skeleton"))?;

    let manifest_files = vec![
        build_manifest_file(
            "document_vectors.f32",
            "document_vectors",
            "f32le-row-major",
            document_records.len() as u64,
            &document_vector_bytes,
        ),
        build_manifest_file(
            &hnsw_graph_path,
            "vector_hnsw_graph",
            "hnsw-rs-graph",
            document_records.len() as u64,
            &hnsw_graph_bytes,
        ),
        build_manifest_file(
            &hnsw_data_path,
            "vector_hnsw_data",
            "hnsw-rs-data",
            document_records.len() as u64,
            &hnsw_data_bytes,
        ),
        build_manifest_file(
            "document_vectors.q8",
            "document_vectors_preview_q8",
            "i8-row-major",
            document_records.len() as u64,
            &document_vector_preview_bytes,
        ),
        build_manifest_file(
            "vector_lane.skel",
            "vector_lane_skeleton",
            "wax-vector-lane-skeleton-v1",
            document_records.len() as u64,
            &vector_lane_skeleton_bytes,
        ),
    ];

    Ok(EmittedVectorArtifacts {
        document_vector_bytes,
        manifest_files,
    })
}

pub(crate) fn emit_query_artifacts(
    out_dir: &Path,
    spec: QueryArtifactSpec<'_>,
    dimensions: u32,
) -> Result<EmittedQueryArtifacts, PackError> {
    let summary = analyze_query_set(spec.query_bytes)?;

    write_payload(out_dir, spec.query_path, spec.query_bytes)
        .map_err(|_| PackError::new("failed to write query set"))?;
    write_payload(out_dir, spec.ground_truth_path, spec.ground_truth_bytes)
        .map_err(|_| PackError::new("failed to write ground truth"))?;
    if let (Some(qrels_path), Some(qrels_bytes)) = (spec.qrels_path, spec.qrels_bytes) {
        write_payload(out_dir, qrels_path, qrels_bytes)
            .map_err(|_| PackError::new("failed to write qrels"))?;
    }

    let query_vector_bytes = build_query_vector_payload(spec.query_bytes, dimensions)?;
    write_payload(out_dir, spec.query_vector_path, &query_vector_bytes)
        .map_err(|_| PackError::new("failed to write query vector payload"))?;

    let mut manifest_files = vec![
        build_manifest_file(
            spec.query_path,
            "query_set",
            "jsonl",
            summary.query_count,
            spec.query_bytes,
        ),
        build_manifest_file(
            spec.ground_truth_path,
            "ground_truth",
            "jsonl",
            non_empty_line_count(spec.ground_truth_bytes),
            spec.ground_truth_bytes,
        ),
        build_manifest_file(
            spec.query_vector_path,
            "query_vectors",
            "jsonl",
            summary.query_count,
            &query_vector_bytes,
        ),
    ];
    if let (Some(qrels_path), Some(qrels_bytes)) = (spec.qrels_path, spec.qrels_bytes) {
        manifest_files.push(build_manifest_file(
            qrels_path,
            "qrels",
            "jsonl",
            non_empty_line_count(qrels_bytes),
            qrels_bytes,
        ));
    }

    Ok(EmittedQueryArtifacts {
        query_vector_bytes,
        manifest_files,
        summary,
    })
}

fn analyze_query_set(bytes: &[u8]) -> Result<QuerySetSummary, PackError> {
    let text = std::str::from_utf8(bytes).map_err(|_| PackError::new("query set must be utf-8"))?;
    let mut classes = BTreeSet::new();
    let mut easy = 0;
    let mut medium = 0;
    let mut hard = 0;
    let mut query_count = 0;

    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: QueryDefinitionStub = serde_json::from_str(line)
            .map_err(|_| PackError::new("query_set file contains invalid json"))?;
        query_count += 1;
        classes.insert(record.query_class);
        match record.difficulty.as_str() {
            "easy" => easy += 1,
            "medium" => medium += 1,
            "hard" => hard += 1,
            _ => {}
        }
    }

    Ok(QuerySetSummary {
        query_count,
        classes,
        difficulty_distribution: DifficultyDistribution { easy, medium, hard },
    })
}

fn write_payload(out_dir: &Path, relative_path: &str, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = out_dir.join(relative_path).parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(out_dir.join(relative_path), bytes)
}
