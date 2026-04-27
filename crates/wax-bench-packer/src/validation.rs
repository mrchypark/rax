use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use wax_bench_model::{DatasetPackManifest, QrelRecord};

use crate::manifest_builder::embedding_dimensions_from_spec_id;
use crate::payloads::{GroundTruthStub, QueryDefinitionStub};
use crate::{checksum_file, ValidationError};

pub(crate) fn validate_manifest_inner(
    manifest: &DatasetPackManifest,
    pack_root: &Path,
) -> Result<(), ValidationError> {
    validate_constrained_values(manifest)?;
    validate_file_references(manifest)?;
    validate_query_ids(manifest, pack_root)?;
    validate_vector_profile(manifest)?;
    validate_dirty_profile(manifest)?;
    validate_vector_checksum_requirements(manifest)?;
    validate_file_payloads(manifest, pack_root)?;

    Ok(())
}

fn validate_query_ids(
    manifest: &DatasetPackManifest,
    pack_root: &Path,
) -> Result<(), ValidationError> {
    let mut seen_query_set_ids = HashSet::new();
    let mut seen_query_ids = HashSet::new();
    for query_set in &manifest.query_sets {
        if !seen_query_set_ids.insert(query_set.query_set_id.clone()) {
            return Err(ValidationError::new("duplicate query_set_id"));
        }

        let query_ids = load_query_ids(pack_root.join(&query_set.path))?;
        if query_set.query_count != query_ids.len() as u64 {
            return Err(ValidationError::new(
                "query_count must match query file contents",
            ));
        }

        let ground_truth_ids = load_ground_truth_ids(pack_root.join(&query_set.ground_truth_path))?;
        if query_ids.iter().cloned().collect::<HashSet<_>>()
            != ground_truth_ids.into_iter().collect::<HashSet<_>>()
        {
            return Err(ValidationError::new(
                "ground_truth file must align with query ids",
            ));
        }
        if let Some(qrels_path) = &query_set.qrels_path {
            validate_qrels(pack_root.join(qrels_path), &query_ids)?;
        }

        for query_id in query_ids {
            if !seen_query_ids.insert(query_id) {
                return Err(ValidationError::new("duplicate query_id"));
            }
        }
    }

    Ok(())
}

fn validate_vector_profile(manifest: &DatasetPackManifest) -> Result<(), ValidationError> {
    if let Some(expected_dimensions) =
        embedding_dimensions_from_spec_id(&manifest.identity.embedding_spec_id)
    {
        if manifest.vector_profile.embedding_dimensions != expected_dimensions {
            return Err(ValidationError::new(
                "embedding_dimensions does not match embedding_spec_id",
            ));
        }
    }

    if !manifest.vector_profile.enabled {
        if manifest.corpus.vector_count != 0 {
            return Err(ValidationError::new(
                "vector_count must be 0 when vectors are disabled",
            ));
        }

        if manifest.vector_profile.embedding_dimensions != 0 {
            return Err(ValidationError::new(
                "embedding_dimensions must be 0 when vectors are disabled",
            ));
        }
    }

    let has_hnsw_sidecars = manifest
        .files
        .iter()
        .any(|file| matches!(file.kind.as_str(), "vector_hnsw_graph" | "vector_hnsw_data"));
    if has_hnsw_sidecars {
        if manifest.vector_profile.ann_index_backend.as_deref() != Some("hnsw_rs") {
            return Err(ValidationError::new(
                "hnsw sidecars must declare ann_index_backend=hnsw_rs",
            ));
        }
        if manifest
            .vector_profile
            .ann_sidecar_reproducibility
            .as_deref()
            != Some("derived_nondeterministic")
        {
            return Err(ValidationError::new(
                "hnsw sidecars must declare ann_sidecar_reproducibility=derived_nondeterministic",
            ));
        }
    }

    Ok(())
}

fn validate_dirty_profile(manifest: &DatasetPackManifest) -> Result<(), ValidationError> {
    let is_clean_profile = manifest.dirty_profile.profile == "clean";
    let has_dirty_behavior = manifest.dirty_profile.delete_ratio > 0.0
        || manifest.dirty_profile.update_ratio > 0.0
        || manifest.dirty_profile.append_ratio > 0.0;

    if manifest.identity.variant_id != manifest.dirty_profile.profile {
        return Err(ValidationError::new(
            "variant_id must match dirty_profile.profile",
        ));
    }

    if is_clean_profile {
        if has_dirty_behavior {
            return Err(ValidationError::new(
                "clean profile must use zero dirty ratios",
            ));
        }

        if manifest.dirty_profile.compaction_state != "clean" {
            return Err(ValidationError::new(
                "clean profile must use compaction_state=clean",
            ));
        }
    } else if manifest.dirty_profile.base_dataset_id.is_none() {
        return Err(ValidationError::new(
            "dirty variants must declare base_dataset_id",
        ));
    }

    Ok(())
}

fn validate_file_references(manifest: &DatasetPackManifest) -> Result<(), ValidationError> {
    let mut file_paths = HashSet::new();
    for file in &manifest.files {
        if !file_paths.insert(file.path.as_str()) {
            return Err(ValidationError::new("duplicate file path"));
        }

        if !is_pack_relative_path(&file.path) {
            return Err(ValidationError::new(
                "file paths must stay within the dataset pack root",
            ));
        }

        if !file.checksum.starts_with("sha256:") {
            return Err(ValidationError::new("file checksum must use sha256 format"));
        }

        if !matches!(
            file.kind.as_str(),
            "documents"
                | "metadata"
                | "query_set"
                | "ground_truth"
                | "qrels"
                | "text_postings"
                | "document_ids"
                | "document_offsets"
                | "document_vectors"
                | "vector_hnsw_graph"
                | "vector_hnsw_data"
                | "document_vectors_preview_q8"
                | "vector_lane_skeleton"
                | "query_vectors"
                | "store"
                | "prebuilt_store"
        ) {
            return Err(ValidationError::new("invalid file kind"));
        }
    }

    for query_set in &manifest.query_sets {
        if !file_paths.contains(query_set.path.as_str()) {
            return Err(ValidationError::new(
                "query_set path must reference a file entry",
            ));
        }

        if !file_paths.contains(query_set.ground_truth_path.as_str()) {
            return Err(ValidationError::new(
                "ground_truth_path must reference a file entry",
            ));
        }
        if let Some(qrels_path) = &query_set.qrels_path {
            if !file_paths.contains(qrels_path.as_str()) {
                return Err(ValidationError::new(
                    "qrels_path must reference a file entry",
                ));
            }
        }

        let query_file = manifest
            .files
            .iter()
            .find(|file| file.path == query_set.path)
            .expect("query_set path must exist after membership check");
        if query_file.kind != "query_set" {
            return Err(ValidationError::new(
                "query_set path must point to a query_set file",
            ));
        }

        let ground_truth_file = manifest
            .files
            .iter()
            .find(|file| file.path == query_set.ground_truth_path)
            .expect("ground_truth_path must exist after membership check");
        if ground_truth_file.kind != "ground_truth" {
            return Err(ValidationError::new(
                "ground_truth_path must point to a ground_truth file",
            ));
        }
        if let Some(qrels_path) = &query_set.qrels_path {
            let qrels_file = manifest
                .files
                .iter()
                .find(|file| file.path == *qrels_path)
                .expect("qrels_path must exist after membership check");
            if qrels_file.kind != "qrels" {
                return Err(ValidationError::new(
                    "qrels_path must point to a qrels file",
                ));
            }
        }
    }

    Ok(())
}

fn validate_file_payloads(
    manifest: &DatasetPackManifest,
    pack_root: &Path,
) -> Result<(), ValidationError> {
    for file in &manifest.files {
        let path = pack_root.join(&file.path);
        let checksum = checksum_file(&path)
            .map_err(|_| ValidationError::new("file path must exist in the dataset pack"))?;

        if file.checksum != checksum {
            return Err(ValidationError::new(
                "file checksum must match file contents",
            ));
        }
    }

    Ok(())
}

fn validate_constrained_values(manifest: &DatasetPackManifest) -> Result<(), ValidationError> {
    if !matches!(
        manifest.identity.dataset_tier.as_str(),
        "small" | "medium" | "large"
    ) {
        return Err(ValidationError::new("invalid dataset_tier"));
    }

    if !matches!(
        manifest.vector_profile.embedding_dtype.as_str(),
        "f32" | "i8" | "u8"
    ) {
        return Err(ValidationError::new("invalid embedding_dtype"));
    }

    if !matches!(
        manifest.vector_profile.distance_metric.as_str(),
        "cosine" | "dot" | "l2"
    ) {
        return Err(ValidationError::new("invalid distance_metric"));
    }

    if !matches!(
        manifest.dirty_profile.compaction_state.as_str(),
        "clean" | "pre_compaction" | "post_compaction"
    ) {
        return Err(ValidationError::new("invalid compaction_state"));
    }

    for query_set in &manifest.query_sets {
        for class in &query_set.classes {
            if !matches!(
                class.as_str(),
                "keyword"
                    | "prefix"
                    | "fuzzy_keyword"
                    | "topical"
                    | "vector"
                    | "hybrid"
                    | "metadata_filtered"
                    | "no_hit"
                    | "high_recall"
            ) {
                return Err(ValidationError::new("invalid query class"));
            }
        }
    }

    Ok(())
}

fn validate_vector_checksum_requirements(
    manifest: &DatasetPackManifest,
) -> Result<(), ValidationError> {
    let has_vector_payload = manifest
        .files
        .iter()
        .any(|file| file.kind == "document_vectors");
    if has_vector_payload && manifest.checksums.logical_vector_payload_checksum.is_none() {
        return Err(ValidationError::new(
            "vector payload checksum is required when vector payload exists",
        ));
    }

    Ok(())
}

fn load_query_ids(path: PathBuf) -> Result<Vec<String>, ValidationError> {
    let text = fs::read_to_string(&path)
        .map_err(|_| ValidationError::new("query_set file must be readable"))?;
    let mut query_ids = Vec::new();

    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: QueryDefinitionStub = serde_json::from_str(line)
            .map_err(|_| ValidationError::new("query_set file contains invalid json"))?;
        query_ids.push(record.query_id);
    }

    Ok(query_ids)
}

fn load_ground_truth_ids(path: PathBuf) -> Result<Vec<String>, ValidationError> {
    let text = fs::read_to_string(&path)
        .map_err(|_| ValidationError::new("ground_truth file must be readable"))?;
    let mut query_ids = Vec::new();
    let mut seen_query_ids = HashSet::new();

    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: GroundTruthStub = serde_json::from_str(line)
            .map_err(|_| ValidationError::new("ground_truth file contains invalid json"))?;
        if !seen_query_ids.insert(record.query_id.clone()) {
            return Err(ValidationError::new(
                "ground_truth file contains duplicate query_id",
            ));
        }
        query_ids.push(record.query_id);
    }

    Ok(query_ids)
}

fn validate_qrels(path: PathBuf, query_ids: &[String]) -> Result<(), ValidationError> {
    let qrels = load_qrels(path)?;
    let expected_query_ids = query_ids.iter().map(String::as_str).collect::<HashSet<_>>();
    let mut qrel_query_ids = HashSet::new();
    let mut seen_pairs = HashSet::new();

    for qrel in qrels {
        if qrel.relevance > 3 {
            return Err(ValidationError::new(
                "qrels file contains invalid relevance",
            ));
        }
        if !expected_query_ids.contains(qrel.query_id.as_str()) {
            return Err(ValidationError::new("qrels file must align with query ids"));
        }
        qrel_query_ids.insert(qrel.query_id.clone());
        if !seen_pairs.insert((qrel.query_id, qrel.doc_id)) {
            return Err(ValidationError::new(
                "qrels file contains duplicate query_id/doc_id",
            ));
        }
    }
    if qrel_query_ids.len() != expected_query_ids.len() {
        return Err(ValidationError::new("qrels file must align with query ids"));
    }

    Ok(())
}

fn load_qrels(path: PathBuf) -> Result<Vec<QrelRecord>, ValidationError> {
    let text = fs::read_to_string(&path)
        .map_err(|_| ValidationError::new("qrels file must be readable"))?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .map_err(|_| ValidationError::new("qrels file contains invalid json"))
        })
        .collect()
}

fn is_pack_relative_path(path: &str) -> bool {
    let path = Path::new(path);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}
