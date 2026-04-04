use sha2::{Digest, Sha256};
use wax_bench_model::{DirtyProfile, QueryVectorProfile, SegmentTopologyEntry, VectorProfile};

use crate::PackError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyntheticEmbeddingIdentity {
    pub(crate) spec_id: String,
    pub(crate) model_version: String,
    pub(crate) model_hash: String,
}

pub(crate) fn build_vector_profile(embedding_spec_id: &str) -> VectorProfile {
    VectorProfile {
        enabled: true,
        embedding_dimensions: embedding_dimensions_from_spec_id(embedding_spec_id).unwrap_or(0),
        embedding_dtype: embedding_dtype_from_spec_id(embedding_spec_id).to_owned(),
        distance_metric: distance_metric_from_spec_id(embedding_spec_id).to_owned(),
        ann_index_backend: Some("hnsw_rs".to_owned()),
        ann_sidecar_reproducibility: Some("derived_nondeterministic".to_owned()),
        query_vectors: QueryVectorProfile {
            precomputed_available: false,
            runtime_embedding_supported: true,
        },
    }
}

pub(crate) fn synthetic_embedding_identity(embedding_spec_id: &str) -> SyntheticEmbeddingIdentity {
    let dimensions = embedding_dimensions_from_spec_id(embedding_spec_id).unwrap_or(0);
    let dtype = embedding_dtype_from_spec_id(embedding_spec_id);
    let metric = distance_metric_from_spec_id(embedding_spec_id);
    let generator_tag = "feature-hash-stub-v1";
    SyntheticEmbeddingIdentity {
        spec_id: format!("feature-hash-stub-{dimensions}-{dtype}-{metric}"),
        model_version: generator_tag.to_owned(),
        model_hash: checksum_label(generator_tag.as_bytes()),
    }
}

pub(crate) fn build_dirty_profile(
    variant: &str,
    clean_dataset_id: &str,
) -> Result<DirtyProfile, PackError> {
    match variant {
        "clean" => Ok(DirtyProfile {
            profile: "clean".to_owned(),
            base_dataset_id: None,
            seed: 0,
            delete_ratio: 0.0,
            update_ratio: 0.0,
            append_ratio: 0.0,
            target_segment_count_range: [1, 1],
            target_segment_topology: vec![SegmentTopologyEntry {
                tier: "large".to_owned(),
                count: 1,
            }],
            target_tombstone_ratio: 0.0,
            compaction_state: "clean".to_owned(),
        }),
        "dirty_light" => Ok(DirtyProfile {
            profile: "dirty_light".to_owned(),
            base_dataset_id: Some(clean_dataset_id.to_owned()),
            seed: 7,
            delete_ratio: 0.05,
            update_ratio: 0.02,
            append_ratio: 0.01,
            target_segment_count_range: [2, 4],
            target_segment_topology: vec![
                SegmentTopologyEntry {
                    tier: "large".to_owned(),
                    count: 1,
                },
                SegmentTopologyEntry {
                    tier: "small".to_owned(),
                    count: 2,
                },
            ],
            target_tombstone_ratio: 0.08,
            compaction_state: "pre_compaction".to_owned(),
        }),
        _ => Err(PackError::new("unsupported variant")),
    }
}

pub(crate) fn dataset_id(
    dataset_family: &str,
    tier: &str,
    variant: &str,
    dataset_version: &str,
) -> String {
    format!("{dataset_family}-{tier}-{variant}-{dataset_version}")
}

pub(crate) fn checksum_label(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn embedding_dtype_from_spec_id(spec_id: &str) -> &str {
    spec_id
        .split('-')
        .find(|part| matches!(*part, "f32" | "i8" | "u8"))
        .unwrap_or("f32")
}

fn distance_metric_from_spec_id(spec_id: &str) -> &str {
    spec_id.rsplit('-').next().unwrap_or("cosine")
}

pub(crate) fn manifest_query_fingerprint(
    tier: &str,
    variant: &str,
    clean_dataset_id: &str,
) -> serde_json::Value {
    serde_json::json!({
        "tier": tier,
        "variant": variant,
        "clean_dataset_id": clean_dataset_id,
    })
}

pub(crate) fn embedding_dimensions_from_spec_id(spec_id: &str) -> Option<u32> {
    let parts: Vec<&str> = spec_id.split('-').collect();
    for window in parts.windows(2) {
        if matches!(window[1], "f32" | "i8" | "u8") {
            if let Ok(dimensions) = window[0].parse::<u32>() {
                return Some(dimensions);
            }
        }
    }

    None
}

pub(crate) fn require_embedding_dimensions(spec_id: &str) -> Result<u32, PackError> {
    embedding_dimensions_from_spec_id(spec_id)
        .ok_or_else(|| PackError::new("embedding_spec_id must declare vector dimensions"))
}
