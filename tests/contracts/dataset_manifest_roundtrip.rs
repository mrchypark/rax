use std::path::{Path, PathBuf};

use wax_bench_model::DatasetPackManifest;
use wax_bench_packer::validate_manifest;

#[test]
fn dataset_manifest_round_trips_and_validates() {
    let (manifest, _manifest_text, manifest_root) = load_manifest();

    validate_manifest(&manifest, &manifest_root).unwrap();

    let rewritten = serde_json::to_string_pretty(&manifest).unwrap();
    let reparsed: DatasetPackManifest = serde_json::from_str(&rewritten).unwrap();
    let rewritten_again = serde_json::to_string_pretty(&reparsed).unwrap();

    assert_eq!(rewritten, rewritten_again);

    assert_eq!(reparsed.identity.dataset_id, "knowledge-small-clean-v1");
    assert_eq!(reparsed.query_sets[0].query_count, 3);
    assert_eq!(
        reparsed.vector_profile.ann_index_backend.as_deref(),
        Some("hnsw_rs")
    );
    assert_eq!(
        reparsed
            .vector_profile
            .ann_sidecar_reproducibility
            .as_deref(),
        Some("derived_nondeterministic")
    );
}

#[test]
fn dataset_manifest_validation_rejects_duplicate_query_id() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.query_sets.push(wax_bench_model::QuerySetEntry {
        query_set_id: "knowledge-small-duplicate-v1".to_owned(),
        path: "queries/duplicate.jsonl".to_owned(),
        ground_truth_path: "queries/duplicate-query-ground-truth.jsonl".to_owned(),
        qrels_path: None,
        query_count: 3,
        classes: vec!["keyword".to_owned()],
        difficulty_distribution: wax_bench_model::DifficultyDistribution {
            easy: 3,
            medium: 0,
            hard: 0,
        },
    });
    manifest.files.push(wax_bench_model::ManifestFile {
        path: "queries/duplicate-query-ground-truth.jsonl".to_owned(),
        kind: "ground_truth".to_owned(),
        format: "jsonl".to_owned(),
        record_count: 2,
        checksum: "sha256:duplicate-query-ground-truth".to_owned(),
    });
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "duplicate query_id"
    );
}

#[test]
fn dataset_manifest_validation_rejects_inconsistent_embedding_dimensions() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.vector_profile.embedding_dimensions = 256;
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "embedding_dimensions does not match embedding_spec_id"
    );
}

#[test]
fn dataset_manifest_validation_rejects_missing_dirty_base_dataset() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.identity.variant_id = "dirty_light".to_owned();
    manifest.dirty_profile.base_dataset_id = None;
    manifest.dirty_profile.profile = "dirty_light".to_owned();
    manifest.dirty_profile.delete_ratio = 0.1;
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "dirty variants must declare base_dataset_id"
    );
}

#[test]
fn dataset_manifest_validation_rejects_missing_file_references() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.query_sets[0].ground_truth_path = "queries/missing.jsonl".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "ground_truth_path must reference a file entry"
    );
}

#[test]
fn dataset_manifest_validation_rejects_file_path_traversal() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.files[0].path = "../outside.ndjson".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "file paths must stay within the dataset pack root"
    );
}

#[test]
fn dataset_manifest_validation_rejects_missing_pack_file() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.files[0].path = "missing-docs.ndjson".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "file path must exist in the dataset pack"
    );
}

#[test]
fn dataset_manifest_validation_rejects_checksum_mismatch() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.files[0].checksum = "sha256:not-the-real-checksum".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "file checksum must match file contents"
    );
}

#[test]
fn dataset_manifest_validation_rejects_clean_profile_with_dirty_ratios() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.dirty_profile.delete_ratio = 0.1;
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "clean profile must use zero dirty ratios"
    );
}

#[test]
fn dataset_manifest_validation_rejects_disabled_vectors_with_payload() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.vector_profile.enabled = false;
    manifest.corpus.vector_count = 1;
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "vector_count must be 0 when vectors are disabled"
    );
}

#[test]
fn dataset_manifest_validation_rejects_duplicate_ground_truth_rows() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.query_sets[0].ground_truth_path =
        "queries/duplicate-ground-truth-rows.jsonl".to_owned();
    manifest.files[2].path = "queries/duplicate-ground-truth-rows.jsonl".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "ground_truth file contains duplicate query_id"
    );
}

#[test]
fn dataset_manifest_validation_rejects_unknown_query_class() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.query_sets[0].classes[0] = "mystery".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "invalid query class"
    );
}

#[test]
fn dataset_manifest_validation_rejects_wrong_file_kind_for_query_set() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.files[1].kind = "documents".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "query_set path must point to a query_set file"
    );
}

#[test]
fn dataset_manifest_validation_rejects_malformed_checksum() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.files[0].checksum = "md5:oops".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "file checksum must use sha256 format"
    );
}

#[test]
fn dataset_manifest_validation_rejects_duplicate_query_set_id() {
    let (mut manifest, _, manifest_root) = load_manifest();
    let duplicate = manifest.query_sets[0].clone();
    manifest.query_sets.push(duplicate);
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "duplicate query_set_id"
    );
}

#[test]
fn dataset_manifest_validation_rejects_query_count_mismatch() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.query_sets[0].query_count = 2;
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "query_count must match query file contents"
    );
}

#[test]
fn dataset_manifest_validation_rejects_disabled_vectors_with_nonzero_dimensions() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.vector_profile.enabled = false;
    manifest.corpus.vector_count = 0;
    manifest.vector_profile.embedding_dimensions = 384;
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "embedding_dimensions must be 0 when vectors are disabled"
    );
}

#[test]
fn dataset_manifest_validation_rejects_hnsw_sidecars_without_explicit_reproducibility_policy() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.files.push(wax_bench_model::ManifestFile {
        path: "vector_hnsw.hnsw.graph".to_owned(),
        kind: "vector_hnsw_graph".to_owned(),
        format: "hnsw-rs-graph".to_owned(),
        record_count: 1000,
        checksum: "sha256:graph".to_owned(),
    });
    manifest.vector_profile.ann_sidecar_reproducibility = None;
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "hnsw sidecars must declare ann_sidecar_reproducibility=derived_nondeterministic"
    );
}

#[test]
fn dataset_manifest_validation_rejects_hnsw_sidecars_without_backend_label() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.files.push(wax_bench_model::ManifestFile {
        path: "vector_hnsw.hnsw.graph".to_owned(),
        kind: "vector_hnsw_graph".to_owned(),
        format: "hnsw-rs-graph".to_owned(),
        record_count: 1000,
        checksum: "sha256:graph".to_owned(),
    });
    manifest.vector_profile.ann_index_backend = None;
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "hnsw sidecars must declare ann_index_backend=hnsw_rs"
    );
}

#[test]
fn dataset_manifest_validation_rejects_variant_profile_mismatch() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.identity.variant_id = "dirty_light".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "variant_id must match dirty_profile.profile"
    );
}

#[test]
fn dataset_manifest_validation_rejects_clean_profile_with_wrong_compaction_state() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.dirty_profile.compaction_state = "pre_compaction".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "clean profile must use compaction_state=clean"
    );
}

#[test]
fn dataset_manifest_validation_rejects_invalid_dataset_tier() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.identity.dataset_tier = "tiny".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "invalid dataset_tier"
    );
}

#[test]
fn dataset_manifest_validation_rejects_invalid_embedding_dtype() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.vector_profile.embedding_dtype = "f16".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "invalid embedding_dtype"
    );
}

#[test]
fn dataset_manifest_validation_rejects_invalid_distance_metric() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.vector_profile.distance_metric = "manhattan".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "invalid distance_metric"
    );
}

#[test]
fn dataset_manifest_validation_rejects_invalid_compaction_state() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.dirty_profile.profile = "dirty_light".to_owned();
    manifest.identity.variant_id = "dirty_light".to_owned();
    manifest.dirty_profile.base_dataset_id = Some("knowledge-small-clean-v1".to_owned());
    manifest.dirty_profile.delete_ratio = 0.1;
    manifest.dirty_profile.compaction_state = "unknown".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "invalid compaction_state"
    );
}

#[test]
fn dataset_manifest_validation_rejects_missing_vector_payload_checksum() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.files.push(wax_bench_model::ManifestFile {
        path: "vectors/doc-vectors.bin".to_owned(),
        kind: "document_vectors".to_owned(),
        format: "bin".to_owned(),
        record_count: 3,
        checksum: "sha256:vectors".to_owned(),
    });
    manifest.checksums.logical_vector_payload_checksum = None;
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "vector payload checksum is required when vector payload exists"
    );
}

#[test]
fn dataset_manifest_validation_rejects_duplicate_file_path() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.files.push(manifest.files[0].clone());
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "duplicate file path"
    );
}

#[test]
fn dataset_manifest_validation_rejects_misaligned_ground_truth_file() {
    let (mut manifest, _, manifest_root) = load_manifest();
    manifest.query_sets[0].ground_truth_path = "queries/mismatch-ground-truth.jsonl".to_owned();
    manifest.files[2].path = "queries/mismatch-ground-truth.jsonl".to_owned();
    assert_eq!(
        validate_manifest(&manifest, &manifest_root)
            .unwrap_err()
            .message,
        "ground_truth file must align with query ids"
    );
}

fn load_manifest() -> (DatasetPackManifest, String, PathBuf) {
    let manifest_path = Path::new("fixtures/bench/minimal-dataset-pack/manifest.json");
    let manifest_root = manifest_path.parent().unwrap().to_path_buf();
    let manifest_text = std::fs::read_to_string(manifest_path).unwrap();
    let manifest: DatasetPackManifest = serde_json::from_str(&manifest_text).unwrap();

    (manifest, manifest_text, manifest_root)
}
