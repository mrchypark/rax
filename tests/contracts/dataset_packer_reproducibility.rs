use std::fs;
use std::path::Path;

use tempfile::tempdir;
use wax_bench_model::DatasetPackManifest;
use wax_bench_packer::{pack_dataset, PackRequest};

#[test]
fn dataset_packer_produces_byte_stable_manifest_for_same_source_and_config() {
    let source = Path::new("fixtures/bench/source/minimal");
    let out_a = tempdir().unwrap();
    let out_b = tempdir().unwrap();

    pack_dataset(&PackRequest::new(source, out_a.path(), "small", "clean")).unwrap();
    pack_dataset(&PackRequest::new(source, out_b.path(), "small", "clean")).unwrap();

    let manifest_a = fs::read_to_string(out_a.path().join("manifest.json")).unwrap();
    let manifest_b = fs::read_to_string(out_b.path().join("manifest.json")).unwrap();

    assert_eq!(manifest_a, manifest_b);
}

#[test]
fn dataset_packer_emits_expected_clean_and_dirty_metadata() {
    let source = Path::new("fixtures/bench/source/minimal");
    let clean_out = tempdir().unwrap();
    let dirty_out = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        source,
        clean_out.path(),
        "small",
        "clean",
    ))
    .unwrap();
    pack_dataset(&PackRequest::new(
        source,
        dirty_out.path(),
        "small",
        "dirty_light",
    ))
    .unwrap();

    let clean = read_manifest(clean_out.path());
    let dirty = read_manifest(dirty_out.path());

    assert_eq!(clean.identity.variant_id, "clean");
    assert_eq!(clean.dirty_profile.profile, "clean");
    assert_eq!(clean.dirty_profile.delete_ratio, 0.0);

    assert_eq!(dirty.identity.variant_id, "dirty_light");
    assert_eq!(dirty.dirty_profile.profile, "dirty_light");
    assert!(dirty.dirty_profile.base_dataset_id.is_some());
    assert!(dirty.dirty_profile.delete_ratio > 0.0);
}

#[test]
fn dataset_packer_keeps_query_set_ids_stable_across_variants() {
    let source = Path::new("fixtures/bench/source/minimal");
    let clean_out = tempdir().unwrap();
    let dirty_out = tempdir().unwrap();

    pack_dataset(&PackRequest::new(
        source,
        clean_out.path(),
        "small",
        "clean",
    ))
    .unwrap();
    pack_dataset(&PackRequest::new(
        source,
        dirty_out.path(),
        "small",
        "dirty_light",
    ))
    .unwrap();

    let clean = read_manifest(clean_out.path());
    let dirty = read_manifest(dirty_out.path());

    let clean_ids: Vec<_> = clean
        .query_sets
        .iter()
        .map(|query_set| query_set.query_set_id.clone())
        .collect();
    let dirty_ids: Vec<_> = dirty
        .query_sets
        .iter()
        .map(|query_set| query_set.query_set_id.clone())
        .collect();

    assert_eq!(clean_ids, dirty_ids);
}

#[test]
fn dataset_packer_emits_sidecar_artifacts_for_text_and_vector_lanes() {
    let source = Path::new("fixtures/bench/source/minimal");
    let out_dir = tempdir().unwrap();

    let manifest = pack_dataset(&PackRequest::new(source, out_dir.path(), "small", "clean")).unwrap();

    assert!(manifest.files.iter().any(|file| file.kind == "text_postings"));
    assert!(manifest.files.iter().any(|file| file.kind == "document_ids"));
    assert!(out_dir.path().join("text_postings.jsonl").exists());
    assert!(out_dir.path().join("document_ids.jsonl").exists());
}

#[test]
fn dataset_packer_rejects_vector_enabled_source_without_vector_query() {
    let source_dir = tempdir().unwrap();
    let out_dir = tempdir().unwrap();

    std::fs::write(
        source_dir.path().join("source.json"),
        r#"{
  "dataset_family": "knowledge",
  "dataset_version": "v1",
  "generated_at": "2026-03-30T00:00:00Z",
  "embedding_spec_id": "minilm-l6-384-f32-cosine",
  "embedding_model_version": "2026-03-15",
  "embedding_model_hash": "sha256:model",
  "environment_constraints": { "min_ram_gb": 4, "recommended_ram_gb": 8 },
  "languages": [{ "code": "en", "ratio": 1.0 }],
  "metadata_profile": {
    "facets": [],
    "selectivity_exemplars": {
      "broad": "workspace_id = w1",
      "medium": "workspace_id = w1",
      "narrow": "workspace_id = w1",
      "zero_hit": "workspace_id = missing"
    }
  },
  "query_sets": [
    { "name": "core", "path": "queries/core.jsonl", "ground_truth_path": "queries/core-ground-truth.jsonl" }
  ]
}"#,
    )
    .unwrap();
    std::fs::write(
        source_dir.path().join("docs.ndjson"),
        "{\"doc_id\":\"doc-001\",\"text\":\"rust benchmark\"}\n",
    )
    .unwrap();
    std::fs::create_dir_all(source_dir.path().join("queries")).unwrap();
    std::fs::write(
        source_dir.path().join("queries/core.jsonl"),
        "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"rust benchmark\",\"top_k\":10,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":true}}\n",
    )
    .unwrap();
    std::fs::write(
        source_dir.path().join("queries/core-ground-truth.jsonl"),
        "{\"query_id\":\"q-001\",\"doc_ids\":[\"doc-001\"]}\n",
    )
    .unwrap();

    let error = pack_dataset(&PackRequest::new(
        source_dir.path(),
        out_dir.path(),
        "small",
        "clean",
    ))
    .unwrap_err();

    assert_eq!(error.message, "vector-enabled datasets require a vector query");
}

#[test]
fn dataset_packer_rejects_malformed_embedding_spec_for_vector_payloads() {
    let source_dir = tempdir().unwrap();
    let out_dir = tempdir().unwrap();

    std::fs::write(
        source_dir.path().join("source.json"),
        r#"{
  "dataset_family": "knowledge",
  "dataset_version": "v1",
  "generated_at": "2026-03-30T00:00:00Z",
  "embedding_spec_id": "minilm-l6-cosine",
  "embedding_model_version": "2026-03-15",
  "embedding_model_hash": "sha256:model",
  "environment_constraints": { "min_ram_gb": 4, "recommended_ram_gb": 8 },
  "languages": [{ "code": "en", "ratio": 1.0 }],
  "metadata_profile": {
    "facets": [],
    "selectivity_exemplars": {
      "broad": "workspace_id = w1",
      "medium": "workspace_id = w1",
      "narrow": "workspace_id = w1",
      "zero_hit": "workspace_id = missing"
    }
  },
  "query_sets": [
    { "name": "core", "path": "queries/core.jsonl", "ground_truth_path": "queries/core-ground-truth.jsonl" }
  ]
}"#,
    )
    .unwrap();
    std::fs::write(
        source_dir.path().join("docs.ndjson"),
        "{\"doc_id\":\"doc-001\",\"text\":\"semantic latency\"}\n",
    )
    .unwrap();
    std::fs::create_dir_all(source_dir.path().join("queries")).unwrap();
    std::fs::write(
        source_dir.path().join("queries/core.jsonl"),
        "{\"query_id\":\"q-001\",\"query_class\":\"vector\",\"difficulty\":\"easy\",\"query_text\":\"semantic latency\",\"top_k\":10,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":false,\"vector\":true,\"hybrid\":true}}\n",
    )
    .unwrap();
    std::fs::write(
        source_dir.path().join("queries/core-ground-truth.jsonl"),
        "{\"query_id\":\"q-001\",\"doc_ids\":[\"doc-001\"]}\n",
    )
    .unwrap();

    let error = pack_dataset(&PackRequest::new(
        source_dir.path(),
        out_dir.path(),
        "small",
        "clean",
    ))
    .unwrap_err();

    assert_eq!(
        error.message,
        "embedding_spec_id must declare vector dimensions"
    );
}

fn read_manifest(out_dir: &Path) -> DatasetPackManifest {
    let text = fs::read_to_string(out_dir.join("manifest.json")).unwrap();
    serde_json::from_str(&text).unwrap()
}
