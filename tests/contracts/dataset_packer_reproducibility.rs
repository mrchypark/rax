use std::fs;
use std::path::Path;

use sha2::{Digest, Sha256};
use tempfile::tempdir;
use wax_bench_model::DatasetPackManifest;
use wax_bench_packer::{pack_adhoc_dataset, pack_dataset, AdhocPackRequest, PackRequest};

#[test]
fn dataset_packer_produces_stable_logical_manifest_for_same_source_and_config() {
    let source = Path::new("fixtures/bench/source/minimal");
    let out_a = tempdir().unwrap();
    let out_b = tempdir().unwrap();

    pack_dataset(&PackRequest::new(source, out_a.path(), "small", "clean")).unwrap();
    pack_dataset(&PackRequest::new(source, out_b.path(), "small", "clean")).unwrap();

    let mut manifest_a: DatasetPackManifest =
        serde_json::from_str(&fs::read_to_string(out_a.path().join("manifest.json")).unwrap())
            .unwrap();
    let mut manifest_b: DatasetPackManifest =
        serde_json::from_str(&fs::read_to_string(out_b.path().join("manifest.json")).unwrap())
            .unwrap();

    normalize_derived_ann_sidecars(&mut manifest_a);
    normalize_derived_ann_sidecars(&mut manifest_b);

    let manifest_a = serde_json::to_string_pretty(&manifest_a).unwrap();
    let manifest_b = serde_json::to_string_pretty(&manifest_b).unwrap();

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

    let manifest =
        pack_dataset(&PackRequest::new(source, out_dir.path(), "small", "clean")).unwrap();

    assert!(manifest
        .files
        .iter()
        .any(|file| file.kind == "text_postings"));
    assert!(manifest
        .files
        .iter()
        .any(|file| file.kind == "document_ids"));
    assert!(manifest
        .files
        .iter()
        .any(|file| file.kind == "document_offsets"));
    assert!(out_dir.path().join("text_postings.jsonl").exists());
    assert!(out_dir.path().join("document_ids.jsonl").exists());
    assert!(out_dir.path().join("document_offsets.jsonl").exists());
}

#[test]
fn dataset_packer_marks_synthetic_embedding_provenance_in_manifest_identity() {
    let source = Path::new("fixtures/bench/source/minimal");
    let out_dir = tempdir().unwrap();

    let manifest =
        pack_dataset(&PackRequest::new(source, out_dir.path(), "small", "clean")).unwrap();

    assert_eq!(
        manifest.identity.embedding_spec_id,
        "feature-hash-stub-384-f32-cosine"
    );
    assert_eq!(
        manifest.identity.embedding_model_version,
        "feature-hash-stub-v1"
    );
    assert_eq!(manifest.vector_profile.embedding_dimensions, 384,);
    assert_eq!(manifest.vector_profile.embedding_dtype, "f32");
    assert_eq!(manifest.vector_profile.distance_metric, "cosine");
}

#[test]
fn dataset_packer_records_logical_query_checksum_in_manifest_identity() {
    let source = Path::new("fixtures/bench/source/minimal");
    let out_dir = tempdir().unwrap();

    let manifest =
        pack_dataset(&PackRequest::new(source, out_dir.path(), "small", "clean")).unwrap();
    let query_bytes = fs::read(source.join("queries/core.jsonl")).unwrap();
    let expected = format!("sha256:{:x}", Sha256::digest(&query_bytes));

    assert_eq!(manifest.identity.query_checksum, expected);
}

#[test]
fn adhoc_packer_marks_synthetic_embedding_provenance_in_manifest_identity() {
    let docs_dir = tempdir().unwrap();
    let out_dir = tempdir().unwrap();
    fs::write(
        docs_dir.path().join("docs.ndjson"),
        "{\"doc_id\":\"doc-001\",\"text\":\"rust benchmark\"}\n",
    )
    .unwrap();

    let manifest = pack_adhoc_dataset(&AdhocPackRequest::new(
        docs_dir.path().join("docs.ndjson"),
        out_dir.path(),
        "small",
    ))
    .unwrap();

    assert_eq!(
        manifest.identity.embedding_spec_id,
        "feature-hash-stub-384-f32-cosine"
    );
    assert_eq!(
        manifest.identity.embedding_model_version,
        "feature-hash-stub-v1"
    );
    assert!(out_dir.path().join("document_offsets.jsonl").exists());
}

#[test]
fn dataset_packer_emits_persisted_vector_lane_skeleton() {
    let source = Path::new("fixtures/bench/source/minimal");
    let out_dir = tempdir().unwrap();

    let manifest =
        pack_dataset(&PackRequest::new(source, out_dir.path(), "small", "clean")).unwrap();

    assert!(manifest
        .files
        .iter()
        .any(|file| file.kind == "vector_lane_skeleton"));
    assert!(out_dir.path().join("vector_lane.skel").exists());
}

#[test]
fn dataset_packer_emits_quantized_vector_preview_sidecar() {
    let source = Path::new("fixtures/bench/source/minimal");
    let out_dir = tempdir().unwrap();

    let manifest =
        pack_dataset(&PackRequest::new(source, out_dir.path(), "small", "clean")).unwrap();

    assert!(manifest
        .files
        .iter()
        .any(|file| file.kind == "document_vectors_preview_q8"));
    assert!(out_dir.path().join("document_vectors.q8").exists());
}

#[test]
fn dataset_packer_emits_persisted_vector_hnsw_sidecars() {
    let source = Path::new("fixtures/bench/source/minimal");
    let out_dir = tempdir().unwrap();

    let manifest =
        pack_dataset(&PackRequest::new(source, out_dir.path(), "small", "clean")).unwrap();

    assert!(manifest
        .files
        .iter()
        .any(|file| file.kind == "vector_hnsw_graph"));
    assert!(manifest
        .files
        .iter()
        .any(|file| file.kind == "vector_hnsw_data"));
    assert!(out_dir.path().join("vector_hnsw.hnsw.graph").exists());
    assert!(out_dir.path().join("vector_hnsw.hnsw.data").exists());
}

#[test]
fn dataset_packer_reuses_stable_hnsw_sidecar_paths_when_output_dir_is_dirty() {
    let source = Path::new("fixtures/bench/source/minimal");
    let out_dir = tempdir().unwrap();

    fs::write(
        out_dir.path().join("vector_hnsw.hnsw.graph"),
        b"stale-graph",
    )
    .unwrap();
    fs::write(out_dir.path().join("vector_hnsw.hnsw.data"), b"stale-data").unwrap();

    let manifest =
        pack_dataset(&PackRequest::new(source, out_dir.path(), "small", "clean")).unwrap();

    let graph = manifest
        .files
        .iter()
        .find(|file| file.kind == "vector_hnsw_graph")
        .unwrap();
    let data = manifest
        .files
        .iter()
        .find(|file| file.kind == "vector_hnsw_data")
        .unwrap();

    assert_eq!(graph.path, "vector_hnsw.hnsw.graph");
    assert_eq!(data.path, "vector_hnsw.hnsw.data");
    assert_ne!(
        fs::read(out_dir.path().join(&graph.path)).unwrap(),
        b"stale-graph"
    );
    assert_ne!(
        fs::read(out_dir.path().join(&data.path)).unwrap(),
        b"stale-data"
    );
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

    assert_eq!(
        error.message,
        "vector-enabled datasets require a vector query"
    );
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

fn normalize_derived_ann_sidecars(manifest: &mut DatasetPackManifest) {
    for file in &mut manifest.files {
        if matches!(file.kind.as_str(), "vector_hnsw_graph" | "vector_hnsw_data") {
            file.checksum = "sha256:derived-ann-sidecar".to_owned();
        }
    }
    manifest.checksums.manifest_payload_checksum = "sha256:normalized-for-test".to_owned();
}
