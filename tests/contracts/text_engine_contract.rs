use std::fs;

use tempfile::tempdir;
use wax_bench_model::{MountRequest, OpenRequest, SearchRequest, WaxEngine};
use wax_bench_packer::{pack_dataset, PackRequest};
use wax_bench_text_engine::PackedTextEngine;

#[test]
fn packed_text_engine_materializes_text_lane_on_first_query() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    assert!(!engine.is_text_lane_materialized());

    let first = engine
        .search(SearchRequest {
            query_text: "__ttfq_text__".to_owned(),
        })
        .unwrap();

    assert!(engine.is_text_lane_materialized());
    assert_eq!(first.hits.first().map(String::as_str), Some("doc-001"));

    let explicit = engine
        .search(SearchRequest {
            query_text: "cold open".to_owned(),
        })
        .unwrap();
    assert_eq!(explicit.hits.first().map(String::as_str), Some("doc-003"));
}

#[test]
fn packed_text_engine_uses_precomputed_text_artifacts_without_docs_file() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();
    fs::remove_file(dataset_dir.path().join("docs.ndjson")).unwrap();

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    let first = engine
        .search(SearchRequest {
            query_text: "__ttfq_text__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-001"));
}

#[test]
fn packed_text_engine_finds_first_text_query_across_multiple_query_sets() {
    let source_dir = tempdir().unwrap();
    let dataset_dir = tempdir().unwrap();

    fs::write(
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
    { "name": "alpha", "path": "queries/alpha.jsonl", "ground_truth_path": "queries/alpha-ground-truth.jsonl" },
    { "name": "beta", "path": "queries/beta.jsonl", "ground_truth_path": "queries/beta-ground-truth.jsonl" }
  ]
}"#,
    )
    .unwrap();
    fs::write(
        source_dir.path().join("docs.ndjson"),
        concat!(
            "{\"doc_id\":\"doc-001\",\"text\":\"semantic latency\"}\n",
            "{\"doc_id\":\"doc-002\",\"text\":\"rust benchmark\"}\n"
        ),
    )
    .unwrap();
    fs::create_dir_all(source_dir.path().join("queries")).unwrap();
    fs::write(
        source_dir.path().join("queries/alpha.jsonl"),
        "{\"query_id\":\"q-001\",\"query_class\":\"vector\",\"difficulty\":\"easy\",\"query_text\":\"semantic latency\",\"top_k\":10,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":false,\"vector\":true,\"hybrid\":true}}\n",
    )
    .unwrap();
    fs::write(
        source_dir.path().join("queries/alpha-ground-truth.jsonl"),
        "{\"query_id\":\"q-001\",\"doc_ids\":[\"doc-001\"]}\n",
    )
    .unwrap();
    fs::write(
        source_dir.path().join("queries/beta.jsonl"),
        "{\"query_id\":\"q-002\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"rust benchmark\",\"top_k\":10,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":true}}\n",
    )
    .unwrap();
    fs::write(
        source_dir.path().join("queries/beta-ground-truth.jsonl"),
        "{\"query_id\":\"q-002\",\"doc_ids\":[\"doc-002\"]}\n",
    )
    .unwrap();

    pack_dataset(&PackRequest::new(
        source_dir.path(),
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    let first = engine
        .search(SearchRequest {
            query_text: "__ttfq_text__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-002"));
}

#[test]
fn packed_text_engine_executes_hybrid_query_with_sidecar_lanes() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();
    fs::remove_file(dataset_dir.path().join("docs.ndjson")).unwrap();

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    let first = engine
        .search(SearchRequest {
            query_text: "__warm_hybrid__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-001"));
    assert!(first.hits.len() >= 2);
}
