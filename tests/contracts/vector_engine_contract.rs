use std::fmt::Write as _;
use std::fs;

use tempfile::tempdir;
use wax_bench_model::{
    DatasetPackManifest, MountRequest, OpenRequest, SearchRequest, VectorQueryMode, WaxEngine,
};
use wax_bench_packer::{pack_dataset, PackRequest};
use wax_bench_text_engine::{profile_first_vector_query, PackedTextEngine};
use wax_v2_core::create_empty_store;
use wax_v2_vector::publish_compatibility_vector_segment;

fn write_large_auto_source(source_dir: &std::path::Path, doc_count: usize) {
    fs::write(
        source_dir.join("source.json"),
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

    let mut docs = String::new();
    for index in 0..doc_count {
        let text = if index == 0 {
            "vector target warm hybrid exact first hnsw later"
        } else {
            "background filler document"
        };
        writeln!(
            &mut docs,
            "{{\"doc_id\":\"doc-{index:03}\",\"text\":\"{text}\"}}"
        )
        .unwrap();
    }
    fs::write(source_dir.join("docs.ndjson"), docs).unwrap();

    fs::create_dir_all(source_dir.join("queries")).unwrap();
    fs::write(
        source_dir.join("queries/core.jsonl"),
        "{\"query_id\":\"q-001\",\"query_class\":\"hybrid\",\"difficulty\":\"easy\",\"query_text\":\"vector target warm hybrid\",\"top_k\":5,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":true,\"vector\":true,\"hybrid\":true}}\n",
    )
    .unwrap();
    fs::write(
        source_dir.join("queries/core-ground-truth.jsonl"),
        "{\"query_id\":\"q-001\",\"doc_ids\":[\"doc-000\"]}\n",
    )
    .unwrap();
}

#[test]
fn packed_engine_materializes_vector_lane_on_first_vector_query() {
    let dataset_dir = tempdir().unwrap();
    let manifest = pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    assert!(manifest
        .files
        .iter()
        .any(|file| file.kind == "document_vectors"));
    assert!(manifest
        .files
        .iter()
        .any(|file| file.kind == "query_vectors"));

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    assert!(!engine.is_vector_lane_materialized());

    let first = engine
        .search(SearchRequest {
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert!(engine.is_vector_lane_materialized());
    assert_eq!(first.hits.first().map(String::as_str), Some("doc-002"));
}

#[test]
fn packed_engine_finds_first_vector_query_across_multiple_query_vector_files() {
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
            "{\"doc_id\":\"doc-001\",\"text\":\"rust benchmark\"}\n",
            "{\"doc_id\":\"doc-002\",\"text\":\"semantic latency\"}\n"
        ),
    )
    .unwrap();
    fs::create_dir_all(source_dir.path().join("queries")).unwrap();
    fs::write(
        source_dir.path().join("queries/alpha.jsonl"),
        "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"rust benchmark\",\"top_k\":10,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":true}}\n",
    )
    .unwrap();
    fs::write(
        source_dir.path().join("queries/alpha-ground-truth.jsonl"),
        "{\"query_id\":\"q-001\",\"doc_ids\":[\"doc-001\"]}\n",
    )
    .unwrap();
    fs::write(
        source_dir.path().join("queries/beta.jsonl"),
        "{\"query_id\":\"q-002\",\"query_class\":\"vector\",\"difficulty\":\"easy\",\"query_text\":\"semantic latency\",\"top_k\":10,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":false,\"vector\":true,\"hybrid\":true}}\n",
    )
    .unwrap();
    fs::write(
        source_dir.path().join("queries/beta-ground-truth.jsonl"),
        "{\"query_id\":\"q-002\",\"doc_ids\":[\"doc-002\"]}\n",
    )
    .unwrap();

    let manifest = pack_dataset(&PackRequest::new(
        source_dir.path(),
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    assert_eq!(
        manifest
            .files
            .iter()
            .filter(|file| file.kind == "query_vectors")
            .count(),
        2
    );

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    let first = engine
        .search(SearchRequest {
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-002"));
}

#[test]
fn packed_engine_uses_vector_sidecars_without_docs_file() {
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
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-002"));
}

#[test]
fn packed_engine_uses_persisted_vector_lane_without_document_id_sidecar() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();
    fs::remove_file(dataset_dir.path().join("docs.ndjson")).unwrap();
    fs::remove_file(dataset_dir.path().join("document_ids.jsonl")).unwrap();

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    let first = engine
        .search(SearchRequest {
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-002"));
}

#[test]
fn packed_engine_open_rejects_store_vector_segment_that_does_not_match_mounted_pack() {
    let dataset_dir = tempdir().unwrap();
    let manifest = pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let store_path = dataset_dir.path().join("store.wax");
    create_empty_store(&store_path).unwrap();
    publish_compatibility_vector_segment(dataset_dir.path(), &manifest, &store_path).unwrap();
    let document_vectors_path = manifest
        .files
        .iter()
        .find(|file| file.kind == "document_vectors")
        .map(|file| dataset_dir.path().join(&file.path))
        .unwrap();
    let mut mutated_vectors = fs::read(&document_vectors_path).unwrap();
    mutated_vectors[0] ^= 0x01;
    fs::write(&document_vectors_path, mutated_vectors).unwrap();

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    let error = engine.open(OpenRequest).unwrap_err();

    assert!(
        error.contains("store vector segment does not match mounted dataset vectors"),
        "unexpected error: {error}"
    );
}

#[test]
fn profile_first_vector_query_rejects_store_vector_segment_that_does_not_match_mounted_pack() {
    let dataset_dir = tempdir().unwrap();
    let manifest = pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let store_path = dataset_dir.path().join("store.wax");
    create_empty_store(&store_path).unwrap();
    publish_compatibility_vector_segment(dataset_dir.path(), &manifest, &store_path).unwrap();
    let document_vectors_path = manifest
        .files
        .iter()
        .find(|file| file.kind == "document_vectors")
        .map(|file| dataset_dir.path().join(&file.path))
        .unwrap();
    let mut mutated_vectors = fs::read(&document_vectors_path).unwrap();
    mutated_vectors[0] ^= 0x01;
    fs::write(&document_vectors_path, mutated_vectors).unwrap();

    let error = profile_first_vector_query(dataset_dir.path(), VectorQueryMode::Auto).unwrap_err();

    assert!(
        error.contains("store vector segment does not match mounted dataset vectors"),
        "unexpected error: {error}"
    );
}

#[test]
fn packed_engine_prefers_manifest_visible_vector_segment_when_sidecars_are_missing() {
    let dataset_dir = tempdir().unwrap();
    let manifest = pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();
    let store_path = dataset_dir.path().join("store.wax");
    create_empty_store(&store_path).unwrap();
    publish_compatibility_vector_segment(dataset_dir.path(), &manifest, &store_path).unwrap();

    for kind in [
        "document_ids",
        "document_vectors",
        "document_vectors_preview_q8",
    ] {
        let path = manifest
            .files
            .iter()
            .find(|file| file.kind == kind)
            .map(|file| dataset_dir.path().join(&file.path))
            .unwrap();
        fs::remove_file(path).unwrap();
    }

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    let first = engine
        .search(SearchRequest {
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-002"));
}

#[test]
fn packed_engine_hnsw_matches_exact_flat_top_hit_for_first_vector_query() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let mut exact_engine = PackedTextEngine::with_vector_mode(VectorQueryMode::ExactFlat);
    exact_engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    exact_engine.open(OpenRequest).unwrap();
    let exact = exact_engine
        .search(SearchRequest {
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    let mut hnsw_engine = PackedTextEngine::with_vector_mode(VectorQueryMode::Hnsw);
    hnsw_engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    hnsw_engine.open(OpenRequest).unwrap();
    let hnsw = hnsw_engine
        .search(SearchRequest {
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(hnsw.hits.first(), exact.hits.first());
}

#[test]
fn packed_engine_falls_back_to_exact_scan_when_ann_sidecars_are_missing() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let manifest_path = dataset_dir.path().join("manifest.json");
    let mut manifest: DatasetPackManifest =
        serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
    manifest.files.retain(|file| {
        file.kind != "document_vectors_preview_q8"
            && file.kind != "vector_hnsw_graph"
            && file.kind != "vector_hnsw_data"
    });
    fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();
    fs::remove_file(dataset_dir.path().join("document_vectors.q8")).unwrap();
    fs::remove_file(dataset_dir.path().join("vector_hnsw.hnsw.graph")).unwrap();
    fs::remove_file(dataset_dir.path().join("vector_hnsw.hnsw.data")).unwrap();

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    let first = engine
        .search(SearchRequest {
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-002"));
}

#[test]
fn packed_engine_falls_back_to_documents_when_old_pack_has_no_vector_skeleton_or_document_ids() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let manifest_path = dataset_dir.path().join("manifest.json");
    let mut manifest: DatasetPackManifest =
        serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
    manifest
        .files
        .retain(|file| file.kind != "document_ids" && file.kind != "vector_lane_skeleton");
    fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();
    fs::remove_file(dataset_dir.path().join("document_ids.jsonl")).unwrap();
    fs::remove_file(dataset_dir.path().join("vector_lane.skel")).unwrap();

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    let first = engine
        .search(SearchRequest {
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-002"));
}

#[test]
fn packed_engine_preserves_doc_id_tiebreak_for_equal_vector_scores() {
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
    { "name": "alpha", "path": "queries/alpha.jsonl", "ground_truth_path": "queries/alpha-ground-truth.jsonl" }
  ]
}"#,
    )
    .unwrap();
    fs::write(
        source_dir.path().join("docs.ndjson"),
        concat!(
            "{\"doc_id\":\"doc-001\",\"text\":\"equal score\"}\n",
            "{\"doc_id\":\"doc-002\",\"text\":\"equal score\"}\n"
        ),
    )
    .unwrap();
    fs::create_dir_all(source_dir.path().join("queries")).unwrap();
    fs::write(
        source_dir.path().join("queries/alpha.jsonl"),
        "{\"query_id\":\"q-001\",\"query_class\":\"vector\",\"difficulty\":\"easy\",\"query_text\":\"equal score\",\"top_k\":10,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":false,\"vector\":true,\"hybrid\":false}}\n",
    )
    .unwrap();
    fs::write(
        source_dir.path().join("queries/alpha-ground-truth.jsonl"),
        "{\"query_id\":\"q-001\",\"doc_ids\":[\"doc-001\",\"doc-002\"]}\n",
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
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(
        first
            .hits
            .iter()
            .take(2)
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["doc-001", "doc-002"]
    );
}

#[test]
fn packed_engine_replaces_worse_hit_when_top_k_buffer_is_full() {
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
    { "name": "alpha", "path": "queries/alpha.jsonl", "ground_truth_path": "queries/alpha-ground-truth.jsonl" }
  ]
}"#,
    )
    .unwrap();
    fs::write(
        source_dir.path().join("docs.ndjson"),
        concat!(
            "{\"doc_id\":\"doc-001\",\"text\":\"noise only\"}\n",
            "{\"doc_id\":\"doc-002\",\"text\":\"late\"}\n",
            "{\"doc_id\":\"doc-003\",\"text\":\"late best match\"}\n"
        ),
    )
    .unwrap();
    fs::create_dir_all(source_dir.path().join("queries")).unwrap();
    fs::write(
        source_dir.path().join("queries/alpha.jsonl"),
        "{\"query_id\":\"q-001\",\"query_class\":\"vector\",\"difficulty\":\"easy\",\"query_text\":\"late best match\",\"top_k\":1,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":false,\"vector\":true,\"hybrid\":false}}\n",
    )
    .unwrap();
    fs::write(
        source_dir.path().join("queries/alpha-ground-truth.jsonl"),
        "{\"query_id\":\"q-001\",\"doc_ids\":[\"doc-003\"]}\n",
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
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits, vec!["doc-003".to_owned()]);
}

#[test]
fn packed_engine_warm_text_does_not_materialize_vector_lane() {
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
    assert!(!engine.is_vector_lane_materialized());

    let result = engine
        .search(SearchRequest {
            query_text: "__warm_text__".to_owned(),
        })
        .unwrap();

    assert!(engine.is_text_lane_materialized());
    assert!(!engine.is_vector_lane_materialized());
    assert_eq!(result.hits.first().map(String::as_str), Some("doc-001"));
}

#[test]
fn vector_query_profile_uses_exact_breakdown_for_small_pack_auto_mode() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let profile = profile_first_vector_query(dataset_dir.path(), VectorQueryMode::Auto).unwrap();

    assert_eq!(profile.selected_mode, VectorQueryMode::ExactFlat);
    assert!(profile.vector_lane_load_ms >= 0.0);
    assert!(profile.hnsw_sidecar_load_ms.is_none());
    assert!(profile.total_search_ms >= 0.0);
    assert!(profile.exact_scan_ms.is_some());
    assert!(profile.approximate_search_ms.is_none());
    assert!(profile.rerank_ms.is_none());
    assert_eq!(profile.hits.first().map(String::as_str), Some("doc-002"));
}

#[test]
fn vector_query_profile_uses_exact_breakdown_for_large_pack_auto_mode() {
    let source_dir = tempdir().unwrap();
    let dataset_dir = tempdir().unwrap();
    write_large_auto_source(source_dir.path(), 96);
    pack_dataset(&PackRequest::new(
        source_dir.path(),
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let profile = profile_first_vector_query(dataset_dir.path(), VectorQueryMode::Auto).unwrap();

    assert_eq!(profile.selected_mode, VectorQueryMode::ExactFlat);
    assert!(profile.vector_lane_load_ms >= 0.0);
    assert!(profile.hnsw_sidecar_load_ms.is_none());
    assert!(profile.total_search_ms >= 0.0);
    assert!(profile.exact_scan_ms.is_some());
    assert!(profile.approximate_search_ms.is_none());
    assert!(profile.rerank_ms.is_none());
    assert_eq!(profile.hits.first().map(String::as_str), Some("doc-000"));
}

#[test]
fn vector_query_profile_reports_hnsw_breakdown_when_forced() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let profile = profile_first_vector_query(dataset_dir.path(), VectorQueryMode::Hnsw).unwrap();

    assert_eq!(profile.selected_mode, VectorQueryMode::Hnsw);
    assert!(profile.vector_lane_load_ms >= 0.0);
    assert!(profile.hnsw_sidecar_load_ms.is_some());
    assert!(profile.total_search_ms >= 0.0);
    assert!(profile.exact_scan_ms.is_none());
    assert!(profile.approximate_search_ms.is_some());
    assert!(profile.rerank_ms.is_some());
    assert!(profile.candidate_count >= profile.hits.len());
    assert_eq!(profile.hits.first().map(String::as_str), Some("doc-002"));
}

#[test]
fn packed_engine_auto_keeps_hnsw_sidecar_cold_until_second_vector_query() {
    let source_dir = tempdir().unwrap();
    let dataset_dir = tempdir().unwrap();
    write_large_auto_source(source_dir.path(), 96);
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

    assert!(!engine.is_vector_lane_materialized());
    assert!(!engine.is_vector_hnsw_sidecar_materialized());

    let first = engine
        .search(SearchRequest {
            query_text: "__ttfq_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-000"));
    assert!(engine.is_vector_lane_materialized());
    assert!(!engine.is_vector_hnsw_sidecar_materialized());

    let second = engine
        .search(SearchRequest {
            query_text: "__warm_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(second.hits.first().map(String::as_str), Some("doc-000"));
    assert!(engine.is_vector_hnsw_sidecar_materialized());
}

#[test]
fn packed_engine_auto_warmup_vector_primes_hnsw_sidecar_for_measured_query() {
    let source_dir = tempdir().unwrap();
    let dataset_dir = tempdir().unwrap();
    write_large_auto_source(source_dir.path(), 96);
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

    let warmup = engine
        .search(SearchRequest {
            query_text: "__warmup_vector__".to_owned(),
        })
        .unwrap();

    assert_eq!(warmup.hits.first().map(String::as_str), Some("doc-000"));
    assert!(engine.is_vector_hnsw_sidecar_materialized());
}

#[test]
fn packed_engine_auto_keeps_hnsw_sidecar_cold_until_second_hybrid_query() {
    let source_dir = tempdir().unwrap();
    let dataset_dir = tempdir().unwrap();
    write_large_auto_source(source_dir.path(), 96);
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
            query_text: "__ttfq_hybrid__".to_owned(),
        })
        .unwrap();

    assert_eq!(first.hits.first().map(String::as_str), Some("doc-000"));
    assert!(engine.is_vector_lane_materialized());
    assert!(!engine.is_vector_hnsw_sidecar_materialized());

    let second = engine
        .search(SearchRequest {
            query_text: "__warm_hybrid__".to_owned(),
        })
        .unwrap();

    assert_eq!(second.hits.first().map(String::as_str), Some("doc-000"));
    assert!(engine.is_vector_hnsw_sidecar_materialized());
}

#[test]
fn packed_engine_auto_warmup_hybrid_primes_hnsw_sidecar_for_measured_query() {
    let source_dir = tempdir().unwrap();
    let dataset_dir = tempdir().unwrap();
    write_large_auto_source(source_dir.path(), 96);
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

    let warmup = engine
        .search(SearchRequest {
            query_text: "__warmup_hybrid__".to_owned(),
        })
        .unwrap();

    assert_eq!(warmup.hits.first().map(String::as_str), Some("doc-000"));
    assert!(engine.is_vector_hnsw_sidecar_materialized());
}
