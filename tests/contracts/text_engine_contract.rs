use std::fs;

use tempfile::tempdir;
use wax_bench_model::{MountRequest, OpenRequest, SearchRequest, WaxEngine};
use wax_bench_packer::{pack_dataset, PackRequest};
use wax_bench_text_engine::{query_batch_ranked_results, query_text_preview, PackedTextEngine};
use wax_v2_core::create_empty_store;
use wax_v2_docstore::Docstore;
use wax_v2_runtime::{NewDocument, RuntimeStore};
use wax_v2_text::publish_compatibility_text_segment;

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

#[test]
fn packed_text_engine_prefers_manifest_visible_text_segment_when_sidecar_is_missing() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let store_path = dataset_dir.path().join("store.wax");
    create_empty_store(&store_path).unwrap();
    let manifest: wax_bench_model::DatasetPackManifest = serde_json::from_str(
        &fs::read_to_string(dataset_dir.path().join("manifest.json")).unwrap(),
    )
    .unwrap();
    publish_compatibility_text_segment(dataset_dir.path(), &manifest, &store_path).unwrap();

    let postings_path = manifest
        .files
        .iter()
        .find(|file| file.kind == "text_postings")
        .map(|file| dataset_dir.path().join(&file.path))
        .unwrap();
    fs::remove_file(postings_path).unwrap();

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
fn packed_text_engine_open_rejects_store_segments_that_do_not_match_mounted_pack() {
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
    let dataset_docstore = Docstore::open_dataset_pack(dataset_dir.path(), &manifest).unwrap();
    dataset_docstore.publish_to_store(&store_path).unwrap();
    publish_compatibility_text_segment(dataset_dir.path(), &manifest, &store_path).unwrap();
    fs::write(
        dataset_dir.path().join("docs.ndjson"),
        concat!(
            "{\"doc_id\":\"doc-001\",\"text\":\"rust benchmark guide changed\",\"metadata\":{\"kind\":\"guide\",\"workspace\":\"prod\"}}\n",
            "{\"doc_id\":\"doc-002\",\"text\":\"semantic latency notes\",\"metadata\":{\"kind\":\"note\",\"workspace\":\"prod\"}}\n",
            "{\"doc_id\":\"doc-003\",\"text\":\"cold open overview\",\"metadata\":{\"kind\":\"memo\",\"workspace\":\"dev\"}}\n"
        ),
    )
    .unwrap();
    if let Some(path) = manifest
        .files
        .iter()
        .find(|file| file.kind == "document_offsets")
        .map(|file| dataset_dir.path().join(&file.path))
    {
        fs::remove_file(path).unwrap();
    }

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    let error = engine.open(OpenRequest).unwrap_err();

    assert!(
        error.contains("does not match mounted dataset"),
        "unexpected error: {error}"
    );
}

#[test]
fn query_text_preview_uses_manifest_visible_doc_segment_when_docs_sidecar_is_missing() {
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
    let dataset_docstore = Docstore::open_dataset_pack(dataset_dir.path(), &manifest).unwrap();
    dataset_docstore.publish_to_store(&store_path).unwrap();
    publish_compatibility_text_segment(dataset_dir.path(), &manifest, &store_path).unwrap();
    fs::remove_file(dataset_dir.path().join("docs.ndjson")).unwrap();
    let document_offsets = manifest
        .files
        .iter()
        .find(|file| file.kind == "document_offsets")
        .map(|file| dataset_dir.path().join(&file.path));
    if let Some(path) = document_offsets {
        fs::remove_file(path).unwrap();
    }

    let preview = query_text_preview(dataset_dir.path(), "semantic search", 1).unwrap();

    assert_eq!(preview.len(), 1);
    assert_eq!(preview[0].doc_id, "doc-002");
    assert_eq!(preview[0].text, "semantic latency notes");
}

#[test]
fn query_text_preview_rejects_store_segments_that_do_not_match_mounted_pack() {
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
    let dataset_docstore = Docstore::open_dataset_pack(dataset_dir.path(), &manifest).unwrap();
    dataset_docstore.publish_to_store(&store_path).unwrap();
    publish_compatibility_text_segment(dataset_dir.path(), &manifest, &store_path).unwrap();
    fs::write(
        dataset_dir.path().join("docs.ndjson"),
        concat!(
            "{\"doc_id\":\"doc-001\",\"text\":\"rust benchmark guide changed\",\"metadata\":{\"kind\":\"guide\",\"workspace\":\"prod\"}}\n",
            "{\"doc_id\":\"doc-002\",\"text\":\"semantic latency notes\",\"metadata\":{\"kind\":\"note\",\"workspace\":\"prod\"}}\n",
            "{\"doc_id\":\"doc-003\",\"text\":\"cold open overview\",\"metadata\":{\"kind\":\"memo\",\"workspace\":\"dev\"}}\n"
        ),
    )
    .unwrap();
    if let Some(path) = manifest
        .files
        .iter()
        .find(|file| file.kind == "document_offsets")
        .map(|file| dataset_dir.path().join(&file.path))
    {
        fs::remove_file(path).unwrap();
    }

    let error = query_text_preview(dataset_dir.path(), "rust benchmark", 1).unwrap_err();

    assert!(
        error.contains("does not match mounted dataset"),
        "unexpected error: {error}"
    );
}

#[test]
fn packed_text_engine_open_allows_text_queries_when_vectors_are_stale() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
    runtime
        .writer()
        .unwrap()
        .import_compatibility_snapshot()
        .unwrap();
    runtime
        .writer()
        .unwrap()
        .publish_raw_documents(vec![
            NewDocument::new("doc-001", "vector stale text still works")
                .with_metadata(serde_json::json!({"kind":"guide","workspace":"prod"})),
            NewDocument::new("doc-002", "semantic latency notes")
                .with_metadata(serde_json::json!({"kind":"note","workspace":"prod"})),
            NewDocument::new("doc-003", "cold open overview")
                .with_metadata(serde_json::json!({"kind":"memo","workspace":"dev"})),
        ])
        .unwrap();
    runtime.close().unwrap();
    fs::write(
        dataset_dir.path().join("docs.ndjson"),
        concat!(
            "{\"doc_id\":\"doc-001\",\"text\":\"vector stale text still works\",\"metadata\":{\"kind\":\"guide\",\"workspace\":\"prod\"}}\n",
            "{\"doc_id\":\"doc-002\",\"text\":\"semantic latency notes\",\"metadata\":{\"kind\":\"note\",\"workspace\":\"prod\"}}\n",
            "{\"doc_id\":\"doc-003\",\"text\":\"cold open overview\",\"metadata\":{\"kind\":\"memo\",\"workspace\":\"dev\"}}\n"
        ),
    )
    .unwrap();
    if let Some(path) = serde_json::from_str::<wax_bench_model::DatasetPackManifest>(
        &fs::read_to_string(dataset_dir.path().join("manifest.json")).unwrap(),
    )
    .unwrap()
    .files
    .iter()
    .find(|file| file.kind == "document_offsets")
    .map(|file| dataset_dir.path().join(&file.path))
    {
        fs::remove_file(path).unwrap();
    }

    let mut engine = PackedTextEngine::default();
    engine
        .mount(MountRequest {
            store_path: dataset_dir.path().to_path_buf(),
        })
        .unwrap();
    engine.open(OpenRequest).unwrap();

    let result = engine
        .search(SearchRequest {
            query_text: "stale text".to_owned(),
        })
        .unwrap();

    assert_eq!(result.hits.first().map(String::as_str), Some("doc-001"));
}

#[test]
fn query_batch_filtered_text_uses_active_store_doc_count_for_overfetch() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
    runtime
        .writer()
        .unwrap()
        .publish_raw_documents(vec![
            NewDocument::new("doc-001", "shared token").with_metadata(serde_json::json!({
                "workspace": "other"
            })),
            NewDocument::new("doc-002", "shared token").with_metadata(serde_json::json!({
                "workspace": "other"
            })),
            NewDocument::new("doc-003", "shared token").with_metadata(serde_json::json!({
                "workspace": "other"
            })),
            NewDocument::new("doc-004", "shared token").with_metadata(serde_json::json!({
                "workspace": "target"
            })),
        ])
        .unwrap();
    runtime.close().unwrap();

    fs::remove_file(dataset_dir.path().join("docs.ndjson")).unwrap();
    let manifest: wax_bench_model::DatasetPackManifest = serde_json::from_str(
        &fs::read_to_string(dataset_dir.path().join("manifest.json")).unwrap(),
    )
    .unwrap();
    for file in manifest.files.iter().filter(|file| {
        matches!(
            file.kind.as_str(),
            "document_offsets" | "text_postings" | "document_ids"
        )
    }) {
        let path = dataset_dir.path().join(&file.path);
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
    }

    let query_set_path = dataset_dir.path().join("filtered-query.jsonl");
    fs::write(
        &query_set_path,
        "{\"query_id\":\"q-filtered\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"shared token\",\"top_k\":1,\"filter_spec\":{\"metadata.workspace\":\"target\"},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":false}}\n",
    )
    .unwrap();

    let results = query_batch_ranked_results(
        dataset_dir.path(),
        &query_set_path,
        wax_bench_model::VectorQueryMode::ExactFlat,
    )
    .unwrap();

    assert_eq!(results[0].hits[0].doc_id, "doc-004");
}

#[test]
fn query_batch_returns_empty_hits_when_query_has_no_eligible_lanes() {
    let dataset_dir = tempdir().unwrap();
    pack_dataset(&PackRequest::new(
        "fixtures/bench/source/minimal",
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let query_set_path = dataset_dir.path().join("no-lane-query.jsonl");
    fs::write(
        &query_set_path,
        "{\"query_id\":\"q-no-lane\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"ignored\",\"top_k\":10,\"filter_spec\":{},\"preview_expected\":false,\"embedding_available\":false,\"lane_eligibility\":{\"text\":false,\"vector\":false,\"hybrid\":false}}\n",
    )
    .unwrap();

    let results = query_batch_ranked_results(
        dataset_dir.path(),
        &query_set_path,
        wax_bench_model::VectorQueryMode::ExactFlat,
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].query_id, "q-no-lane");
    assert!(results[0].hits.is_empty());
}
