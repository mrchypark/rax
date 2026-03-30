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
