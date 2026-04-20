use std::fs;

use tempfile::tempdir;
use wax_bench_packer::{pack_dataset, PackRequest};
use wax_v2_core::open_store;
use wax_v2_runtime::{RuntimeSearchMode, RuntimeSearchRequest, RuntimeStore};

#[test]
fn staged_compatibility_publish_creates_one_visible_generation_for_multiple_families() {
    let dataset_dir = tempdir().unwrap();
    let fixture_root =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/bench/source/minimal");
    let manifest = pack_dataset(&PackRequest::new(
        &fixture_root,
        dataset_dir.path(),
        "small",
        "clean",
    ))
    .unwrap();

    let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
    let report = runtime
        .writer()
        .unwrap()
        .publish_staged_compatibility_snapshot()
        .unwrap();
    assert_eq!(report.generation, 1);
    assert_eq!(
        report.published_families,
        vec![
            wax_v2_runtime::RuntimePublishFamily::Doc,
            wax_v2_runtime::RuntimePublishFamily::Text,
            wax_v2_runtime::RuntimePublishFamily::Vector,
        ]
    );
    runtime.close().unwrap();

    let store_path = dataset_dir.path().join("store.wax");
    let opened = open_store(&store_path).unwrap();
    assert_eq!(opened.manifest.generation, 1);
    assert_eq!(opened.manifest.segments.len(), 3);
    assert_eq!(
        opened
            .manifest
            .segments
            .iter()
            .map(|segment| segment.segment_generation)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([1])
    );
    let mut families = opened
        .manifest
        .segments
        .iter()
        .map(|segment| format!("{:?}", segment.family))
        .collect::<Vec<_>>();
    families.sort();
    assert_eq!(families, vec!["Doc", "Txt", "Vec"]);

    for kind in [
        "documents",
        "document_offsets",
        "text_postings",
        "document_ids",
        "document_vectors",
        "document_vectors_preview_q8",
    ] {
        for file in manifest.files.iter().filter(|file| file.kind == kind) {
            fs::remove_file(dataset_dir.path().join(&file.path)).unwrap();
        }
    }

    let mut reopened = RuntimeStore::open(dataset_dir.path()).unwrap();
    let response = reopened
        .search(RuntimeSearchRequest {
            mode: RuntimeSearchMode::Text,
            text_query: Some("rust benchmark".to_owned()),
            vector_query: None,
            top_k: 2,
            include_preview: true,
        })
        .unwrap();
    assert_eq!(response.hits[0].doc_id, "doc-001");
    assert_eq!(
        response.hits[0].preview.as_deref(),
        Some("rust benchmark guide")
    );
}
