use std::fs;

use tempfile::tempdir;
use wax_bench_packer::{pack_adhoc_dataset, AdhocPackRequest};
use wax_v2_core::{open_store, read_segment_object, SegmentKind};
use wax_v2_docstore::{BinaryDocSegment, Docstore};
use wax_v2_runtime::RuntimeStore;

#[test]
fn compatibility_reimport_preserves_existing_wax_doc_ids_when_document_order_changes() {
    let source_dir = tempdir().unwrap();
    let dataset_dir = tempdir().unwrap();
    let docs_path = source_dir.path().join("docs.ndjson");

    fs::write(
        &docs_path,
        concat!(
            "{\"doc_id\":\"doc-a\",\"text\":\"alpha rust\"}\n",
            "{\"doc_id\":\"doc-b\",\"text\":\"beta semantic\"}\n",
        ),
    )
    .unwrap();
    pack_adhoc_dataset(&AdhocPackRequest::new(
        &docs_path,
        dataset_dir.path(),
        "small",
    ))
    .unwrap();

    let mut runtime = RuntimeStore::create(dataset_dir.path()).unwrap();
    runtime
        .writer()
        .unwrap()
        .import_compatibility_snapshot()
        .unwrap();
    runtime.close().unwrap();

    fs::write(
        &docs_path,
        concat!(
            "{\"doc_id\":\"doc-b\",\"text\":\"beta semantic\"}\n",
            "{\"doc_id\":\"doc-a\",\"text\":\"alpha rust\"}\n",
            "{\"doc_id\":\"doc-c\",\"text\":\"gamma hybrid\"}\n",
        ),
    )
    .unwrap();
    let manifest = pack_adhoc_dataset(&AdhocPackRequest::new(
        &docs_path,
        dataset_dir.path(),
        "small",
    ))
    .unwrap();

    let mut reopened_runtime = RuntimeStore::open(dataset_dir.path()).unwrap();
    reopened_runtime
        .writer()
        .unwrap()
        .import_compatibility_snapshot()
        .unwrap();
    reopened_runtime.close().unwrap();

    let reopened_docstore = Docstore::open(dataset_dir.path(), &manifest).unwrap();
    let doc_id_map = reopened_docstore.build_doc_id_map().unwrap();
    assert_eq!(doc_id_map.wax_doc_id("doc-a"), Some(0));
    assert_eq!(doc_id_map.wax_doc_id("doc-b"), Some(1));
    assert_eq!(doc_id_map.wax_doc_id("doc-c"), Some(2));

    let store_path = dataset_dir.path().join("store.wax");
    let opened = open_store(&store_path).unwrap();
    let latest_doc_descriptor = opened
        .manifest
        .segments
        .iter()
        .filter(|segment| segment.family == SegmentKind::Doc)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
        .unwrap();
    let bytes = read_segment_object(&store_path, latest_doc_descriptor).unwrap();
    let segment = BinaryDocSegment::decode(&bytes).unwrap();

    assert_eq!(
        segment
            .records
            .iter()
            .map(|record| record.row.doc_id)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}
