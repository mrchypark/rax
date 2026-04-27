use std::fs;

use serde_json::json;
use tempfile::tempdir;
use wax_bench_model::embed_text;
use wax_bench_packer::{pack_adhoc_dataset, AdhocPackRequest};
use wax_v2_core::{open_store, SegmentKind};
use wax_v2_runtime::{NewDocument, NewDocumentVector, RuntimeStore};

#[test]
fn raw_full_snapshot_publish_matches_compatibility_import_semantics_for_equivalent_inputs() {
    let source_dir = tempdir().unwrap();
    let compat_root = tempdir().unwrap();
    let raw_root = tempdir().unwrap();
    let docs_path = source_dir.path().join("docs.ndjson");
    let docs = [
        (
            "doc-001",
            "rust benchmark guide",
            json!({"kind":"guide","workspace":"prod"}),
        ),
        (
            "doc-002",
            "semantic latency checklist",
            json!({"kind":"checklist","workspace":"prod"}),
        ),
        (
            "doc-003",
            "hybrid search tuning notes",
            json!({"kind":"notes","workspace":"prod"}),
        ),
    ];
    fs::write(
        &docs_path,
        docs.iter()
            .map(|(doc_id, text, metadata)| {
                json!({
                    "doc_id": doc_id,
                    "text": text,
                    "metadata": metadata,
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();

    pack_adhoc_dataset(&AdhocPackRequest::new(
        &docs_path,
        compat_root.path(),
        "small",
    ))
    .unwrap();
    pack_adhoc_dataset(&AdhocPackRequest::new(&docs_path, raw_root.path(), "small")).unwrap();

    let mut compat_runtime = RuntimeStore::create(compat_root.path()).unwrap();
    let compat_report = compat_runtime
        .writer()
        .unwrap()
        .import_compatibility_snapshot()
        .unwrap();
    compat_runtime.close().unwrap();

    let mut raw_runtime = RuntimeStore::create(raw_root.path()).unwrap();
    let raw_report = raw_runtime
        .writer()
        .unwrap()
        .publish_raw_snapshot(
            vec![
                NewDocument::new("doc-001", "rust benchmark guide")
                    .with_metadata(json!({"kind":"guide","workspace":"prod"})),
                NewDocument::new("doc-002", "semantic latency checklist")
                    .with_metadata(json!({"kind":"checklist","workspace":"prod"})),
                NewDocument::new("doc-003", "hybrid search tuning notes")
                    .with_metadata(json!({"kind":"notes","workspace":"prod"})),
            ],
            Some(vec![
                NewDocumentVector::new("doc-001", embed_text("rust benchmark guide", 384)),
                NewDocumentVector::new("doc-002", embed_text("semantic latency checklist", 384)),
                NewDocumentVector::new("doc-003", embed_text("hybrid search tuning notes", 384)),
            ]),
        )
        .unwrap();
    raw_runtime.close().unwrap();

    assert_eq!(compat_report, raw_report);

    let compat_store = open_store(&compat_root.path().join("store.wax")).unwrap();
    let raw_store = open_store(&raw_root.path().join("store.wax")).unwrap();

    assert_eq!(compat_store.manifest.generation, 1);
    assert_eq!(raw_store.manifest.generation, 1);
    assert_eq!(
        semantic_segment_summary(&compat_store.manifest.segments),
        semantic_segment_summary(&raw_store.manifest.segments)
    );
}

fn semantic_segment_summary(
    segments: &[wax_v2_core::SegmentDescriptor],
) -> Vec<(SegmentKind, u64, u64, u64, u64, u64)> {
    let mut summary = segments
        .iter()
        .map(|segment| {
            (
                segment.family,
                segment.segment_generation,
                segment.doc_id_start,
                segment.doc_id_end_exclusive,
                segment.live_items,
                segment.tombstoned_items,
            )
        })
        .collect::<Vec<_>>();
    summary.sort_by_key(|entry| entry.0 as u8);
    summary
}
