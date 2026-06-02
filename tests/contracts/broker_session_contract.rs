use rax_bench_model::embed_text;
use rax_core::{open_store, read_segment_object, SegmentKind};
use rax_docstore::BinaryDocSegment;
use serde_json::json;
use tempfile::tempdir;

use rax_broker::{RaxBroker, SessionNewDocument, SessionNewDocumentVector, SessionSearchRequest};

#[test]
fn broker_store_session_ingests_reopens_and_searches_raw_documents_without_dataset_manifest() {
    let temp_dir = tempdir().unwrap();
    let store_path = temp_dir.path().join("projection.rax");

    let mut broker = RaxBroker::default();
    let session_id = broker.open_store_session(&store_path).unwrap();
    let mut extra_fields = serde_json::Map::new();
    extra_fields.insert("channel".to_owned(), json!("direct-api"));
    extra_fields.insert("rank".to_owned(), json!(7));

    broker
        .ingest_documents(
            session_id,
            vec![SessionNewDocument {
                doc_id: "caller-doc-42".to_owned(),
                text: "direct projection raw token".to_owned(),
                metadata: json!({"workspace":"product","kind":"note"}),
                timestamp_ms: Some(1_714_567_890_123),
                extra_fields,
            }],
        )
        .unwrap();
    broker.close_session(session_id).unwrap();

    let reopened_id = broker.open_store_session(&store_path).unwrap();
    let response = broker
        .search(
            reopened_id,
            SessionSearchRequest::text("projection raw")
                .with_top_k(1)
                .with_preview(true),
        )
        .unwrap();

    assert_eq!(response.hits[0].doc_id, "caller-doc-42");
    assert_eq!(
        response.hits[0].preview.as_deref(),
        Some("direct projection raw token")
    );

    let opened_store = open_store(&store_path).unwrap();
    let doc_descriptor = opened_store
        .manifest
        .segments
        .iter()
        .filter(|segment| segment.family == SegmentKind::Doc)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
        .unwrap();
    let doc_segment_bytes = read_segment_object(&store_path, doc_descriptor).unwrap();
    let doc_segment = BinaryDocSegment::decode(&doc_segment_bytes).unwrap();
    let caller_rax_doc_id = doc_segment.doc_id_map.rax_doc_id("caller-doc-42").unwrap();
    let caller_payload = doc_segment
        .records
        .iter()
        .find(|record| record.row.doc_id == caller_rax_doc_id)
        .map(|record| serde_json::from_slice::<serde_json::Value>(&record.payload).unwrap())
        .unwrap();

    assert_eq!(caller_payload["doc_id"], json!("caller-doc-42"));
    assert_eq!(caller_payload["text"], json!("direct projection raw token"));
    assert_eq!(
        caller_payload["metadata"],
        json!({"workspace":"product","kind":"note"})
    );
    assert_eq!(caller_payload["timestamp_ms"], json!(1_714_567_890_123_u64));
    assert_eq!(caller_payload["channel"], json!("direct-api"));
    assert_eq!(caller_payload["rank"], json!(7));

    broker.close_session(reopened_id).unwrap();
}

#[test]
fn broker_store_session_search_refreshes_documents_published_by_another_session() {
    let temp_dir = tempdir().unwrap();
    let store_path = temp_dir.path().join("projection-refresh.rax");

    let mut broker = RaxBroker::default();
    let reader_id = broker.open_store_session(&store_path).unwrap();
    let writer_id = broker.open_store_session(&store_path).unwrap();

    broker
        .ingest_documents(
            writer_id,
            vec![SessionNewDocument {
                doc_id: "doc-004".to_owned(),
                text: "fresh broker token".to_owned(),
                metadata: serde_json::json!({"kind":"note"}),
                timestamp_ms: None,
                extra_fields: Default::default(),
            }],
        )
        .unwrap();

    let refreshed = broker
        .search(
            reader_id,
            SessionSearchRequest::text("fresh broker token")
                .with_top_k(1)
                .with_preview(true),
        )
        .unwrap();
    assert_eq!(refreshed.hits[0].doc_id, "doc-004");
    assert_eq!(
        refreshed.hits[0].preview.as_deref(),
        Some("fresh broker token")
    );

    broker.close_session(reader_id).unwrap();
    broker.close_session(writer_id).unwrap();
}

#[test]
fn broker_store_session_ingests_vectors_and_searches_direct_store() {
    let temp_dir = tempdir().unwrap();
    let store_path = temp_dir.path().join("projection-vectors.rax");

    let mut broker = RaxBroker::default();
    let session_id = broker.open_store_session(&store_path).unwrap();
    broker
        .ingest_documents(
            session_id,
            vec![
                SessionNewDocument {
                    doc_id: "vec-doc-1".to_owned(),
                    text: "direct vector rust guide".to_owned(),
                    metadata: json!({"kind":"guide"}),
                    timestamp_ms: None,
                    extra_fields: Default::default(),
                },
                SessionNewDocument {
                    doc_id: "vec-doc-2".to_owned(),
                    text: "direct semantic latency checklist".to_owned(),
                    metadata: json!({"kind":"checklist"}),
                    timestamp_ms: None,
                    extra_fields: Default::default(),
                },
            ],
        )
        .unwrap();
    broker
        .ingest_vectors(
            session_id,
            vec![
                SessionNewDocumentVector {
                    doc_id: "vec-doc-1".to_owned(),
                    values: embed_text("direct vector rust guide", 384),
                },
                SessionNewDocumentVector {
                    doc_id: "vec-doc-2".to_owned(),
                    values: embed_text("direct semantic latency checklist", 384),
                },
            ],
        )
        .unwrap();
    broker.close_session(session_id).unwrap();

    let reopened_id = broker.open_store_session(&store_path).unwrap();
    let vector_response = broker
        .search(
            reopened_id,
            SessionSearchRequest::vector(embed_text("direct semantic latency checklist", 384))
                .with_top_k(1)
                .with_preview(true),
        )
        .unwrap();
    assert_eq!(vector_response.hits[0].doc_id, "vec-doc-2");
    assert_eq!(
        vector_response.hits[0].preview.as_deref(),
        Some("direct semantic latency checklist")
    );

    let hybrid_response = broker
        .search(
            reopened_id,
            SessionSearchRequest::hybrid(
                "semantic latency",
                embed_text("direct semantic latency checklist", 384),
            )
            .with_top_k(1)
            .with_preview(true),
        )
        .unwrap();
    assert_eq!(hybrid_response.hits[0].doc_id, "vec-doc-2");

    broker.close_session(reopened_id).unwrap();
}
