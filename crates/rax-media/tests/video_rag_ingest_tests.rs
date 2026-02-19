use rax_media::video_orchestrator::{VideoRAGOrchestrator, VIDEO_PIPELINE_VERSION};

#[tokio::test]
async fn video_ingest_creates_segment_frames_and_embeddings() {
    let o = VideoRAGOrchestrator::new();
    let report = o
        .ingest_segments(vec!["seg1".to_string(), "seg2".to_string()])
        .await;

    assert_eq!(report.frame_ids.len(), 2);
    assert_eq!(report.embeddings_generated, 2);
    assert_eq!(report.pipeline_version, VIDEO_PIPELINE_VERSION);
}
