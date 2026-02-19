use rax_media::photo_orchestrator::{PhotoRAGOrchestrator, PHOTO_PIPELINE_VERSION};

#[tokio::test]
async fn photo_ingest_sets_pipeline_version() {
    let o = PhotoRAGOrchestrator::new();
    let report = o.ingest(vec!["img".to_string()]).await;
    assert_eq!(report.pipeline_version, PHOTO_PIPELINE_VERSION);
    assert_eq!(report.embeddings_generated, 1);
}
