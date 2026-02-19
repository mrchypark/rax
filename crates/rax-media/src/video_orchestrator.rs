use tokio::sync::Mutex;

pub const VIDEO_PIPELINE_VERSION: &str = "video_rag_v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoIngestReport {
    pub frame_ids: Vec<u64>,
    pub embeddings_generated: usize,
    pub pipeline_version: &'static str,
}

#[derive(Default)]
pub struct VideoRAGOrchestrator {
    next_id: Mutex<u64>,
}

impl VideoRAGOrchestrator {
    pub fn new() -> Self {
        Self {
            next_id: Mutex::new(1),
        }
    }

    pub async fn ingest_segments(&self, segments: Vec<String>) -> VideoIngestReport {
        let mut ids = Vec::with_capacity(segments.len());
        let mut guard = self.next_id.lock().await;
        for _ in segments {
            ids.push(*guard);
            *guard += 1;
        }
        VideoIngestReport {
            embeddings_generated: ids.len(),
            frame_ids: ids,
            pipeline_version: VIDEO_PIPELINE_VERSION,
        }
    }
}
