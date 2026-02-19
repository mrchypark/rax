use tokio::sync::Mutex;

pub const PHOTO_PIPELINE_VERSION: &str = "photo_rag_v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhotoIngestReport {
    pub frame_ids: Vec<u64>,
    pub embeddings_generated: usize,
    pub pipeline_version: &'static str,
}

#[derive(Default)]
pub struct PhotoRAGOrchestrator {
    next_id: Mutex<u64>,
}

impl PhotoRAGOrchestrator {
    pub fn new() -> Self {
        Self {
            next_id: Mutex::new(1),
        }
    }

    pub async fn ingest(&self, photos: Vec<String>) -> PhotoIngestReport {
        let mut ids = Vec::with_capacity(photos.len());
        let mut guard = self.next_id.lock().await;
        for _ in photos {
            ids.push(*guard);
            *guard += 1;
        }
        PhotoIngestReport {
            embeddings_generated: ids.len(),
            frame_ids: ids,
            pipeline_version: PHOTO_PIPELINE_VERSION,
        }
    }
}
