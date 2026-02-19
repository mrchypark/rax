use tokio::sync::Mutex;

use crate::session::WaxSession;

#[derive(Default)]
pub struct MemoryOrchestrator {
    session: Mutex<WaxSession>,
}

impl MemoryOrchestrator {
    pub fn new() -> Self {
        Self {
            session: Mutex::new(WaxSession::new()),
        }
    }

    pub async fn remember(&self, text: impl Into<String>) {
        let mut s = self.session.lock().await;
        s.remember(text);
    }

    pub async fn recall(&self, query: &str) -> Vec<String> {
        let s = self.session.lock().await;
        s.recall(query)
    }

    pub async fn flush(&self) -> usize {
        let mut s = self.session.lock().await;
        s.flush()
    }
}
