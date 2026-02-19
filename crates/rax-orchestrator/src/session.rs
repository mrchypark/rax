#[derive(Default, Debug, Clone)]
pub struct WaxSession {
    memories: Vec<String>,
}

impl WaxSession {
    pub fn new() -> Self {
        Self {
            memories: Vec::new(),
        }
    }

    pub fn remember(&mut self, text: impl Into<String>) {
        self.memories.push(text.into());
    }

    pub fn recall(&self, query: &str) -> Vec<String> {
        self.memories
            .iter()
            .filter(|m| m.contains(query))
            .cloned()
            .collect()
    }

    pub fn flush(&mut self) -> usize {
        let count = self.memories.len();
        self.memories.clear();
        count
    }
}
