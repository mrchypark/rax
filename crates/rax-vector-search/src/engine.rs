#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub id: u64,
    pub score: f32,
}

pub trait VectorSearch {
    fn upsert(&mut self, id: u64, vector: Vec<f32>);
    fn remove(&mut self, id: u64);
    fn search(&self, query: &[f32], k: usize) -> Vec<SearchHit>;
}
