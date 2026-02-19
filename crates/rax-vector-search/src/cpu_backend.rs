use std::collections::HashMap;

use crate::engine::{SearchHit, VectorSearch};

#[derive(Default)]
pub struct CpuVectorEngine {
    vectors: HashMap<u64, Vec<f32>>,
}

impl CpuVectorEngine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl VectorSearch for CpuVectorEngine {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        self.vectors.insert(id, vector);
    }

    fn remove(&mut self, id: u64) {
        self.vectors.remove(&id);
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<SearchHit> {
        let mut hits: Vec<SearchHit> = self
            .vectors
            .iter()
            .map(|(id, v)| SearchHit {
                id: *id,
                score: cosine(query, v),
            })
            .collect();

        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });
        hits.truncate(k);
        hits
    }
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}
