use std::collections::HashMap;

use rayon::prelude::*;

use crate::embedvec_backend::EmbedVecVectorEngine;
use crate::engine::{SearchHit, VectorSearch};
use crate::hnsw_rs_backend::HnswRsVectorEngine;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HybridConfig {
    pub enable_conditional_fallback: bool,
    pub embedvec_primary_multiplier: usize,
    pub embedvec_confident_kth_score: f32,
    pub embedvec_candidate_multiplier: usize,
    pub hnsw_candidate_multiplier: usize,
    pub rerank_candidate_cap: usize,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            enable_conditional_fallback: true,
            embedvec_primary_multiplier: 2,
            embedvec_confident_kth_score: 0.82,
            embedvec_candidate_multiplier: 20,
            hnsw_candidate_multiplier: 10,
            rerank_candidate_cap: 128,
        }
    }
}

pub struct HybridVectorEngine {
    config: HybridConfig,
    dims: Option<usize>,
    vectors: HashMap<u64, Vec<f32>>,
    embedvec: EmbedVecVectorEngine,
    hnsw: HnswRsVectorEngine,
}

impl Default for HybridVectorEngine {
    fn default() -> Self {
        Self {
            config: HybridConfig::default(),
            dims: None,
            vectors: HashMap::new(),
            embedvec: EmbedVecVectorEngine::new(),
            hnsw: HnswRsVectorEngine::new(),
        }
    }
}

impl HybridVectorEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: HybridConfig) -> Self {
        Self {
            config,
            dims: None,
            vectors: HashMap::new(),
            embedvec: EmbedVecVectorEngine::new(),
            hnsw: HnswRsVectorEngine::new(),
        }
    }

    pub fn config(&self) -> HybridConfig {
        self.config
    }
}

impl VectorSearch for HybridVectorEngine {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        if vector.is_empty() {
            return;
        }

        match self.dims {
            Some(d) if d != vector.len() => return,
            None => self.dims = Some(vector.len()),
            _ => {}
        }

        self.embedvec.upsert(id, vector.clone());
        self.hnsw.upsert(id, vector.clone());
        self.vectors.insert(id, vector);
    }

    fn remove(&mut self, id: u64) {
        self.embedvec.remove(id);
        self.hnsw.remove(id);
        self.vectors.remove(&id);
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<SearchHit> {
        if query.is_empty() || k == 0 {
            return Vec::new();
        }

        let Some(dims) = self.dims else {
            return Vec::new();
        };
        if query.len() != dims || self.vectors.is_empty() {
            return Vec::new();
        }

        let max_candidates = self.vectors.len();
        let primary_k = expanded_k(k, self.config.embedvec_primary_multiplier, max_candidates);
        let primary_embed = self.embedvec.search(query, primary_k);
        if self.config.enable_conditional_fallback
            && is_confident_embed_topk(&primary_embed, k, self.config.embedvec_confident_kth_score)
        {
            return take_top_k(primary_embed, k);
        }

        let embed_k = expanded_k(k, self.config.embedvec_candidate_multiplier, max_candidates);
        let hnsw_k = expanded_k(k, self.config.hnsw_candidate_multiplier, max_candidates);
        let (embed_hits, hnsw_hits) = rayon::join(
            || self.embedvec.search(query, embed_k),
            || self.hnsw.search(query, hnsw_k),
        );

        let mut candidate_scores: HashMap<u64, f32> =
            HashMap::with_capacity(embed_hits.len().saturating_add(hnsw_hits.len()));
        for hit in embed_hits {
            candidate_scores
                .entry(hit.id)
                .and_modify(|s| *s = s.max(hit.score))
                .or_insert(hit.score);
        }
        for hit in hnsw_hits {
            candidate_scores
                .entry(hit.id)
                .and_modify(|s| *s = s.max(hit.score))
                .or_insert(hit.score);
        }
        if candidate_scores.is_empty() {
            return Vec::new();
        }

        let rerank_cap = self
            .config
            .rerank_candidate_cap
            .max(k)
            .min(candidate_scores.len());
        let mut ranked_candidates: Vec<(u64, f32)> = candidate_scores.into_iter().collect();
        ranked_candidates.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        ranked_candidates.truncate(rerank_cap);

        let mut out: Vec<SearchHit> = ranked_candidates
            .par_iter()
            .filter_map(|(id, _)| {
                let vector = self.vectors.get(id)?;
                Some(SearchHit {
                    id: *id,
                    score: cosine(query, vector),
                })
            })
            .collect();

        out.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        out.truncate(k);
        out
    }
}

fn expanded_k(base_k: usize, multiplier: usize, max_candidates: usize) -> usize {
    let ask = base_k.saturating_mul(multiplier.max(1));
    ask.clamp(1, max_candidates)
}

fn take_top_k(mut hits: Vec<SearchHit>, k: usize) -> Vec<SearchHit> {
    hits.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
    hits.truncate(k);
    hits
}

fn is_confident_embed_topk(hits: &[SearchHit], k: usize, threshold: f32) -> bool {
    if hits.len() < k {
        return false;
    }
    let mut top = hits.to_vec();
    top.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
    let kth = top.get(k - 1).map(|h| h.score).unwrap_or(0.0);
    kth >= threshold
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot = x.mul_add(*y, dot);
        na = x.mul_add(*x, na);
        nb = y.mul_add(*y, nb);
    }
    if na <= 0.0 || nb <= 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}
