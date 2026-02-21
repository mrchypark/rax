use std::collections::{HashMap, HashSet};

use embedvec::{Distance, EmbedVec, Quantization};
use serde_json::Value;

use crate::engine::{SearchHit, VectorSearch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbedVecConfig {
    pub m: usize,
    pub ef_construction: usize,
    pub search_oversampling: usize,
}

impl Default for EmbedVecConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
            search_oversampling: 8,
        }
    }
}

pub struct EmbedVecVectorEngine {
    config: EmbedVecConfig,
    dims: Option<usize>,
    db: Option<EmbedVec>,
    vectors: HashMap<u64, Vec<f32>>,
    external_to_internal: HashMap<u64, usize>,
    internal_to_external: HashMap<usize, u64>,
    total_internal_vectors: usize,
}

impl Default for EmbedVecVectorEngine {
    fn default() -> Self {
        Self {
            config: EmbedVecConfig::default(),
            dims: None,
            db: None,
            vectors: HashMap::new(),
            external_to_internal: HashMap::new(),
            internal_to_external: HashMap::new(),
            total_internal_vectors: 0,
        }
    }
}

impl EmbedVecVectorEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: EmbedVecConfig) -> Self {
        Self {
            config,
            dims: None,
            db: None,
            vectors: HashMap::new(),
            external_to_internal: HashMap::new(),
            internal_to_external: HashMap::new(),
            total_internal_vectors: 0,
        }
    }

    pub fn config(&self) -> EmbedVecConfig {
        self.config
    }

    fn ensure_db(&mut self) {
        let Some(dim) = self.dims else {
            return;
        };
        if self.db.is_none() {
            let db = EmbedVec::new_internal(
                dim,
                Distance::Cosine,
                self.config.m,
                self.config.ef_construction,
                Quantization::None,
            );
            self.db = db.ok();
        }
    }
}

impl VectorSearch for EmbedVecVectorEngine {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        if vector.is_empty() {
            return;
        }

        match self.dims {
            Some(d) if d != vector.len() => return,
            None => self.dims = Some(vector.len()),
            _ => {}
        }

        self.ensure_db();
        let Some(db) = self.db.as_mut() else {
            return;
        };

        let Ok(internal_id) = db.add_internal(vector.as_slice(), Value::Null) else {
            return;
        };

        if let Some(old_internal) = self.external_to_internal.insert(id, internal_id) {
            self.internal_to_external.remove(&old_internal);
        }
        self.internal_to_external.insert(internal_id, id);
        self.vectors.insert(id, vector);
        self.total_internal_vectors = self.total_internal_vectors.saturating_add(1);
    }

    fn remove(&mut self, id: u64) {
        if let Some(internal_id) = self.external_to_internal.remove(&id) {
            self.internal_to_external.remove(&internal_id);
        }
        self.vectors.remove(&id);
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<SearchHit> {
        if query.is_empty() || k == 0 {
            return Vec::new();
        }

        let Some(dims) = self.dims else {
            return Vec::new();
        };
        if query.len() != dims {
            return Vec::new();
        }

        let Some(db) = self.db.as_ref() else {
            return Vec::new();
        };

        let live_count = self.vectors.len();
        if live_count == 0 {
            return Vec::new();
        }

        let max_ask = self.total_internal_vectors.max(live_count);
        let mut ask = k.min(max_ask);
        if ask == 0 {
            return Vec::new();
        }

        let mut best = Vec::new();
        while ask <= max_ask {
            let ef = ask
                .saturating_mul(self.config.search_oversampling.max(1))
                .max(ask);
            let Ok(results) = db.search_internal(query, ask, ef, None) else {
                return Vec::new();
            };

            let mut seen = HashSet::new();
            let mut hits = Vec::with_capacity(k);
            for result in results {
                let Some(&id) = self.internal_to_external.get(&result.id) else {
                    continue;
                };
                if self.external_to_internal.get(&id).copied() != Some(result.id) {
                    continue;
                }
                if !seen.insert(id) {
                    continue;
                }
                let Some(vector) = self.vectors.get(&id) else {
                    continue;
                };
                hits.push(SearchHit {
                    id,
                    score: cosine(query, vector),
                });
                if hits.len() >= k {
                    break;
                }
            }
            best = hits;
            if best.len() >= k || ask == max_ask {
                break;
            }
            let next = ask.saturating_mul(2).min(max_ask);
            if next == ask {
                break;
            }
            ask = next;
        }

        best.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        best.truncate(k);
        best
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
