use std::collections::{HashMap, HashSet};

use hnsw_rs::prelude::{DistCosine, Hnsw};

use crate::engine::{SearchHit, VectorSearch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswRsConfig {
    pub max_nb_connection: usize,
    pub max_elements_hint: usize,
    pub max_layer: usize,
    pub ef_construction: usize,
    pub search_oversampling: usize,
}

impl Default for HnswRsConfig {
    fn default() -> Self {
        Self {
            max_nb_connection: 16,
            max_elements_hint: 1_000_000,
            max_layer: 16,
            ef_construction: 200,
            search_oversampling: 8,
        }
    }
}

pub struct HnswRsVectorEngine {
    config: HnswRsConfig,
    dims: Option<usize>,
    index: Option<Hnsw<'static, f32, DistCosine>>,
    vectors: HashMap<u64, Vec<f32>>,
}

impl Default for HnswRsVectorEngine {
    fn default() -> Self {
        Self {
            config: HnswRsConfig::default(),
            dims: None,
            index: None,
            vectors: HashMap::new(),
        }
    }
}

impl HnswRsVectorEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: HnswRsConfig) -> Self {
        Self {
            config,
            dims: None,
            index: None,
            vectors: HashMap::new(),
        }
    }

    pub fn config(&self) -> HnswRsConfig {
        self.config
    }

    fn ensure_index(&mut self) {
        if self.index.is_none() {
            let mut index = Hnsw::new(
                self.config.max_nb_connection,
                self.config.max_elements_hint,
                self.config.max_layer,
                self.config.ef_construction,
                DistCosine {},
            );
            index.set_searching_mode(true);
            self.index = Some(index);
        }
    }
}

impl VectorSearch for HnswRsVectorEngine {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        if vector.is_empty() {
            return;
        }

        let Ok(data_id) = usize::try_from(id) else {
            return;
        };

        match self.dims {
            Some(d) if d != vector.len() => return,
            None => self.dims = Some(vector.len()),
            _ => {}
        }

        self.ensure_index();
        if let Some(index) = self.index.as_ref() {
            index.insert((vector.as_slice(), data_id));
        }
        self.vectors.insert(id, vector);
    }

    fn remove(&mut self, id: u64) {
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

        let Some(index) = self.index.as_ref() else {
            return Vec::new();
        };
        if self.vectors.is_empty() {
            return Vec::new();
        }

        let total_live = self.vectors.len();
        let max_ask = index.get_nb_point().max(total_live);
        let mut ask = k.min(max_ask);
        if ask == 0 {
            return Vec::new();
        }

        let mut best = Vec::new();
        while ask <= max_ask {
            let ef = ask
                .saturating_mul(self.config.search_oversampling.max(1))
                .max(ask);
            let neighbors = index.search(query, ask, ef);
            let mut seen = HashSet::new();
            let mut hits = Vec::with_capacity(k);
            for n in neighbors {
                let Ok(id) = u64::try_from(n.d_id) else {
                    continue;
                };
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
