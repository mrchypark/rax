use std::collections::{HashMap, HashSet};

use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

use crate::engine::{SearchHit, VectorSearch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct USearchConfig {
    pub connectivity: usize,
    pub expansion_add: usize,
    pub expansion_search: usize,
}

impl Default for USearchConfig {
    fn default() -> Self {
        Self {
            connectivity: 16,
            expansion_add: 200,
            expansion_search: 80,
        }
    }
}

pub struct USearchVectorEngine {
    config: USearchConfig,
    dims: Option<usize>,
    index: Option<Index>,
    vectors: HashMap<u64, Vec<f32>>,
}

impl Default for USearchVectorEngine {
    fn default() -> Self {
        Self {
            config: USearchConfig::default(),
            dims: None,
            index: None,
            vectors: HashMap::new(),
        }
    }
}

impl USearchVectorEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: USearchConfig) -> Self {
        Self {
            config,
            dims: None,
            index: None,
            vectors: HashMap::new(),
        }
    }

    pub fn config(&self) -> USearchConfig {
        self.config
    }

    fn ensure_index(&mut self) {
        if self.index.is_some() {
            return;
        }
        let Some(dimensions) = self.dims else {
            return;
        };

        let options = IndexOptions {
            dimensions,
            metric: MetricKind::Cos,
            quantization: ScalarKind::F32,
            connectivity: self.config.connectivity,
            expansion_add: self.config.expansion_add,
            expansion_search: self.config.expansion_search,
            multi: false,
        };
        let Ok(index) = Index::new(&options) else {
            return;
        };
        let _ = index.reserve(self.vectors.len().saturating_add(1));
        self.index = Some(index);
    }
}

impl VectorSearch for USearchVectorEngine {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        if vector.is_empty() {
            return;
        }

        match self.dims {
            Some(d) if d != vector.len() => return,
            None => self.dims = Some(vector.len()),
            _ => {}
        }

        self.ensure_index();
        let Some(index) = self.index.as_ref() else {
            return;
        };

        let required_capacity = self.vectors.len().saturating_add(1);
        if index.capacity() < required_capacity {
            let target_capacity = required_capacity.next_power_of_two();
            let _ = index.reserve(target_capacity);
        }

        if self.vectors.contains_key(&id) {
            let _ = index.remove(id);
        }
        if index.add(id, vector.as_slice()).is_err() {
            return;
        }
        self.vectors.insert(id, vector);
    }

    fn remove(&mut self, id: u64) {
        self.vectors.remove(&id);
        if let Some(index) = self.index.as_ref() {
            let _ = index.remove(id);
        }
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

        let Some(index) = self.index.as_ref() else {
            return Vec::new();
        };

        let max_ask = self.vectors.len();
        let mut ask = k.min(max_ask);
        if ask == 0 {
            return Vec::new();
        }

        let mut best = Vec::new();
        while ask <= max_ask {
            let Ok(matches) = index.search(query, ask) else {
                return Vec::new();
            };

            let mut seen = HashSet::new();
            let mut hits = Vec::with_capacity(k);
            for (id, distance) in matches.keys.into_iter().zip(matches.distances.into_iter()) {
                if !self.vectors.contains_key(&id) {
                    continue;
                }
                if !seen.insert(id) {
                    continue;
                }
                hits.push(SearchHit {
                    id,
                    score: distance_to_score(distance),
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

fn distance_to_score(distance: f32) -> f32 {
    if !distance.is_finite() {
        return f32::NEG_INFINITY;
    }
    (1.0 - distance).clamp(-1.0, 1.0)
}
