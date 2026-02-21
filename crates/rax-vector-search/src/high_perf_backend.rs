use std::collections::HashMap;

use rayon::prelude::*;

use crate::engine::{SearchHit, VectorSearch};

#[derive(Debug, Clone)]
struct StoredVector {
    id: u64,
    values: Vec<f32>,
    inv_norm: f32,
}

#[derive(Default)]
pub struct HighPerfVectorEngine {
    dims: Option<usize>,
    vectors: Vec<StoredVector>,
    id_to_index: HashMap<u64, usize>,
    pending_mutation_count: usize,
}

impl HighPerfVectorEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pending_mutations(&self) -> usize {
        self.pending_mutation_count
    }

    pub fn stage_for_commit(&mut self) -> VectorIndexSnapshot {
        let mut entries: Vec<(u64, Vec<f32>)> = self
            .vectors
            .iter()
            .map(|v| (v.id, v.values.clone()))
            .collect();
        entries.sort_unstable_by_key(|(id, _)| *id);
        let snapshot = VectorIndexSnapshot {
            dimension: self.dims.unwrap_or(0),
            vector_count: entries.len(),
            entries,
        };
        self.pending_mutation_count = 0;
        snapshot
    }

    pub fn restore_from_snapshot(&mut self, snapshot: VectorIndexSnapshot) {
        self.vectors.clear();
        self.id_to_index.clear();
        self.dims = if snapshot.dimension == 0 {
            None
        } else {
            Some(snapshot.dimension)
        };

        for (id, values) in snapshot.entries {
            if let Some(d) = self.dims {
                if values.len() != d {
                    continue;
                }
            }
            let idx = self.vectors.len();
            self.vectors.push(StoredVector {
                id,
                inv_norm: inv_l2_norm(&values),
                values,
            });
            self.id_to_index.insert(id, idx);
        }
        self.pending_mutation_count = 0;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorIndexSnapshot {
    pub dimension: usize,
    pub vector_count: usize,
    pub entries: Vec<(u64, Vec<f32>)>,
}

impl VectorSearch for HighPerfVectorEngine {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        if vector.is_empty() {
            return;
        }

        match self.dims {
            Some(d) if d != vector.len() => return,
            None => self.dims = Some(vector.len()),
            _ => {}
        }

        let stored = StoredVector {
            id,
            inv_norm: inv_l2_norm(&vector),
            values: vector,
        };

        if let Some(idx) = self.id_to_index.get(&id).copied() {
            self.vectors[idx] = stored;
        } else {
            self.vectors.push(stored);
            let idx = self.vectors.len() - 1;
            self.id_to_index.insert(id, idx);
        }
        self.pending_mutation_count += 1;
    }

    fn remove(&mut self, id: u64) {
        let Some(idx) = self.id_to_index.remove(&id) else {
            return;
        };

        let removed = self.vectors.swap_remove(idx);
        if removed.id != id {
            // Should not happen, but preserve map integrity if state is inconsistent.
            self.id_to_index.remove(&removed.id);
        }

        if idx < self.vectors.len() {
            let moved = self.vectors[idx].id;
            self.id_to_index.insert(moved, idx);
        }
        self.pending_mutation_count += 1;
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<SearchHit> {
        if k == 0 || query.is_empty() || self.vectors.is_empty() {
            return Vec::new();
        }

        let Some(dims) = self.dims else {
            return Vec::new();
        };
        if query.len() != dims {
            return Vec::new();
        }

        let inv_query_norm = inv_l2_norm(query);
        if inv_query_norm == 0.0 {
            return Vec::new();
        }

        let mut hits: Vec<SearchHit> = self
            .vectors
            .par_iter()
            .map(|v| SearchHit {
                id: v.id,
                score: dot(query, &v.values) * inv_query_norm * v.inv_norm,
            })
            .collect();

        let cmp = |a: &SearchHit, b: &SearchHit| {
            b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id))
        };

        if hits.len() > k {
            hits.select_nth_unstable_by(k - 1, cmp);
            hits.truncate(k);
        }

        hits.sort_by(cmp);
        hits
    }
}

fn inv_l2_norm(v: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for x in v {
        sum = x.mul_add(*x, sum);
    }
    if sum <= 0.0 {
        0.0
    } else {
        1.0 / sum.sqrt()
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    let mut s0 = 0.0f32;
    let mut s1 = 0.0f32;
    let mut s2 = 0.0f32;
    let mut s3 = 0.0f32;

    let lanes = a.len() / 4;
    for i in 0..lanes {
        let base = i * 4;
        s0 = a[base].mul_add(b[base], s0);
        s1 = a[base + 1].mul_add(b[base + 1], s1);
        s2 = a[base + 2].mul_add(b[base + 2], s2);
        s3 = a[base + 3].mul_add(b[base + 3], s3);
    }

    let mut sum = (s0 + s1) + (s2 + s3);
    let rem_start = lanes * 4;
    for i in rem_start..a.len() {
        sum = a[i].mul_add(b[i], sum);
    }
    sum
}
