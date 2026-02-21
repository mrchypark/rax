use crate::engine::{SearchHit, VectorSearch};
use crate::usearch_backend::USearchVectorEngine;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorBackend {
    USearch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorSearchConfig {
    pub backend: VectorBackend,
}

impl Default for VectorSearchConfig {
    fn default() -> Self {
        Self {
            backend: VectorBackend::USearch,
        }
    }
}

pub enum ConfiguredVectorEngine {
    USearch(USearchVectorEngine),
}

impl ConfiguredVectorEngine {
    pub fn backend(&self) -> VectorBackend {
        match self {
            Self::USearch(_) => VectorBackend::USearch,
        }
    }
}

impl VectorSearch for ConfiguredVectorEngine {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        match self {
            Self::USearch(engine) => engine.upsert(id, vector),
        }
    }

    fn remove(&mut self, id: u64) {
        match self {
            Self::USearch(engine) => engine.remove(id),
        }
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<SearchHit> {
        match self {
            Self::USearch(engine) => engine.search(query, k),
        }
    }
}

pub fn create_engine(config: VectorSearchConfig) -> ConfiguredVectorEngine {
    match config.backend {
        VectorBackend::USearch => ConfiguredVectorEngine::USearch(USearchVectorEngine::new()),
    }
}

pub fn create_default_engine() -> ConfiguredVectorEngine {
    create_engine(VectorSearchConfig::default())
}
