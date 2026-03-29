use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkId {
    pub dataset_id: String,
    pub workload_id: String,
    pub sample_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountRequest {
    pub store_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OpenRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    pub query_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchResult {
    pub hits: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnginePhase {
    #[default]
    New,
    Mounted,
    Open,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineStats {
    pub phase: EnginePhase,
    pub last_mounted_path: Option<PathBuf>,
}

pub trait WaxEngine {
    type Error;

    /// Mount may bind a store location, but must not hide search-path work.
    fn mount(&mut self, request: MountRequest) -> Result<(), Self::Error>;

    /// Open success means routing metadata is ready, but query-driven work must remain outside open.
    fn open(&mut self, request: OpenRequest) -> Result<OpenResult, Self::Error>;

    fn search(&mut self, request: SearchRequest) -> Result<SearchResult, Self::Error>;

    fn get_stats(&self) -> EngineStats;
}
