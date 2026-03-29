use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use strum::EnumIter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkId {
    pub dataset_id: String,
    pub workload_id: String,
    pub sample_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter)]
pub enum CacheState {
    #[serde(rename = "warm_process")]
    WarmProcess,
    #[serde(rename = "cold_process")]
    ColdProcess,
    #[serde(rename = "cold_process_warm_fs_cache")]
    ColdProcessWarmFsCache,
    #[serde(rename = "cold_process_cold_fs_cache")]
    ColdProcessColdFsCache,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter)]
pub enum ColdState {
    #[serde(rename = "restart_cold")]
    RestartCold,
    #[serde(rename = "pressure_cold")]
    PressureCold,
    #[serde(rename = "reboot_cold")]
    RebootCold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter)]
pub enum MaterializationMode {
    #[serde(rename = "no_forced_lane_materialization")]
    NoForcedLaneMaterialization,
    #[serde(rename = "force_text_lane")]
    ForceTextLane,
    #[serde(rename = "force_vector_lane")]
    ForceVectorLane,
    #[serde(rename = "force_all_lanes")]
    ForceAllLanes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter)]
pub enum PreviewMode {
    #[serde(rename = "no_preview")]
    NoPreview,
    #[serde(rename = "with_preview")]
    WithPreview,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter)]
pub enum QueryEmbeddingMode {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "precomputed")]
    Precomputed,
    #[serde(rename = "runtime_generic")]
    RuntimeGeneric,
    #[serde(rename = "runtime_ane_cold")]
    RuntimeAneCold,
    #[serde(rename = "runtime_ane_warm")]
    RuntimeAneWarm,
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
