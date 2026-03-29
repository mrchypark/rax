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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetPackManifest {
    pub schema_version: String,
    pub generated_at: String,
    pub generator: ManifestGenerator,
    pub identity: DatasetIdentity,
    pub environment_constraints: EnvironmentConstraints,
    pub corpus: CorpusProfile,
    pub text_profile: TextProfile,
    pub metadata_profile: MetadataProfile,
    pub vector_profile: VectorProfile,
    pub dirty_profile: DirtyProfile,
    pub files: Vec<ManifestFile>,
    pub query_sets: Vec<QuerySetEntry>,
    pub checksums: ManifestChecksums,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestGenerator {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetIdentity {
    pub dataset_id: String,
    pub dataset_version: String,
    pub dataset_family: String,
    pub dataset_tier: String,
    pub variant_id: String,
    pub embedding_spec_id: String,
    pub embedding_model_version: String,
    pub embedding_model_hash: String,
    pub corpus_checksum: String,
    pub query_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentConstraints {
    pub min_ram_gb: u32,
    pub recommended_ram_gb: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CorpusProfile {
    pub doc_count: u64,
    pub vector_count: u64,
    pub total_text_bytes: u64,
    pub avg_doc_length: f64,
    pub median_doc_length: u64,
    pub p95_doc_length: u64,
    pub max_doc_length: u64,
    pub languages: Vec<LanguageShare>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LanguageShare {
    pub code: String,
    pub ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TextProfile {
    pub length_buckets: LengthBuckets,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokenization_notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LengthBuckets {
    pub short_ratio: f64,
    pub medium_ratio: f64,
    pub long_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataProfile {
    pub facets: Vec<FacetProfile>,
    pub selectivity_exemplars: SelectivityExemplars,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FacetProfile {
    pub name: String,
    pub kind: String,
    pub cardinality: u64,
    pub null_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectivityExemplars {
    pub broad: String,
    pub medium: String,
    pub narrow: String,
    pub zero_hit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorProfile {
    pub enabled: bool,
    pub embedding_dimensions: u32,
    pub embedding_dtype: String,
    pub distance_metric: String,
    pub query_vectors: QueryVectorProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryVectorProfile {
    pub precomputed_available: bool,
    pub runtime_embedding_supported: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DirtyProfile {
    pub profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_dataset_id: Option<String>,
    pub seed: u64,
    pub delete_ratio: f64,
    pub update_ratio: f64,
    pub append_ratio: f64,
    pub target_segment_count_range: [u32; 2],
    pub target_segment_topology: Vec<SegmentTopologyEntry>,
    pub target_tombstone_ratio: f64,
    pub compaction_state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentTopologyEntry {
    pub tier: String,
    pub count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestFile {
    pub path: String,
    pub kind: String,
    pub format: String,
    pub record_count: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySetEntry {
    pub query_set_id: String,
    pub path: String,
    pub ground_truth_path: String,
    pub query_count: u64,
    pub classes: Vec<String>,
    pub difficulty_distribution: DifficultyDistribution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DifficultyDistribution {
    pub easy: u64,
    pub medium: u64,
    pub hard: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestChecksums {
    pub manifest_payload_checksum: String,
    pub logical_documents_checksum: String,
    pub logical_metadata_checksum: String,
    pub logical_query_definitions_checksum: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_vector_payload_checksum: Option<String>,
    pub fairness_fingerprint: String,
}
