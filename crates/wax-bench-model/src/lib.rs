use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use strum::EnumIter;

pub const VECTOR_LANE_SKELETON_HEADER_SIZE: usize = 64;
const VECTOR_LANE_SKELETON_MAGIC: &[u8; 8] = b"WXVSKEL1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorLaneSkeletonHeader {
    pub dimensions: u32,
    pub doc_count: u64,
    pub doc_id_offsets_offset: u64,
    pub doc_id_blob_offset: u64,
    pub doc_id_blob_length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter, Default)]
pub enum VectorQueryMode {
    #[serde(rename = "auto")]
    #[default]
    Auto,
    #[serde(rename = "exact_flat")]
    ExactFlat,
    #[serde(rename = "hnsw")]
    Hnsw,
    #[serde(rename = "preview_q8")]
    PreviewQ8,
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

pub fn build_vector_lane_skeleton(doc_ids: &[String], dimensions: u32) -> Vec<u8> {
    let mut doc_id_blob = Vec::new();
    let mut offsets = Vec::with_capacity(doc_ids.len() + 1);
    offsets.push(0u64);
    for doc_id in doc_ids {
        doc_id_blob.extend_from_slice(doc_id.as_bytes());
        offsets.push(doc_id_blob.len() as u64);
    }

    let doc_id_offsets_offset = VECTOR_LANE_SKELETON_HEADER_SIZE as u64;
    let doc_id_blob_offset = doc_id_offsets_offset + (offsets.len() as u64 * 8);
    let doc_id_blob_length = doc_id_blob.len() as u64;

    let mut bytes = vec![0u8; VECTOR_LANE_SKELETON_HEADER_SIZE];
    bytes[..8].copy_from_slice(VECTOR_LANE_SKELETON_MAGIC);
    bytes[8..10].copy_from_slice(&1u16.to_le_bytes());
    bytes[10..12].copy_from_slice(&0u16.to_le_bytes());
    bytes[12..16].copy_from_slice(&dimensions.to_le_bytes());
    bytes[16..24].copy_from_slice(&(doc_ids.len() as u64).to_le_bytes());
    bytes[24..32].copy_from_slice(&doc_id_offsets_offset.to_le_bytes());
    bytes[32..40].copy_from_slice(&doc_id_blob_offset.to_le_bytes());
    bytes[40..48].copy_from_slice(&doc_id_blob_length.to_le_bytes());

    for offset in offsets {
        bytes.extend_from_slice(&offset.to_le_bytes());
    }
    bytes.extend_from_slice(&doc_id_blob);
    bytes
}

pub fn parse_vector_lane_skeleton_header(bytes: &[u8]) -> Result<VectorLaneSkeletonHeader, String> {
    if bytes.len() < VECTOR_LANE_SKELETON_HEADER_SIZE {
        return Err("vector lane skeleton header is truncated".to_owned());
    }
    if &bytes[..8] != VECTOR_LANE_SKELETON_MAGIC {
        return Err("vector lane skeleton magic is invalid".to_owned());
    }

    let major = u16::from_le_bytes([bytes[8], bytes[9]]);
    let minor = u16::from_le_bytes([bytes[10], bytes[11]]);
    if (major, minor) != (1, 0) {
        return Err("vector lane skeleton version is unsupported".to_owned());
    }

    let header = VectorLaneSkeletonHeader {
        dimensions: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        doc_count: read_u64_le(bytes, 16)?,
        doc_id_offsets_offset: read_u64_le(bytes, 24)?,
        doc_id_blob_offset: read_u64_le(bytes, 32)?,
        doc_id_blob_length: read_u64_le(bytes, 40)?,
    };

    let offsets_bytes = ((header.doc_count + 1) * 8) as usize;
    let offsets_end = header.doc_id_offsets_offset as usize + offsets_bytes;
    let blob_end = header.doc_id_blob_offset as usize + header.doc_id_blob_length as usize;
    if offsets_end > bytes.len() || blob_end > bytes.len() {
        return Err("vector lane skeleton body is truncated".to_owned());
    }
    if header.doc_id_offsets_offset < VECTOR_LANE_SKELETON_HEADER_SIZE as u64 {
        return Err("vector lane skeleton offsets block is invalid".to_owned());
    }
    if header.doc_id_blob_offset < header.doc_id_offsets_offset + offsets_bytes as u64 {
        return Err("vector lane skeleton doc_id blob offset is invalid".to_owned());
    }

    Ok(header)
}

pub fn vector_lane_doc_id<'a>(
    bytes: &'a [u8],
    header: &VectorLaneSkeletonHeader,
    index: usize,
) -> Result<&'a str, String> {
    if index >= header.doc_count as usize {
        return Err("vector lane doc_id index is out of bounds".to_owned());
    }

    let offsets_base = header.doc_id_offsets_offset as usize;
    let start = read_u64_le(bytes, offsets_base + index * 8)? as usize;
    let end = read_u64_le(bytes, offsets_base + (index + 1) * 8)? as usize;
    let blob_base = header.doc_id_blob_offset as usize;
    let blob = &bytes[blob_base..blob_base + header.doc_id_blob_length as usize];
    let doc_id_bytes = blob
        .get(start..end)
        .ok_or_else(|| "vector lane doc_id offsets are invalid".to_owned())?;

    std::str::from_utf8(doc_id_bytes)
        .map_err(|_| "vector lane doc_id bytes are not utf-8".to_owned())
}

pub fn vector_lane_doc_id_offsets(
    bytes: &[u8],
    header: &VectorLaneSkeletonHeader,
) -> Result<Vec<u64>, String> {
    let offsets_base = header.doc_id_offsets_offset as usize;
    let mut offsets = Vec::with_capacity(header.doc_count as usize + 1);
    for index in 0..=header.doc_count as usize {
        offsets.push(read_u64_le(bytes, offsets_base + index * 8)?);
    }

    if offsets.windows(2).any(|window| window[0] > window[1]) {
        return Err("vector lane doc_id offsets are not monotonic".to_owned());
    }
    if offsets.last().copied().unwrap_or(0) != header.doc_id_blob_length {
        return Err("vector lane doc_id offsets do not cover the blob length".to_owned());
    }

    Ok(offsets)
}

fn read_u64_le(bytes: &[u8], offset: usize) -> Result<u64, String> {
    let slice = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| "vector lane skeleton field is truncated".to_owned())?;
    Ok(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_index_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_sidecar_reproducibility: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qrels_path: Option<String>,
    pub query_count: u64,
    pub classes: Vec<String>,
    pub difficulty_distribution: DifficultyDistribution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QrelRecord {
    pub query_id: String,
    pub doc_id: String,
    pub relevance: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankedDocumentHit {
    pub doc_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankedQueryResult {
    pub query_id: String,
    pub hits: Vec<RankedDocumentHit>,
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
