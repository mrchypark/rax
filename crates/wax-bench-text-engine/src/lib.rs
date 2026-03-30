use std::collections::HashMap;
use std::fs::File;
use std::fs;
use std::path::{Path, PathBuf};

use bytemuck::try_cast_slice;
use memmap2::{Mmap, MmapOptions};
use serde::Deserialize;
use wax_bench_model::{
    build_vector_lane_skeleton, parse_vector_lane_skeleton_header, vector_lane_doc_id_offsets,
    DatasetPackManifest, EnginePhase, EngineStats, MountRequest, OpenRequest, OpenResult,
    SearchRequest, SearchResult, VectorLaneSkeletonHeader, WaxEngine,
};

#[derive(Debug, Default)]
pub struct PackedTextEngine {
    mounted_path: Option<PathBuf>,
    phase: EnginePhase,
    manifest: Option<DatasetPackManifest>,
    text_lane: Option<TextLane>,
    vector_lane: Option<VectorLane>,
}

impl PackedTextEngine {
    pub fn is_text_lane_materialized(&self) -> bool {
        self.text_lane.is_some()
    }

    pub fn is_vector_lane_materialized(&self) -> bool {
        self.vector_lane.is_some()
    }

    fn manifest(&self) -> Result<&DatasetPackManifest, String> {
        self.manifest
            .as_ref()
            .ok_or_else(|| "manifest not loaded".to_owned())
    }

    fn mount_root(&self) -> Result<&Path, String> {
        self.mounted_path
            .as_deref()
            .ok_or_else(|| "dataset path not mounted".to_owned())
    }

    fn ensure_text_lane(&mut self) -> Result<&TextLane, String> {
        if self.text_lane.is_none() {
            let mount_root = self.mount_root()?.to_path_buf();
            let manifest = self.manifest()?.clone();
            self.text_lane = Some(TextLane::load(&mount_root, &manifest)?);
        }
        self.text_lane
            .as_ref()
            .ok_or_else(|| "text lane not materialized".to_owned())
    }

    fn ensure_vector_lane(&mut self) -> Result<&VectorLane, String> {
        if self.vector_lane.is_none() {
            let mount_root = self.mount_root()?.to_path_buf();
            let manifest = self.manifest()?.clone();
            self.vector_lane = Some(VectorLane::load(&mount_root, &manifest)?);
        }
        self.vector_lane
            .as_ref()
            .ok_or_else(|| "vector lane not materialized".to_owned())
    }
}

impl WaxEngine for PackedTextEngine {
    type Error = String;

    fn mount(&mut self, request: MountRequest) -> Result<(), Self::Error> {
        self.mounted_path = Some(request.store_path);
        self.phase = EnginePhase::Mounted;
        self.manifest = None;
        self.text_lane = None;
        self.vector_lane = None;
        Ok(())
    }

    fn open(&mut self, _request: OpenRequest) -> Result<OpenResult, Self::Error> {
        if self.phase != EnginePhase::Mounted {
            return Err("engine must be mounted before open".to_owned());
        }

        let manifest_text = fs::read_to_string(self.mount_root()?.join("manifest.json"))
            .map_err(|error| error.to_string())?;
        let manifest: DatasetPackManifest =
            serde_json::from_str(&manifest_text).map_err(|error| error.to_string())?;

        self.manifest = Some(manifest);
        self.phase = EnginePhase::Open;
        Ok(OpenResult)
    }

    fn search(&mut self, request: SearchRequest) -> Result<SearchResult, Self::Error> {
        if self.phase != EnginePhase::Open {
            return Err("engine must be open before search".to_owned());
        }

        if request.query_text == "__materialize_text_lane__" {
            self.ensure_text_lane()?;
            return Ok(SearchResult { hits: Vec::new() });
        }
        if request.query_text == "__materialize_vector_lane__" {
            self.ensure_vector_lane()?;
            return Ok(SearchResult { hits: Vec::new() });
        }
        if matches!(request.query_text.as_str(), "__ttfq_vector__" | "__warm_vector__") {
            let lane = self.ensure_vector_lane()?;
            return Ok(SearchResult {
                hits: lane.search_first_vector_query(),
            });
        }
        if matches!(request.query_text.as_str(), "__ttfq_hybrid__" | "__warm_hybrid__") {
            self.ensure_text_lane()?;
            self.ensure_vector_lane()?;
            let text_lane = self
                .text_lane
                .as_ref()
                .ok_or_else(|| "text lane not materialized".to_owned())?;
            let vector_lane = self
                .vector_lane
                .as_ref()
                .ok_or_else(|| "vector lane not materialized".to_owned())?;
            return Ok(SearchResult {
                hits: search_first_hybrid_query(text_lane, vector_lane),
            });
        }

        let lane = self.ensure_text_lane()?;
        let hits = if matches!(request.query_text.as_str(), "__ttfq_text__" | "__warm_text__") {
            lane.search_first_text_query()
        } else {
            lane.search(&request.query_text)
        };
        Ok(SearchResult { hits })
    }

    fn get_stats(&self) -> EngineStats {
        EngineStats {
            phase: self.phase,
            last_mounted_path: self.mounted_path.clone(),
        }
    }
}

#[derive(Debug)]
struct TextLane {
    first_text_query: String,
    first_text_top_k: usize,
    first_hybrid_query: Option<String>,
    first_hybrid_top_k: usize,
    inverted: HashMap<String, Vec<String>>,
}

#[derive(Debug)]
struct VectorLane {
    first_vector_query: Vec<f32>,
    first_vector_top_k: usize,
    first_hybrid_query: Option<Vec<f32>>,
    first_hybrid_top_k: usize,
    doc_ids: ByteStorage,
    skeleton_header: VectorLaneSkeletonHeader,
    doc_id_offsets: Vec<u64>,
    doc_vectors: Mmap,
    dimensions: usize,
}

#[derive(Debug)]
enum ByteStorage {
    Mapped(Mmap),
    Owned(Vec<u8>),
}

impl ByteStorage {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mapped(bytes) => bytes.as_ref(),
            Self::Owned(bytes) => bytes.as_slice(),
        }
    }
}

impl TextLane {
    fn load(mount_root: &Path, manifest: &DatasetPackManifest) -> Result<Self, String> {
        let postings_path = manifest
            .files
            .iter()
            .find(|file| file.kind == "text_postings")
            .map(|file| mount_root.join(&file.path))
            .ok_or_else(|| "text_postings file missing from manifest".to_owned())?;
        let query_paths = manifest
            .query_sets
            .iter()
            .map(|query_set| mount_root.join(&query_set.path))
            .collect::<Vec<_>>();
        if query_paths.is_empty() {
            return Err("query_set missing from manifest".to_owned());
        }

        let (first_text_query, first_text_top_k) = load_first_text_query(&query_paths)?;
        let first_hybrid_query = load_first_hybrid_text_query(&query_paths)?;
        let inverted = load_text_postings(&postings_path)?;

        Ok(Self {
            first_text_query,
            first_text_top_k,
            first_hybrid_query: first_hybrid_query
                .as_ref()
                .map(|query| query.query_text.clone()),
            first_hybrid_top_k: first_hybrid_query.map(|query| query.top_k).unwrap_or(0),
            inverted,
        })
    }

    fn search_first_text_query(&self) -> Vec<String> {
        self.search_with_limit(&self.first_text_query, self.first_text_top_k)
    }

    fn search(&self, query: &str) -> Vec<String> {
        self.search_with_limit(query, usize::MAX)
    }

    fn search_with_limit(&self, query: &str, limit: usize) -> Vec<String> {
        let mut scores: HashMap<String, u32> = HashMap::new();
        for token in tokenize(query) {
            if let Some(doc_ids) = self.inverted.get(&token) {
                for doc_id in doc_ids {
                    *scores.entry(doc_id.clone()).or_insert(0) += 1;
                }
            }
        }

        let mut hits: Vec<(String, u32)> = scores.into_iter().collect();
        hits.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        hits.into_iter()
            .take(limit)
            .map(|(doc_id, _)| doc_id)
            .collect()
    }
}

impl VectorLane {
    fn load(mount_root: &Path, manifest: &DatasetPackManifest) -> Result<Self, String> {
        let document_ids_path = manifest
            .files
            .iter()
            .find(|file| file.kind == "document_ids")
            .map(|file| mount_root.join(&file.path));

        let document_vectors_path = manifest
            .files
            .iter()
            .find(|file| file.kind == "document_vectors")
            .map(|file| mount_root.join(&file.path))
            .ok_or_else(|| "document_vectors file missing from manifest".to_owned())?;
        let query_vectors_path = manifest
            .files
            .iter()
            .filter(|file| file.kind == "query_vectors")
            .map(|file| mount_root.join(&file.path))
            .collect::<Vec<_>>();
        if query_vectors_path.is_empty() {
            return Err("query_vectors file missing from manifest".to_owned());
        }

        let dimensions = manifest.vector_profile.embedding_dimensions as usize;
        let doc_count = manifest.corpus.vector_count as usize;
        let doc_ids =
            load_vector_lane_skeleton(mount_root, manifest, dimensions as u32, document_ids_path)?;
        let skeleton_header = parse_vector_lane_skeleton_header(doc_ids.as_slice())?;
        if skeleton_header.dimensions as usize != dimensions {
            return Err("vector lane skeleton dimensions do not match manifest".to_owned());
        }
        if skeleton_header.doc_count as usize != doc_count {
            return Err("vector lane skeleton doc_count does not match manifest".to_owned());
        }
        let doc_id_offsets = vector_lane_doc_id_offsets(doc_ids.as_slice(), &skeleton_header)?;

        let doc_vectors = map_read_only(&document_vectors_path)?;
        validate_document_vectors(doc_vectors.as_ref(), dimensions, doc_count)?;
        let first_vector_query = load_first_vector_query(&query_vectors_path)?;
        let first_hybrid_query = load_first_hybrid_vector_query(&query_vectors_path)?;

        Ok(Self {
            first_vector_query: first_vector_query.vector,
            first_vector_top_k: first_vector_query.top_k,
            first_hybrid_query: first_hybrid_query.as_ref().map(|query| query.vector.clone()),
            first_hybrid_top_k: first_hybrid_query.map(|query| query.top_k).unwrap_or(0),
            doc_ids,
            skeleton_header,
            doc_id_offsets,
            doc_vectors,
            dimensions,
        })
    }

    fn search_first_vector_query(&self) -> Vec<String> {
        if self.first_vector_top_k == 0 {
            return Vec::new();
        }

        self.search_with_query(&self.first_vector_query, self.first_vector_top_k)
    }

    fn search_with_query(&self, query: &[f32], limit: usize) -> Vec<String> {
        if limit == 0 || self.dimensions == 0 {
            return Vec::new();
        }

        let mut hits = Vec::with_capacity(limit.min(self.skeleton_header.doc_count as usize));
        let vector_values =
            try_cast_slice::<u8, f32>(self.doc_vectors.as_ref()).expect("validated vector mmap");
        for (index, vector) in vector_values.chunks_exact(self.dimensions).enumerate() {
            let score = dot_product(query, vector);
            self.collect_top_hit(&mut hits, limit, index, score);
        }

        hits.sort_by(|left, right| self.compare_hits(*left, *right));
        hits.into_iter()
            .map(|(index, _)| self.doc_id(index).to_owned())
            .collect()
    }

    fn collect_top_hit(&self, hits: &mut Vec<(usize, f32)>, limit: usize, index: usize, score: f32) {
        let candidate = (index, score);
        if hits.len() < limit {
            hits.push(candidate);
            return;
        }

        let Some((worst_index, worst_hit)) = hits
            .iter()
            .copied()
            .enumerate()
            .max_by(|(_, left), (_, right)| self.compare_hits(*left, *right))
        else {
            return;
        };

        if self.compare_hits(candidate, worst_hit).is_lt() {
            hits[worst_index] = candidate;
        }
    }

    fn compare_hits(&self, left: (usize, f32), right: (usize, f32)) -> std::cmp::Ordering {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| self.doc_id_bytes(left.0).cmp(self.doc_id_bytes(right.0)))
    }

    fn doc_id(&self, index: usize) -> &str {
        std::str::from_utf8(self.doc_id_bytes(index)).expect("validated vector lane skeleton")
    }

    fn doc_id_bytes(&self, index: usize) -> &[u8] {
        let start = self.doc_id_offsets[index] as usize;
        let end = self.doc_id_offsets[index + 1] as usize;
        let blob_base = self.skeleton_header.doc_id_blob_offset as usize;
        let blob = &self.doc_ids.as_slice()
            [blob_base..blob_base + self.skeleton_header.doc_id_blob_length as usize];
        &blob[start..end]
    }
}

fn load_first_text_query(paths: &[PathBuf]) -> Result<(String, usize), String> {
    for path in paths {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let query: QueryRecord =
                serde_json::from_str(line).map_err(|error| error.to_string())?;
            if query.lane_eligibility.text {
                return Ok((query.query_text, query.top_k as usize));
            }
        }
    }

    Err("no text-eligible query found".to_owned())
}

fn load_first_hybrid_text_query(paths: &[PathBuf]) -> Result<Option<FirstTextQuery>, String> {
    for path in paths {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let query: QueryRecord =
                serde_json::from_str(line).map_err(|error| error.to_string())?;
            if query.lane_eligibility.hybrid {
                return Ok(Some(FirstTextQuery {
                    query_text: query.query_text,
                    top_k: query.top_k as usize,
                }));
            }
        }
    }

    Ok(None)
}

fn load_text_postings(path: &Path) -> Result<HashMap<String, Vec<String>>, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let mut postings = HashMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let posting: TextPostingRecord =
            serde_json::from_str(line).map_err(|error| error.to_string())?;
        postings.insert(posting.token, posting.doc_ids);
    }
    Ok(postings)
}

fn load_document_ids(path: &Path) -> Result<Vec<String>, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<DocumentIdRecord>(line)
                .map(|record| record.doc_id)
                .map_err(|error| error.to_string())
        })
        .collect()
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

#[derive(Debug, Deserialize)]
struct QueryRecord {
    query_text: String,
    top_k: u32,
    lane_eligibility: LaneEligibility,
}

#[derive(Debug, Clone, Deserialize)]
struct LaneEligibility {
    text: bool,
    vector: bool,
    hybrid: bool,
}

#[derive(Debug, Deserialize)]
struct QueryVectorRecord {
    top_k: u32,
    vector: Vec<f32>,
    lane_eligibility: LaneEligibility,
}

#[derive(Debug, Deserialize)]
struct DocumentIdRecord {
    doc_id: String,
}

#[derive(Debug, Deserialize)]
struct TextPostingRecord {
    token: String,
    doc_ids: Vec<String>,
}

#[derive(Debug)]
struct FirstVectorQuery {
    vector: Vec<f32>,
    top_k: usize,
}

#[derive(Debug)]
struct FirstTextQuery {
    query_text: String,
    top_k: usize,
}

fn validate_document_vectors(bytes: &[u8], dimensions: usize, doc_count: usize) -> Result<(), String> {
    if dimensions == 0 {
        return Ok(());
    }
    if !bytes.len().is_multiple_of(dimensions * 4) {
        return Err("document vector payload has invalid length".to_owned());
    }
    let values = try_cast_slice::<u8, f32>(bytes)
        .map_err(|_| "document vector payload alignment is invalid".to_owned())?;
    if values.len() != doc_count * dimensions {
        return Err("document vector payload row count does not match manifest".to_owned());
    }
    Ok(())
}

fn load_first_vector_query(paths: &[PathBuf]) -> Result<FirstVectorQuery, String> {
    for path in paths {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let query: QueryVectorRecord =
                serde_json::from_str(line).map_err(|error| error.to_string())?;
            if query.lane_eligibility.vector {
                return Ok(FirstVectorQuery {
                    vector: query.vector,
                    top_k: query.top_k as usize,
                });
            }
        }
    }

    Err("no vector-eligible query found".to_owned())
}

fn load_first_hybrid_vector_query(paths: &[PathBuf]) -> Result<Option<FirstVectorQuery>, String> {
    for path in paths {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let query: QueryVectorRecord =
                serde_json::from_str(line).map_err(|error| error.to_string())?;
            if query.lane_eligibility.hybrid {
                return Ok(Some(FirstVectorQuery {
                    vector: query.vector,
                    top_k: query.top_k as usize,
                }));
            }
        }
    }

    Ok(None)
}

fn search_first_hybrid_query(text_lane: &TextLane, vector_lane: &VectorLane) -> Vec<String> {
    let Some(hybrid_text_query) = text_lane.first_hybrid_query.as_ref() else {
        return Vec::new();
    };
    let Some(hybrid_vector_query) = vector_lane.first_hybrid_query.as_ref() else {
        return Vec::new();
    };
    let limit = text_lane
        .first_hybrid_top_k
        .max(vector_lane.first_hybrid_top_k)
        .max(1);
    let text_hits = text_lane.search_with_limit(hybrid_text_query, limit);
    let vector_hits = vector_lane.search_with_query(hybrid_vector_query, limit);
    fuse_ranked_hits(&text_hits, &vector_hits, limit)
}

fn fuse_ranked_hits(text_hits: &[String], vector_hits: &[String], limit: usize) -> Vec<String> {
    let mut scores = HashMap::<String, f64>::new();
    for (rank, doc_id) in text_hits.iter().enumerate() {
        *scores.entry(doc_id.clone()).or_insert(0.0) += 1.0 / (rank as f64 + 1.0);
    }
    for (rank, doc_id) in vector_hits.iter().enumerate() {
        *scores.entry(doc_id.clone()).or_insert(0.0) += 1.0 / (rank as f64 + 1.0);
    }

    let mut fused = scores.into_iter().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap()
            .then_with(|| left.0.cmp(&right.0))
    });
    fused.into_iter().take(limit).map(|(doc_id, _)| doc_id).collect()
}

fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(l, r)| l * r).sum()
}

fn load_vector_lane_skeleton(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
    dimensions: u32,
    fallback_document_ids_path: Option<PathBuf>,
) -> Result<ByteStorage, String> {
    if let Some(path) = manifest
        .files
        .iter()
        .find(|file| file.kind == "vector_lane_skeleton")
        .map(|file| mount_root.join(&file.path))
    {
        return map_read_only(&path).map(ByteStorage::Mapped);
    }

    let doc_ids = if let Some(document_ids_path) = fallback_document_ids_path {
        load_document_ids(&document_ids_path)?
    } else {
        let documents_path = mount_root.join("docs.ndjson");
        load_document_ids_from_documents(&documents_path)?
    };
    Ok(ByteStorage::Owned(build_vector_lane_skeleton(&doc_ids, dimensions)))
}

fn map_read_only(path: &Path) -> Result<Mmap, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    // Read-only mappings let the first query use persisted lane state without heap rebuild.
    unsafe { MmapOptions::new().map(&file) }.map_err(|error| error.to_string())
}

fn load_document_ids_from_documents(path: &Path) -> Result<Vec<String>, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<DocumentRecord>(line)
                .map(|record| record.doc_id)
                .map_err(|error| error.to_string())
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct DocumentRecord {
    doc_id: String,
}
