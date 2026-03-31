use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};

use bytemuck::try_cast_slice;
use hnsw_rs::prelude::{DistCosine, Hnsw, HnswIo};
use memmap2::{Mmap, MmapOptions};
use self_cell::self_cell;
use serde::Deserialize;
use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use wax_bench_model::{
    build_vector_lane_skeleton, parse_vector_lane_skeleton_header, vector_lane_doc_id_offsets,
    DatasetPackManifest, EnginePhase, EngineStats, MountRequest, OpenRequest, OpenResult,
    RankedDocumentHit, RankedQueryResult, SearchRequest, SearchResult, VectorLaneSkeletonHeader,
    VectorQueryMode, WaxEngine,
};

type BorrowedHnsw<'a> = Hnsw<'a, f32, DistCosine>;

struct HnswIoOwner(UnsafeCell<HnswIo>);

impl HnswIoOwner {
    fn new(mount_root: &Path, basename: &str) -> Self {
        Self(UnsafeCell::new(HnswIo::new(mount_root, basename)))
    }

    fn load<'a>(&'a self) -> Result<BorrowedHnsw<'a>, String> {
        // SAFETY: self_cell constructs the dependent exactly once while the owner is pinned
        // in place. No other references to the wrapped HnswIo exist during this call.
        unsafe {
            (&mut *self.0.get())
                .load_hnsw::<f32, DistCosine>()
                .map_err(|error| error.to_string())
        }
    }
}

self_cell!(
    struct HnswIndexCell {
        owner: HnswIoOwner,

        #[not_covariant]
        dependent: BorrowedHnsw,
    }
);

pub struct PackedTextEngine {
    mounted_path: Option<PathBuf>,
    phase: EnginePhase,
    manifest: Option<DatasetPackManifest>,
    text_lane: Option<TextLane>,
    vector_lane: Option<VectorLane>,
    preview_store: Option<HashMap<String, Value>>,
    vector_mode: VectorQueryMode,
}

impl PackedTextEngine {
    pub fn with_vector_mode(vector_mode: VectorQueryMode) -> Self {
        Self {
            mounted_path: None,
            phase: EnginePhase::New,
            manifest: None,
            text_lane: None,
            vector_lane: None,
            preview_store: None,
            vector_mode,
        }
    }

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

    fn ensure_preview_store(&mut self) -> Result<&mut HashMap<String, Value>, String> {
        if self.preview_store.is_none() {
            self.preview_store = Some(HashMap::new());
        }
        self.preview_store
            .as_mut()
            .ok_or_else(|| "preview store not materialized".to_owned())
    }
}

impl Default for PackedTextEngine {
    fn default() -> Self {
        Self::with_vector_mode(VectorQueryMode::Auto)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TextQueryHit {
    pub rank: usize,
    pub doc_id: String,
    pub text: String,
    pub document: Value,
}

pub fn query_text_preview(
    dataset_path: &Path,
    query_text: &str,
    top_k: usize,
) -> Result<Vec<TextQueryHit>, String> {
    let manifest_text = fs::read_to_string(dataset_path.join("manifest.json"))
        .map_err(|error| error.to_string())?;
    let manifest: DatasetPackManifest =
        serde_json::from_str(&manifest_text).map_err(|error| error.to_string())?;
    let text_lane = TextLane::load(dataset_path, &manifest)?;
    let doc_ids = text_lane.search_with_limit(query_text, top_k);
    let documents = load_documents_by_id(&dataset_path.join("docs.ndjson"), &doc_ids)?;
    doc_ids
        .into_iter()
        .enumerate()
        .map(|(index, doc_id)| {
            let document = documents
                .get(&doc_id)
                .cloned()
                .ok_or_else(|| format!("document missing for hit doc_id: {doc_id}"))?;
            let text = document
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("text missing for hit doc_id: {doc_id}"))?
                .to_owned();
            Ok(TextQueryHit {
                rank: index + 1,
                doc_id,
                text,
                document,
            })
        })
        .collect()
}

pub fn query_batch_ranked_results(
    dataset_path: &Path,
    query_set_path: &Path,
    vector_mode: VectorQueryMode,
) -> Result<Vec<RankedQueryResult>, String> {
    let manifest_text = fs::read_to_string(dataset_path.join("manifest.json"))
        .map_err(|error| error.to_string())?;
    let manifest: DatasetPackManifest =
        serde_json::from_str(&manifest_text).map_err(|error| error.to_string())?;
    let text_lane = TextLane::load(dataset_path, &manifest)?;
    let vector_lane = VectorLane::load(dataset_path, &manifest)?;
    let query_vectors = load_query_vector_records(query_set_path, vector_lane.dimensions)?;
    let queries = load_query_records(query_set_path)?;

    if queries.len() != query_vectors.len() {
        return Err("query_set and query vector records must align".to_owned());
    }

    queries
        .into_iter()
        .zip(query_vectors)
        .map(|(query, vector_record)| {
            let limit = query.top_k as usize;
            let hits = if query.lane_eligibility.hybrid {
                search_query_hybrid(
                    &text_lane,
                    &vector_lane,
                    &query.query_text,
                    &vector_record.vector,
                    limit,
                    vector_mode,
                )
            } else if query.lane_eligibility.vector && !query.lane_eligibility.text {
                vector_lane.search_with_query(&vector_record.vector, limit, vector_mode)
            } else {
                text_lane.search_with_limit(&query.query_text, limit)
            };

            Ok(RankedQueryResult {
                query_id: vector_record.query_id,
                hits: hits
                    .into_iter()
                    .map(|doc_id| RankedDocumentHit { doc_id })
                    .collect(),
            })
        })
        .collect()
}

impl WaxEngine for PackedTextEngine {
    type Error = String;

    fn mount(&mut self, request: MountRequest) -> Result<(), Self::Error> {
        self.mounted_path = Some(request.store_path);
        self.phase = EnginePhase::Mounted;
        self.manifest = None;
        self.text_lane = None;
        self.vector_lane = None;
        self.preview_store = None;
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
        if request.query_text == "__ttfq_vector__" {
            let vector_mode = self.vector_mode;
            let lane = self.ensure_vector_lane()?;
            return Ok(SearchResult {
                hits: lane.search_first_vector_query(vector_mode),
            });
        }
        if matches!(
            request.query_text.as_str(),
            "__warmup_vector__" | "__warm_vector__"
        ) {
            let vector_mode = self.vector_mode;
            let lane = self.ensure_vector_lane()?;
            return Ok(SearchResult {
                hits: lane.search_first_vector_query(vector_mode),
            });
        }
        if request.query_text == "__ttfq_hybrid__" {
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
                hits: search_first_hybrid_query(text_lane, vector_lane, self.vector_mode),
            });
        }
        if matches!(
            request.query_text.as_str(),
            "__warmup_hybrid__" | "__warm_hybrid__"
        ) {
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
                hits: search_first_hybrid_query(text_lane, vector_lane, self.vector_mode),
            });
        }
        if matches!(
            request.query_text.as_str(),
            "__warmup_hybrid_with_previews__" | "__warm_hybrid_with_previews__"
        ) {
            self.ensure_text_lane()?;
            self.ensure_vector_lane()?;
            let docs_path = self.mount_root()?.join("docs.ndjson");
            let hits = {
                let text_lane = self
                    .text_lane
                    .as_ref()
                    .ok_or_else(|| "text lane not materialized".to_owned())?;
                let vector_lane = self
                    .vector_lane
                    .as_ref()
                    .ok_or_else(|| "vector lane not materialized".to_owned())?;
                search_first_hybrid_query(text_lane, vector_lane, self.vector_mode)
            };
            materialize_document_previews(self.ensure_preview_store()?, &docs_path, &hits)?;
            return Ok(SearchResult { hits });
        }

        let lane = self.ensure_text_lane()?;
        let hits = if matches!(
            request.query_text.as_str(),
            "__ttfq_text__" | "__warm_text__"
        ) {
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

struct VectorLane {
    first_vector_query: Vec<f32>,
    first_vector_top_k: usize,
    first_hybrid_query: Option<Vec<f32>>,
    first_hybrid_top_k: usize,
    doc_ids: ByteStorage,
    skeleton_header: VectorLaneSkeletonHeader,
    doc_id_offsets: Vec<u64>,
    doc_vectors: Mmap,
    hnsw_index: Option<HnswIndexCell>,
    preview_vectors: Option<Mmap>,
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
        let (first_text_query, first_text_top_k, first_hybrid_query) = if query_paths.is_empty() {
            (String::new(), 0, None)
        } else {
            let (first_text_query, first_text_top_k) = load_first_text_query(&query_paths)?;
            let first_hybrid_query = load_first_hybrid_text_query(&query_paths)?;
            (first_text_query, first_text_top_k, first_hybrid_query)
        };
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
        let preview_vectors_path = manifest
            .files
            .iter()
            .find(|file| file.kind == "document_vectors_preview_q8")
            .map(|file| mount_root.join(&file.path));
        let hnsw_graph_basename = manifest
            .files
            .iter()
            .find(|file| file.kind == "vector_hnsw_graph")
            .and_then(|file| file.path.strip_suffix(".hnsw.graph"))
            .map(str::to_owned);
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
        let hnsw_index = hnsw_graph_basename
            .as_deref()
            .map(|basename| load_hnsw_index(mount_root, basename))
            .transpose()?;
        let preview_vectors = preview_vectors_path
            .map(|path| -> Result<Mmap, String> {
                let mapped = map_read_only(&path)?;
                validate_preview_vectors(mapped.as_ref(), dimensions, doc_count)?;
                Ok(mapped)
            })
            .transpose()?;
        let first_vector_query = load_first_vector_query(&query_vectors_path)?;
        let first_hybrid_query = load_first_hybrid_vector_query(&query_vectors_path)?;

        Ok(Self {
            first_vector_query: first_vector_query.vector,
            first_vector_top_k: first_vector_query.top_k,
            first_hybrid_query: first_hybrid_query
                .as_ref()
                .map(|query| query.vector.clone()),
            first_hybrid_top_k: first_hybrid_query.map(|query| query.top_k).unwrap_or(0),
            doc_ids,
            skeleton_header,
            doc_id_offsets,
            doc_vectors,
            hnsw_index,
            preview_vectors,
            dimensions,
        })
    }

    fn search_first_vector_query(&self, mode: VectorQueryMode) -> Vec<String> {
        if self.first_vector_top_k == 0 {
            return Vec::new();
        }

        self.search_with_query(&self.first_vector_query, self.first_vector_top_k, mode)
    }

    fn search_with_query(&self, query: &[f32], limit: usize, mode: VectorQueryMode) -> Vec<String> {
        if limit == 0 || self.dimensions == 0 {
            return Vec::new();
        }

        match mode {
            VectorQueryMode::ExactFlat => self.search_exact(query, limit),
            VectorQueryMode::Hnsw => self.search_with_hnsw_or_exact(query, limit),
            VectorQueryMode::PreviewQ8 => self.search_with_preview_or_exact(query, limit),
            VectorQueryMode::Auto => self.search_with_auto_mode(query, limit),
        }
    }

    fn search_with_auto_mode(&self, query: &[f32], limit: usize) -> Vec<String> {
        if self.hnsw_index.is_some() {
            return self.search_with_hnsw(query, limit);
        }
        if self.preview_vectors.is_some() {
            return self.search_with_quantized_preview(query, limit);
        }

        self.search_exact(query, limit)
    }

    fn search_with_preview_or_exact(&self, query: &[f32], limit: usize) -> Vec<String> {
        if self.preview_vectors.is_some() {
            return self.search_with_quantized_preview(query, limit);
        }

        self.search_exact(query, limit)
    }

    fn search_with_hnsw_or_exact(&self, query: &[f32], limit: usize) -> Vec<String> {
        if self.hnsw_index.is_some() {
            return self.search_with_hnsw(query, limit);
        }

        self.search_exact(query, limit)
    }

    fn search_exact(&self, query: &[f32], limit: usize) -> Vec<String> {
        let mut hits = Vec::with_capacity(limit.min(self.skeleton_header.doc_count as usize));
        for (index, vector) in self
            .vector_values()
            .chunks_exact(self.dimensions)
            .enumerate()
        {
            let score = dot_product(query, vector);
            self.collect_top_hit(&mut hits, limit, index, score);
        }

        hits.sort_by(|left, right| self.compare_hits(*left, *right));
        hits.into_iter()
            .map(|(index, _)| self.doc_id(index).to_owned())
            .collect()
    }

    fn search_with_quantized_preview(&self, query: &[f32], limit: usize) -> Vec<String> {
        let preview_vectors = self
            .preview_vectors
            .as_ref()
            .expect("preview path checked by caller");
        let preview_limit = self.preview_limit(limit);
        let mut candidates = Vec::with_capacity(preview_limit);
        for (index, vector) in preview_vectors
            .as_ref()
            .chunks_exact(self.dimensions)
            .enumerate()
        {
            let score = dot_product_i8_preview(query, vector);
            self.collect_top_hit(&mut candidates, preview_limit, index, score);
        }

        let mut reranked = Vec::with_capacity(candidates.len());
        for (index, _) in candidates {
            let start = index * self.dimensions;
            let end = start + self.dimensions;
            let exact_score = dot_product(query, &self.vector_values()[start..end]);
            reranked.push((index, exact_score));
        }

        reranked.sort_by(|left, right| self.compare_hits(*left, *right));
        reranked
            .into_iter()
            .take(limit)
            .map(|(index, _)| self.doc_id(index).to_owned())
            .collect()
    }

    fn search_with_hnsw(&self, query: &[f32], limit: usize) -> Vec<String> {
        let candidate_limit = self.hnsw_candidate_limit(limit);
        let ef_search = candidate_limit.max(limit).max(32);
        let neighbours = self
            .hnsw_index
            .as_ref()
            .expect("checked by caller")
            .with_dependent(|_, hnsw_index| hnsw_index.search(query, candidate_limit, ef_search));
        let mut reranked = Vec::with_capacity(neighbours.len());
        for neighbour in neighbours {
            let index = neighbour.d_id;
            let start = index * self.dimensions;
            let end = start + self.dimensions;
            let exact_score = dot_product(query, &self.vector_values()[start..end]);
            reranked.push((index, exact_score));
        }

        reranked.sort_by(|left, right| self.compare_hits(*left, *right));
        reranked
            .into_iter()
            .take(limit)
            .map(|(index, _)| self.doc_id(index).to_owned())
            .collect()
    }

    fn collect_top_hit(
        &self,
        hits: &mut Vec<(usize, f32)>,
        limit: usize,
        index: usize,
        score: f32,
    ) {
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

    fn vector_values(&self) -> &[f32] {
        try_cast_slice::<u8, f32>(self.doc_vectors.as_ref()).expect("validated vector mmap")
    }

    fn preview_limit(&self, limit: usize) -> usize {
        let doc_count = self.skeleton_header.doc_count as usize;
        limit.saturating_mul(16).max(64).min(doc_count)
    }

    fn hnsw_candidate_limit(&self, limit: usize) -> usize {
        let doc_count = self.skeleton_header.doc_count as usize;
        limit.saturating_mul(8).max(64).min(doc_count)
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

fn load_query_records(path: &Path) -> Result<Vec<QueryRecord>, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(|error| error.to_string()))
        .collect()
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
    query_id: String,
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
    query_id: String,
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

fn validate_document_vectors(
    bytes: &[u8],
    dimensions: usize,
    doc_count: usize,
) -> Result<(), String> {
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

fn validate_preview_vectors(
    bytes: &[u8],
    dimensions: usize,
    doc_count: usize,
) -> Result<(), String> {
    if dimensions == 0 {
        return Ok(());
    }
    if bytes.len() != doc_count * dimensions {
        return Err("preview vector payload row count does not match manifest".to_owned());
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

fn load_query_vector_records(
    query_set_path: &Path,
    dimensions: usize,
) -> Result<Vec<QueryVectorRecord>, String> {
    let text = fs::read_to_string(query_set_path).map_err(|error| error.to_string())?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let query: QueryRecord =
                serde_json::from_str(line).map_err(|error| error.to_string())?;
            Ok(QueryVectorRecord {
                query_id: query.query_id,
                top_k: query.top_k,
                vector: embed_text(&query.query_text, dimensions as u32),
                lane_eligibility: query.lane_eligibility,
            })
        })
        .collect()
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

fn search_first_hybrid_query(
    text_lane: &TextLane,
    vector_lane: &VectorLane,
    vector_mode: VectorQueryMode,
) -> Vec<String> {
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
    let vector_hits = vector_lane.search_with_query(hybrid_vector_query, limit, vector_mode);
    fuse_ranked_hits(&text_hits, &vector_hits, limit)
}

fn search_query_hybrid(
    text_lane: &TextLane,
    vector_lane: &VectorLane,
    query_text: &str,
    query_vector: &[f32],
    limit: usize,
    vector_mode: VectorQueryMode,
) -> Vec<String> {
    let text_hits = text_lane.search_with_limit(query_text, limit);
    let vector_hits = vector_lane.search_with_query(query_vector, limit, vector_mode);
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
    fused
        .into_iter()
        .take(limit)
        .map(|(doc_id, _)| doc_id)
        .collect()
}

fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(l, r)| l * r).sum()
}

fn dot_product_i8_preview(left: &[f32], right: &[u8]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(l, r)| l * (*r as i8 as f32))
        .sum()
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
    Ok(ByteStorage::Owned(build_vector_lane_skeleton(
        &doc_ids, dimensions,
    )))
}

fn load_hnsw_index(mount_root: &Path, basename: &str) -> Result<HnswIndexCell, String> {
    HnswIndexCell::try_new(HnswIoOwner::new(mount_root, basename), |owner| owner.load())
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

fn materialize_document_previews(
    documents: &mut HashMap<String, Value>,
    path: &Path,
    doc_ids: &[String],
) -> Result<(), String> {
    let missing = doc_ids
        .iter()
        .filter(|doc_id| !documents.contains_key(doc_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        let loaded = load_documents_by_id(path, &missing)?;
        for (doc_id, document) in loaded {
            documents.insert(doc_id, document);
        }
    }
    for doc_id in doc_ids {
        let document = documents
            .get(doc_id)
            .ok_or_else(|| format!("document missing for hit doc_id: {doc_id}"))?;
        document
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("text missing for hit doc_id: {doc_id}"))?;
    }
    Ok(())
}

fn load_documents_by_id(
    path: &Path,
    target_doc_ids: &[String],
) -> Result<HashMap<String, Value>, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let mut remaining = target_doc_ids
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let mut documents = HashMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).map_err(|error| error.to_string())?;
        let object = value
            .as_object()
            .ok_or_else(|| "document line must be a json object".to_owned())?;
        let doc_id = object
            .get("doc_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "document line missing doc_id".to_owned())?;
        if remaining.remove(doc_id) {
            documents.insert(doc_id.to_owned(), Value::Object(clone_object(object)));
            if remaining.is_empty() {
                break;
            }
        }
    }
    Ok(documents)
}

fn clone_object(object: &Map<String, Value>) -> Map<String, Value> {
    object
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn embed_text(text: &str, dimensions: u32) -> Vec<f32> {
    let dimensions = dimensions as usize;
    if dimensions == 0 {
        return Vec::new();
    }

    let mut vector = vec![0.0f32; dimensions];
    for token in tokenize(text) {
        let mut digest = Sha256::new();
        digest.update(token.as_bytes());
        let bytes = digest.finalize();
        let bucket =
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize % dimensions;
        vector[bucket] += 1.0;
    }

    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut vector {
            *value /= norm;
        }
    }

    vector
}

#[derive(Debug, Deserialize)]
struct DocumentRecord {
    doc_id: String,
}
