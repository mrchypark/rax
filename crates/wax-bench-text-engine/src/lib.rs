use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use wax_bench_model::{
    DatasetPackManifest, EnginePhase, EngineStats, MountRequest, OpenRequest, OpenResult,
    SearchRequest, SearchResult, WaxEngine,
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
    doc_ids: Vec<String>,
    doc_vectors: Vec<f32>,
    dimensions: usize,
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
            .map(|file| mount_root.join(&file.path))
            .ok_or_else(|| "document_ids file missing from manifest".to_owned())?;
        let doc_ids = load_document_ids(&document_ids_path)?;

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
        let document_vector_bytes =
            fs::read(&document_vectors_path).map_err(|error| error.to_string())?;
        let doc_vectors = load_document_vectors(&document_vector_bytes, dimensions)?;
        let first_vector_query = load_first_vector_query(&query_vectors_path)?;
        let first_hybrid_query = load_first_hybrid_vector_query(&query_vectors_path)?;

        Ok(Self {
            first_vector_query: first_vector_query.vector,
            first_vector_top_k: first_vector_query.top_k,
            first_hybrid_query: first_hybrid_query.as_ref().map(|query| query.vector.clone()),
            first_hybrid_top_k: first_hybrid_query.map(|query| query.top_k).unwrap_or(0),
            doc_ids,
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

        let mut hits = Vec::with_capacity(limit.min(self.doc_ids.len()));
        for (index, vector) in self.doc_vectors.chunks_exact(self.dimensions).enumerate() {
            let score = dot_product(query, vector);
            collect_top_hit(&mut hits, limit, index, score, &self.doc_ids);
        }

        hits.sort_by(|left, right| compare_hits(*left, *right, &self.doc_ids));
        hits.into_iter()
            .map(|(index, _)| self.doc_ids[index].clone())
            .collect()
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

fn load_document_vectors(bytes: &[u8], dimensions: usize) -> Result<Vec<f32>, String> {
    if dimensions == 0 {
        return Ok(Vec::new());
    }
    if !bytes.len().is_multiple_of(dimensions * 4) {
        return Err("document vector payload has invalid length".to_owned());
    }

    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
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

fn collect_top_hit(
    hits: &mut Vec<(usize, f32)>,
    limit: usize,
    index: usize,
    score: f32,
    doc_ids: &[String],
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
        .max_by(|(_, left), (_, right)| compare_hits(*left, *right, doc_ids))
    else {
        return;
    };

    if compare_hits(candidate, worst_hit, doc_ids).is_lt() {
        hits[worst_index] = candidate;
    }
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

fn compare_hits(left: (usize, f32), right: (usize, f32), doc_ids: &[String]) -> std::cmp::Ordering {
    right
        .1
        .total_cmp(&left.1)
        .then_with(|| doc_ids[left.0].cmp(&doc_ids[right.0]))
}

fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(l, r)| l * r).sum()
}
