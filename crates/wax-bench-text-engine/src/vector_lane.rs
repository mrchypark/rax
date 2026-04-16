use std::cell::UnsafeCell;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

use bytemuck::try_cast_slice;
use hnsw_rs::prelude::{DistCosine, Hnsw, HnswIo};
use memmap2::{Mmap, MmapOptions};
use self_cell::self_cell;
use wax_bench_model::{
    build_vector_lane_skeleton, parse_vector_lane_skeleton_header, vector_lane_doc_id_offsets,
    DatasetPackManifest, VectorLaneSkeletonHeader, VectorQueryMode,
};

use crate::documents::load_document_ids_from_documents;
use crate::query_support::{
    dot_product, dot_product_i8_preview, first_hybrid_vector_query_from_records,
    first_vector_query_from_records, load_document_ids, load_query_vector_records_from_paths,
    validate_document_vectors, validate_preview_vectors,
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SearchPhaseProfile {
    pub(crate) selected_mode: VectorQueryMode,
    pub(crate) total_search_ms: f64,
    pub(crate) exact_scan_ms: Option<f64>,
    pub(crate) approximate_search_ms: Option<f64>,
    pub(crate) rerank_ms: Option<f64>,
    pub(crate) candidate_count: usize,
    pub(crate) hits: Vec<String>,
}

pub(crate) struct VectorLane {
    mount_root: PathBuf,
    hnsw_graph_basename: Option<String>,
    first_vector_query: Vec<f32>,
    pub(crate) first_vector_top_k: usize,
    pub(crate) first_hybrid_query: Option<Vec<f32>>,
    pub(crate) first_hybrid_top_k: usize,
    doc_ids: ByteStorage,
    pub(crate) skeleton_header: VectorLaneSkeletonHeader,
    doc_id_offsets: Vec<u64>,
    doc_vectors: Mmap,
    hnsw_available: bool,
    hnsw_index: Option<HnswIndexCell>,
    preview_vectors: Option<Mmap>,
    pub(crate) dimensions: usize,
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

impl VectorLane {
    pub(crate) fn load(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        vector_mode: VectorQueryMode,
    ) -> Result<Self, String> {
        Self::load_with_report(mount_root, manifest, vector_mode).map(|(lane, _)| lane)
    }

    pub(crate) fn load_with_report(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        vector_mode: VectorQueryMode,
    ) -> Result<(Self, Option<f64>), String> {
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
        let preview_vectors = preview_vectors_path
            .map(|path| -> Result<Mmap, String> {
                let mapped = map_read_only(&path)?;
                validate_preview_vectors(mapped.as_ref(), dimensions, doc_count)?;
                Ok(mapped)
            })
            .transpose()?;
        let query_vector_records = load_query_vector_records_from_paths(&query_vectors_path)?;
        let first_vector_query = first_vector_query_from_records(&query_vector_records)?;
        let first_hybrid_query = first_hybrid_vector_query_from_records(&query_vector_records);
        let hnsw_available = hnsw_graph_basename.is_some();
        let should_load_hnsw = match vector_mode {
            VectorQueryMode::Auto => false,
            VectorQueryMode::Hnsw => true,
            VectorQueryMode::ExactFlat | VectorQueryMode::PreviewQ8 => false,
        };
        let (hnsw_index, hnsw_sidecar_load_ms) = if should_load_hnsw {
            let load_start = Instant::now();
            let index = hnsw_graph_basename
                .as_deref()
                .map(|basename| load_hnsw_index(mount_root, basename))
                .transpose()?;
            (index, Some(elapsed_ms(load_start.elapsed())))
        } else {
            (None, None)
        };

        Ok((
            Self {
                mount_root: mount_root.to_path_buf(),
                hnsw_graph_basename,
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
                hnsw_available,
                hnsw_index,
                preview_vectors,
                dimensions,
            },
            hnsw_sidecar_load_ms,
        ))
    }

    pub(crate) fn is_hnsw_sidecar_materialized(&self) -> bool {
        self.hnsw_index.is_some()
    }

    pub(crate) fn search_first_vector_query(
        &mut self,
        mode: VectorQueryMode,
        auto_force_exact: bool,
    ) -> Result<Vec<String>, String> {
        if self.first_vector_top_k == 0 {
            return Ok(Vec::new());
        }

        let query = self.first_vector_query.clone();
        self.search_with_query(&query, self.first_vector_top_k, mode, auto_force_exact)
    }

    pub(crate) fn profile_first_vector_query(&self, mode: VectorQueryMode) -> SearchPhaseProfile {
        if self.first_vector_top_k == 0 {
            return SearchPhaseProfile {
                selected_mode: self.resolve_runtime_query_mode(
                    1,
                    mode,
                    matches!(mode, VectorQueryMode::Auto),
                ),
                total_search_ms: 0.0,
                exact_scan_ms: None,
                approximate_search_ms: None,
                rerank_ms: None,
                candidate_count: 0,
                hits: Vec::new(),
            };
        }

        self.profile_search_with_query(
            &self.first_vector_query,
            self.first_vector_top_k,
            mode,
            matches!(mode, VectorQueryMode::Auto),
        )
    }

    pub(crate) fn search_with_query(
        &mut self,
        query: &[f32],
        limit: usize,
        mode: VectorQueryMode,
        auto_force_exact: bool,
    ) -> Result<Vec<String>, String> {
        if limit == 0 || self.dimensions == 0 {
            return Ok(Vec::new());
        }

        match self.resolve_runtime_query_mode(limit, mode, auto_force_exact) {
            VectorQueryMode::ExactFlat => Ok(self.search_exact(query, limit)),
            VectorQueryMode::Hnsw => self.search_with_hnsw_or_exact(query, limit),
            VectorQueryMode::PreviewQ8 => Ok(self.search_with_preview_or_exact(query, limit)),
            VectorQueryMode::Auto => Ok(self.search_exact(query, limit)),
        }
    }

    pub(crate) fn prime_followup_mode_for_first_vector_query(
        &mut self,
        mode: VectorQueryMode,
    ) -> Result<(), String> {
        self.prime_followup_mode(self.first_vector_top_k.max(1), mode)
    }

    pub(crate) fn prime_followup_mode_for_first_hybrid_query(
        &mut self,
        mode: VectorQueryMode,
    ) -> Result<(), String> {
        self.prime_followup_mode(self.first_hybrid_top_k.max(1), mode)
    }

    fn profile_search_with_query(
        &self,
        query: &[f32],
        limit: usize,
        mode: VectorQueryMode,
        auto_force_exact: bool,
    ) -> SearchPhaseProfile {
        if limit == 0 || self.dimensions == 0 {
            return SearchPhaseProfile {
                selected_mode: self.resolve_runtime_query_mode(
                    limit.max(1),
                    mode,
                    auto_force_exact,
                ),
                total_search_ms: 0.0,
                exact_scan_ms: None,
                approximate_search_ms: None,
                rerank_ms: None,
                candidate_count: 0,
                hits: Vec::new(),
            };
        }

        let selected_mode = self.resolve_runtime_query_mode(limit, mode, auto_force_exact);
        match selected_mode {
            VectorQueryMode::ExactFlat => self.profile_exact_search(query, limit),
            VectorQueryMode::Hnsw => self.profile_hnsw_search(query, limit),
            VectorQueryMode::PreviewQ8 => self.profile_preview_search(query, limit),
            VectorQueryMode::Auto => self.profile_exact_search(query, limit),
        }
    }

    fn resolve_query_mode(&self, limit: usize, mode: VectorQueryMode) -> VectorQueryMode {
        match mode {
            VectorQueryMode::Auto => resolve_auto_vector_mode(
                self.skeleton_header.doc_count as usize,
                limit,
                self.hnsw_available,
                self.preview_vectors.is_some(),
            ),
            other => other,
        }
    }

    fn resolve_runtime_query_mode(
        &self,
        limit: usize,
        mode: VectorQueryMode,
        auto_force_exact: bool,
    ) -> VectorQueryMode {
        if auto_force_exact && matches!(mode, VectorQueryMode::Auto) {
            return VectorQueryMode::ExactFlat;
        }

        self.resolve_query_mode(limit, mode)
    }

    fn profile_exact_search(&self, query: &[f32], limit: usize) -> SearchPhaseProfile {
        let exact_start = Instant::now();
        let hits = self.search_exact(query, limit);
        let exact_scan_ms = elapsed_ms(exact_start.elapsed());
        SearchPhaseProfile {
            selected_mode: VectorQueryMode::ExactFlat,
            total_search_ms: exact_scan_ms,
            exact_scan_ms: Some(exact_scan_ms),
            approximate_search_ms: None,
            rerank_ms: None,
            candidate_count: self.skeleton_header.doc_count as usize,
            hits,
        }
    }

    fn profile_preview_search(&self, query: &[f32], limit: usize) -> SearchPhaseProfile {
        let total_start = Instant::now();
        let preview_vectors = self
            .preview_vectors
            .as_ref()
            .expect("preview path checked by caller");
        let preview_limit = self.preview_limit(limit);
        let approximate_start = Instant::now();
        let mut candidates = Vec::with_capacity(preview_limit);
        for (index, vector) in preview_vectors
            .as_ref()
            .chunks_exact(self.dimensions)
            .enumerate()
        {
            let score = dot_product_i8_preview(query, vector);
            self.collect_top_hit(&mut candidates, preview_limit, index, score);
        }
        let approximate_search_ms = elapsed_ms(approximate_start.elapsed());

        let rerank_start = Instant::now();
        let mut reranked = Vec::with_capacity(candidates.len());
        for (index, _) in &candidates {
            let start = index * self.dimensions;
            let end = start + self.dimensions;
            let exact_score = dot_product(query, &self.vector_values()[start..end]);
            reranked.push((*index, exact_score));
        }

        reranked.sort_by(|left, right| self.compare_hits(*left, *right));
        let hits = reranked
            .into_iter()
            .take(limit)
            .map(|(index, _)| self.doc_id(index).to_owned())
            .collect::<Vec<_>>();
        let rerank_ms = elapsed_ms(rerank_start.elapsed());

        SearchPhaseProfile {
            selected_mode: VectorQueryMode::PreviewQ8,
            total_search_ms: elapsed_ms(total_start.elapsed()),
            exact_scan_ms: None,
            approximate_search_ms: Some(approximate_search_ms),
            rerank_ms: Some(rerank_ms),
            candidate_count: candidates.len(),
            hits,
        }
    }

    fn profile_hnsw_search(&self, query: &[f32], limit: usize) -> SearchPhaseProfile {
        let total_start = Instant::now();
        let candidate_limit = self.hnsw_candidate_limit(limit);
        let ef_search = candidate_limit.max(limit).max(32);
        let approximate_start = Instant::now();
        let neighbours = self
            .hnsw_index
            .as_ref()
            .expect("checked by caller")
            .with_dependent(|_, hnsw_index| hnsw_index.search(query, candidate_limit, ef_search));
        let approximate_search_ms = elapsed_ms(approximate_start.elapsed());

        let rerank_start = Instant::now();
        let mut reranked = Vec::with_capacity(neighbours.len());
        for neighbour in &neighbours {
            let index = neighbour.d_id;
            let start = index * self.dimensions;
            let end = start + self.dimensions;
            let exact_score = dot_product(query, &self.vector_values()[start..end]);
            reranked.push((index, exact_score));
        }

        reranked.sort_by(|left, right| self.compare_hits(*left, *right));
        let hits = reranked
            .into_iter()
            .take(limit)
            .map(|(index, _)| self.doc_id(index).to_owned())
            .collect::<Vec<_>>();
        let rerank_ms = elapsed_ms(rerank_start.elapsed());

        SearchPhaseProfile {
            selected_mode: VectorQueryMode::Hnsw,
            total_search_ms: elapsed_ms(total_start.elapsed()),
            exact_scan_ms: None,
            approximate_search_ms: Some(approximate_search_ms),
            rerank_ms: Some(rerank_ms),
            candidate_count: neighbours.len(),
            hits,
        }
    }

    fn search_with_preview_or_exact(&self, query: &[f32], limit: usize) -> Vec<String> {
        if self.preview_vectors.is_some() {
            return self.search_with_quantized_preview(query, limit);
        }

        self.search_exact(query, limit)
    }

    fn ensure_hnsw_sidecar(&mut self) -> Result<bool, String> {
        if self.hnsw_index.is_none() {
            if let Some(basename) = self.hnsw_graph_basename.as_deref() {
                self.hnsw_index = Some(load_hnsw_index(&self.mount_root, basename)?);
            }
        }

        Ok(self.hnsw_index.is_some())
    }

    fn prime_followup_mode(&mut self, limit: usize, mode: VectorQueryMode) -> Result<(), String> {
        if matches!(self.resolve_query_mode(limit, mode), VectorQueryMode::Hnsw) {
            self.ensure_hnsw_sidecar()?;
        }

        Ok(())
    }

    fn search_with_hnsw_or_exact(
        &mut self,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<String>, String> {
        if self.ensure_hnsw_sidecar()? {
            return Ok(self.search_with_hnsw(query, limit));
        }

        Ok(self.search_exact(query, limit))
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

pub(crate) fn resolve_auto_vector_mode(
    doc_count: usize,
    limit: usize,
    has_hnsw: bool,
    has_preview: bool,
) -> VectorQueryMode {
    if doc_count <= auto_exact_fallback_doc_count(limit) {
        return VectorQueryMode::ExactFlat;
    }
    if has_hnsw {
        return VectorQueryMode::Hnsw;
    }
    if has_preview {
        return VectorQueryMode::PreviewQ8;
    }

    VectorQueryMode::ExactFlat
}

fn auto_exact_fallback_doc_count(_limit: usize) -> usize {
    64
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

pub(crate) fn elapsed_ms(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
