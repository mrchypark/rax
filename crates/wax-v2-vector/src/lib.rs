use std::borrow::Cow;
use std::cell::UnsafeCell;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use bytemuck::{cast_slice, try_cast_slice};
use hnsw_rs::prelude::{DistCosine, Hnsw, HnswIo};
use memmap2::{Mmap, MmapOptions};
use self_cell::self_cell;
use serde::Deserialize;
use wax_bench_model::{
    build_vector_lane_skeleton, parse_vector_lane_skeleton_header, vector_lane_doc_id_offsets,
    DatasetPackManifest, VectorLaneSkeletonHeader, VectorQueryMode,
};
use wax_v2_core::{PendingSegmentDescriptor, PendingSegmentWrite, SegmentDescriptor, SegmentKind};
use wax_v2_docstore::{load_document_ids_from_documents, parse_document_id};

type BorrowedHnsw<'a> = Hnsw<'a, f32, DistCosine>;
const VECTOR_SEGMENT_MAGIC: &[u8; 4] = b"WXVG";
const VECTOR_SEGMENT_MAJOR: u16 = 1;
const VECTOR_SEGMENT_MINOR: u16 = 0;
const VECTOR_SEGMENT_HEADER_LENGTH: usize = 48;
const VECTOR_SEGMENT_FLAG_HAS_PREVIEW: u32 = 1;

struct HnswIoOwner(UnsafeCell<HnswIo>);

impl HnswIoOwner {
    fn new(mount_root: &Path, basename: &str) -> Self {
        Self(UnsafeCell::new(HnswIo::new(mount_root, basename)))
    }

    fn load<'a>(&'a self) -> Result<BorrowedHnsw<'a>, String> {
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
pub struct SearchPhaseProfile {
    pub selected_mode: VectorQueryMode,
    pub total_search_ms: f64,
    pub exact_scan_ms: Option<f64>,
    pub approximate_search_ms: Option<f64>,
    pub rerank_ms: Option<f64>,
    pub candidate_count: usize,
    pub hits: Vec<String>,
}

pub struct VectorLane {
    mount_root: PathBuf,
    metadata: VectorLaneMetadata,
    first_vector_query: Vec<f32>,
    pub first_vector_top_k: usize,
    pub first_hybrid_query: Option<Vec<f32>>,
    pub first_hybrid_top_k: usize,
    doc_ids: ByteStorage,
    pub skeleton_header: VectorLaneSkeletonHeader,
    doc_id_offsets: Vec<u64>,
    doc_vectors: ByteStorage,
    hnsw_available: bool,
    hnsw_index: Option<HnswIndexCell>,
    preview_vectors: Option<ByteStorage>,
    pub dimensions: usize,
}

#[derive(Debug)]
enum ByteStorage {
    Mapped(Mmap),
    Owned(Vec<u8>),
    SegmentSlice {
        object: Arc<wax_v2_core::SegmentObject>,
        range: Range<usize>,
    },
}

impl ByteStorage {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mapped(bytes) => bytes.as_ref(),
            Self::Owned(bytes) => bytes.as_slice(),
            Self::SegmentSlice { object, range } => &object[range.start..range.end],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VectorLaneMetadata {
    dimensions: usize,
    doc_count: usize,
    vector_segment: Option<StoreVectorSegment>,
    vector_lane_skeleton_path: Option<PathBuf>,
    documents_path: Option<PathBuf>,
    document_ids_path: Option<PathBuf>,
    document_vectors_path: PathBuf,
    preview_vectors_path: Option<PathBuf>,
    hnsw_graph_basename: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoreVectorSegment {
    store_path: PathBuf,
    descriptor: SegmentDescriptor,
    has_preview: bool,
}

impl VectorLaneMetadata {
    fn resolve(mount_root: &Path, manifest: &DatasetPackManifest) -> Result<Self, String> {
        Self::resolve_with_store_preference(mount_root, manifest, true)
    }

    fn resolve_compatibility(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
    ) -> Result<Self, String> {
        Self::resolve_with_store_preference(mount_root, manifest, false)
    }

    fn resolve_with_store_preference(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        prefer_store_segment: bool,
    ) -> Result<Self, String> {
        let document_vectors_path = manifest
            .files
            .iter()
            .find(|file| file.kind == "document_vectors")
            .map(|file| mount_root.join(&file.path))
            .ok_or_else(|| "document_vectors file missing from manifest".to_owned())?;

        let vector_segment = if prefer_store_segment {
            resolve_store_vector_segment(mount_root)?
        } else {
            None
        };
        if prefer_store_segment
            && vector_segment.is_none()
            && store_has_manifest_visible_family(mount_root, SegmentKind::Doc)?
        {
            return Err(
                "current store generation has manifest-visible documents but no matching vector segment; publish vectors before runtime vector search"
                    .to_owned(),
            );
        }
        let doc_count = vector_segment
            .as_ref()
            .map(|segment| {
                usize::try_from(segment.descriptor.live_items)
                    .map_err(|_| "vector segment live_items exceeds addressable memory".to_owned())
            })
            .transpose()?
            .unwrap_or(manifest.corpus.vector_count as usize);

        Ok(Self {
            dimensions: manifest.vector_profile.embedding_dimensions as usize,
            doc_count,
            vector_segment,
            vector_lane_skeleton_path: manifest
                .files
                .iter()
                .find(|file| file.kind == "vector_lane_skeleton")
                .map(|file| mount_root.join(&file.path)),
            documents_path: manifest
                .files
                .iter()
                .find(|file| file.kind == "documents")
                .map(|file| mount_root.join(&file.path)),
            document_ids_path: manifest
                .files
                .iter()
                .find(|file| file.kind == "document_ids")
                .map(|file| mount_root.join(&file.path)),
            document_vectors_path,
            preview_vectors_path: manifest
                .files
                .iter()
                .find(|file| file.kind == "document_vectors_preview_q8")
                .map(|file| mount_root.join(&file.path)),
            hnsw_graph_basename: manifest
                .files
                .iter()
                .find(|file| file.kind == "vector_hnsw_graph")
                .and_then(|file| file.path.strip_suffix(".hnsw.graph"))
                .map(str::to_owned),
        })
    }

    fn has_preview(&self) -> bool {
        self.vector_segment
            .as_ref()
            .is_some_and(|segment| segment.has_preview)
            || self.preview_vectors_path.is_some()
    }

    fn has_hnsw_sidecar(&self, mount_root: &Path) -> bool {
        if self.vector_segment.is_some() {
            return false;
        }
        let Some(basename) = self.hnsw_graph_basename.as_deref() else {
            return false;
        };
        mount_root.join(format!("{basename}.hnsw.graph")).exists()
            && mount_root.join(format!("{basename}.hnsw.data")).exists()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VectorQueryInputs {
    query_vector_paths: Vec<PathBuf>,
}

impl VectorQueryInputs {
    fn resolve(mount_root: &Path, manifest: &DatasetPackManifest) -> Result<Self, String> {
        let query_vector_paths = manifest
            .files
            .iter()
            .filter(|file| file.kind == "query_vectors")
            .map(|file| mount_root.join(&file.path))
            .collect::<Vec<_>>();
        if query_vector_paths.is_empty() {
            return Err("query_vectors file missing from manifest".to_owned());
        }

        Ok(Self { query_vector_paths })
    }
}

trait VectorBackend {
    fn search(
        &self,
        lane: &mut VectorLane,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<String>, String>;

    fn profile(&self, lane: &VectorLane, query: &[f32], limit: usize) -> SearchPhaseProfile;

    fn warmup(&self, _lane: &mut VectorLane) -> Result<(), String> {
        Ok(())
    }
}

struct ExactBackend;
struct PreviewBackend;
struct HnswBackend;

const EXACT_BACKEND: ExactBackend = ExactBackend;
const PREVIEW_BACKEND: PreviewBackend = PreviewBackend;
const HNSW_BACKEND: HnswBackend = HnswBackend;

impl VectorBackend for ExactBackend {
    fn search(
        &self,
        lane: &mut VectorLane,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<String>, String> {
        Ok(lane.search_exact(query, limit))
    }

    fn profile(&self, lane: &VectorLane, query: &[f32], limit: usize) -> SearchPhaseProfile {
        lane.profile_exact_search(query, limit)
    }
}

impl VectorBackend for PreviewBackend {
    fn search(
        &self,
        lane: &mut VectorLane,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<String>, String> {
        if lane.preview_vectors.is_some() {
            return Ok(lane.search_with_quantized_preview(query, limit));
        }

        Ok(lane.search_exact(query, limit))
    }

    fn profile(&self, lane: &VectorLane, query: &[f32], limit: usize) -> SearchPhaseProfile {
        if lane.preview_vectors.is_some() {
            return lane.profile_preview_search(query, limit);
        }

        lane.profile_exact_search(query, limit)
    }
}

impl VectorBackend for HnswBackend {
    fn search(
        &self,
        lane: &mut VectorLane,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<String>, String> {
        if lane.ensure_hnsw_sidecar()? {
            return Ok(lane.search_with_hnsw(query, limit));
        }

        Ok(lane.search_exact(query, limit))
    }

    fn profile(&self, lane: &VectorLane, query: &[f32], limit: usize) -> SearchPhaseProfile {
        if lane.hnsw_index.is_some() {
            return lane.profile_hnsw_search(query, limit);
        }

        lane.profile_exact_search(query, limit)
    }

    fn warmup(&self, lane: &mut VectorLane) -> Result<(), String> {
        lane.ensure_hnsw_sidecar()?;
        Ok(())
    }
}

impl VectorLane {
    pub fn load(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        vector_mode: VectorQueryMode,
    ) -> Result<Self, String> {
        Self::load_with_report(mount_root, manifest, vector_mode).map(|(lane, _)| lane)
    }

    pub fn load_runtime(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        vector_mode: VectorQueryMode,
    ) -> Result<Self, String> {
        Self::load_runtime_with_report(mount_root, manifest, vector_mode).map(|(lane, _)| lane)
    }

    pub fn load_with_report(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        vector_mode: VectorQueryMode,
    ) -> Result<(Self, Option<f64>), String> {
        let metadata = VectorLaneMetadata::resolve(mount_root, manifest)?;
        let query_inputs = Some(VectorQueryInputs::resolve(mount_root, manifest)?);

        Self::load_from_parts(mount_root, metadata, query_inputs, vector_mode)
    }

    pub fn load_runtime_with_report(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        vector_mode: VectorQueryMode,
    ) -> Result<(Self, Option<f64>), String> {
        let metadata = VectorLaneMetadata::resolve(mount_root, manifest)?;

        Self::load_from_parts(mount_root, metadata, None, vector_mode)
    }

    fn load_from_parts(
        mount_root: &Path,
        metadata: VectorLaneMetadata,
        query_inputs: Option<VectorQueryInputs>,
        vector_mode: VectorQueryMode,
    ) -> Result<(Self, Option<f64>), String> {
        let loaded_vectors = if let Some(segment) = metadata.vector_segment.as_ref() {
            load_vector_segment(&metadata, segment)?
        } else {
            load_compatibility_vector_payloads(&metadata)?
        };
        let doc_ids = loaded_vectors.doc_ids;
        let skeleton_header = parse_vector_lane_skeleton_header(doc_ids.as_slice())?;
        if skeleton_header.dimensions as usize != metadata.dimensions {
            return Err("vector lane skeleton dimensions do not match manifest".to_owned());
        }
        if skeleton_header.doc_count as usize != metadata.doc_count {
            return Err("vector lane skeleton doc_count does not match manifest".to_owned());
        }
        let doc_id_offsets = vector_lane_doc_id_offsets(doc_ids.as_slice(), &skeleton_header)?;
        let doc_vectors = loaded_vectors.doc_vectors;
        let preview_vectors = loaded_vectors.preview_vectors;
        let (first_vector_query, first_hybrid_query) = if let Some(query_inputs) = query_inputs {
            let query_vector_records =
                load_query_vector_records_from_paths(&query_inputs.query_vector_paths)?;
            (
                Some(first_vector_query_from_records(&query_vector_records)?),
                first_hybrid_vector_query_from_records(&query_vector_records),
            )
        } else {
            (None, None)
        };
        let hnsw_available = metadata.has_hnsw_sidecar(mount_root);
        let dimensions = metadata.dimensions;
        let should_load_hnsw = match vector_mode {
            VectorQueryMode::Auto => false,
            VectorQueryMode::Hnsw => true,
            VectorQueryMode::ExactFlat | VectorQueryMode::PreviewQ8 => false,
        };
        let (hnsw_index, hnsw_sidecar_load_ms) = if should_load_hnsw {
            let load_start = Instant::now();
            let index = metadata
                .hnsw_graph_basename
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
                metadata,
                first_vector_query: first_vector_query
                    .as_ref()
                    .map(|query| query.vector.clone())
                    .unwrap_or_default(),
                first_vector_top_k: first_vector_query.as_ref().map_or(0, |query| query.top_k),
                first_hybrid_query: first_hybrid_query
                    .as_ref()
                    .map(|query| query.vector.clone()),
                first_hybrid_top_k: first_hybrid_query.as_ref().map_or(0, |query| query.top_k),
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

    pub fn is_hnsw_sidecar_materialized(&self) -> bool {
        self.hnsw_index.is_some()
    }

    pub fn search_first_vector_query(
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

    pub fn profile_first_vector_query(&self, mode: VectorQueryMode) -> SearchPhaseProfile {
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

    pub fn search_with_query(
        &mut self,
        query: &[f32],
        limit: usize,
        mode: VectorQueryMode,
        auto_force_exact: bool,
    ) -> Result<Vec<String>, String> {
        if limit == 0 || self.dimensions == 0 {
            return Ok(Vec::new());
        }
        validate_query_dimensions(query, self.dimensions)?;

        let selected_mode = self.resolve_runtime_query_mode(limit, mode, auto_force_exact);
        self.backend_for_mode(selected_mode)
            .search(self, query, limit)
    }

    pub fn prime_followup_mode_for_first_vector_query(
        &mut self,
        mode: VectorQueryMode,
    ) -> Result<(), String> {
        self.prime_followup_mode(self.first_vector_top_k.max(1), mode)
    }

    pub fn prime_followup_mode_for_first_hybrid_query(
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
        self.backend_for_mode(selected_mode)
            .profile(self, query, limit)
    }

    fn resolve_query_mode(&self, limit: usize, mode: VectorQueryMode) -> VectorQueryMode {
        match mode {
            VectorQueryMode::Auto => resolve_auto_vector_mode(
                self.metadata.doc_count,
                limit,
                self.hnsw_available,
                self.metadata.has_preview(),
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

    fn backend_for_mode(&self, mode: VectorQueryMode) -> &'static dyn VectorBackend {
        match mode {
            VectorQueryMode::ExactFlat | VectorQueryMode::Auto => &EXACT_BACKEND,
            VectorQueryMode::PreviewQ8 => &PREVIEW_BACKEND,
            VectorQueryMode::Hnsw => &HNSW_BACKEND,
        }
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
            .as_slice()
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
            let exact_score = dot_product_f32le(query, self.vector_bytes(*index));
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
            if let Some(hit) = self.checked_exact_hit(query, index) {
                reranked.push(hit);
            }
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

    fn ensure_hnsw_sidecar(&mut self) -> Result<bool, String> {
        if self.hnsw_index.is_none() {
            if let Some(basename) = self.metadata.hnsw_graph_basename.as_deref() {
                if !self
                    .mount_root
                    .join(format!("{basename}.hnsw.graph"))
                    .exists()
                    || !self
                        .mount_root
                        .join(format!("{basename}.hnsw.data"))
                        .exists()
                {
                    return Ok(false);
                }
                self.hnsw_index = Some(load_hnsw_index(&self.mount_root, basename)?);
            }
        }

        Ok(self.hnsw_index.is_some())
    }

    fn prime_followup_mode(&mut self, limit: usize, mode: VectorQueryMode) -> Result<(), String> {
        let selected_mode = self.resolve_query_mode(limit, mode);
        self.backend_for_mode(selected_mode).warmup(self)
    }

    fn search_exact(&self, query: &[f32], limit: usize) -> Vec<String> {
        let mut hits = Vec::with_capacity(limit.min(self.skeleton_header.doc_count as usize));
        for index in 0..self.skeleton_header.doc_count as usize {
            let score = dot_product_f32le(query, self.vector_bytes(index));
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
            .as_slice()
            .chunks_exact(self.dimensions)
            .enumerate()
        {
            let score = dot_product_i8_preview(query, vector);
            self.collect_top_hit(&mut candidates, preview_limit, index, score);
        }

        let mut reranked = Vec::with_capacity(candidates.len());
        for (index, _) in candidates {
            let exact_score = dot_product_f32le(query, self.vector_bytes(index));
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
            if let Some(hit) = self.checked_exact_hit(query, index) {
                reranked.push(hit);
            }
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

    fn checked_exact_hit(&self, query: &[f32], index: usize) -> Option<(usize, f32)> {
        let exact_vector = self.checked_vector_bytes(index)?;
        Some((index, dot_product_f32le(query, exact_vector)))
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

    fn vector_bytes(&self, index: usize) -> &[u8] {
        let row_length = self.dimensions * 4;
        let start = index * row_length;
        let end = start + row_length;
        &self.doc_vectors.as_slice()[start..end]
    }

    fn checked_vector_bytes(&self, index: usize) -> Option<&[u8]> {
        if index >= self.skeleton_header.doc_count as usize {
            return None;
        }
        let row_length = self.dimensions.checked_mul(4)?;
        let start = index.checked_mul(row_length)?;
        let end = start.checked_add(row_length)?;
        self.doc_vectors.as_slice().get(start..end)
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

pub fn resolve_auto_vector_mode(
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

fn load_vector_lane_skeleton(metadata: &VectorLaneMetadata) -> Result<ByteStorage, String> {
    if let Some(path) = metadata.vector_lane_skeleton_path.as_ref() {
        return map_read_only(&path).map(ByteStorage::Mapped);
    }

    let doc_ids = if let Some(document_ids_path) = metadata.document_ids_path.as_ref() {
        load_document_ids(&document_ids_path)?
    } else {
        let documents_path = metadata
            .documents_path
            .as_ref()
            .ok_or_else(|| "documents file missing from manifest".to_owned())?;
        load_document_ids_from_documents(&documents_path).map_err(docstore_error)?
    };
    Ok(ByteStorage::Owned(build_vector_lane_skeleton(
        &doc_ids,
        metadata.dimensions as u32,
    )))
}

struct LoadedVectorPayloads {
    doc_ids: ByteStorage,
    doc_vectors: ByteStorage,
    preview_vectors: Option<ByteStorage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BinaryVectorSegmentLayout {
    dimensions: usize,
    doc_ids: Vec<String>,
    exact_vectors_range: Range<usize>,
    preview_vectors_range: Option<Range<usize>>,
}

fn resolve_store_vector_segment(mount_root: &Path) -> Result<Option<StoreVectorSegment>, String> {
    let store_path = mount_root.join("store.wax");
    if !store_path.exists() {
        return Ok(None);
    }
    let opened = wax_v2_core::open_store(&store_path).map_err(|error| error.to_string())?;
    let latest_vec = opened
        .manifest
        .segments
        .iter()
        .filter(|segment| segment.family == SegmentKind::Vec)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
        .cloned();
    let latest_doc_generation = opened
        .manifest
        .segments
        .iter()
        .filter(|segment| segment.family == SegmentKind::Doc)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
        .map(|segment| segment.segment_generation);
    if let (Some(doc_generation), Some(descriptor)) = (latest_doc_generation, latest_vec.as_ref()) {
        if descriptor.segment_generation < doc_generation {
            return Err(
                "latest vector segment is stale relative to the current document generation; republish vectors before runtime vector search"
                    .to_owned(),
            );
        }
    }
    let Some(descriptor) = latest_vec else {
        return Ok(None);
    };
    let has_preview = descriptor.backend_aux != 0;
    Ok(Some(StoreVectorSegment {
        store_path,
        descriptor,
        has_preview,
    }))
}

fn store_has_manifest_visible_family(
    mount_root: &Path,
    family: SegmentKind,
) -> Result<bool, String> {
    let store_path = mount_root.join("store.wax");
    if !store_path.exists() {
        return Ok(false);
    }
    let opened = wax_v2_core::open_store(&store_path).map_err(|error| error.to_string())?;
    Ok(opened
        .manifest
        .segments
        .iter()
        .any(|segment| segment.family == family))
}

fn load_vector_segment(
    metadata: &VectorLaneMetadata,
    segment: &StoreVectorSegment,
) -> Result<LoadedVectorPayloads, String> {
    let bytes = Arc::new(
        wax_v2_core::map_segment_object(&segment.store_path, &segment.descriptor)
            .map_err(|error| error.to_string())?,
    );
    let layout = BinaryVectorSegmentLayout::decode(&bytes)?;
    if layout.dimensions != metadata.dimensions {
        return Err("vector segment dimensions do not match manifest".to_owned());
    }
    if layout.doc_ids.len() != metadata.doc_count {
        return Err("vector segment doc_count does not match manifest".to_owned());
    }

    let doc_ids = ByteStorage::Owned(build_vector_lane_skeleton(
        &layout.doc_ids,
        layout.dimensions as u32,
    ));
    let doc_vectors = ByteStorage::SegmentSlice {
        object: bytes.clone(),
        range: layout.exact_vectors_range.clone(),
    };
    let preview_vectors = layout
        .preview_vectors_range
        .map(|range| ByteStorage::SegmentSlice {
            object: bytes.clone(),
            range,
        });

    Ok(LoadedVectorPayloads {
        doc_ids,
        doc_vectors,
        preview_vectors,
    })
}

fn load_compatibility_vector_payloads(
    metadata: &VectorLaneMetadata,
) -> Result<LoadedVectorPayloads, String> {
    let doc_ids = load_vector_lane_skeleton(metadata)?;
    let doc_vectors = ByteStorage::Mapped(map_read_only(&metadata.document_vectors_path)?);
    validate_document_vectors(
        doc_vectors.as_slice(),
        metadata.dimensions,
        metadata.doc_count,
    )?;
    let preview_vectors = metadata
        .preview_vectors_path
        .as_ref()
        .map(|path| -> Result<ByteStorage, String> {
            let mapped = map_read_only(path)?;
            validate_preview_vectors(mapped.as_ref(), metadata.dimensions, metadata.doc_count)?;
            Ok(ByteStorage::Mapped(mapped))
        })
        .transpose()?;

    Ok(LoadedVectorPayloads {
        doc_ids,
        doc_vectors,
        preview_vectors,
    })
}

fn load_hnsw_index(mount_root: &Path, basename: &str) -> Result<HnswIndexCell, String> {
    HnswIndexCell::try_new(HnswIoOwner::new(mount_root, basename), |owner| owner.load())
}

fn map_read_only(path: &Path) -> Result<Mmap, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    unsafe { MmapOptions::new().map(&file) }.map_err(|error| error.to_string())
}

pub fn elapsed_ms(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

pub fn publish_compatibility_vector_segment(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
    store_path: &Path,
) -> Result<(), String> {
    let prepared = prepare_compatibility_vector_segment(mount_root, manifest)?;
    wax_v2_core::publish_segment(store_path, prepared.descriptor, &prepared.object_bytes)
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub fn prepare_compatibility_vector_segment(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<PendingSegmentWrite, String> {
    let raw_vectors = load_compatibility_raw_vectors(mount_root, manifest)?;
    prepare_raw_vector_segment(
        manifest.vector_profile.embedding_dimensions as usize,
        &raw_vectors,
    )
}

pub fn prepare_raw_vector_segment(
    expected_dimensions: usize,
    vector_inputs: &[(String, Vec<f32>)],
) -> Result<PendingSegmentWrite, String> {
    let segment = BinaryVectorSegment::from_raw_vectors(expected_dimensions, vector_inputs)?;
    let object_bytes = segment.encode()?;
    Ok(PendingSegmentWrite {
        descriptor: PendingSegmentDescriptor {
            family: SegmentKind::Vec,
            family_version: 1,
            flags: 0,
            doc_id_start: 0,
            doc_id_end_exclusive: vector_inputs.len() as u64,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
            live_items: vector_inputs.len() as u64,
            tombstoned_items: 0,
            backend_id: 0,
            backend_aux: u64::from(segment.preview_vectors.is_some()),
        },
        object_bytes,
    })
}

fn load_document_ids(path: &Path) -> Result<Vec<String>, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let reader = BufReader::new(file);
    reader
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some(other),
        })
        .map(|line| {
            let line = line.map_err(|error| error.to_string())?;
            parse_document_id(&line, "document id line").map(Cow::into_owned)
        })
        .collect()
}

pub fn load_compatibility_raw_vectors(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<Vec<(String, Vec<f32>)>, String> {
    let metadata = VectorLaneMetadata::resolve_compatibility(mount_root, manifest)?;
    let doc_ids = if let Some(document_ids_path) = metadata.document_ids_path.as_ref() {
        load_document_ids(document_ids_path)?
    } else {
        let documents_path = metadata
            .documents_path
            .as_ref()
            .ok_or_else(|| "documents file missing from manifest".to_owned())?;
        load_document_ids_from_documents(documents_path).map_err(docstore_error)?
    };
    let exact_vectors =
        fs::read(&metadata.document_vectors_path).map_err(|error| error.to_string())?;
    validate_document_vectors(&exact_vectors, metadata.dimensions, metadata.doc_count)?;
    if doc_ids.len() != metadata.doc_count {
        return Err("document_ids row count does not match manifest vector_count".to_owned());
    }

    Ok(doc_ids
        .into_iter()
        .enumerate()
        .map(|(index, doc_id)| {
            let start = index * metadata.dimensions * 4;
            let end = start + metadata.dimensions * 4;
            (doc_id, decode_f32le_slice(&exact_vectors[start..end]))
        })
        .collect())
}

pub fn validate_store_segment_against_dataset_pack(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<(), String> {
    let Some(store_segment) = resolve_store_vector_segment(mount_root)? else {
        return Ok(());
    };
    let metadata = VectorLaneMetadata::resolve_compatibility(mount_root, manifest)?;
    if !metadata.document_vectors_path.exists() {
        return Ok(());
    }

    let expected_raw_vectors = load_compatibility_raw_vectors(mount_root, manifest)?;
    let expected_without_preview =
        BinaryVectorSegment::from_raw_vectors(metadata.dimensions, &expected_raw_vectors)?;
    let mut expected_with_preview = expected_without_preview.clone();
    if let Some(preview_path) = metadata
        .preview_vectors_path
        .as_ref()
        .filter(|path| path.exists())
    {
        let preview_vectors = fs::read(preview_path).map_err(|error| error.to_string())?;
        validate_preview_vectors(
            &preview_vectors,
            metadata.dimensions,
            expected_raw_vectors.len(),
        )?;
        expected_with_preview.preview_vectors = Some(preview_vectors);
    }

    let bytes =
        wax_v2_core::map_segment_object(&store_segment.store_path, &store_segment.descriptor)
            .map_err(|error| error.to_string())?;
    let persisted_segment = BinaryVectorSegment::decode(&bytes)?;
    if persisted_segment != expected_without_preview && persisted_segment != expected_with_preview {
        return Err("store vector segment does not match mounted dataset vectors".to_owned());
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BinaryVectorSegment {
    dimensions: usize,
    doc_ids: Vec<String>,
    exact_vectors: Vec<u8>,
    preview_vectors: Option<Vec<u8>>,
}

impl BinaryVectorSegment {
    fn from_raw_vectors(
        expected_dimensions: usize,
        vector_inputs: &[(String, Vec<f32>)],
    ) -> Result<Self, String> {
        if vector_inputs.is_empty() {
            return Err("raw vector segment requires at least one vector".to_owned());
        }
        if expected_dimensions == 0 {
            return Err("raw vector segment requires non-zero dimensions".to_owned());
        }

        let mut seen_doc_ids = std::collections::BTreeSet::new();
        let mut doc_ids = Vec::with_capacity(vector_inputs.len());
        let mut exact_values = Vec::with_capacity(vector_inputs.len() * expected_dimensions);

        for (doc_id, values) in vector_inputs {
            if values.len() != expected_dimensions {
                return Err(format!(
                    "raw vector for {doc_id} has {} values but expected {expected_dimensions}",
                    values.len()
                ));
            }
            if !seen_doc_ids.insert(doc_id.clone()) {
                return Err(format!(
                    "raw vector doc_id {doc_id} was provided more than once"
                ));
            }
            doc_ids.push(doc_id.clone());
            exact_values.extend_from_slice(values);
        }

        Ok(Self {
            dimensions: expected_dimensions,
            doc_ids,
            exact_vectors: cast_slice::<f32, u8>(&exact_values).to_vec(),
            preview_vectors: None,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        if self.doc_ids.is_empty() && self.dimensions != 0 {
            return Err("vector segment cannot encode dimensions without rows".to_owned());
        }
        validate_document_vectors(&self.exact_vectors, self.dimensions, self.doc_ids.len())?;
        if let Some(preview_vectors) = self.preview_vectors.as_ref() {
            validate_preview_vectors(preview_vectors, self.dimensions, self.doc_ids.len())?;
        }

        let flags = if self.preview_vectors.is_some() {
            VECTOR_SEGMENT_FLAG_HAS_PREVIEW
        } else {
            0
        };
        let mut doc_ids_section = Vec::new();
        for doc_id in &self.doc_ids {
            doc_ids_section.extend_from_slice(&(doc_id.len() as u32).to_le_bytes());
            doc_ids_section.extend_from_slice(doc_id.as_bytes());
        }
        let doc_ids_offset = VECTOR_SEGMENT_HEADER_LENGTH as u64;
        let exact_vectors_offset =
            align_up_usize(doc_ids_offset as usize + doc_ids_section.len(), 4) as u64;
        doc_ids_section.resize(exact_vectors_offset as usize - doc_ids_offset as usize, 0);
        let preview_vectors_offset = if self.preview_vectors.is_some() {
            exact_vectors_offset + self.exact_vectors.len() as u64
        } else {
            0
        };

        let mut bytes = Vec::new();
        bytes.extend_from_slice(VECTOR_SEGMENT_MAGIC);
        bytes.extend_from_slice(&VECTOR_SEGMENT_MAJOR.to_le_bytes());
        bytes.extend_from_slice(&VECTOR_SEGMENT_MINOR.to_le_bytes());
        bytes.extend_from_slice(&(self.dimensions as u32).to_le_bytes());
        bytes.extend_from_slice(&flags.to_le_bytes());
        bytes.extend_from_slice(&(self.doc_ids.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&doc_ids_offset.to_le_bytes());
        bytes.extend_from_slice(&exact_vectors_offset.to_le_bytes());
        bytes.extend_from_slice(&preview_vectors_offset.to_le_bytes());
        bytes.extend_from_slice(&doc_ids_section);
        bytes.extend_from_slice(&self.exact_vectors);
        if let Some(preview_vectors) = self.preview_vectors.as_ref() {
            bytes.extend_from_slice(preview_vectors);
        }
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        let layout = BinaryVectorSegmentLayout::decode(bytes)?;
        let exact_vectors = bytes[layout.exact_vectors_range.clone()].to_vec();
        let preview_vectors = layout
            .preview_vectors_range
            .clone()
            .map(|range| bytes[range].to_vec());

        Ok(Self {
            dimensions: layout.dimensions,
            doc_ids: layout.doc_ids,
            exact_vectors,
            preview_vectors,
        })
    }
}

impl BinaryVectorSegmentLayout {
    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < VECTOR_SEGMENT_HEADER_LENGTH {
            return Err(format!(
                "vector segment too short: expected at least {VECTOR_SEGMENT_HEADER_LENGTH} bytes"
            ));
        }
        if &bytes[..4] != VECTOR_SEGMENT_MAGIC {
            return Err("vector segment magic mismatch".to_owned());
        }
        if read_u16(bytes, 4) != VECTOR_SEGMENT_MAJOR || read_u16(bytes, 6) != VECTOR_SEGMENT_MINOR
        {
            return Err("unsupported vector segment version".to_owned());
        }

        let dimensions = read_u32(bytes, 8) as usize;
        let flags = read_u32(bytes, 12);
        let doc_count = read_u64(bytes, 16) as usize;
        let doc_ids_offset = read_u64(bytes, 24) as usize;
        let exact_vectors_offset = read_u64(bytes, 32) as usize;
        let preview_vectors_offset = read_u64(bytes, 40) as usize;
        if doc_ids_offset != VECTOR_SEGMENT_HEADER_LENGTH
            || doc_ids_offset > exact_vectors_offset
            || exact_vectors_offset > bytes.len()
            || exact_vectors_offset % 4 != 0
        {
            return Err("vector segment section offsets are invalid".to_owned());
        }
        if flags & VECTOR_SEGMENT_FLAG_HAS_PREVIEW != 0
            && (preview_vectors_offset < exact_vectors_offset
                || preview_vectors_offset > bytes.len())
        {
            return Err("vector segment preview offset is invalid".to_owned());
        }
        if flags & VECTOR_SEGMENT_FLAG_HAS_PREVIEW == 0 && preview_vectors_offset != 0 {
            return Err(
                "vector segment preview offset must be zero when preview is absent".to_owned(),
            );
        }

        let doc_ids_section = &bytes[doc_ids_offset..exact_vectors_offset];
        let exact_vectors_end = if flags & VECTOR_SEGMENT_FLAG_HAS_PREVIEW != 0 {
            preview_vectors_offset
        } else {
            bytes.len()
        };
        let exact_vectors_range = exact_vectors_offset..exact_vectors_end;
        let preview_vectors_range = if flags & VECTOR_SEGMENT_FLAG_HAS_PREVIEW != 0 {
            Some(preview_vectors_offset..bytes.len())
        } else {
            None
        };

        let mut cursor = 0usize;
        let doc_ids = read_length_prefixed_strings(doc_ids_section, doc_count, &mut cursor)?;
        if doc_ids_section[cursor..].iter().any(|byte| *byte != 0) {
            return Err("vector segment doc_id section length mismatch".to_owned());
        }
        validate_document_vectors(&bytes[exact_vectors_range.clone()], dimensions, doc_count)?;
        if let Some(preview_vectors_range) = preview_vectors_range.as_ref() {
            validate_preview_vectors(&bytes[preview_vectors_range.clone()], dimensions, doc_count)?;
        }

        Ok(Self {
            dimensions,
            doc_ids,
            exact_vectors_range,
            preview_vectors_range,
        })
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("u16 slice"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("u32 slice"))
}

fn read_u32_at(bytes: &[u8], cursor: &mut usize, context: &str) -> Result<u32, String> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| format!("vector segment {context} length overflow"))?;
    if end > bytes.len() {
        return Err(format!(
            "vector segment truncated while reading {context} length"
        ));
    }
    let value = u32::from_le_bytes(bytes[*cursor..end].try_into().expect("u32 slice"));
    *cursor = end;
    Ok(value)
}

fn read_string_at(
    bytes: &[u8],
    cursor: &mut usize,
    length: usize,
    context: &str,
) -> Result<String, String> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| format!("vector segment {context} range overflow"))?;
    if end > bytes.len() {
        return Err(format!("vector segment truncated while reading {context}"));
    }
    let value = std::str::from_utf8(&bytes[*cursor..end])
        .map_err(|error| error.to_string())?
        .to_owned();
    *cursor = end;
    Ok(value)
}

fn read_length_prefixed_strings(
    bytes: &[u8],
    count: usize,
    cursor: &mut usize,
) -> Result<Vec<String>, String> {
    if count > bytes[*cursor..].len() / 4 {
        return Err("vector segment string count exceeds possible records in slice".to_owned());
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let length = read_u32_at(bytes, cursor, "doc_id")? as usize;
        values.push(read_string_at(bytes, cursor, length, "doc_id")?);
    }
    Ok(values)
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("u64 slice"))
}

fn align_up_usize(value: usize, alignment: usize) -> usize {
    if alignment == 0 {
        return value;
    }
    let remainder = value % alignment;
    if remainder == 0 {
        value
    } else {
        value + (alignment - remainder)
    }
}

fn validate_document_vectors(
    bytes: &[u8],
    dimensions: usize,
    doc_count: usize,
) -> Result<(), String> {
    if dimensions == 0 {
        return Ok(());
    }
    let bytes_per_row = dimensions
        .checked_mul(4)
        .ok_or_else(|| "document vector payload shape overflows addressable memory".to_owned())?;
    let expected_values = doc_count
        .checked_mul(dimensions)
        .ok_or_else(|| "document vector payload shape overflows addressable memory".to_owned())?;
    if !bytes.len().is_multiple_of(bytes_per_row) {
        return Err("document vector payload has invalid length".to_owned());
    }
    if bytes.len() / 4 != expected_values {
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
    let expected_bytes = doc_count
        .checked_mul(dimensions)
        .ok_or_else(|| "preview vector payload shape overflows addressable memory".to_owned())?;
    if bytes.len() != expected_bytes {
        return Err("preview vector payload row count does not match manifest".to_owned());
    }
    Ok(())
}

fn load_query_vector_records_from_paths(
    paths: &[PathBuf],
) -> Result<Vec<QueryVectorRecord>, String> {
    let mut records = Vec::new();
    for path in paths {
        let reader = BufReader::new(File::open(path).map_err(|error| error.to_string())?);
        for line in reader.lines() {
            let line = line.map_err(|error| error.to_string())?;
            if line.trim().is_empty() {
                continue;
            }
            let query: QueryVectorRecord =
                serde_json::from_str(&line).map_err(|error| error.to_string())?;
            records.push(query);
        }
    }
    Ok(records)
}

fn validate_query_dimensions(query: &[f32], expected_dimensions: usize) -> Result<(), String> {
    if query.len() != expected_dimensions {
        return Err(format!(
            "query vector dimensions do not match lane dimensions: expected {expected_dimensions}, got {}",
            query.len()
        ));
    }
    Ok(())
}

fn decode_f32le_slice(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("4-byte f32 chunk")))
        .collect()
}

fn first_vector_query_from_records(
    records: &[QueryVectorRecord],
) -> Result<FirstVectorQuery, String> {
    records
        .iter()
        .find(|query| query.lane_eligibility.vector)
        .map(|query| FirstVectorQuery {
            vector: query.vector.clone(),
            top_k: query.top_k as usize,
        })
        .ok_or_else(|| "no vector-eligible query found".to_owned())
}

fn first_hybrid_vector_query_from_records(
    records: &[QueryVectorRecord],
) -> Option<FirstVectorQuery> {
    records
        .iter()
        .find(|query| query.lane_eligibility.hybrid)
        .map(|query| FirstVectorQuery {
            vector: query.vector.clone(),
            top_k: query.top_k as usize,
        })
}

fn dot_product_f32le(left: &[f32], right: &[u8]) -> f32 {
    if let Ok(right_f32) = try_cast_slice::<u8, f32>(right) {
        return dot_product_f32_slice(left, right_f32);
    }

    left.iter()
        .zip(right.chunks_exact(4))
        .map(|(lhs, rhs)| {
            let rhs = f32::from_le_bytes(rhs.try_into().expect("validated vector chunk"));
            lhs * rhs
        })
        .sum()
}

fn dot_product_f32_slice(left: &[f32], right: &[f32]) -> f32 {
    let len = left.len().min(right.len());
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;
    let mut index = 0usize;

    while index + 4 <= len {
        sum0 += left[index] * right[index];
        sum1 += left[index + 1] * right[index + 1];
        sum2 += left[index + 2] * right[index + 2];
        sum3 += left[index + 3] * right[index + 3];
        index += 4;
    }

    let mut tail = 0.0f32;
    while index < len {
        tail += left[index] * right[index];
        index += 1;
    }

    sum0 + sum1 + sum2 + sum3 + tail
}

fn dot_product_i8_preview(left: &[f32], right: &[u8]) -> f32 {
    let len = left.len().min(right.len());
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;
    let mut index = 0usize;

    while index + 4 <= len {
        sum0 += left[index] * (right[index] as i8 as f32);
        sum1 += left[index + 1] * (right[index + 1] as i8 as f32);
        sum2 += left[index + 2] * (right[index + 2] as i8 as f32);
        sum3 += left[index + 3] * (right[index + 3] as i8 as f32);
        index += 4;
    }

    let mut tail = 0.0f32;
    while index < len {
        tail += left[index] * (right[index] as i8 as f32);
        index += 1;
    }

    sum0 + sum1 + sum2 + sum3 + tail
}

#[derive(Debug, Clone, Deserialize)]
struct LaneEligibility {
    vector: bool,
    hybrid: bool,
}

#[derive(Debug, Deserialize)]
struct QueryVectorRecord {
    top_k: u32,
    vector: Vec<f32>,
    lane_eligibility: LaneEligibility,
}

#[derive(Debug)]
struct FirstVectorQuery {
    vector: Vec<f32>,
    top_k: usize,
}

fn docstore_error(error: wax_v2_docstore::DocstoreError) -> String {
    match error {
        wax_v2_docstore::DocstoreError::Io(message)
        | wax_v2_docstore::DocstoreError::Json(message)
        | wax_v2_docstore::DocstoreError::InvalidDocument(message) => message,
        wax_v2_docstore::DocstoreError::MissingDocumentsFile => {
            "dataset pack missing documents file".to_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use serde_json::json;
    use tempfile::tempdir;
    use wax_bench_model::{DatasetPackManifest, VectorQueryMode};
    use wax_v2_core::{create_empty_store, publish_segments, SegmentKind};
    use wax_v2_docstore::prepare_raw_documents_segment;

    use crate::{
        dot_product_f32le, load_compatibility_raw_vectors, load_vector_segment,
        prepare_raw_vector_segment, publish_compatibility_vector_segment,
        read_length_prefixed_strings, read_u64, resolve_auto_vector_mode,
        validate_document_vectors, validate_preview_vectors,
        validate_store_segment_against_dataset_pack, BinaryVectorSegment, ByteStorage,
        StoreVectorSegment, VectorLane, VectorLaneMetadata, VectorQueryInputs,
    };

    #[test]
    fn dot_product_f32le_uses_aligned_unrolled_path_with_tail() {
        let right = [2.0f32, 3.0, 4.0, 5.0, 6.0];
        let score = dot_product_f32le(&[1.0, 1.0, 1.0, 1.0, 1.0], bytemuck::cast_slice(&right));

        assert_eq!(score, 20.0);
    }

    #[test]
    fn vector_segment_string_reader_rejects_impossible_count_before_allocation() {
        let mut cursor = 0usize;
        let error = read_length_prefixed_strings(&[], usize::MAX, &mut cursor)
            .expect_err("string count should exceed payload");

        assert!(error.contains("count"));
    }

    #[test]
    fn validate_document_vectors_rejects_overflowing_shape_before_multiplication() {
        let error =
            validate_document_vectors(&[], usize::MAX / 2 + 1, 3).expect_err("shape overflows");

        assert!(error.contains("overflows"));
    }

    #[test]
    fn validate_preview_vectors_rejects_overflowing_shape_before_multiplication() {
        let error =
            validate_preview_vectors(&[], usize::MAX / 2 + 1, 3).expect_err("shape overflows");

        assert!(error.contains("overflows"));
    }

    #[test]
    fn vector_lane_loads_exact_search_hits_from_compatibility_files() {
        let temp_dir = tempdir().unwrap();
        fs::write(
            temp_dir.path().join("document_ids.txt"),
            concat!(
                "{\"doc_id\":\"doc-1\"}\n",
                "{\"doc_id\":\"doc-2\"}\n",
                "{\"doc_id\":\"doc-3\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("document_vectors.bin"),
            bytemuck::cast_slice::<f32, u8>(&[
                1.0f32, 0.0f32, //
                0.9f32, 0.1f32, //
                0.0f32, 1.0f32, //
            ]),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("query_vectors.jsonl"),
            concat!(
                "{\"query_id\":\"q-001\",\"top_k\":2,\"vector\":[1.0,0.0],\"lane_eligibility\":{\"text\":false,\"vector\":true,\"hybrid\":false}}\n",
                "{\"query_id\":\"q-002\",\"top_k\":2,\"vector\":[0.9,0.1],\"lane_eligibility\":{\"text\":true,\"vector\":true,\"hybrid\":true}}\n",
            ),
        )
        .unwrap();

        let mut lane = VectorLane::load(
            temp_dir.path(),
            &test_manifest(false, false),
            VectorQueryMode::ExactFlat,
        )
        .unwrap();

        assert_eq!(
            lane.search_first_vector_query(VectorQueryMode::ExactFlat, false)
                .unwrap(),
            vec!["doc-1", "doc-2"]
        );
        assert_eq!(
            lane.search_with_query(&[0.0, 1.0], 2, VectorQueryMode::ExactFlat, false)
                .unwrap(),
            vec!["doc-3", "doc-2"]
        );
    }

    #[test]
    fn resolve_auto_vector_mode_prefers_preview_when_large_without_hnsw() {
        assert_eq!(
            resolve_auto_vector_mode(128, 10, false, true),
            VectorQueryMode::PreviewQ8
        );
    }

    #[test]
    fn vector_lane_metadata_resolves_persisted_inputs_without_query_sidecars() {
        let mount_root = PathBuf::from("/tmp/wax-vector");
        let metadata =
            VectorLaneMetadata::resolve(&mount_root, &test_manifest(true, true)).unwrap();
        let query_inputs =
            VectorQueryInputs::resolve(&mount_root, &test_manifest(true, true)).unwrap();

        assert_eq!(metadata.dimensions, 2);
        assert_eq!(metadata.doc_count, 3);
        assert_eq!(
            metadata.document_vectors_path,
            mount_root.join("document_vectors.bin")
        );
        assert_eq!(
            metadata.preview_vectors_path,
            Some(mount_root.join("preview.bin"))
        );
        assert_eq!(metadata.hnsw_graph_basename.as_deref(), Some("graph"));
        assert_eq!(
            metadata.document_ids_path,
            Some(mount_root.join("document_ids.txt"))
        );
        assert_eq!(metadata.vector_lane_skeleton_path, None);
        assert_eq!(
            query_inputs.query_vector_paths,
            vec![mount_root.join("query_vectors.jsonl")]
        );
    }

    #[test]
    fn vector_lane_metadata_counts_store_rows_from_live_items_for_sparse_doc_id_ranges() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        create_empty_store(&store_path).unwrap();
        let mut pending = prepare_raw_vector_segment(
            2,
            &[
                ("doc-1".to_owned(), vec![1.0f32, 0.0f32]),
                ("doc-9".to_owned(), vec![0.0f32, 1.0f32]),
            ],
        )
        .unwrap();
        pending.descriptor.doc_id_start = 1;
        pending.descriptor.doc_id_end_exclusive = 10;
        pending.descriptor.live_items = 2;
        publish_segments(&store_path, vec![pending]).unwrap();

        let metadata = VectorLaneMetadata::resolve(
            temp_dir.path(),
            &test_manifest_with_count(9, false, false),
        )
        .unwrap();

        assert_eq!(metadata.doc_count, 2);
    }

    #[test]
    fn profile_first_vector_query_falls_back_to_exact_when_hnsw_sidecar_is_missing() {
        let temp_dir = tempdir().unwrap();
        fs::write(
            temp_dir.path().join("document_ids.txt"),
            concat!(
                "{\"doc_id\":\"doc-1\"}\n",
                "{\"doc_id\":\"doc-2\"}\n",
                "{\"doc_id\":\"doc-3\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("document_vectors.bin"),
            bytemuck::cast_slice::<f32, u8>(&[
                1.0f32, 0.0f32, //
                0.9f32, 0.1f32, //
                0.0f32, 1.0f32, //
            ]),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("query_vectors.jsonl"),
            concat!(
                "{\"query_id\":\"q-001\",\"top_k\":2,\"vector\":[1.0,0.0],\"lane_eligibility\":{\"text\":false,\"vector\":true,\"hybrid\":false}}\n",
                "{\"query_id\":\"q-002\",\"top_k\":2,\"vector\":[0.9,0.1],\"lane_eligibility\":{\"text\":true,\"vector\":true,\"hybrid\":true}}\n",
            ),
        )
        .unwrap();

        let lane = VectorLane::load(
            temp_dir.path(),
            &test_manifest(false, false),
            VectorQueryMode::Hnsw,
        )
        .unwrap();

        let profile = lane.profile_first_vector_query(VectorQueryMode::Hnsw);

        assert_eq!(profile.selected_mode, VectorQueryMode::ExactFlat);
        assert_eq!(profile.hits, vec!["doc-1", "doc-2"]);
    }

    #[test]
    fn vector_lane_prefers_manifest_visible_segment_when_sidecars_are_missing() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        fs::write(
            temp_dir.path().join("document_ids.txt"),
            concat!(
                "{\"doc_id\":\"doc-1\"}\n",
                "{\"doc_id\":\"doc-2\"}\n",
                "{\"doc_id\":\"doc-3\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("document_vectors.bin"),
            bytemuck::cast_slice::<f32, u8>(&[
                1.0f32, 0.0f32, //
                0.9f32, 0.1f32, //
                0.0f32, 1.0f32, //
            ]),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("preview.bin"),
            vec![
                127_i8 as u8,
                0_i8 as u8, //
                114_i8 as u8,
                13_i8 as u8, //
                0_i8 as u8,
                127_i8 as u8, //
            ],
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("query_vectors.jsonl"),
            concat!(
                "{\"query_id\":\"q-001\",\"top_k\":2,\"vector\":[1.0,0.0],\"lane_eligibility\":{\"text\":false,\"vector\":true,\"hybrid\":false}}\n",
                "{\"query_id\":\"q-002\",\"top_k\":2,\"vector\":[0.9,0.1],\"lane_eligibility\":{\"text\":true,\"vector\":true,\"hybrid\":true}}\n",
            ),
        )
        .unwrap();
        create_empty_store(&store_path).unwrap();

        publish_compatibility_vector_segment(
            temp_dir.path(),
            &test_manifest(true, false),
            &store_path,
        )
        .unwrap();
        fs::remove_file(temp_dir.path().join("document_ids.txt")).unwrap();
        fs::remove_file(temp_dir.path().join("document_vectors.bin")).unwrap();
        fs::remove_file(temp_dir.path().join("preview.bin")).unwrap();

        let mut lane = VectorLane::load(
            temp_dir.path(),
            &test_manifest(true, false),
            VectorQueryMode::PreviewQ8,
        )
        .unwrap();

        assert_eq!(
            lane.search_first_vector_query(VectorQueryMode::ExactFlat, false)
                .unwrap(),
            vec!["doc-1", "doc-2"]
        );
        assert_eq!(
            lane.search_first_vector_query(VectorQueryMode::PreviewQ8, false)
                .unwrap(),
            vec!["doc-1", "doc-2"]
        );
    }

    #[test]
    fn vector_lane_runtime_loads_persisted_segment_without_query_sidecars() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        create_empty_store(&store_path).unwrap();

        let pending = prepare_raw_vector_segment(
            2,
            &[
                ("doc-1".to_owned(), vec![1.0f32, 0.0f32]),
                ("doc-2".to_owned(), vec![0.0f32, 1.0f32]),
                ("doc-3".to_owned(), vec![0.9f32, 0.1f32]),
            ],
        )
        .unwrap();
        publish_segments(&store_path, vec![pending]).unwrap();

        let mut lane = VectorLane::load_runtime(
            temp_dir.path(),
            &test_manifest(false, false),
            VectorQueryMode::Auto,
        )
        .unwrap();

        assert_eq!(
            lane.search_with_query(&[0.0, 1.0], 2, VectorQueryMode::ExactFlat, false)
                .unwrap(),
            vec!["doc-2", "doc-3"]
        );
    }

    #[test]
    fn search_with_query_rejects_mismatched_query_dimensions() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        create_empty_store(&store_path).unwrap();

        let pending = prepare_raw_vector_segment(
            2,
            &[
                ("doc-1".to_owned(), vec![1.0f32, 0.0f32]),
                ("doc-2".to_owned(), vec![0.0f32, 1.0f32]),
            ],
        )
        .unwrap();
        publish_segments(&store_path, vec![pending]).unwrap();

        let mut lane = VectorLane::load_runtime(
            temp_dir.path(),
            &test_manifest(false, false),
            VectorQueryMode::ExactFlat,
        )
        .unwrap();

        let error = lane
            .search_with_query(&[1.0, 0.0, 0.5], 2, VectorQueryMode::ExactFlat, false)
            .expect_err("mismatched query dimensions should fail");

        assert!(error.contains("dimensions"));
    }

    #[test]
    fn store_backed_vector_payloads_reuse_shared_segment_bytes() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        create_empty_store(&store_path).unwrap();

        let pending = prepare_raw_vector_segment(
            2,
            &[
                ("doc-1".to_owned(), vec![1.0f32, 0.0f32]),
                ("doc-2".to_owned(), vec![0.0f32, 1.0f32]),
            ],
        )
        .unwrap();
        let published = publish_segments(&store_path, vec![pending]).unwrap();
        let descriptor = published
            .manifest
            .segments
            .iter()
            .find(|segment| segment.family == SegmentKind::Vec)
            .cloned()
            .unwrap();
        let metadata = VectorLaneMetadata {
            dimensions: 2,
            doc_count: 2,
            vector_segment: Some(StoreVectorSegment {
                store_path: store_path.clone(),
                descriptor,
                has_preview: false,
            }),
            vector_lane_skeleton_path: None,
            documents_path: None,
            document_ids_path: None,
            document_vectors_path: temp_dir.path().join("unused.bin"),
            preview_vectors_path: None,
            hnsw_graph_basename: None,
        };

        let loaded =
            load_vector_segment(&metadata, metadata.vector_segment.as_ref().unwrap()).unwrap();

        assert!(matches!(
            loaded.doc_vectors,
            ByteStorage::SegmentSlice { .. }
        ));
        assert!(loaded.preview_vectors.is_none());
    }

    #[test]
    fn vector_lane_runtime_auto_falls_back_to_exact_when_hnsw_sidecars_are_missing() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        create_empty_store(&store_path).unwrap();

        let raw_vectors = (0..65)
            .map(|index| {
                let doc_id = format!("doc-{index:03}");
                let values = if index == 17 {
                    vec![1.0f32, 0.0f32]
                } else {
                    vec![0.0f32, 1.0f32]
                };
                (doc_id, values)
            })
            .collect::<Vec<_>>();
        let pending = prepare_raw_vector_segment(2, &raw_vectors).unwrap();
        publish_segments(&store_path, vec![pending]).unwrap();

        let mut lane = VectorLane::load_runtime(
            temp_dir.path(),
            &test_manifest_with_count(65, false, true),
            VectorQueryMode::Auto,
        )
        .unwrap();

        assert_eq!(
            lane.search_with_query(&[1.0, 0.0], 3, VectorQueryMode::Auto, false)
                .unwrap()
                .first()
                .map(String::as_str),
            Some("doc-017")
        );
        assert!(!lane.is_hnsw_sidecar_materialized());
    }

    #[test]
    fn vector_lane_runtime_ignores_compatibility_hnsw_sidecars_when_store_segment_is_active() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        create_empty_store(&store_path).unwrap();
        fs::write(temp_dir.path().join("graph.hnsw.graph"), b"stale-graph").unwrap();
        fs::write(temp_dir.path().join("graph.hnsw.data"), b"stale-data").unwrap();

        let raw_vectors = (0..65)
            .map(|index| {
                let doc_id = format!("doc-{index:03}");
                let values = if index == 17 {
                    vec![1.0f32, 0.0f32]
                } else {
                    vec![0.0f32, 1.0f32]
                };
                (doc_id, values)
            })
            .collect::<Vec<_>>();
        let pending = prepare_raw_vector_segment(2, &raw_vectors).unwrap();
        publish_segments(&store_path, vec![pending]).unwrap();

        let mut lane = VectorLane::load_runtime(
            temp_dir.path(),
            &test_manifest_with_count(65, false, true),
            VectorQueryMode::Auto,
        )
        .unwrap();

        assert_eq!(
            lane.search_with_query(&[1.0, 0.0], 3, VectorQueryMode::Auto, false)
                .unwrap()
                .first()
                .map(String::as_str),
            Some("doc-017")
        );
        assert!(!lane.is_hnsw_sidecar_materialized());
    }

    #[test]
    fn store_vector_validation_allows_raw_segment_without_preview_when_sidecar_exists() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        create_empty_store(&store_path).unwrap();
        fs::write(
            temp_dir.path().join("document_ids.txt"),
            concat!("{\"doc_id\":\"doc-1\"}\n", "{\"doc_id\":\"doc-2\"}\n"),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("document_vectors.bin"),
            bytemuck::cast_slice::<f32, u8>(&[
                1.0f32, 0.0f32, //
                0.0f32, 1.0f32, //
            ]),
        )
        .unwrap();
        fs::write(temp_dir.path().join("preview.bin"), [1_u8, 0, 0, 1]).unwrap();

        let pending = prepare_raw_vector_segment(
            2,
            &[
                ("doc-1".to_owned(), vec![1.0f32, 0.0f32]),
                ("doc-2".to_owned(), vec![0.0f32, 1.0f32]),
            ],
        )
        .unwrap();
        publish_segments(&store_path, vec![pending]).unwrap();

        validate_store_segment_against_dataset_pack(
            temp_dir.path(),
            &test_manifest_with_count(2, true, false),
        )
        .unwrap();
    }

    #[test]
    fn vector_lane_runtime_rejects_stale_store_segment_when_documents_are_newer() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        create_empty_store(&store_path).unwrap();

        let vector_pending = prepare_raw_vector_segment(
            2,
            &[
                ("doc-1".to_owned(), vec![1.0f32, 0.0f32]),
                ("doc-2".to_owned(), vec![0.0f32, 1.0f32]),
            ],
        )
        .unwrap();
        publish_segments(&store_path, vec![vector_pending]).unwrap();

        let doc_pending = prepare_raw_documents_segment(
            &store_path,
            vec![
                (
                    "doc-1".to_owned(),
                    json!({"doc_id":"doc-1","text":"updated"}),
                ),
                (
                    "doc-3".to_owned(),
                    json!({"doc_id":"doc-3","text":"replacement"}),
                ),
            ],
        )
        .unwrap();
        publish_segments(&store_path, vec![doc_pending]).unwrap();

        let error = match VectorLane::load_runtime(
            temp_dir.path(),
            &test_manifest(false, false),
            VectorQueryMode::Auto,
        ) {
            Ok(_) => panic!("stale vector segment should be rejected"),
            Err(error) => error,
        };

        assert!(error.contains("stale"));
    }

    #[test]
    fn checked_vector_bytes_returns_none_for_out_of_range_hnsw_ids() {
        let temp_dir = tempdir().unwrap();
        fs::write(
            temp_dir.path().join("document_ids.txt"),
            concat!("{\"doc_id\":\"doc-1\"}\n", "{\"doc_id\":\"doc-2\"}\n",),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("document_vectors.bin"),
            bytemuck::cast_slice::<f32, u8>(&[
                1.0f32, 0.0f32, //
                0.0f32, 1.0f32, //
            ]),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("query_vectors.jsonl"),
            "{\"query_id\":\"q-001\",\"top_k\":2,\"vector\":[1.0,0.0],\"lane_eligibility\":{\"text\":false,\"vector\":true,\"hybrid\":false}}\n",
        )
        .unwrap();

        let lane = VectorLane::load(
            temp_dir.path(),
            &test_manifest_with_count(2, false, false),
            VectorQueryMode::ExactFlat,
        )
        .unwrap();

        assert!(lane.checked_vector_bytes(2).is_none());
        assert!(lane.checked_exact_hit(&[1.0, 0.0], 2).is_none());
    }

    #[test]
    fn compatibility_raw_vector_loader_ignores_persisted_store_segment_shape() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        fs::write(
            temp_dir.path().join("document_ids.txt"),
            concat!(
                "{\"doc_id\":\"doc-1\"}\n",
                "{\"doc_id\":\"doc-2\"}\n",
                "{\"doc_id\":\"doc-3\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("document_vectors.bin"),
            bytemuck::cast_slice::<f32, u8>(&[
                1.0f32, 0.0f32, //
                0.0f32, 1.0f32, //
                0.5f32, 0.5f32, //
            ]),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("query_vectors.jsonl"),
            concat!(
                "{\"query_id\":\"q-001\",\"top_k\":2,\"vector\":[1.0,0.0],\"lane_eligibility\":{\"text\":false,\"vector\":true,\"hybrid\":false}}\n",
                "{\"query_id\":\"q-002\",\"top_k\":2,\"vector\":[0.0,1.0],\"lane_eligibility\":{\"text\":true,\"vector\":true,\"hybrid\":true}}\n",
            ),
        )
        .unwrap();
        create_empty_store(&store_path).unwrap();

        let pending = prepare_raw_vector_segment(
            2,
            &[
                ("doc-1".to_owned(), vec![1.0f32, 0.0f32]),
                ("doc-2".to_owned(), vec![0.0f32, 1.0f32]),
            ],
        )
        .unwrap();
        publish_segments(&store_path, vec![pending]).unwrap();

        let raw_vectors =
            load_compatibility_raw_vectors(temp_dir.path(), &test_manifest(false, false)).unwrap();

        assert_eq!(raw_vectors.len(), 3);
        assert_eq!(raw_vectors[0].0, "doc-1");
        assert_eq!(raw_vectors[1].0, "doc-2");
        assert_eq!(raw_vectors[2].0, "doc-3");
    }

    #[test]
    fn compatibility_raw_vector_loader_rejects_document_id_count_mismatch() {
        let temp_dir = tempdir().unwrap();
        fs::write(
            temp_dir.path().join("document_ids.txt"),
            concat!(
                "{\"doc_id\":\"doc-1\"}\n",
                "{\"doc_id\":\"doc-2\"}\n",
                "{\"doc_id\":\"doc-3\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("document_vectors.bin"),
            bytemuck::cast_slice::<f32, u8>(&[
                1.0f32, 0.0f32, //
                0.0f32, 1.0f32, //
            ]),
        )
        .unwrap();

        let error = load_compatibility_raw_vectors(
            temp_dir.path(),
            &test_manifest_with_count(2, false, false),
        )
        .unwrap_err();

        assert!(error.contains("document_ids row count does not match manifest vector_count"));
    }

    #[test]
    fn compatibility_raw_vector_loader_uses_manifest_documents_path_when_document_ids_missing() {
        let temp_dir = tempdir().unwrap();
        let docs_dir = temp_dir.path().join("corpus");
        fs::create_dir_all(&docs_dir).unwrap();
        fs::write(
            docs_dir.join("documents.ndjson"),
            concat!(
                "{\"doc_id\":\"doc-1\",\"text\":\"first\"}\n",
                "{\"doc_id\":\"doc-2\",\"text\":\"second\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("document_vectors.bin"),
            bytemuck::cast_slice::<f32, u8>(&[
                1.0f32, 0.0f32, //
                0.0f32, 1.0f32, //
            ]),
        )
        .unwrap();

        let raw_vectors = load_compatibility_raw_vectors(
            temp_dir.path(),
            &test_manifest_without_document_ids("corpus/documents.ndjson", 2),
        )
        .unwrap();

        assert_eq!(raw_vectors[0].0, "doc-1");
        assert_eq!(raw_vectors[1].0, "doc-2");
    }

    #[test]
    fn binary_vector_segment_aligns_exact_vector_payloads_to_four_bytes() {
        let bytes = BinaryVectorSegment::from_raw_vectors(
            3,
            &[("doc-1".to_owned(), vec![1.0f32, 0.0f32, 0.5f32])],
        )
        .unwrap()
        .encode()
        .unwrap();

        let exact_vectors_offset = read_u64(&bytes, 32) as usize;

        assert_eq!(exact_vectors_offset % 4, 0);
        assert!(BinaryVectorSegment::decode(&bytes).is_ok());
    }

    fn test_manifest(with_preview: bool, with_hnsw: bool) -> DatasetPackManifest {
        test_manifest_with_count(3, with_preview, with_hnsw)
    }

    fn test_manifest_with_count(
        doc_count: usize,
        with_preview: bool,
        with_hnsw: bool,
    ) -> DatasetPackManifest {
        let mut files = vec![
            json!({"path":"document_ids.txt","kind":"document_ids","format":"text","record_count":doc_count,"checksum":"sha256:docids"}),
            json!({"path":"document_vectors.bin","kind":"document_vectors","format":"f32le","record_count":doc_count,"checksum":"sha256:vec"}),
            json!({"path":"query_vectors.jsonl","kind":"query_vectors","format":"jsonl","record_count":2,"checksum":"sha256:qvec"}),
        ];
        if with_preview {
            files.push(json!({"path":"preview.bin","kind":"document_vectors_preview_q8","format":"i8","record_count":doc_count,"checksum":"sha256:preview"}));
        }
        if with_hnsw {
            files.push(json!({"path":"graph.hnsw.graph","kind":"vector_hnsw_graph","format":"hnsw","record_count":doc_count,"checksum":"sha256:hnsw"}));
        }
        serde_json::from_value(json!({
            "schema_version": "wax_dataset_pack_v1",
            "generated_at": "2026-04-19T00:00:00Z",
            "generator": {"name":"test","version":"0.1.0"},
            "identity": {
                "dataset_id":"knowledge-small-clean-v1",
                "dataset_version":"v1",
                "dataset_family":"knowledge",
                "dataset_tier":"small",
                "variant_id":"clean",
                "embedding_spec_id":"minilm-l6-384-f32-cosine",
                "embedding_model_version":"test",
                "embedding_model_hash":"sha256:model",
                "corpus_checksum":"sha256:corpus",
                "query_checksum":"sha256:query"
            },
            "environment_constraints": {"min_ram_gb":1,"recommended_ram_gb":1},
            "corpus": {
                "doc_count":doc_count,
                "vector_count":doc_count,
                "total_text_bytes":9,
                "avg_doc_length":3.0,
                "median_doc_length":3,
                "p95_doc_length":3,
                "max_doc_length":3,
                "languages":[{"code":"en","ratio":1.0}]
            },
            "text_profile": {
                "length_buckets":{"short_ratio":1.0,"medium_ratio":0.0,"long_ratio":0.0}
            },
            "metadata_profile": {
                "facets":[],
                "selectivity_exemplars":{
                    "broad":"*",
                    "medium":"kind = note",
                    "narrow":"kind = task",
                    "zero_hit":"kind = missing"
                }
            },
            "vector_profile": {
                "enabled": true,
                "embedding_dimensions": 2,
                "embedding_dtype":"f32",
                "distance_metric":"cosine",
                "query_vectors":{"precomputed_available":true,"runtime_embedding_supported":false}
            },
            "dirty_profile": {
                "profile":"clean",
                "seed":0,
                "delete_ratio":0.0,
                "update_ratio":0.0,
                "append_ratio":0.0,
                "target_segment_count_range":[1,1],
                "target_segment_topology":[],
                "target_tombstone_ratio":0.0,
                "compaction_state":"clean"
            },
            "files": files,
            "query_sets": [
                {
                    "query_set_id":"core",
                    "path":"queries.jsonl",
                    "ground_truth_path":"ground_truth.jsonl",
                    "query_count":2,
                    "classes":["vector","hybrid"],
                    "difficulty_distribution":{"easy":1,"medium":1,"hard":0}
                }
            ],
            "checksums": {
                "manifest_payload_checksum":"sha256:manifest",
                "logical_documents_checksum":"sha256:documents",
                "logical_metadata_checksum":"sha256:meta",
                "logical_query_definitions_checksum":"sha256:logical-query",
                "logical_vector_payload_checksum":"sha256:vector",
                "fairness_fingerprint":"sha256:fair"
            }
        }))
        .unwrap()
    }

    fn test_manifest_without_document_ids(
        documents_path: &str,
        doc_count: usize,
    ) -> DatasetPackManifest {
        let mut value =
            serde_json::to_value(test_manifest_with_count(doc_count, false, false)).unwrap();
        let files = value["files"].as_array_mut().unwrap();
        files.retain(|file| file["kind"] != "document_ids");
        files.push(json!({
            "path": documents_path,
            "kind": "documents",
            "format": "jsonl",
            "record_count": doc_count,
            "checksum": "sha256:docs"
        }));
        serde_json::from_value(value).unwrap()
    }
}
