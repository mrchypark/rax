mod documents;
mod query_support;
mod vector_lane;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::Serialize;
use serde_json::Value;
use wax_bench_model::{
    parse_benchmark_query, tokenize, BenchmarkQuery, DatasetPackManifest, EnginePhase, EngineStats,
    MountRequest, OpenRequest, OpenResult, RankedDocumentHit, RankedQueryResult, SearchRequest,
    SearchResult, VectorQueryMode, WaxEngine,
};

use crate::documents::{
    document_offsets_path, load_document_offset_index, load_documents_by_id,
    materialize_document_previews, DocumentOffsetEntry,
};
use crate::query_support::{
    load_first_hybrid_text_query, load_first_text_query, load_query_records,
    load_query_vector_records, load_text_postings, search_first_hybrid_query, search_query_hybrid,
};
use crate::vector_lane::{elapsed_ms, VectorLane};
const RRF_K: f64 = 60.0;

pub struct PackedTextEngine {
    mounted_path: Option<PathBuf>,
    phase: EnginePhase,
    manifest: Option<DatasetPackManifest>,
    text_lane: Option<TextLane>,
    vector_lane: Option<VectorLane>,
    preview_store: Option<HashMap<String, Value>>,
    document_offset_path: Option<PathBuf>,
    document_offset_index: Option<HashMap<String, DocumentOffsetEntry>>,
    vector_mode: VectorQueryMode,
    auto_vector_query_pending: bool,
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
            document_offset_path: None,
            document_offset_index: None,
            vector_mode,
            auto_vector_query_pending: true,
        }
    }

    pub fn is_text_lane_materialized(&self) -> bool {
        self.text_lane.is_some()
    }

    pub fn is_vector_lane_materialized(&self) -> bool {
        self.vector_lane.is_some()
    }

    pub fn is_vector_hnsw_sidecar_materialized(&self) -> bool {
        self.vector_lane
            .as_ref()
            .is_some_and(VectorLane::is_hnsw_sidecar_materialized)
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

    fn ensure_vector_lane(&mut self) -> Result<&mut VectorLane, String> {
        if self.vector_lane.is_none() {
            let mount_root = self.mount_root()?.to_path_buf();
            let manifest = self.manifest()?.clone();
            self.vector_lane = Some(VectorLane::load(&mount_root, &manifest, self.vector_mode)?);
        }
        self.vector_lane
            .as_mut()
            .ok_or_else(|| "vector lane not materialized".to_owned())
    }

    fn should_force_exact_on_auto_vector_query(&self) -> bool {
        matches!(self.vector_mode, VectorQueryMode::Auto) && self.auto_vector_query_pending
    }

    fn note_vector_query_executed(&mut self) {
        if matches!(self.vector_mode, VectorQueryMode::Auto) {
            self.auto_vector_query_pending = false;
        }
    }

    fn ensure_preview_store(&mut self) -> Result<&mut HashMap<String, Value>, String> {
        if self.preview_store.is_none() {
            self.preview_store = Some(HashMap::new());
        }
        self.preview_store
            .as_mut()
            .ok_or_else(|| "preview store not materialized".to_owned())
    }

    fn ensure_previews_for_hits(&mut self, doc_ids: &[String]) -> Result<(), String> {
        let docs_path = self.mount_root()?.join("docs.ndjson");
        if self.document_offset_index.is_none() {
            if let Some(path) = self.document_offset_path.clone() {
                self.document_offset_index = Some(load_document_offset_index(&path)?);
            }
        }
        let offset_index = self.document_offset_index.as_ref().map(|index| {
            doc_ids
                .iter()
                .filter_map(|doc_id| {
                    index
                        .get(doc_id.as_str())
                        .cloned()
                        .map(|entry| (doc_id.clone(), entry))
                })
                .collect::<HashMap<_, _>>()
        });
        let preview_store = self.ensure_preview_store()?;
        materialize_document_previews(preview_store, &docs_path, offset_index.as_ref(), doc_ids)
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FirstVectorQueryProfile {
    pub selected_mode: VectorQueryMode,
    pub doc_count: usize,
    pub top_k: usize,
    pub vector_lane_load_ms: f64,
    pub hnsw_sidecar_load_ms: Option<f64>,
    pub total_search_ms: f64,
    pub exact_scan_ms: Option<f64>,
    pub approximate_search_ms: Option<f64>,
    pub rerank_ms: Option<f64>,
    pub candidate_count: usize,
    pub hits: Vec<String>,
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
    let offset_index = document_offsets_path(dataset_path, &manifest)
        .map(|path| load_document_offset_index(&path))
        .transpose()?;
    let documents = load_documents_by_id(
        &dataset_path.join("docs.ndjson"),
        offset_index.as_ref(),
        &doc_ids,
    )?;
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
    let mut vector_lane = VectorLane::load(dataset_path, &manifest, vector_mode)?;
    let query_vectors = load_query_vector_records(query_set_path, vector_lane.dimensions)?;
    let queries = load_query_records(query_set_path)?;

    if queries.len() != query_vectors.len() {
        return Err("query_set and query vector records must align".to_owned());
    }

    let mut auto_vector_query_pending = matches!(vector_mode, VectorQueryMode::Auto);
    queries
        .into_iter()
        .zip(query_vectors)
        .map(|(query, vector_record)| {
            let limit = query.top_k as usize;
            let uses_vector_lane = query.lane_eligibility.hybrid
                || (query.lane_eligibility.vector && !query.lane_eligibility.text);
            let force_exact = auto_vector_query_pending && uses_vector_lane;
            let hits = if query.lane_eligibility.hybrid {
                search_query_hybrid(
                    &text_lane,
                    &mut vector_lane,
                    &query.query_text,
                    &vector_record.vector,
                    limit,
                    vector_mode,
                    force_exact,
                )?
            } else if query.lane_eligibility.vector && !query.lane_eligibility.text {
                vector_lane.search_with_query(
                    &vector_record.vector,
                    limit,
                    vector_mode,
                    force_exact,
                )?
            } else {
                text_lane.search_with_limit(&query.query_text, limit)
            };
            if uses_vector_lane {
                auto_vector_query_pending = false;
            }

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

pub fn profile_first_vector_query(
    dataset_path: &Path,
    vector_mode: VectorQueryMode,
) -> Result<FirstVectorQueryProfile, String> {
    let manifest_text = fs::read_to_string(dataset_path.join("manifest.json"))
        .map_err(|error| error.to_string())?;
    let manifest: DatasetPackManifest =
        serde_json::from_str(&manifest_text).map_err(|error| error.to_string())?;
    let load_start = Instant::now();
    let (vector_lane, hnsw_sidecar_load_ms) =
        VectorLane::load_with_report(dataset_path, &manifest, vector_mode)?;
    let vector_lane_load_ms = elapsed_ms(load_start.elapsed());
    let search_profile = vector_lane.profile_first_vector_query(vector_mode);

    Ok(FirstVectorQueryProfile {
        selected_mode: search_profile.selected_mode,
        doc_count: vector_lane.skeleton_header.doc_count as usize,
        top_k: vector_lane.first_vector_top_k,
        vector_lane_load_ms,
        hnsw_sidecar_load_ms,
        total_search_ms: search_profile.total_search_ms,
        exact_scan_ms: search_profile.exact_scan_ms,
        approximate_search_ms: search_profile.approximate_search_ms,
        rerank_ms: search_profile.rerank_ms,
        candidate_count: search_profile.candidate_count,
        hits: search_profile.hits,
    })
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
        self.document_offset_path = None;
        self.document_offset_index = None;
        self.auto_vector_query_pending = true;
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

        self.document_offset_path = document_offsets_path(self.mount_root()?, &manifest);
        self.document_offset_index = None;
        self.manifest = Some(manifest);
        self.phase = EnginePhase::Open;
        self.auto_vector_query_pending = true;
        Ok(OpenResult)
    }

    fn search(&mut self, request: SearchRequest) -> Result<SearchResult, Self::Error> {
        if self.phase != EnginePhase::Open {
            return Err("engine must be open before search".to_owned());
        }

        if let Some(query) = parse_benchmark_query(&request.query_text) {
            match query {
                BenchmarkQuery::MaterializeTextLane => {
                    self.ensure_text_lane()?;
                    return Ok(SearchResult { hits: Vec::new() });
                }
                BenchmarkQuery::MaterializeVectorLane => {
                    self.ensure_vector_lane()?;
                    return Ok(SearchResult { hits: Vec::new() });
                }
                BenchmarkQuery::TtfqVector
                | BenchmarkQuery::WarmupVector
                | BenchmarkQuery::WarmVector => {
                    let vector_mode = self.vector_mode;
                    let force_exact = self.should_force_exact_on_auto_vector_query();
                    let hits = {
                        let lane = self.ensure_vector_lane()?;
                        lane.search_first_vector_query(vector_mode, force_exact)?
                    };
                    if matches!(query, BenchmarkQuery::WarmupVector) {
                        let lane = self.ensure_vector_lane()?;
                        lane.prime_followup_mode_for_first_vector_query(vector_mode)?;
                    }
                    self.note_vector_query_executed();
                    return Ok(SearchResult { hits });
                }
                BenchmarkQuery::TtfqHybrid
                | BenchmarkQuery::WarmupHybrid
                | BenchmarkQuery::WarmHybrid
                | BenchmarkQuery::WarmupHybridWithPreviews
                | BenchmarkQuery::WarmHybridWithPreviews => {
                    self.ensure_text_lane()?;
                    self.ensure_vector_lane()?;
                    let force_exact = self.should_force_exact_on_auto_vector_query();
                    let hits = {
                        let text_lane = self
                            .text_lane
                            .as_ref()
                            .ok_or_else(|| "text lane not materialized".to_owned())?;
                        let vector_lane = self
                            .vector_lane
                            .as_mut()
                            .ok_or_else(|| "vector lane not materialized".to_owned())?;
                        search_first_hybrid_query(
                            text_lane,
                            vector_lane,
                            self.vector_mode,
                            force_exact,
                        )?
                    };
                    if matches!(
                        query,
                        BenchmarkQuery::WarmupHybrid | BenchmarkQuery::WarmupHybridWithPreviews
                    ) {
                        let vector_lane = self
                            .vector_lane
                            .as_mut()
                            .ok_or_else(|| "vector lane not materialized".to_owned())?;
                        vector_lane.prime_followup_mode_for_first_hybrid_query(self.vector_mode)?;
                    }
                    self.note_vector_query_executed();
                    if matches!(
                        query,
                        BenchmarkQuery::WarmupHybridWithPreviews
                            | BenchmarkQuery::WarmHybridWithPreviews
                    ) {
                        self.ensure_previews_for_hits(&hits)?;
                    }
                    return Ok(SearchResult { hits });
                }
                BenchmarkQuery::TtfqText | BenchmarkQuery::WarmText => {}
            }
        }

        let lane = self.ensure_text_lane()?;
        let hits = if matches!(
            parse_benchmark_query(&request.query_text),
            Some(BenchmarkQuery::TtfqText | BenchmarkQuery::WarmText)
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

#[cfg(test)]
mod tests {
    use wax_bench_model::VectorQueryMode;

    use crate::documents::parse_document_id;
    use crate::vector_lane::resolve_auto_vector_mode;

    #[test]
    fn auto_mode_prefers_exact_flat_for_small_corpora() {
        assert_eq!(
            resolve_auto_vector_mode(31, 10, true, true),
            VectorQueryMode::ExactFlat
        );
        assert_eq!(
            resolve_auto_vector_mode(64, 1, true, false),
            VectorQueryMode::ExactFlat
        );
    }

    #[test]
    fn auto_mode_switches_to_hnsw_once_doc_count_exceeds_cutoff() {
        assert_eq!(
            resolve_auto_vector_mode(65, 1, true, true),
            VectorQueryMode::Hnsw
        );
        assert_eq!(
            resolve_auto_vector_mode(81, 10, true, false),
            VectorQueryMode::Hnsw
        );
        assert_eq!(
            resolve_auto_vector_mode(200, 100, true, false),
            VectorQueryMode::Hnsw
        );
    }

    #[test]
    fn auto_mode_uses_preview_when_hnsw_is_missing_on_large_corpus() {
        assert_eq!(
            resolve_auto_vector_mode(65, 1, false, true),
            VectorQueryMode::PreviewQ8
        );
    }

    #[test]
    fn auto_mode_uses_exact_flat_at_inclusive_cutoff_boundary() {
        assert_eq!(
            resolve_auto_vector_mode(64, 1, true, true),
            VectorQueryMode::ExactFlat
        );
        assert_eq!(
            resolve_auto_vector_mode(64, 1, false, true),
            VectorQueryMode::ExactFlat
        );
    }

    #[test]
    fn parse_document_id_handles_whitespace_and_escaped_quotes() {
        let line = "  {\"text\":\"escaped\",\"doc_id\":\"doc-\\\"001\"}  ";
        let doc_id = parse_document_id(line, "document line").unwrap();

        assert_eq!(doc_id.as_ref(), "doc-\"001");
    }

    #[test]
    fn parse_document_id_ignores_doc_id_looking_text_inside_other_fields() {
        let line = "{\"doc_id\":\"doc-001\",\"text\":\"fake key: \\\"doc_id\\\":\\\"wrong\\\"\"}";
        let doc_id = parse_document_id(line, "document line").unwrap();

        assert_eq!(doc_id.as_ref(), "doc-001");
    }

    #[test]
    fn parse_document_id_rejects_missing_doc_id() {
        let error = parse_document_id("{\"text\":\"missing\"}", "document line").unwrap_err();

        assert_eq!(error, "document line missing doc_id");
    }

    #[test]
    fn parse_document_id_rejects_non_string_doc_id() {
        assert!(parse_document_id("{\"doc_id\":1}", "document line").is_err());
    }
}
