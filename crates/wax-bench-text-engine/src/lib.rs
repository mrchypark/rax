mod documents;
mod query_support;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::Serialize;
use serde_json::Value;
use wax_bench_model::{
    parse_benchmark_query, BenchmarkQuery, DatasetPackManifest, EnginePhase, EngineStats,
    MountRequest, OpenRequest, OpenResult, RankedDocumentHit, RankedQueryResult, SearchRequest,
    SearchResult, VectorQueryMode, WaxEngine,
};
use wax_v2_docstore::Docstore;
use wax_v2_search::{
    filter_hits_by_metadata, hybrid_search_with_diagnostics, search_first_hybrid_query,
    MetadataFilter, MetadataSource,
};
use wax_v2_text::{TextBatchQuery, TextLane};
use wax_v2_vector::{elapsed_ms, VectorLane};

use crate::documents::{
    load_documents_by_id, open_docstore, validate_store_segments_against_dataset_pack,
};
use crate::query_support::load_query_vector_records;

pub struct PackedTextEngine {
    mounted_path: Option<PathBuf>,
    phase: EnginePhase,
    manifest: Option<DatasetPackManifest>,
    text_lane: Option<TextLane>,
    vector_lane: Option<VectorLane>,
    docstore: Option<Docstore>,
    preview_store: Option<HashMap<String, Value>>,
    vector_mode: VectorQueryMode,
    auto_vector_query_pending: bool,
}

struct JsonDocumentMetadata<'a> {
    documents: &'a HashMap<String, Value>,
}

impl MetadataSource for JsonDocumentMetadata<'_> {
    fn field_value(&self, doc_id: &str, field: &str) -> Option<&str> {
        self.documents.get(doc_id)?.get(field)?.as_str()
    }
}

impl PackedTextEngine {
    pub fn with_vector_mode(vector_mode: VectorQueryMode) -> Self {
        Self {
            mounted_path: None,
            phase: EnginePhase::New,
            manifest: None,
            text_lane: None,
            vector_lane: None,
            docstore: None,
            preview_store: None,
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

    fn core_store_path(&self) -> Result<PathBuf, String> {
        Ok(self.mount_root()?.join("store.wax"))
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
        let missing = self
            .preview_store
            .as_ref()
            .map(|documents| {
                doc_ids
                    .iter()
                    .filter(|doc_id| !documents.contains_key(doc_id.as_str()))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| doc_ids.to_vec());
        let loaded = if missing.is_empty() {
            HashMap::new()
        } else {
            let docstore = self
                .docstore
                .as_ref()
                .ok_or_else(|| "docstore not materialized".to_owned())?;
            load_documents_by_id(docstore, &missing)?
        };
        let preview_store = self.ensure_preview_store()?;
        for (doc_id, document) in loaded {
            preview_store.insert(doc_id, document);
        }
        for doc_id in doc_ids {
            let document = preview_store
                .get(doc_id)
                .ok_or_else(|| format!("document missing for hit doc_id: {doc_id}"))?;
            document
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("text missing for hit doc_id: {doc_id}"))?;
        }
        Ok(())
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
        validate_store_segments_against_dataset_pack(dataset_path, &manifest)?;
        let text_lane = TextLane::load(dataset_path, &manifest)?;
        let doc_ids = text_lane.search_with_limit(query_text, top_k);
        let docstore = open_docstore(dataset_path, &manifest)?;
    let documents = load_documents_by_id(&docstore, &doc_ids)?;
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
        validate_store_segments_against_dataset_pack(dataset_path, &manifest)?;
        let text_lane = TextLane::load(dataset_path, &manifest)?;
        let mut vector_lane = VectorLane::load(dataset_path, &manifest, vector_mode)?;
    let queries = TextBatchQuery::load_jsonl(query_set_path)?;
    let filter_candidate_limit = manifest.corpus.doc_count as usize;
    let query_vectors = load_query_vector_records(&queries, vector_lane.dimensions)?;
    let docstore = if queries.iter().any(|query| !query.filter_spec.is_empty()) {
        Some(open_docstore(dataset_path, &manifest)?)
    } else {
        None
    };
    let text_hits_by_query = text_lane
        .search_batch(
            &queries
                .iter()
                .filter(|query| query.uses_text_lane())
                .cloned()
                .map(|mut query| {
                    if !query.filter_spec.is_empty() {
                        query.top_k = filter_candidate_limit.max(query.top_k);
                    }
                    query
                })
                .collect::<Vec<_>>(),
        )
        .into_iter()
        .map(|result| (result.query_id, result.hits))
        .collect::<HashMap<_, _>>();

    if queries.len() != query_vectors.len() {
        return Err("query_set and query vector records must align".to_owned());
    }

    let mut auto_vector_query_pending = matches!(vector_mode, VectorQueryMode::Auto);
    queries
        .into_iter()
        .zip(query_vectors)
        .map(|(query, vector_record)| {
            let limit = query.top_k;
            let search_limit = if query.filter_spec.is_empty() {
                limit
            } else {
                filter_candidate_limit.max(limit)
            };
            let uses_vector_lane = query.lane_eligibility.hybrid
                || (query.lane_eligibility.vector && !query.lane_eligibility.text);
            let force_exact = auto_vector_query_pending && uses_vector_lane;
            let hits = if query.lane_eligibility.hybrid {
                let text_hits = text_hits_by_query
                    .get(&query.query_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!("text hits missing for hybrid query_id: {}", query.query_id)
                    })?;
                let report = hybrid_search_with_diagnostics(
                    &text_hits,
                    &mut vector_lane,
                    &vector_record.vector,
                    search_limit,
                    vector_mode,
                    force_exact,
                )?;
                report.fused_hits
            } else if query.lane_eligibility.vector && !query.lane_eligibility.text {
                vector_lane.search_with_query(
                    &vector_record.vector,
                    search_limit,
                    vector_mode,
                    force_exact,
                )?
            } else {
                text_hits_by_query
                    .get(&query.query_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!("text hits missing for text query_id: {}", query.query_id)
                    })?
            };
            let hits: Vec<String> = if query.filter_spec.is_empty() {
                hits.into_iter().take(limit).collect()
            } else {
                let docstore = docstore
                    .as_ref()
                    .ok_or_else(|| "docstore not available for metadata filtering".to_owned())?;
                let documents = load_documents_by_id(docstore, &hits)?;
                let filter = MetadataFilter::from_pairs(query.filter_spec.equals.iter().cloned());
                let metadata = JsonDocumentMetadata {
                    documents: &documents,
                };
                filter_hits_by_metadata(&hits, &metadata, &filter)
                    .into_iter()
                    .take(limit)
                    .collect()
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
    validate_store_segments_against_dataset_pack(dataset_path, &manifest)?;
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
        self.docstore = None;
        self.preview_store = None;
        self.auto_vector_query_pending = true;
        Ok(())
    }

    fn open(&mut self, _request: OpenRequest) -> Result<OpenResult, Self::Error> {
        if self.phase != EnginePhase::Mounted {
            return Err("engine must be mounted before open".to_owned());
        }

        let core_store_path = self.core_store_path()?;
        if core_store_path.exists() {
            wax_v2_core::open_store(&core_store_path)
                .map_err(|error| format!("core store open failed: {error}"))?;
        }

        let manifest_text = fs::read_to_string(self.mount_root()?.join("manifest.json"))
            .map_err(|error| error.to_string())?;
        let manifest: DatasetPackManifest =
            serde_json::from_str(&manifest_text).map_err(|error| error.to_string())?;
        validate_store_segments_against_dataset_pack(self.mount_root()?, &manifest)?;

        self.docstore = Some(open_docstore(self.mount_root()?, &manifest)?);
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

#[cfg(test)]
mod tests {
    use wax_bench_model::VectorQueryMode;
    use wax_v2_docstore::parse_document_id;
    use wax_v2_vector::resolve_auto_vector_mode;

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
