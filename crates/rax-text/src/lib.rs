use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};

use rax_bench_model::{tokenize as simple_tokenize, DatasetPackManifest};
use rax_core::{PendingSegmentDescriptor, PendingSegmentWrite, SegmentDescriptor, SegmentKind};
use serde::Deserialize;
use serde_json::Value;

const TEXT_SEGMENT_MAGIC: &[u8; 4] = b"RXTG";
const TEXT_SEGMENT_MAJOR: u16 = 2;
const TEXT_SEGMENT_MINOR: u16 = 0;
const TEXT_SEGMENT_HEADER_LENGTH: usize = 16;
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;
const CURRENT_ANALYZER_NAME: &str = "rax-simple-alnum-lower";
const LEGACY_ANALYZER_NAME: &str = "rax-simple-alnum-lower-legacy";
const EXPERIMENTAL_ALYZE_ANALYZER_NAME: &str = "rax-alyze-uax29-ascii-fold";
const CURRENT_ANALYZER_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextLane {
    first_text_query: String,
    first_text_top_k: usize,
    first_hybrid_query: Option<String>,
    first_hybrid_top_k: usize,
    index: TextIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TextIndex {
    analyzer_profile: TextAnalyzerProfile,
    inverted: HashMap<String, Vec<TextPosting>>,
    doc_lengths: HashMap<String, u32>,
    doc_count: usize,
    total_doc_length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TextPosting {
    doc_id: String,
    term_frequency: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TextAnalyzerProfile {
    name: String,
    version: u32,
}

impl TextAnalyzerProfile {
    fn current() -> Self {
        Self {
            name: CURRENT_ANALYZER_NAME.to_owned(),
            version: CURRENT_ANALYZER_VERSION,
        }
    }

    fn legacy_v1() -> Self {
        Self {
            name: LEGACY_ANALYZER_NAME.to_owned(),
            version: CURRENT_ANALYZER_VERSION,
        }
    }

    fn experimental_alyze_v1() -> Self {
        Self {
            name: EXPERIMENTAL_ALYZE_ANALYZER_NAME.to_owned(),
            version: CURRENT_ANALYZER_VERSION,
        }
    }

    fn is_supported(&self) -> bool {
        self.version == CURRENT_ANALYZER_VERSION
            && matches!(
                self.name.as_str(),
                CURRENT_ANALYZER_NAME | LEGACY_ANALYZER_NAME | EXPERIMENTAL_ALYZE_ANALYZER_NAME
            )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextSearchDiagnostic {
    pub doc_id: String,
    pub score: f64,
    pub doc_length: u32,
    pub terms: Vec<TextSearchTermDiagnostic>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextSearchTermDiagnostic {
    pub token: String,
    pub term_frequency: u32,
    pub document_frequency: usize,
    pub idf: f64,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextBatchQuery {
    pub query_id: String,
    pub query_text: String,
    pub top_k: usize,
    pub lane_eligibility: TextLaneEligibility,
    pub filter_spec: TextFilterSpec,
}

impl TextBatchQuery {
    pub fn load_jsonl(path: &Path) -> Result<Vec<Self>, String> {
        BufReader::new(open_read_no_symlinks(path)?)
            .lines()
            .filter_map(|line| match line {
                Ok(line) if line.trim().is_empty() => None,
                other => Some(other),
            })
            .map(|line| {
                let line = line.map_err(|error| error.to_string())?;
                let query: QueryRecord =
                    serde_json::from_str(&line).map_err(|error| error.to_string())?;
                Ok(Self {
                    query_id: query.query_id,
                    query_text: query.query_text,
                    top_k: query.top_k as usize,
                    lane_eligibility: TextLaneEligibility {
                        text: query.lane_eligibility.text,
                        vector: query.lane_eligibility.vector,
                        hybrid: query.lane_eligibility.hybrid,
                    },
                    filter_spec: TextFilterSpec::from_json_map(query.filter_spec),
                })
            })
            .collect()
    }

    pub fn uses_text_lane(&self) -> bool {
        self.lane_eligibility.text || self.lane_eligibility.hybrid
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextBatchResult {
    pub query_id: String,
    pub hits: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TextFilterSpec {
    pub equals: Vec<(String, String)>,
}

impl TextFilterSpec {
    fn from_json_map(filter_spec: serde_json::Map<String, serde_json::Value>) -> Self {
        Self {
            equals: filter_spec
                .into_iter()
                .filter_map(|(field, value)| value.as_str().map(|value| (field, value.to_owned())))
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.equals.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextLaneEligibility {
    pub text: bool,
    pub vector: bool,
    pub hybrid: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TextLaneMetadata {
    source: TextLaneSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TextLaneSource {
    Compatibility {
        postings_path: PathBuf,
    },
    Store {
        store_path: PathBuf,
        descriptor: SegmentDescriptor,
    },
}

impl TextLaneMetadata {
    fn resolve(mount_root: &Path, manifest: &DatasetPackManifest) -> Result<Self, String> {
        let store_path = store_path_from_manifest(mount_root, manifest)?;
        Self::resolve_with_store_path(mount_root, manifest, &store_path)
    }

    fn resolve_with_store_path(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        store_path: &Path,
    ) -> Result<Self, String> {
        if store_path.exists() {
            let opened = rax_core::open_store(store_path).map_err(|error| error.to_string())?;
            let latest_doc_descriptor = opened
                .manifest
                .segments
                .iter()
                .filter(|segment| segment.family == SegmentKind::Doc)
                .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
                .cloned();
            if let Some(descriptor) = opened
                .manifest
                .segments
                .iter()
                .filter(|segment| segment.family == SegmentKind::Txt)
                .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
                .cloned()
            {
                let Some(doc_descriptor) = latest_doc_descriptor.as_ref() else {
                    return Err(
                        "store-backed text segment requires a matching document segment; republish documents and text before runtime text search"
                            .to_owned(),
                    );
                };
                if descriptor.segment_generation < doc_descriptor.segment_generation {
                    return Err(
                        "latest text segment is stale relative to the current document generation; republish text before runtime text search"
                            .to_owned(),
                    );
                }
                if !same_active_doc_coverage(&descriptor, doc_descriptor) {
                    return Err(
                        "latest text segment does not cover the active document segment; republish text before runtime text search"
                            .to_owned(),
                    );
                }
                validate_text_segment_against_store_doc_descriptor(
                    store_path,
                    &descriptor,
                    doc_descriptor,
                )?;
                return Ok(Self {
                    source: TextLaneSource::Store {
                        store_path: store_path.to_path_buf(),
                        descriptor,
                    },
                });
            }
            if latest_doc_descriptor.is_some() {
                return Err(
                    "current store generation has manifest-visible documents but no matching text segment; publish text before runtime text search"
                        .to_owned(),
                );
            }
        }

        Ok(Self {
            source: TextLaneSource::Compatibility {
                postings_path: compatibility_postings_path(mount_root, manifest)?,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TextQueryInputs {
    query_paths: Vec<PathBuf>,
}

impl TextQueryInputs {
    fn resolve(mount_root: &Path, manifest: &DatasetPackManifest) -> Result<Self, String> {
        let query_paths = manifest
            .query_sets
            .iter()
            .map(|query_set| manifest_query_set_path(mount_root, &query_set.path))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { query_paths })
    }
}

impl TextLane {
    pub fn load(mount_root: &Path, manifest: &DatasetPackManifest) -> Result<Self, String> {
        let metadata = TextLaneMetadata::resolve(mount_root, manifest)?;
        Self::load_with_metadata(mount_root, manifest, metadata)
    }

    pub fn load_with_store_path(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        store_path: &Path,
    ) -> Result<Self, String> {
        let metadata = TextLaneMetadata::resolve_with_store_path(mount_root, manifest, store_path)?;
        Self::load_with_metadata(mount_root, manifest, metadata)
    }

    fn load_with_metadata(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        metadata: TextLaneMetadata,
    ) -> Result<Self, String> {
        let query_inputs = TextQueryInputs::resolve(mount_root, manifest)?;
        let (first_text_query, first_text_top_k) =
            load_first_text_query(&query_inputs.query_paths)?;
        let first_hybrid_query = load_first_hybrid_text_query(&query_inputs.query_paths)?;
        let index = load_text_index(&metadata)?;

        Ok(Self {
            first_text_query,
            first_text_top_k,
            first_hybrid_query: first_hybrid_query
                .as_ref()
                .map(|query| query.query_text.clone()),
            first_hybrid_top_k: first_hybrid_query.map(|query| query.top_k).unwrap_or(0),
            index,
        })
    }

    pub fn first_hybrid_query(&self) -> Option<&str> {
        self.first_hybrid_query.as_deref()
    }

    pub fn first_hybrid_top_k(&self) -> usize {
        self.first_hybrid_top_k
    }

    pub fn search_first_text_query(&self) -> Vec<String> {
        self.search_with_limit(&self.first_text_query, self.first_text_top_k)
    }

    pub fn search(&self, query: &str) -> Vec<String> {
        self.search_with_limit(query, usize::MAX)
    }

    pub fn search_batch(&self, queries: &[TextBatchQuery]) -> Vec<TextBatchResult> {
        queries
            .iter()
            .map(|query| TextBatchResult {
                query_id: query.query_id.clone(),
                hits: self.search_with_limit(&query.query_text, query.top_k),
            })
            .collect()
    }

    pub fn search_with_limit(&self, query: &str, limit: usize) -> Vec<String> {
        self.search_with_diagnostics(query, limit)
            .into_iter()
            .map(|hit| hit.doc_id)
            .collect()
    }

    pub fn search_with_diagnostics(&self, query: &str, limit: usize) -> Vec<TextSearchDiagnostic> {
        if limit == 0 || self.index.doc_count == 0 {
            return Vec::new();
        }

        let average_doc_length = self.index.average_doc_length();
        let mut scores: HashMap<String, TextSearchAccumulator> = HashMap::new();
        for token in analyze_text(&self.index.analyzer_profile, query) {
            if let Some(postings) = self.index.inverted.get(&token) {
                let document_frequency = postings.len();
                let idf = bm25_idf(self.index.doc_count, document_frequency);
                for posting in postings {
                    let doc_length = self.index.doc_length(&posting.doc_id);
                    let term_score = bm25_term_score(
                        posting.term_frequency,
                        doc_length,
                        average_doc_length,
                        idf,
                    );
                    let accumulator = scores.entry(posting.doc_id.clone()).or_insert_with(|| {
                        TextSearchAccumulator {
                            score: 0.0,
                            doc_length,
                            terms: Vec::new(),
                        }
                    });
                    accumulator.score += term_score;
                    accumulator.terms.push(TextSearchTermDiagnostic {
                        token: token.clone(),
                        term_frequency: posting.term_frequency,
                        document_frequency,
                        idf,
                        score: term_score,
                    });
                }
            }
        }

        let mut hits = scores
            .into_iter()
            .map(|(doc_id, accumulator)| TextSearchDiagnostic {
                doc_id,
                score: accumulator.score,
                doc_length: accumulator.doc_length,
                terms: accumulator.terms,
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.doc_id.cmp(&right.doc_id))
        });
        hits.into_iter().take(limit).collect()
    }
}

#[derive(Debug, Clone)]
struct TextSearchAccumulator {
    score: f64,
    doc_length: u32,
    terms: Vec<TextSearchTermDiagnostic>,
}

impl TextIndex {
    fn average_doc_length(&self) -> f64 {
        if self.doc_count == 0 {
            1.0
        } else {
            (self.total_doc_length as f64 / self.doc_count as f64).max(1.0)
        }
    }

    fn doc_length(&self, doc_id: &str) -> u32 {
        self.doc_lengths.get(doc_id).copied().unwrap_or(1).max(1)
    }
}

fn bm25_idf(doc_count: usize, document_frequency: usize) -> f64 {
    (((doc_count as f64 - document_frequency as f64 + 0.5) / (document_frequency as f64 + 0.5))
        + 1.0)
        .ln()
}

fn bm25_term_score(term_frequency: u32, doc_length: u32, average_doc_length: f64, idf: f64) -> f64 {
    let term_frequency = term_frequency as f64;
    let length_ratio = doc_length as f64 / average_doc_length.max(1.0);
    let denominator = term_frequency + BM25_K1 * (1.0 - BM25_B + BM25_B * length_ratio);
    idf * (term_frequency * (BM25_K1 + 1.0)) / denominator
}

fn analyze_text(profile: &TextAnalyzerProfile, text: &str) -> Vec<String> {
    match profile.name.as_str() {
        EXPERIMENTAL_ALYZE_ANALYZER_NAME => analyze_text_with_alyze(text),
        _ => simple_tokenize(text),
    }
}

fn analyze_text_with_alyze(text: &str) -> Vec<String> {
    use alyze::analyze::{AnalysisOptions, Analyzer, ReusableBuffer, TokenizerOptions};

    let analyzer = Analyzer::new(AnalysisOptions {
        tokenizer: TokenizerOptions::UAX29Word(Default::default()),
        maximum_token_length: None,
        case_sensitive: false,
        stopword_removal: None,
        stemming: None,
        ascii_folding: true,
    });
    let mut buffer = ReusableBuffer::new();
    let mut tokens = Vec::new();
    analyzer.analyze(text, &mut buffer, |token| {
        tokens.push(token.text.to_owned());
        true
    });
    tokens
}

pub fn publish_compatibility_text_segment(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
    store_path: &Path,
) -> Result<(), String> {
    let opened = rax_core::open_store(store_path).map_err(|error| error.to_string())?;
    let expected_generation = opened.manifest.generation;
    let expected_doc_descriptor = active_doc_descriptor(&opened.manifest);
    if let Some(descriptor) = expected_doc_descriptor.as_ref() {
        rax_docstore::validate_store_doc_descriptor_against_dataset_pack(
            store_path, descriptor, mount_root, manifest,
        )
        .map_err(docstore_error)?;
    }
    let (prepared, doc_pending) = if let Some(descriptor) = expected_doc_descriptor.as_ref() {
        let documents = rax_docstore::load_store_ordered_documents(store_path, descriptor)
            .map_err(docstore_error)?;
        let mut prepared = prepare_text_segment_from_document_values(&documents)?;
        align_pending_descriptor_to_doc_range(
            &mut prepared.descriptor,
            descriptor.doc_id_start,
            descriptor.doc_id_end_exclusive,
            descriptor.live_items,
        );
        (prepared, None)
    } else if documents_path_from_manifest(mount_root, manifest)?.is_some() {
        let documents = rax_docstore::load_dataset_ordered_documents(mount_root, manifest)
            .map_err(docstore_error)?;
        let doc_pending =
            rax_docstore::prepare_raw_documents_segment(store_path, documents.clone())
                .map_err(docstore_error)?;
        let mut prepared = prepare_text_segment_from_document_values(&documents)?;
        align_pending_descriptor_to_doc_range(
            &mut prepared.descriptor,
            doc_pending.descriptor.doc_id_start,
            doc_pending.descriptor.doc_id_end_exclusive,
            doc_pending.descriptor.live_items,
        );
        (prepared, Some(doc_pending))
    } else {
        (
            prepare_compatibility_text_segment(mount_root, manifest)?,
            None,
        )
    };
    let pending_segments = doc_pending
        .into_iter()
        .chain(std::iter::once(prepared))
        .collect::<Vec<_>>();
    rax_core::publish_segments_with_precondition(store_path, pending_segments, |manifest| {
        if manifest.generation != expected_generation {
            return Err(rax_core::CoreError::PublishPreconditionFailed(
                format!(
                    "store generation changed during text publish: expected {expected_generation}, found {}",
                    manifest.generation
                ),
            ));
        }
        if active_doc_descriptor(manifest) != expected_doc_descriptor {
            return Err(rax_core::CoreError::PublishPreconditionFailed(
                "store doc segment changed during text publish".to_owned(),
            ));
        }
        Ok(())
    })
    .map_err(|error| error.to_string())?;
    Ok(())
}

pub fn prepare_compatibility_text_segment(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<PendingSegmentWrite, String> {
    let documents_path = manifest
        .files
        .iter()
        .find(|file| file.kind == "documents")
        .map(|file| manifest_file_path(mount_root, file))
        .transpose()?
        .ok_or_else(|| "documents file missing from manifest".to_owned())?;
    let documents = load_documents_for_text_builder(&documents_path)?;
    prepare_text_segment_from_documents(&documents)
}

pub fn prepare_text_segment_from_documents(
    documents: &[(String, String)],
) -> Result<PendingSegmentWrite, String> {
    prepare_text_segment_from_document_refs(
        documents
            .iter()
            .map(|(doc_id, text)| (doc_id.as_str(), text.as_str())),
    )
}

pub fn prepare_experimental_alyze_text_segment_from_documents(
    documents: &[(String, String)],
) -> Result<PendingSegmentWrite, String> {
    prepare_text_segment_from_document_refs_with_analyzer(
        documents
            .iter()
            .map(|(doc_id, text)| (doc_id.as_str(), text.as_str())),
        TextAnalyzerProfile::experimental_alyze_v1(),
    )
}

fn prepare_text_segment_from_document_values(
    documents: &[(String, Value)],
) -> Result<PendingSegmentWrite, String> {
    let documents = documents
        .iter()
        .map(|(doc_id, document)| {
            let text = document
                .as_object()
                .and_then(|object| object.get("text"))
                .and_then(Value::as_str)
                .ok_or_else(|| format!("document {doc_id} missing text"))?;
            Ok((doc_id.as_str(), text))
        })
        .collect::<Result<Vec<_>, String>>()?;
    prepare_text_segment_from_document_refs(documents)
}

pub fn prepare_text_segment_from_document_refs<'a, I>(
    documents: I,
) -> Result<PendingSegmentWrite, String>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    prepare_text_segment_from_document_refs_with_analyzer(documents, TextAnalyzerProfile::current())
}

fn prepare_text_segment_from_document_refs_with_analyzer<'a, I>(
    documents: I,
    analyzer_profile: TextAnalyzerProfile,
) -> Result<PendingSegmentWrite, String>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let (segment, doc_count) =
        BinaryTextSegment::from_document_refs_with_analyzer(documents, analyzer_profile);
    let object_bytes = segment.encode()?;
    Ok(PendingSegmentWrite {
        descriptor: PendingSegmentDescriptor {
            family: SegmentKind::Txt,
            family_version: 1,
            flags: 0,
            doc_id_start: 0,
            doc_id_end_exclusive: doc_count as u64,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
            live_items: doc_count as u64,
            tombstoned_items: 0,
            backend_id: 0,
            backend_aux: segment.postings.len() as u64,
        },
        object_bytes,
    })
}

fn align_pending_descriptor_to_doc_range(
    descriptor: &mut PendingSegmentDescriptor,
    doc_id_start: u64,
    doc_id_end_exclusive: u64,
    live_items: u64,
) {
    descriptor.doc_id_start = doc_id_start;
    descriptor.doc_id_end_exclusive = doc_id_end_exclusive;
    descriptor.live_items = live_items;
}

pub fn validate_store_segment_against_dataset_pack(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<(), String> {
    let store_path = store_path_from_manifest(mount_root, manifest)?;
    validate_store_segment_against_dataset_pack_with_store_path(&store_path, mount_root, manifest)
}

pub fn validate_store_segment_against_dataset_pack_with_store_path(
    store_path: &Path,
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<(), String> {
    let Some(documents_path) = documents_path_from_manifest(mount_root, manifest)? else {
        return Ok(());
    };
    if !store_path.exists() {
        return Ok(());
    }

    let opened = rax_core::open_store(store_path).map_err(|error| error.to_string())?;
    let Some(descriptor) = opened
        .manifest
        .segments
        .iter()
        .filter(|segment| segment.family == SegmentKind::Txt)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
    else {
        return Ok(());
    };

    let bytes =
        rax_core::map_segment_object(store_path, descriptor).map_err(|error| error.to_string())?;
    let persisted_segment = BinaryTextSegment::decode(&bytes)?;
    let documents = load_documents_for_text_builder(&documents_path)?;
    let expected_segment = BinaryTextSegment::from_documents(&documents);
    if persisted_segment != expected_segment {
        return Err("store text segment does not match mounted dataset documents".to_owned());
    }

    Ok(())
}

fn load_first_text_query(paths: &[PathBuf]) -> Result<(String, usize), String> {
    for path in paths {
        for line in BufReader::new(open_read_no_symlinks(path)?).lines() {
            let line = line.map_err(|error| error.to_string())?;
            if line.trim().is_empty() {
                continue;
            }
            let query: QueryRecord =
                serde_json::from_str(&line).map_err(|error| error.to_string())?;
            if query.lane_eligibility.text {
                return Ok((query.query_text, query.top_k as usize));
            }
        }
    }
    Ok((String::new(), 0))
}

fn load_first_hybrid_text_query(paths: &[PathBuf]) -> Result<Option<FirstTextQuery>, String> {
    for path in paths {
        for line in BufReader::new(open_read_no_symlinks(path)?).lines() {
            let line = line.map_err(|error| error.to_string())?;
            if line.trim().is_empty() {
                continue;
            }
            let query: QueryRecord =
                serde_json::from_str(&line).map_err(|error| error.to_string())?;
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

fn compatibility_postings_path(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<PathBuf, String> {
    manifest
        .files
        .iter()
        .find(|file| file.kind == "text_postings")
        .map(|file| manifest_file_path(mount_root, file))
        .transpose()?
        .ok_or_else(|| "text_postings file missing from manifest".to_owned())
}

fn documents_path_from_manifest(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<Option<PathBuf>, String> {
    Ok(manifest
        .files
        .iter()
        .find(|file| file.kind == "documents")
        .map(|file| manifest_file_path(mount_root, file))
        .transpose()?
        .filter(|path| path.exists()))
}

fn store_path_from_manifest(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<PathBuf, String> {
    manifest
        .files
        .iter()
        .find(|file| file.kind == "store")
        .or_else(|| {
            manifest
                .files
                .iter()
                .find(|file| file.kind == "prebuilt_store")
        })
        .map(|file| manifest_file_path(mount_root, file))
        .unwrap_or_else(|| Ok(mount_root.join("store.rax")))
}

fn manifest_file_path(
    mount_root: &Path,
    file: &rax_bench_model::ManifestFile,
) -> Result<PathBuf, String> {
    let path = Path::new(&file.path);
    if !is_pack_relative_path(path) {
        return Err(format!(
            "manifest {} path {} must stay within dataset root",
            file.kind, file.path
        ));
    }
    root_confined_path(mount_root, path, &file.kind)
}

fn manifest_query_set_path(mount_root: &Path, path: &str) -> Result<PathBuf, String> {
    let path = Path::new(path);
    if !is_pack_relative_path(path) {
        return Err(format!(
            "manifest query_set path {} must stay within dataset root",
            path.display()
        ));
    }
    root_confined_path(mount_root, path, "query_set")
}

fn is_pack_relative_path(path: &Path) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn root_confined_path(mount_root: &Path, path: &Path, kind: &str) -> Result<PathBuf, String> {
    if matches!(kind, "store" | "prebuilt_store")
        && is_stable_fd_table_root(mount_root)
        && is_single_numeric_path_component(path)
    {
        return Ok(mount_root.join(path));
    }
    if is_linux_proc_self_fd_root(mount_root) {
        reject_symlink_components(mount_root, path, kind)?;
        return Ok(mount_root.join(path));
    }
    let Ok(root) = mount_root.canonicalize() else {
        return Ok(mount_root.join(path));
    };
    reject_symlink_components(&root, path, kind)?;
    let candidate = root.join(path);
    if candidate.exists() {
        let canonical = candidate
            .canonicalize()
            .map_err(|error| error.to_string())?;
        if !canonical.starts_with(&root) {
            return Err(format!(
                "manifest {kind} path {} resolves outside dataset root {}",
                candidate.display(),
                root.display()
            ));
        }
        return Ok(canonical);
    }
    let mut ancestor = candidate.parent();
    while let Some(path) = ancestor {
        if path.exists() {
            let canonical = path.canonicalize().map_err(|error| error.to_string())?;
            if !canonical.starts_with(&root) {
                return Err(format!(
                    "manifest {kind} ancestor {} resolves outside dataset root {}",
                    path.display(),
                    root.display()
                ));
            }
            return Ok(candidate);
        }
        ancestor = path.parent();
    }
    if !candidate.starts_with(&root) {
        return Err(format!(
            "manifest {kind} path {} is outside dataset root {}",
            candidate.display(),
            root.display()
        ));
    }
    Ok(candidate)
}

#[cfg(unix)]
fn is_stable_fd_table_root(path: &Path) -> bool {
    path_has_absolute_normal_components(path, stable_fd_table_components())
}

#[cfg(not(unix))]
fn is_stable_fd_table_root(_path: &Path) -> bool {
    false
}

#[cfg(target_os = "linux")]
fn stable_fd_table_components() -> &'static [&'static str] {
    &["proc", "self", "fd"]
}

#[cfg(all(unix, not(target_os = "linux")))]
fn stable_fd_table_components() -> &'static [&'static str] {
    &["dev", "fd"]
}

#[cfg(unix)]
fn path_has_absolute_normal_components(path: &Path, expected_components: &[&str]) -> bool {
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::RootDir)) {
        return false;
    }
    for expected in expected_components {
        match components.next() {
            Some(Component::Normal(component)) if component == *expected => {}
            _ => return false,
        }
    }
    components.next().is_none()
}

fn is_single_numeric_path_component(path: &Path) -> bool {
    let mut components = path.components();
    let is_numeric = match components.next() {
        Some(Component::Normal(component)) => component.to_str().is_some_and(|value| {
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
        }),
        _ => false,
    };
    is_numeric && components.next().is_none()
}

#[cfg(target_os = "linux")]
fn is_linux_proc_self_fd_root(path: &Path) -> bool {
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::RootDir)) {
        return false;
    }
    for expected in ["proc", "self", "fd"] {
        match components.next() {
            Some(Component::Normal(component)) if component == expected => {}
            _ => return false,
        }
    }
    let has_valid_fd = match components.next() {
        Some(Component::Normal(fd)) => fd
            .to_str()
            .is_some_and(|fd| !fd.is_empty() && fd.bytes().all(|byte| byte.is_ascii_digit())),
        _ => false,
    };
    has_valid_fd && components.next().is_none()
}

#[cfg(not(target_os = "linux"))]
fn is_linux_proc_self_fd_root(_path: &Path) -> bool {
    false
}

fn reject_symlink_components(root: &Path, path: &Path, kind: &str) -> Result<(), String> {
    let mut current = root.to_path_buf();
    for component in path.components() {
        let Component::Normal(component) = component else {
            continue;
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!(
                    "manifest {kind} path {} contains a symlink component",
                    current.display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

fn active_doc_descriptor(
    manifest: &rax_core::ActiveManifest,
) -> Option<rax_core::SegmentDescriptor> {
    manifest
        .segments
        .iter()
        .filter(|segment| segment.family == SegmentKind::Doc)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
        .cloned()
}

fn same_active_doc_coverage(
    descriptor: &SegmentDescriptor,
    doc_descriptor: &SegmentDescriptor,
) -> bool {
    descriptor.doc_id_start == doc_descriptor.doc_id_start
        && descriptor.doc_id_end_exclusive == doc_descriptor.doc_id_end_exclusive
        && descriptor.live_items == doc_descriptor.live_items
}

fn validate_text_segment_against_store_doc_descriptor(
    store_path: &Path,
    descriptor: &SegmentDescriptor,
    doc_descriptor: &SegmentDescriptor,
) -> Result<(), String> {
    let bytes =
        rax_core::map_segment_object(store_path, descriptor).map_err(|error| error.to_string())?;
    let persisted_segment = BinaryTextSegment::decode(&bytes)?;
    let documents = rax_docstore::load_store_ordered_documents(store_path, doc_descriptor)
        .map_err(docstore_error)?;
    let expected_segment = prepare_text_segment_from_document_values(&documents)
        .and_then(|pending| BinaryTextSegment::decode(&pending.object_bytes))?;
    if persisted_segment != expected_segment {
        return Err(
            "latest text segment does not match the active document segment; republish text before runtime text search"
                .to_owned(),
        );
    }
    Ok(())
}

fn docstore_error(error: rax_docstore::DocstoreError) -> String {
    match error {
        rax_docstore::DocstoreError::Io(message)
        | rax_docstore::DocstoreError::Json(message)
        | rax_docstore::DocstoreError::InvalidDocument(message) => message,
        rax_docstore::DocstoreError::MissingDocumentsFile => {
            "documents file missing from manifest".to_owned()
        }
    }
}

fn load_documents_for_text_builder(path: &Path) -> Result<Vec<(String, String)>, String> {
    BufReader::new(open_read_no_symlinks(path)?)
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some(other),
        })
        .map(|line| {
            let line = line.map_err(|error| error.to_string())?;
            let value: serde_json::Value =
                serde_json::from_str(&line).map_err(|error| error.to_string())?;
            let object = value
                .as_object()
                .ok_or_else(|| "document line must be a json object".to_owned())?;
            let doc_id = object
                .get("doc_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "document line missing doc_id".to_owned())?
                .to_owned();
            let text = object
                .get("text")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "document line missing text".to_owned())?
                .to_owned();
            Ok((doc_id, text))
        })
        .collect()
}

fn load_text_index(metadata: &TextLaneMetadata) -> Result<TextIndex, String> {
    match &metadata.source {
        TextLaneSource::Compatibility { postings_path } => load_text_index_from_path(postings_path),
        TextLaneSource::Store {
            store_path,
            descriptor,
        } => {
            let bytes = rax_core::map_segment_object(store_path, descriptor)
                .map_err(|error| error.to_string())?;
            BinaryTextSegment::decode(&bytes).and_then(BinaryTextSegment::try_into_index)
        }
    }
}

fn load_text_index_from_path(path: &Path) -> Result<TextIndex, String> {
    let reader = BufReader::new(open_read_no_symlinks(path)?);
    let mut inverted = HashMap::new();
    let mut doc_lengths = HashMap::<String, u32>::new();
    for line in reader.lines() {
        let line = line.map_err(|error| error.to_string())?;
        let posting: TextPostingRecord =
            serde_json::from_str(&line).map_err(|error| error.to_string())?;
        let postings = posting
            .doc_ids
            .into_iter()
            .map(|doc_id| {
                *doc_lengths.entry(doc_id.clone()).or_insert(0) += 1;
                TextPosting {
                    doc_id,
                    term_frequency: 1,
                }
            })
            .collect();
        inverted.insert(posting.token, postings);
    }
    Ok(index_from_parts(
        TextAnalyzerProfile::legacy_v1(),
        inverted,
        doc_lengths,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BinaryTextSegment {
    analyzer_profile: TextAnalyzerProfile,
    postings: Vec<BinaryTextPostingRecord>,
    doc_lengths: Vec<DocumentLengthRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BinaryTextPostingRecord {
    token: String,
    postings: Vec<TextPosting>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentLengthRecord {
    doc_id: String,
    length: u32,
}

impl BinaryTextSegment {
    fn from_documents(documents: &[(String, String)]) -> Self {
        Self::from_documents_with_analyzer(documents, TextAnalyzerProfile::current())
    }

    fn from_documents_with_analyzer(
        documents: &[(String, String)],
        analyzer_profile: TextAnalyzerProfile,
    ) -> Self {
        Self::from_document_refs_with_analyzer(
            documents
                .iter()
                .map(|(doc_id, text)| (doc_id.as_str(), text.as_str())),
            analyzer_profile,
        )
        .0
    }

    fn from_document_refs_with_analyzer<'a, I>(
        documents: I,
        analyzer_profile: TextAnalyzerProfile,
    ) -> (Self, usize)
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut inverted: HashMap<String, HashMap<String, u32>> = HashMap::new();
        let mut doc_lengths = HashMap::<String, u32>::new();
        let mut doc_count = 0;
        for (doc_id, text) in documents {
            doc_count += 1;
            doc_lengths.entry(doc_id.to_owned()).or_insert(0);
            for token in analyze_text(&analyzer_profile, text) {
                *doc_lengths.entry(doc_id.to_owned()).or_insert(0) += 1;
                *inverted
                    .entry(token)
                    .or_default()
                    .entry(doc_id.to_owned())
                    .or_insert(0) += 1;
            }
        }
        let mut postings = inverted
            .into_iter()
            .map(|(token, by_doc)| {
                let mut postings = by_doc
                    .into_iter()
                    .map(|(doc_id, term_frequency)| TextPosting {
                        doc_id,
                        term_frequency,
                    })
                    .collect::<Vec<_>>();
                postings.sort_by(|left, right| left.doc_id.cmp(&right.doc_id));
                BinaryTextPostingRecord { token, postings }
            })
            .collect::<Vec<_>>();
        postings.sort_by(|left, right| left.token.cmp(&right.token));
        let mut doc_lengths = doc_lengths
            .into_iter()
            .map(|(doc_id, length)| DocumentLengthRecord { doc_id, length })
            .collect::<Vec<_>>();
        doc_lengths.sort_by(|left, right| left.doc_id.cmp(&right.doc_id));
        (
            Self {
                analyzer_profile,
                postings,
                doc_lengths,
            },
            doc_count,
        )
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        for pair in self.postings.windows(2) {
            if pair[0].token >= pair[1].token {
                return Err("text segment tokens must be sorted and unique".to_owned());
            }
        }
        for pair in self.doc_lengths.windows(2) {
            if pair[0].doc_id >= pair[1].doc_id {
                return Err("text segment document lengths must be sorted and unique".to_owned());
            }
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(TEXT_SEGMENT_MAGIC);
        bytes.extend_from_slice(&TEXT_SEGMENT_MAJOR.to_le_bytes());
        bytes.extend_from_slice(&TEXT_SEGMENT_MINOR.to_le_bytes());
        bytes.extend_from_slice(&(self.postings.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.analyzer_profile.name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.analyzer_profile.version.to_le_bytes());
        bytes.extend_from_slice(self.analyzer_profile.name.as_bytes());
        for posting in &self.postings {
            bytes.extend_from_slice(&(posting.token.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&(posting.postings.len() as u32).to_le_bytes());
            bytes.extend_from_slice(posting.token.as_bytes());
            for doc_posting in &posting.postings {
                bytes.extend_from_slice(&(doc_posting.doc_id.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&doc_posting.term_frequency.to_le_bytes());
                bytes.extend_from_slice(doc_posting.doc_id.as_bytes());
            }
        }
        bytes.extend_from_slice(&(self.doc_lengths.len() as u64).to_le_bytes());
        for record in &self.doc_lengths {
            bytes.extend_from_slice(&(record.doc_id.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&record.length.to_le_bytes());
            bytes.extend_from_slice(record.doc_id.as_bytes());
        }
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < TEXT_SEGMENT_HEADER_LENGTH {
            return Err(format!(
                "text segment too short: expected at least {TEXT_SEGMENT_HEADER_LENGTH} bytes"
            ));
        }
        if &bytes[..4] != TEXT_SEGMENT_MAGIC {
            return Err("text segment magic mismatch".to_owned());
        }
        let major = read_u16(bytes, 4);
        let minor = read_u16(bytes, 6);
        if !matches!(major, 1 | TEXT_SEGMENT_MAJOR) || minor != TEXT_SEGMENT_MINOR {
            return Err("unsupported text segment version".to_owned());
        }

        let record_count = usize::try_from(read_u64(bytes, 8))
            .map_err(|_| "text segment record_count exceeds addressable memory".to_owned())?;
        let mut cursor = TEXT_SEGMENT_HEADER_LENGTH;
        if record_count > bytes[cursor..].len() / 8 {
            return Err("text segment record_count exceeds possible records in slice".to_owned());
        }
        let mut postings = Vec::with_capacity(record_count);
        let mut doc_lengths = HashMap::<String, u32>::new();
        let analyzer_profile;
        match major {
            1 => {
                analyzer_profile = TextAnalyzerProfile::legacy_v1();
                for _ in 0..record_count {
                    let token_length = read_u32_at(bytes, &mut cursor)? as usize;
                    let doc_count = read_u32_at(bytes, &mut cursor)? as usize;
                    let token = read_string_at(bytes, &mut cursor, token_length)?;
                    if doc_count > bytes[cursor..].len() / 4 {
                        return Err(
                            "text segment doc_count exceeds possible records in slice".to_owned()
                        );
                    }
                    let mut token_postings = Vec::with_capacity(doc_count);
                    for _ in 0..doc_count {
                        let doc_id_length = read_u32_at(bytes, &mut cursor)? as usize;
                        let doc_id = read_string_at(bytes, &mut cursor, doc_id_length)?;
                        *doc_lengths.entry(doc_id.clone()).or_insert(0) += 1;
                        token_postings.push(TextPosting {
                            doc_id,
                            term_frequency: 1,
                        });
                    }
                    postings.push(BinaryTextPostingRecord {
                        token,
                        postings: token_postings,
                    });
                }
            }
            TEXT_SEGMENT_MAJOR => {
                let analyzer_name_length = read_u32_at(bytes, &mut cursor)? as usize;
                let analyzer_version = read_u32_at(bytes, &mut cursor)?;
                let analyzer_name = read_string_at(bytes, &mut cursor, analyzer_name_length)?;
                analyzer_profile = TextAnalyzerProfile {
                    name: analyzer_name,
                    version: analyzer_version,
                };
                for _ in 0..record_count {
                    let token_length = read_u32_at(bytes, &mut cursor)? as usize;
                    let doc_count = read_u32_at(bytes, &mut cursor)? as usize;
                    let token = read_string_at(bytes, &mut cursor, token_length)?;
                    if doc_count > bytes[cursor..].len() / 8 {
                        return Err(
                            "text segment doc_count exceeds possible records in slice".to_owned()
                        );
                    }
                    let mut token_postings = Vec::with_capacity(doc_count);
                    for _ in 0..doc_count {
                        let doc_id_length = read_u32_at(bytes, &mut cursor)? as usize;
                        let term_frequency = read_u32_at(bytes, &mut cursor)?;
                        let doc_id = read_string_at(bytes, &mut cursor, doc_id_length)?;
                        token_postings.push(TextPosting {
                            doc_id,
                            term_frequency,
                        });
                    }
                    postings.push(BinaryTextPostingRecord {
                        token,
                        postings: token_postings,
                    });
                }

                let length_count =
                    usize::try_from(read_u64_at(bytes, &mut cursor)?).map_err(|_| {
                        "text segment length_count exceeds addressable memory".to_owned()
                    })?;
                if length_count > bytes[cursor..].len() / 8 {
                    return Err(
                        "text segment length_count exceeds possible records in slice".to_owned(),
                    );
                }
                for _ in 0..length_count {
                    let doc_id_length = read_u32_at(bytes, &mut cursor)? as usize;
                    let length = read_u32_at(bytes, &mut cursor)?;
                    let doc_id = read_string_at(bytes, &mut cursor, doc_id_length)?;
                    doc_lengths.insert(doc_id, length);
                }
            }
            _ => unreachable!(),
        }
        if cursor != bytes.len() {
            return Err("text segment trailing bytes mismatch".to_owned());
        }
        for pair in postings.windows(2) {
            if pair[0].token >= pair[1].token {
                return Err("text segment tokens must be sorted and unique".to_owned());
            }
        }

        let mut doc_lengths = doc_lengths
            .into_iter()
            .map(|(doc_id, length)| DocumentLengthRecord { doc_id, length })
            .collect::<Vec<_>>();
        doc_lengths.sort_by(|left, right| left.doc_id.cmp(&right.doc_id));
        Ok(Self {
            analyzer_profile,
            postings,
            doc_lengths,
        })
    }

    fn try_into_index(self) -> Result<TextIndex, String> {
        if !self.analyzer_profile.is_supported() {
            return Err(format!(
                "unsupported text analyzer profile {}@{}",
                self.analyzer_profile.name, self.analyzer_profile.version
            ));
        }
        let inverted = self
            .postings
            .into_iter()
            .map(|posting| (posting.token, posting.postings))
            .collect();
        let doc_lengths = self
            .doc_lengths
            .into_iter()
            .map(|record| (record.doc_id, record.length))
            .collect();
        Ok(index_from_parts(
            self.analyzer_profile,
            inverted,
            doc_lengths,
        ))
    }
}

fn index_from_parts(
    analyzer_profile: TextAnalyzerProfile,
    inverted: HashMap<String, Vec<TextPosting>>,
    doc_lengths: HashMap<String, u32>,
) -> TextIndex {
    let total_doc_length = doc_lengths.values().map(|length| u64::from(*length)).sum();
    TextIndex {
        analyzer_profile,
        inverted,
        doc_count: doc_lengths.len(),
        total_doc_length,
        doc_lengths,
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("u16 slice"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("u64 slice"))
}

fn read_u32_at(bytes: &[u8], cursor: &mut usize) -> Result<u32, String> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| "text segment cursor overflow".to_owned())?;
    if end > bytes.len() {
        return Err("text segment truncated while reading u32".to_owned());
    }
    let value = u32::from_le_bytes(bytes[*cursor..end].try_into().expect("u32 slice"));
    *cursor = end;
    Ok(value)
}

fn read_u64_at(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| "text segment cursor overflow".to_owned())?;
    if end > bytes.len() {
        return Err("text segment truncated while reading u64".to_owned());
    }
    let value = u64::from_le_bytes(bytes[*cursor..end].try_into().expect("u64 slice"));
    *cursor = end;
    Ok(value)
}

fn read_string_at(bytes: &[u8], cursor: &mut usize, length: usize) -> Result<String, String> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| "text segment string range overflow".to_owned())?;
    if end > bytes.len() {
        return Err("text segment truncated while reading string".to_owned());
    }
    let value = std::str::from_utf8(&bytes[*cursor..end]).map_err(|error| error.to_string())?;
    *cursor = end;
    Ok(value.to_owned())
}

fn open_read_no_symlinks(path: &Path) -> Result<File, String> {
    rax_core::open_file_read_no_symlinks(path).map_err(|error| error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct QueryRecord {
    query_id: String,
    query_text: String,
    top_k: u32,
    #[serde(default)]
    filter_spec: serde_json::Map<String, serde_json::Value>,
    lane_eligibility: LaneEligibility,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LaneEligibility {
    text: bool,
    vector: bool,
    hybrid: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct TextPostingRecord {
    token: String,
    doc_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FirstTextQuery {
    query_text: String,
    top_k: usize,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use rax_bench_model::DatasetPackManifest;
    use rax_core::{create_empty_store, publish_segments};
    use rax_docstore::prepare_raw_documents_segment;
    use serde_json::json;
    use tempfile::tempdir;

    use crate::{
        prepare_compatibility_text_segment, publish_compatibility_text_segment, BinaryTextSegment,
        TextAnalyzerProfile, TextBatchQuery, TextLane, TextLaneMetadata, TextLaneSource,
        TextQueryInputs, TEXT_SEGMENT_MAGIC,
    };

    #[test]
    fn text_segment_decode_rejects_impossible_record_count_before_allocation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(TEXT_SEGMENT_MAGIC);
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());

        let error =
            BinaryTextSegment::decode(&bytes).expect_err("record count should exceed payload");

        assert!(error.contains("record_count"));
    }

    #[test]
    fn text_segment_v2_roundtrips_term_frequency_and_document_lengths() {
        let segment = BinaryTextSegment::from_documents(&[
            ("doc-short".to_owned(), "alpha".to_owned()),
            ("doc-repeated".to_owned(), "alpha alpha alpha".to_owned()),
        ]);

        let bytes = segment.encode().unwrap();
        assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), 2);

        let decoded = BinaryTextSegment::decode(&bytes).unwrap();
        assert_eq!(decoded.analyzer_profile, TextAnalyzerProfile::current());
        let alpha = decoded
            .postings
            .iter()
            .find(|posting| posting.token == "alpha")
            .unwrap();

        assert_eq!(
            alpha
                .postings
                .iter()
                .map(|posting| (posting.doc_id.as_str(), posting.term_frequency))
                .collect::<Vec<_>>(),
            vec![("doc-repeated", 3), ("doc-short", 1)]
        );
        assert_eq!(
            decoded
                .doc_lengths
                .iter()
                .map(|record| (record.doc_id.as_str(), record.length))
                .collect::<Vec<_>>(),
            vec![("doc-repeated", 3), ("doc-short", 1)]
        );
    }

    #[test]
    fn text_segment_rejects_unsupported_analyzer_profile_before_indexing() {
        let mut segment =
            BinaryTextSegment::from_documents(&[("doc-1".to_owned(), "alpha".to_owned())]);
        segment.analyzer_profile.version += 1;

        let error = segment
            .try_into_index()
            .expect_err("unsupported analyzer profile should be rejected");

        assert!(error.contains("unsupported text analyzer profile"));
    }

    #[test]
    fn experimental_alyze_segment_records_profile_and_ascii_folds_tokens() {
        let segment = BinaryTextSegment::from_documents_with_analyzer(
            &[("doc-1".to_owned(), "Café".to_owned())],
            TextAnalyzerProfile::experimental_alyze_v1(),
        );

        let decoded = BinaryTextSegment::decode(&segment.encode().unwrap()).unwrap();

        assert_eq!(
            decoded.analyzer_profile,
            TextAnalyzerProfile::experimental_alyze_v1()
        );
        assert!(decoded
            .postings
            .iter()
            .any(|posting| posting.token == "cafe"));
        decoded.try_into_index().unwrap();
    }

    #[test]
    #[ignore = "microbenchmark; run with --release --ignored --nocapture"]
    fn experimental_alyze_profile_microbench() {
        use std::hint::black_box;
        use std::time::{Duration, Instant};

        let documents = (0..2_000)
            .map(|index| {
                (
                    format!("doc-{index:04}"),
                    format!(
                        "Rust benchmark guide {index} Café Straße hybrid-search vector_index BM25 alpha alpha {}",
                        index % 17
                    ),
                )
            })
            .collect::<Vec<_>>();
        let queries = [
            "rust benchmark cafe",
            "straße hybrid search",
            "vector index bm25 alpha",
        ];

        fn elapsed_per(iterations: u32, mut run: impl FnMut() -> usize) -> (Duration, usize) {
            let start = Instant::now();
            let mut sink = 0usize;
            for _ in 0..iterations {
                sink = sink.wrapping_add(black_box(run()));
            }
            (start.elapsed() / iterations, sink)
        }

        let build_iterations = 100;
        let search_iterations = 10_000;

        let (simple_build, simple_sink) = elapsed_per(build_iterations, || {
            BinaryTextSegment::from_documents(black_box(&documents))
                .encode()
                .unwrap()
                .len()
        });
        let (alyze_build, alyze_sink) = elapsed_per(build_iterations, || {
            BinaryTextSegment::from_documents_with_analyzer(
                black_box(&documents),
                TextAnalyzerProfile::experimental_alyze_v1(),
            )
            .encode()
            .unwrap()
            .len()
        });

        let simple_lane = TextLane {
            first_text_query: String::new(),
            first_text_top_k: 0,
            first_hybrid_query: None,
            first_hybrid_top_k: 0,
            index: BinaryTextSegment::from_documents(&documents)
                .try_into_index()
                .unwrap(),
        };
        let alyze_lane = TextLane {
            first_text_query: String::new(),
            first_text_top_k: 0,
            first_hybrid_query: None,
            first_hybrid_top_k: 0,
            index: BinaryTextSegment::from_documents_with_analyzer(
                &documents,
                TextAnalyzerProfile::experimental_alyze_v1(),
            )
            .try_into_index()
            .unwrap(),
        };

        let (simple_search, simple_search_sink) = elapsed_per(search_iterations, || {
            queries
                .iter()
                .map(|query| simple_lane.search_with_limit(black_box(query), 10).len())
                .sum()
        });
        let (alyze_search, alyze_search_sink) = elapsed_per(search_iterations, || {
            queries
                .iter()
                .map(|query| alyze_lane.search_with_limit(black_box(query), 10).len())
                .sum()
        });

        eprintln!(
            "simple build+encode avg: {:?} sink={simple_sink}",
            simple_build
        );
        eprintln!(
            "alyze build+encode avg: {:?} sink={alyze_sink}",
            alyze_build
        );
        eprintln!(
            "simple search batch avg: {:?} sink={simple_search_sink}",
            simple_search
        );
        eprintln!(
            "alyze search batch avg: {:?} sink={alyze_search_sink}",
            alyze_search
        );
    }

    #[derive(Debug, Clone)]
    struct QualityCase {
        language: &'static str,
        group: &'static str,
        query_id: &'static str,
        query: &'static str,
        relevant_doc_id: &'static str,
    }

    #[derive(Debug, Clone, Default)]
    struct QualitySummary {
        query_count: usize,
        ndcg_at_10: f64,
        recall_at_10: f64,
        mrr_at_10: f64,
        success_at_1: f64,
    }

    #[test]
    #[ignore = "quality benchmark; run with --release --ignored --nocapture"]
    fn experimental_alyze_profile_quality_bench_english_and_korean() {
        let documents = vec![
            (
                "en-cafe".to_owned(),
                "Café guide for espresso tasting and roast notes".to_owned(),
            ),
            (
                "en-diacritics".to_owned(),
                "naïve façade résumé coöperate jalapeño reference".to_owned(),
            ),
            (
                "en-sao".to_owned(),
                "São Paulo travel guide and city map".to_owned(),
            ),
            (
                "en-strasse".to_owned(),
                "Straße transit closure guide for Berlin commuters".to_owned(),
            ),
            (
                "en-hyphen".to_owned(),
                "full-text search implementation notes".to_owned(),
            ),
            (
                "en-apostrophe".to_owned(),
                "O'Reilly can't won't tokenizer notes".to_owned(),
            ),
            (
                "en-vector-index".to_owned(),
                "vector_index tuning notes for approximate nearest neighbor search".to_owned(),
            ),
            (
                "en-control".to_owned(),
                "Rust benchmark guide for hybrid search latency".to_owned(),
            ),
            (
                "ko-cafe".to_owned(),
                "서울 카페 추천 espresso 로스팅 가이드".to_owned(),
            ),
            (
                "ko-search".to_owned(),
                "한국어 검색 품질 테스트 문서".to_owned(),
            ),
            (
                "ko-nospace".to_owned(),
                "한국어검색품질테스트문서".to_owned(),
            ),
            (
                "ko-vector".to_owned(),
                "벡터 검색 하이브리드 랭킹 실험".to_owned(),
            ),
            (
                "ko-control".to_owned(),
                "서울 교통 안내와 환승 정보".to_owned(),
            ),
        ];
        let cases = vec![
            QualityCase {
                language: "en",
                group: "ascii-folding",
                query_id: "en-cafe",
                query: "cafe",
                relevant_doc_id: "en-cafe",
            },
            QualityCase {
                language: "en",
                group: "ascii-folding",
                query_id: "en-diacritics",
                query: "naive facade resume cooperate jalapeno",
                relevant_doc_id: "en-diacritics",
            },
            QualityCase {
                language: "en",
                group: "ascii-folding",
                query_id: "en-sao",
                query: "sao paulo",
                relevant_doc_id: "en-sao",
            },
            QualityCase {
                language: "en",
                group: "ascii-folding",
                query_id: "en-strasse",
                query: "strasse",
                relevant_doc_id: "en-strasse",
            },
            QualityCase {
                language: "en",
                group: "punctuation",
                query_id: "en-hyphen",
                query: "full text search",
                relevant_doc_id: "en-hyphen",
            },
            QualityCase {
                language: "en",
                group: "punctuation",
                query_id: "en-apostrophe",
                query: "oreilly cant wont",
                relevant_doc_id: "en-apostrophe",
            },
            QualityCase {
                language: "en",
                group: "punctuation",
                query_id: "en-vector-index",
                query: "vector index",
                relevant_doc_id: "en-vector-index",
            },
            QualityCase {
                language: "ko",
                group: "whitespace",
                query_id: "ko-cafe",
                query: "서울 카페 추천",
                relevant_doc_id: "ko-cafe",
            },
            QualityCase {
                language: "ko",
                group: "whitespace",
                query_id: "ko-search",
                query: "한국어 검색 품질",
                relevant_doc_id: "ko-search",
            },
            QualityCase {
                language: "ko",
                group: "no-morphology",
                query_id: "ko-nospace",
                query: "한국어 검색 품질",
                relevant_doc_id: "ko-nospace",
            },
            QualityCase {
                language: "ko",
                group: "whitespace",
                query_id: "ko-vector",
                query: "벡터 검색 랭킹",
                relevant_doc_id: "ko-vector",
            },
        ];

        let simple_lane = TextLane {
            first_text_query: String::new(),
            first_text_top_k: 0,
            first_hybrid_query: None,
            first_hybrid_top_k: 0,
            index: BinaryTextSegment::from_documents(&documents)
                .try_into_index()
                .unwrap(),
        };
        let alyze_lane = TextLane {
            first_text_query: String::new(),
            first_text_top_k: 0,
            first_hybrid_query: None,
            first_hybrid_top_k: 0,
            index: BinaryTextSegment::from_documents_with_analyzer(
                &documents,
                TextAnalyzerProfile::experimental_alyze_v1(),
            )
            .try_into_index()
            .unwrap(),
        };

        for language in ["en", "ko"] {
            let language_cases = cases
                .iter()
                .filter(|case| case.language == language)
                .cloned()
                .collect::<Vec<_>>();
            let simple_summary = quality_summary(&simple_lane, &language_cases);
            let alyze_summary = quality_summary(&alyze_lane, &language_cases);
            eprintln!("{language} simple quality: {simple_summary:?}");
            eprintln!("{language} alyze quality: {alyze_summary:?}");

            for case in &language_cases {
                eprintln!(
                    "{language} {} [{}] simple={:?} alyze={:?}",
                    case.query_id,
                    case.group,
                    simple_lane.search_with_limit(case.query, 3),
                    alyze_lane.search_with_limit(case.query, 3)
                );
            }
        }

        for group in [
            "ascii-folding",
            "punctuation",
            "whitespace",
            "no-morphology",
        ] {
            let group_cases = cases
                .iter()
                .filter(|case| case.group == group)
                .cloned()
                .collect::<Vec<_>>();
            let simple_summary = quality_summary(&simple_lane, &group_cases);
            let alyze_summary = quality_summary(&alyze_lane, &group_cases);
            eprintln!("{group} simple quality: {simple_summary:?}");
            eprintln!("{group} alyze quality: {alyze_summary:?}");
        }
    }

    fn quality_summary(lane: &TextLane, cases: &[QualityCase]) -> QualitySummary {
        let mut summary = QualitySummary {
            query_count: cases.len(),
            ..QualitySummary::default()
        };
        for case in cases {
            let hits = lane.search_with_limit(case.query, 10);
            summary.ndcg_at_10 += single_relevant_ndcg_at_10(&hits, case.relevant_doc_id);
            summary.recall_at_10 += if hits.iter().any(|doc_id| doc_id == case.relevant_doc_id) {
                1.0
            } else {
                0.0
            };
            summary.mrr_at_10 += reciprocal_rank_at_10(&hits, case.relevant_doc_id);
            summary.success_at_1 += if hits
                .first()
                .is_some_and(|doc_id| doc_id == case.relevant_doc_id)
            {
                1.0
            } else {
                0.0
            };
        }
        if summary.query_count > 0 {
            let query_count = summary.query_count as f64;
            summary.ndcg_at_10 /= query_count;
            summary.recall_at_10 /= query_count;
            summary.mrr_at_10 /= query_count;
            summary.success_at_1 /= query_count;
        }
        summary
    }

    fn single_relevant_ndcg_at_10(hits: &[String], relevant_doc_id: &str) -> f64 {
        hits.iter()
            .take(10)
            .position(|doc_id| doc_id == relevant_doc_id)
            .map(|index| 1.0 / (index as f64 + 2.0).log2())
            .unwrap_or(0.0)
    }

    fn reciprocal_rank_at_10(hits: &[String], relevant_doc_id: &str) -> f64 {
        hits.iter()
            .take(10)
            .position(|doc_id| doc_id == relevant_doc_id)
            .map(|index| 1.0 / (index as f64 + 1.0))
            .unwrap_or(0.0)
    }

    #[test]
    fn text_lane_loads_postings_and_searches() {
        let temp_dir = tempdir().unwrap();
        fs::write(
            temp_dir.path().join("postings.jsonl"),
            concat!(
                "{\"token\":\"alpha\",\"doc_ids\":[\"doc-1\",\"doc-2\"]}\n",
                "{\"token\":\"beta\",\"doc_ids\":[\"doc-2\"]}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha beta\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":true}}\n",
        )
        .unwrap();

        let lane = TextLane::load(temp_dir.path(), &test_manifest()).unwrap();

        assert_eq!(lane.search_first_text_query(), vec!["doc-2", "doc-1"]);
        assert_eq!(lane.search("alpha"), vec!["doc-1", "doc-2"]);
        assert_eq!(lane.first_hybrid_query(), Some("alpha beta"));
        assert_eq!(lane.first_hybrid_top_k(), 2);
    }

    #[test]
    fn bm25_diagnostics_rank_repeated_terms_and_explain_contributions() {
        let (segment, _) = BinaryTextSegment::from_document_refs_with_analyzer(
            [
                ("doc-short", "alpha"),
                ("doc-repeated", "alpha alpha alpha"),
                ("doc-other", "beta"),
            ],
            TextAnalyzerProfile::current(),
        );
        let lane = TextLane {
            first_text_query: String::new(),
            first_text_top_k: 0,
            first_hybrid_query: None,
            first_hybrid_top_k: 0,
            index: segment.try_into_index().unwrap(),
        };

        let diagnostics = lane.search_with_diagnostics("alpha", 2);

        assert_eq!(
            diagnostics
                .iter()
                .map(|hit| hit.doc_id.as_str())
                .collect::<Vec<_>>(),
            vec!["doc-repeated", "doc-short"]
        );
        assert!(diagnostics[0].score > diagnostics[1].score);
        assert_eq!(diagnostics[0].doc_length, 3);
        assert_eq!(diagnostics[0].terms[0].token, "alpha");
        assert_eq!(diagnostics[0].terms[0].term_frequency, 3);
        assert_eq!(diagnostics[0].terms[0].document_frequency, 2);
    }

    #[test]
    fn text_lane_metadata_resolves_persisted_inputs_without_query_sidecars() {
        let mount_root = PathBuf::from("/tmp/rax-text");
        let metadata = TextLaneMetadata::resolve(&mount_root, &test_manifest()).unwrap();
        let query_inputs = TextQueryInputs::resolve(&mount_root, &test_manifest()).unwrap();

        assert_eq!(
            metadata.source,
            TextLaneSource::Compatibility {
                postings_path: mount_root.join("postings.jsonl")
            }
        );
        assert_eq!(
            query_inputs.query_paths,
            vec![mount_root.join("queries.jsonl")]
        );
    }

    #[test]
    fn text_lane_rejects_manifest_paths_outside_root() {
        let mount_root = tempdir().unwrap();
        let mut manifest = test_manifest();
        manifest.query_sets[0].path = "../queries.jsonl".to_owned();

        let error = TextLane::load(mount_root.path(), &manifest).unwrap_err();

        assert!(error.contains("must stay within dataset root"));
    }

    #[test]
    fn text_lane_loads_postings_without_query_set_metadata() {
        let temp_dir = tempdir().unwrap();
        fs::write(
            temp_dir.path().join("postings.jsonl"),
            "{\"token\":\"alpha\",\"doc_ids\":[\"doc-1\"]}\n",
        )
        .unwrap();
        let mut manifest = test_manifest();
        manifest.query_sets.clear();

        let lane = TextLane::load(temp_dir.path(), &manifest).unwrap();

        assert_eq!(lane.search("alpha"), vec!["doc-1"]);
        assert!(lane.search_first_text_query().is_empty());
        assert_eq!(lane.first_hybrid_query(), None);
        assert_eq!(lane.first_hybrid_top_k(), 0);
    }

    #[test]
    fn text_lane_load_skips_empty_query_lines() {
        let temp_dir = tempdir().unwrap();
        fs::write(
            temp_dir.path().join("postings.jsonl"),
            concat!(
                "{\"token\":\"alpha\",\"doc_ids\":[\"doc-1\"]}\n",
                "{\"token\":\"beta\",\"doc_ids\":[\"doc-2\"]}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            concat!(
                "\n",
                "   \n",
                "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha\",\"top_k\":1,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":false}}\n",
                "\n",
                "{\"query_id\":\"q-002\",\"query_class\":\"hybrid\",\"difficulty\":\"easy\",\"query_text\":\"beta\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":true,\"vector\":true,\"hybrid\":true}}\n",
            ),
        )
        .unwrap();

        let lane = TextLane::load(temp_dir.path(), &test_manifest()).unwrap();

        assert_eq!(lane.search_first_text_query(), vec!["doc-1"]);
        assert_eq!(lane.first_hybrid_query(), Some("beta"));
        assert_eq!(lane.first_hybrid_top_k(), 2);
    }

    #[test]
    fn text_batch_queries_load_and_search_through_text_lane() {
        let temp_dir = tempdir().unwrap();
        fs::write(
            temp_dir.path().join("postings.jsonl"),
            concat!(
                "{\"token\":\"alpha\",\"doc_ids\":[\"doc-1\",\"doc-2\"]}\n",
                "{\"token\":\"beta\",\"doc_ids\":[\"doc-2\",\"doc-3\"]}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            concat!(
                "{\"query_id\":\"q-text\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha beta\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":false}}\n",
                "{\"query_id\":\"q-hybrid\",\"query_class\":\"hybrid\",\"difficulty\":\"easy\",\"query_text\":\"beta\",\"top_k\":3,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":true,\"lane_eligibility\":{\"text\":true,\"vector\":true,\"hybrid\":true}}\n",
            ),
        )
        .unwrap();

        let lane = TextLane::load(temp_dir.path(), &test_manifest()).unwrap();
        let queries = TextBatchQuery::load_jsonl(&temp_dir.path().join("queries.jsonl")).unwrap();
        let results = lane.search_batch(&queries);

        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].query_id, "q-text");
        assert!(queries[0].uses_text_lane());
        assert!(queries[0].filter_spec.is_empty());
        assert_eq!(results[0].query_id, "q-text");
        assert_eq!(results[0].hits, vec!["doc-2", "doc-1"]);
        assert_eq!(results[1].query_id, "q-hybrid");
        assert_eq!(results[1].hits, vec!["doc-3", "doc-2"]);
    }

    #[test]
    fn text_batch_queries_preserve_top_level_string_filter_spec() {
        let temp_dir = tempdir().unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            "{\"query_id\":\"q-filtered\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha\",\"top_k\":2,\"filter_spec\":{\"workspace_id\":\"w1\",\"ignored\":1},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":false}}\n",
        )
        .unwrap();

        let queries = TextBatchQuery::load_jsonl(&temp_dir.path().join("queries.jsonl")).unwrap();

        assert_eq!(
            queries[0].filter_spec.equals,
            vec![("workspace_id".to_owned(), "w1".to_owned())]
        );
    }

    #[test]
    fn text_lane_prefers_manifest_visible_segment_when_sidecar_is_missing() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.rax");
        fs::write(
            temp_dir.path().join("docs.ndjson"),
            concat!(
                "{\"doc_id\":\"doc-1\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-2\",\"text\":\"alpha beta\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("postings.jsonl"),
            concat!(
                "{\"token\":\"alpha\",\"doc_ids\":[\"doc-1\",\"doc-2\"]}\n",
                "{\"token\":\"beta\",\"doc_ids\":[\"doc-2\"]}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha beta\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":true}}\n",
        )
        .unwrap();
        create_empty_store(&store_path).unwrap();

        publish_compatibility_text_segment(temp_dir.path(), &test_manifest(), &store_path).unwrap();
        fs::remove_file(temp_dir.path().join("postings.jsonl")).unwrap();

        let lane = TextLane::load(temp_dir.path(), &test_manifest()).unwrap();

        assert_eq!(lane.search_first_text_query(), vec!["doc-2", "doc-1"]);
        assert_eq!(lane.search("alpha"), vec!["doc-1", "doc-2"]);
    }

    #[test]
    fn text_lane_rejects_stale_store_segment_when_documents_are_newer() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.rax");
        fs::write(
            temp_dir.path().join("docs.ndjson"),
            concat!(
                "{\"doc_id\":\"doc-1\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-2\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("postings.jsonl"),
            concat!(
                "{\"token\":\"alpha\",\"doc_ids\":[\"doc-1\"]}\n",
                "{\"token\":\"beta\",\"doc_ids\":[\"doc-2\"]}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":false}}\n",
        )
        .unwrap();
        create_empty_store(&store_path).unwrap();
        publish_compatibility_text_segment(temp_dir.path(), &test_manifest(), &store_path).unwrap();

        let doc_pending = prepare_raw_documents_segment(
            &store_path,
            vec![
                (
                    "doc-1".to_owned(),
                    json!({"doc_id":"doc-1","text":"alpha updated"}),
                ),
                ("doc-2".to_owned(), json!({"doc_id":"doc-2","text":"beta"})),
            ],
        )
        .unwrap();
        rax_core::publish_segments_with_precondition(&store_path, vec![doc_pending], |_| Ok(()))
            .unwrap();

        let error = TextLane::load(temp_dir.path(), &test_manifest()).unwrap_err();
        assert!(error.contains("stale"));
    }

    #[test]
    fn text_lane_rejects_text_store_segment_without_document_segment() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.rax");
        fs::write(
            temp_dir.path().join("docs.ndjson"),
            concat!(
                "{\"doc_id\":\"doc-1\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-2\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("postings.jsonl"),
            concat!(
                "{\"token\":\"alpha\",\"doc_ids\":[\"doc-1\"]}\n",
                "{\"token\":\"beta\",\"doc_ids\":[\"doc-2\"]}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":false}}\n",
        )
        .unwrap();
        create_empty_store(&store_path).unwrap();
        let text_pending =
            prepare_compatibility_text_segment(temp_dir.path(), &test_manifest()).unwrap();
        publish_segments(&store_path, vec![text_pending]).unwrap();

        let error = TextLane::load(temp_dir.path(), &test_manifest()).unwrap_err();

        assert!(error.contains("requires a matching document segment"));
    }

    #[test]
    fn compatibility_text_publish_rejects_store_documents_from_different_snapshot() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.rax");
        fs::write(
            temp_dir.path().join("docs.ndjson"),
            concat!(
                "{\"doc_id\":\"doc-1\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-2\",\"text\":\"beta\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("postings.jsonl"),
            concat!(
                "{\"token\":\"alpha\",\"doc_ids\":[\"doc-1\"]}\n",
                "{\"token\":\"beta\",\"doc_ids\":[\"doc-2\"]}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":false}}\n",
        )
        .unwrap();
        create_empty_store(&store_path).unwrap();
        let stale_doc_pending = prepare_raw_documents_segment(
            &store_path,
            vec![
                (
                    "doc-1".to_owned(),
                    json!({"doc_id":"doc-1","text":"wrong alpha"}),
                ),
                ("doc-2".to_owned(), json!({"doc_id":"doc-2","text":"beta"})),
            ],
        )
        .unwrap();
        publish_segments(&store_path, vec![stale_doc_pending]).unwrap();

        let error =
            publish_compatibility_text_segment(temp_dir.path(), &test_manifest(), &store_path)
                .unwrap_err();

        assert!(error.contains("store doc segment does not match mounted dataset payload"));
    }

    #[test]
    fn text_lane_rejects_store_documents_without_matching_text_segment() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.rax");
        fs::write(
            temp_dir.path().join("docs.ndjson"),
            concat!(
                "{\"doc_id\":\"doc-1\",\"text\":\"alpha updated\"}\n",
                "{\"doc_id\":\"doc-2\",\"text\":\"beta updated\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("postings.jsonl"),
            concat!(
                "{\"token\":\"alpha\",\"doc_ids\":[\"doc-1\"]}\n",
                "{\"token\":\"beta\",\"doc_ids\":[\"doc-2\"]}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":false}}\n",
        )
        .unwrap();
        create_empty_store(&store_path).unwrap();
        let doc_pending = prepare_raw_documents_segment(
            &store_path,
            vec![
                (
                    "doc-1".to_owned(),
                    json!({"doc_id":"doc-1","text":"alpha updated"}),
                ),
                (
                    "doc-2".to_owned(),
                    json!({"doc_id":"doc-2","text":"beta updated"}),
                ),
            ],
        )
        .unwrap();
        publish_segments(&store_path, vec![doc_pending]).unwrap();

        let error = TextLane::load(temp_dir.path(), &test_manifest()).unwrap_err();

        assert!(error.contains("no matching text segment"));
    }

    #[test]
    fn validate_store_segment_rejects_manifest_visible_segment_when_mounted_docs_do_not_match() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.rax");
        let docs_path = temp_dir.path().join("docs.ndjson");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-1\",\"text\":\"alpha\"}\n",
                "{\"doc_id\":\"doc-2\",\"text\":\"alpha beta\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("postings.jsonl"),
            concat!(
                "{\"token\":\"alpha\",\"doc_ids\":[\"doc-1\",\"doc-2\"]}\n",
                "{\"token\":\"beta\",\"doc_ids\":[\"doc-2\"]}\n",
            ),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha beta\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":true}}\n",
        )
        .unwrap();
        create_empty_store(&store_path).unwrap();

        publish_compatibility_text_segment(temp_dir.path(), &test_manifest(), &store_path).unwrap();
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-1\",\"text\":\"alpha changed\"}\n",
                "{\"doc_id\":\"doc-2\",\"text\":\"alpha beta\"}\n",
            ),
        )
        .unwrap();

        let error =
            crate::validate_store_segment_against_dataset_pack(temp_dir.path(), &test_manifest())
                .unwrap_err();

        assert!(error.contains("store text segment does not match mounted dataset documents"));
    }

    fn test_manifest() -> DatasetPackManifest {
        serde_json::from_value(json!({
            "schema_version": "rax_dataset_pack",
            "generated_at": "2026-04-19T00:00:00Z",
            "generator": {"name":"test","version":"0.1.0"},
            "identity": {
                "dataset_id":"knowledge-small-clean",
                "dataset_version":"current",
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
                "doc_count":2,
                "vector_count":2,
                "total_text_bytes":9,
                "avg_doc_length":4.5,
                "median_doc_length":4,
                "p95_doc_length":5,
                "max_doc_length":5,
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
                "embedding_dimensions": 384,
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
            "files": [
                {"path":"docs.ndjson","kind":"documents","format":"ndjson","record_count":2,"checksum":"sha256:documents"},
                {"path":"postings.jsonl","kind":"text_postings","format":"jsonl","record_count":2,"checksum":"sha256:postings"}
            ],
            "query_sets": [
                {
                    "query_set_id":"core",
                    "path":"queries.jsonl",
                    "ground_truth_path":"ground_truth.jsonl",
                    "query_count":1,
                    "classes":["keyword"],
                    "difficulty_distribution":{"easy":1,"medium":0,"hard":0}
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
}
