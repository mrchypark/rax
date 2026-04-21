use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use wax_bench_model::{tokenize, DatasetPackManifest};
use wax_v2_core::{PendingSegmentDescriptor, PendingSegmentWrite, SegmentDescriptor, SegmentKind};

const TEXT_SEGMENT_MAGIC: &[u8; 4] = b"WXTG";
const TEXT_SEGMENT_MAJOR: u16 = 1;
const TEXT_SEGMENT_MINOR: u16 = 0;
const TEXT_SEGMENT_HEADER_LENGTH: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextLane {
    first_text_query: String,
    first_text_top_k: usize,
    first_hybrid_query: Option<String>,
    first_hybrid_top_k: usize,
    inverted: HashMap<String, Vec<String>>,
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
        BufReader::new(File::open(path).map_err(|error| error.to_string())?)
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
    indexed_doc_count: usize,
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
        let store_path = mount_root.join("store.wax");
        if store_path.exists() {
            let opened = wax_v2_core::open_store(&store_path).map_err(|error| error.to_string())?;
            if let Some(descriptor) = opened
                .manifest
                .segments
                .iter()
                .filter(|segment| segment.family == SegmentKind::Txt)
                .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
                .cloned()
            {
                return Ok(Self {
                    indexed_doc_count: manifest.corpus.doc_count as usize,
                    source: TextLaneSource::Store {
                        store_path,
                        descriptor,
                    },
                });
            }
        }

        Ok(Self {
            indexed_doc_count: manifest.corpus.doc_count as usize,
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
            .map(|query_set| mount_root.join(&query_set.path))
            .collect::<Vec<_>>();

        if query_paths.is_empty() {
            return Err("query_set file missing from manifest".to_owned());
        }

        Ok(Self { query_paths })
    }
}

impl TextLane {
    pub fn load(mount_root: &Path, manifest: &DatasetPackManifest) -> Result<Self, String> {
        let metadata = TextLaneMetadata::resolve(mount_root, manifest)?;
        let query_inputs = TextQueryInputs::resolve(mount_root, manifest).ok();
        let (first_text_query, first_text_top_k, first_hybrid_query) =
            if let Some(query_inputs) = query_inputs.as_ref() {
                let (first_text_query, first_text_top_k) =
                    load_first_text_query(&query_inputs.query_paths)?;
                let first_hybrid_query = load_first_hybrid_text_query(&query_inputs.query_paths)?;
                (first_text_query, first_text_top_k, first_hybrid_query)
            } else {
                (String::new(), 0, None)
            };
        let inverted = load_text_postings(&metadata)?;

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

pub fn publish_compatibility_text_segment(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
    store_path: &Path,
) -> Result<(), String> {
    let prepared = prepare_compatibility_text_segment(mount_root, manifest)?;
    wax_v2_core::publish_segment(store_path, prepared.descriptor, &prepared.object_bytes)
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
        .map(|file| mount_root.join(&file.path))
        .ok_or_else(|| "documents file missing from manifest".to_owned())?;
    let documents = load_documents_for_text_builder(&documents_path)?;
    prepare_text_segment_from_documents(&documents)
}

pub fn prepare_text_segment_from_documents(
    documents: &[(String, String)],
) -> Result<PendingSegmentWrite, String> {
    let segment = BinaryTextSegment::from_documents(documents);
    let object_bytes = segment.encode()?;
    Ok(PendingSegmentWrite {
        descriptor: PendingSegmentDescriptor {
            family: SegmentKind::Txt,
            family_version: 1,
            flags: 0,
            doc_id_start: 0,
            doc_id_end_exclusive: documents.len() as u64,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
            live_items: documents.len() as u64,
            tombstoned_items: 0,
            backend_id: 0,
            backend_aux: segment.postings.len() as u64,
        },
        object_bytes,
    })
}

pub fn validate_store_segment_against_dataset_pack(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<(), String> {
    let Some(documents_path) = documents_path_from_manifest(mount_root, manifest) else {
        return Ok(());
    };
    let store_path = mount_root.join("store.wax");
    if !store_path.exists() {
        return Ok(());
    }

    let opened = wax_v2_core::open_store(&store_path).map_err(|error| error.to_string())?;
    let Some(descriptor) = opened
        .manifest
        .segments
        .iter()
        .filter(|segment| segment.family == SegmentKind::Txt)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
    else {
        return Ok(());
    };

    let bytes = wax_v2_core::map_segment_object(&store_path, descriptor)
        .map_err(|error| error.to_string())?;
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
        for line in BufReader::new(File::open(path).map_err(|error| error.to_string())?).lines() {
            let line = line.map_err(|error| error.to_string())?;
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
        for line in BufReader::new(File::open(path).map_err(|error| error.to_string())?).lines() {
            let line = line.map_err(|error| error.to_string())?;
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
        .map(|file| mount_root.join(&file.path))
        .ok_or_else(|| "text_postings file missing from manifest".to_owned())
}

fn documents_path_from_manifest(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Option<PathBuf> {
    manifest
        .files
        .iter()
        .find(|file| file.kind == "documents")
        .map(|file| mount_root.join(&file.path))
        .filter(|path| path.exists())
}

fn load_documents_for_text_builder(path: &Path) -> Result<Vec<(String, String)>, String> {
    BufReader::new(File::open(path).map_err(|error| error.to_string())?)
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

fn load_text_postings(metadata: &TextLaneMetadata) -> Result<HashMap<String, Vec<String>>, String> {
    match &metadata.source {
        TextLaneSource::Compatibility { postings_path } => {
            load_text_postings_from_path(postings_path)
        }
        TextLaneSource::Store {
            store_path,
            descriptor,
        } => {
            let bytes = wax_v2_core::map_segment_object(store_path, descriptor)
                .map_err(|error| error.to_string())?;
            BinaryTextSegment::decode(&bytes).map(|segment| segment.into_inverted())
        }
    }
}

fn load_text_postings_from_path(path: &Path) -> Result<HashMap<String, Vec<String>>, String> {
    let reader = BufReader::new(File::open(path).map_err(|error| error.to_string())?);
    let mut postings = HashMap::new();
    for line in reader.lines() {
        let line = line.map_err(|error| error.to_string())?;
        let posting: TextPostingRecord =
            serde_json::from_str(&line).map_err(|error| error.to_string())?;
        postings.insert(posting.token, posting.doc_ids);
    }
    Ok(postings)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BinaryTextSegment {
    postings: Vec<TextPostingRecord>,
}

impl BinaryTextSegment {
    fn from_documents(documents: &[(String, String)]) -> Self {
        let mut inverted: HashMap<String, Vec<String>> = HashMap::new();
        for (doc_id, text) in documents {
            let mut seen_tokens = std::collections::HashSet::new();
            for token in tokenize(text) {
                if seen_tokens.insert(token.clone()) {
                    inverted.entry(token).or_default().push(doc_id.clone());
                }
            }
        }
        let mut postings = inverted
            .into_iter()
            .map(|(token, mut doc_ids)| {
                doc_ids.sort();
                TextPostingRecord { token, doc_ids }
            })
            .collect::<Vec<_>>();
        postings.sort_by(|left, right| left.token.cmp(&right.token));
        Self { postings }
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        for pair in self.postings.windows(2) {
            if pair[0].token >= pair[1].token {
                return Err("text segment tokens must be sorted and unique".to_owned());
            }
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(TEXT_SEGMENT_MAGIC);
        bytes.extend_from_slice(&TEXT_SEGMENT_MAJOR.to_le_bytes());
        bytes.extend_from_slice(&TEXT_SEGMENT_MINOR.to_le_bytes());
        bytes.extend_from_slice(&(self.postings.len() as u64).to_le_bytes());
        for posting in &self.postings {
            bytes.extend_from_slice(&(posting.token.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&(posting.doc_ids.len() as u32).to_le_bytes());
            bytes.extend_from_slice(posting.token.as_bytes());
            for doc_id in &posting.doc_ids {
                bytes.extend_from_slice(&(doc_id.len() as u32).to_le_bytes());
                bytes.extend_from_slice(doc_id.as_bytes());
            }
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
        if read_u16(bytes, 4) != TEXT_SEGMENT_MAJOR || read_u16(bytes, 6) != TEXT_SEGMENT_MINOR {
            return Err("unsupported text segment version".to_owned());
        }

        let record_count = read_u64(bytes, 8) as usize;
        let mut cursor = TEXT_SEGMENT_HEADER_LENGTH;
        let mut postings = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let token_length = read_u32_at(bytes, &mut cursor)? as usize;
            let doc_count = read_u32_at(bytes, &mut cursor)? as usize;
            let token = read_string_at(bytes, &mut cursor, token_length)?;
            let mut doc_ids = Vec::with_capacity(doc_count);
            for _ in 0..doc_count {
                let doc_id_length = read_u32_at(bytes, &mut cursor)? as usize;
                doc_ids.push(read_string_at(bytes, &mut cursor, doc_id_length)?);
            }
            postings.push(TextPostingRecord { token, doc_ids });
        }
        if cursor != bytes.len() {
            return Err("text segment trailing bytes mismatch".to_owned());
        }
        for pair in postings.windows(2) {
            if pair[0].token >= pair[1].token {
                return Err("text segment tokens must be sorted and unique".to_owned());
            }
        }

        Ok(Self { postings })
    }

    fn into_inverted(self) -> HashMap<String, Vec<String>> {
        self.postings
            .into_iter()
            .map(|posting| (posting.token, posting.doc_ids))
            .collect()
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

    use serde_json::json;
    use tempfile::tempdir;
    use wax_bench_model::DatasetPackManifest;
    use wax_v2_core::create_empty_store;

    use crate::{
        publish_compatibility_text_segment, TextBatchQuery, TextLane, TextLaneMetadata,
        TextLaneSource, TextQueryInputs,
    };

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
            concat!(
                "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha beta\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":true}}\n",
            ),
        )
        .unwrap();

        let lane = TextLane::load(temp_dir.path(), &test_manifest()).unwrap();

        assert_eq!(lane.search_first_text_query(), vec!["doc-2", "doc-1"]);
        assert_eq!(lane.search("alpha"), vec!["doc-1", "doc-2"]);
        assert_eq!(lane.first_hybrid_query(), Some("alpha beta"));
        assert_eq!(lane.first_hybrid_top_k(), 2);
    }

    #[test]
    fn text_lane_metadata_resolves_persisted_inputs_without_query_sidecars() {
        let mount_root = PathBuf::from("/tmp/wax-text");
        let metadata = TextLaneMetadata::resolve(&mount_root, &test_manifest()).unwrap();
        let query_inputs = TextQueryInputs::resolve(&mount_root, &test_manifest()).unwrap();

        assert_eq!(metadata.indexed_doc_count, 2);
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
        assert_eq!(results[1].hits, vec!["doc-2", "doc-3"]);
    }

    #[test]
    fn text_batch_queries_preserve_top_level_string_filter_spec() {
        let temp_dir = tempdir().unwrap();
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            concat!(
                "{\"query_id\":\"q-filtered\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha\",\"top_k\":2,\"filter_spec\":{\"workspace_id\":\"w1\",\"ignored\":1},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":false}}\n",
            ),
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
        let store_path = temp_dir.path().join("store.wax");
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
            concat!(
                "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha beta\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":true}}\n",
            ),
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
    fn validate_store_segment_rejects_manifest_visible_segment_when_mounted_docs_do_not_match() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
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
            concat!(
                "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha beta\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":true,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":true}}\n",
            ),
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
