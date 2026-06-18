use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;

use rax_bench_model::{tokenize, DatasetPackManifest};
use rax_core::{PendingSegmentDescriptor, PendingSegmentWrite, SegmentDescriptor, SegmentKind};
use serde::Deserialize;
use serde_json::Value;

const TEXT_SEGMENT_MAGIC: &[u8; 4] = b"RXTG";
const TEXT_SEGMENT_MAJOR: u16 = 1;
const TEXT_SEGMENT_MINOR: u16 = 0;
const TEXT_SEGMENT_HEADER_LENGTH: usize = 16;

#[derive(Debug)]
pub struct TextLane {
    first_text_query: String,
    first_text_top_k: usize,
    first_hybrid_query: Option<String>,
    first_hybrid_top_k: usize,
    postings: TextPostings,
}

#[derive(Debug)]
enum TextPostings {
    InMemory(HashMap<String, Vec<String>>),
    LazyStore(LazyStoreTextPostings),
}

#[derive(Debug)]
struct LazyStoreTextPostings {
    bytes: rax_core::SegmentObject,
    cache: Mutex<HashMap<String, Vec<String>>>,
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
        Self::resolve_with_store_path(mount_root, manifest, &store_path, true)
    }

    fn resolve_with_store_path(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        store_path: &Path,
        validate_store_doc_payloads: bool,
    ) -> Result<Self, String> {
        Self::resolve_with_store_path_mode(
            mount_root,
            manifest,
            store_path,
            validate_store_doc_payloads,
            true,
        )
    }

    fn resolve_with_store_path_mode(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        store_path: &Path,
        validate_store_doc_payloads: bool,
        validate_active_segments: bool,
    ) -> Result<Self, String> {
        if store_path.exists() {
            let opened = if validate_active_segments {
                rax_core::open_store(store_path)
            } else {
                rax_core::open_store_shallow(store_path)
            }
            .map_err(|error| error.to_string())?;
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
                if validate_store_doc_payloads {
                    validate_text_segment_against_store_doc_descriptor(
                        store_path,
                        &descriptor,
                        doc_descriptor,
                    )?;
                }
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
        let metadata =
            TextLaneMetadata::resolve_with_store_path(mount_root, manifest, store_path, true)?;
        Self::load_with_metadata(mount_root, manifest, metadata)
    }

    pub fn load_runtime_with_store_path(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        store_path: &Path,
    ) -> Result<Self, String> {
        let metadata =
            TextLaneMetadata::resolve_with_store_path(mount_root, manifest, store_path, false)?;
        Self::load_with_metadata(mount_root, manifest, metadata)
    }

    pub fn load_runtime_snapshot_with_store_path(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
        store_path: &Path,
    ) -> Result<Self, String> {
        let metadata = TextLaneMetadata::resolve_with_store_path_mode(
            mount_root, manifest, store_path, false, false,
        )?;
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
        let postings = load_text_postings(&metadata)?;

        Ok(Self {
            first_text_query,
            first_text_top_k,
            first_hybrid_query: first_hybrid_query
                .as_ref()
                .map(|query| query.query_text.clone()),
            first_hybrid_top_k: first_hybrid_query.map(|query| query.top_k).unwrap_or(0),
            postings,
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
            for doc_id in self.postings.doc_ids_for_token(&token) {
                *scores.entry(doc_id).or_insert(0) += 1;
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
    let (segment, doc_count) = BinaryTextSegment::from_document_refs(documents);
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

impl TextPostings {
    fn doc_ids_for_token(&self, token: &str) -> Vec<String> {
        match self {
            TextPostings::InMemory(inverted) => inverted.get(token).cloned().unwrap_or_default(),
            TextPostings::LazyStore(postings) => postings.doc_ids_for_token(token),
        }
    }
}

impl LazyStoreTextPostings {
    fn new(bytes: rax_core::SegmentObject) -> Self {
        Self {
            bytes,
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn doc_ids_for_token(&self, token: &str) -> Vec<String> {
        if let Some(doc_ids) = self
            .cache
            .lock()
            .expect("text postings cache mutex poisoned")
            .get(token)
            .cloned()
        {
            return doc_ids;
        }
        let doc_ids = find_doc_ids_for_token(&self.bytes, token).unwrap_or_default();
        self.cache
            .lock()
            .expect("text postings cache mutex poisoned")
            .insert(token.to_owned(), doc_ids.clone());
        doc_ids
    }
}

fn load_text_postings(metadata: &TextLaneMetadata) -> Result<TextPostings, String> {
    match &metadata.source {
        TextLaneSource::Compatibility { postings_path } => {
            load_text_postings_from_path(postings_path).map(TextPostings::InMemory)
        }
        TextLaneSource::Store {
            store_path,
            descriptor,
        } => {
            let bytes = rax_core::map_segment_object(store_path, descriptor)
                .map_err(|error| error.to_string())?;
            validate_binary_text_segment_header(&bytes)?;
            Ok(TextPostings::LazyStore(LazyStoreTextPostings::new(bytes)))
        }
    }
}

fn load_text_postings_from_path(path: &Path) -> Result<HashMap<String, Vec<String>>, String> {
    let reader = BufReader::new(open_read_no_symlinks(path)?);
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
        Self::from_document_refs(
            documents
                .iter()
                .map(|(doc_id, text)| (doc_id.as_str(), text.as_str())),
        )
        .0
    }

    fn from_document_refs<'a, I>(documents: I) -> (Self, usize)
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut inverted: HashMap<String, Vec<String>> = HashMap::new();
        let mut doc_count = 0;
        for (doc_id, text) in documents {
            doc_count += 1;
            let mut seen_tokens = std::collections::HashSet::new();
            for token in tokenize(text) {
                if seen_tokens.insert(token.clone()) {
                    inverted.entry(token).or_default().push(doc_id.to_owned());
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
        (Self { postings }, doc_count)
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

        let record_count = usize::try_from(read_u64(bytes, 8))
            .map_err(|_| "text segment record_count exceeds addressable memory".to_owned())?;
        let mut cursor = TEXT_SEGMENT_HEADER_LENGTH;
        if record_count > bytes[cursor..].len() / 8 {
            return Err("text segment record_count exceeds possible records in slice".to_owned());
        }
        let mut postings = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let token_length = read_u32_at(bytes, &mut cursor)? as usize;
            let doc_count = read_u32_at(bytes, &mut cursor)? as usize;
            let token = read_string_at(bytes, &mut cursor, token_length)?;
            if doc_count > bytes[cursor..].len() / 4 {
                return Err("text segment doc_count exceeds possible records in slice".to_owned());
            }
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
}

fn validate_binary_text_segment_header(bytes: &[u8]) -> Result<(), String> {
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
    Ok(())
}

fn find_doc_ids_for_token(bytes: &[u8], wanted: &str) -> Result<Vec<String>, String> {
    validate_binary_text_segment_header(bytes)?;
    let record_count = usize::try_from(read_u64(bytes, 8))
        .map_err(|_| "text segment record_count exceeds addressable memory".to_owned())?;
    let mut cursor = TEXT_SEGMENT_HEADER_LENGTH;
    if record_count > bytes[cursor..].len() / 8 {
        return Err("text segment record_count exceeds possible records in slice".to_owned());
    }
    for _ in 0..record_count {
        let token_length = read_u32_at(bytes, &mut cursor)? as usize;
        let doc_count = read_u32_at(bytes, &mut cursor)? as usize;
        let token_start = cursor;
        let token_end = token_start
            .checked_add(token_length)
            .ok_or_else(|| "text segment token range overflow".to_owned())?;
        if token_end > bytes.len() {
            return Err("text segment truncated while reading token".to_owned());
        }
        cursor = token_end;
        if doc_count > bytes[cursor..].len() / 4 {
            return Err("text segment doc_count exceeds possible records in slice".to_owned());
        }

        let ordering = bytes[token_start..token_end].cmp(wanted.as_bytes());
        if ordering == std::cmp::Ordering::Equal {
            let mut doc_ids = Vec::with_capacity(doc_count);
            for _ in 0..doc_count {
                let doc_id_length = read_u32_at(bytes, &mut cursor)? as usize;
                doc_ids.push(read_string_at(bytes, &mut cursor, doc_id_length)?);
            }
            return Ok(doc_ids);
        }

        for _ in 0..doc_count {
            let doc_id_length = read_u32_at(bytes, &mut cursor)? as usize;
            cursor = cursor
                .checked_add(doc_id_length)
                .ok_or_else(|| "text segment doc_id range overflow".to_owned())?;
            if cursor > bytes.len() {
                return Err("text segment truncated while skipping doc_id".to_owned());
            }
        }
        if ordering == std::cmp::Ordering::Greater {
            return Ok(Vec::new());
        }
    }
    if cursor != bytes.len() {
        return Err("text segment trailing bytes mismatch".to_owned());
    }
    Ok(Vec::new())
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
        TextBatchQuery, TextLane, TextLaneMetadata, TextLaneSource, TextQueryInputs,
        TEXT_SEGMENT_MAGIC,
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
        assert_eq!(results[1].hits, vec!["doc-2", "doc-3"]);
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
    fn text_lane_runtime_load_trusts_manifest_visible_store_segment() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.rax");
        fs::write(
            temp_dir.path().join("queries.jsonl"),
            "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\",\"query_text\":\"alpha\",\"top_k\":2,\"filter_spec\":{},\"preview_expected\":false,\"embedding_available\":false,\"lane_eligibility\":{\"text\":true,\"vector\":false,\"hybrid\":false}}\n",
        )
        .unwrap();
        create_empty_store(&store_path).unwrap();
        let doc_pending = prepare_raw_documents_segment(
            &store_path,
            vec![
                ("doc-1".to_owned(), json!({"doc_id":"doc-1","text":"gamma"})),
                ("doc-2".to_owned(), json!({"doc_id":"doc-2","text":"delta"})),
            ],
        )
        .unwrap();
        let text_pending = crate::prepare_text_segment_from_documents(&[
            ("doc-1".to_owned(), "alpha".to_owned()),
            ("doc-2".to_owned(), "beta".to_owned()),
        ])
        .unwrap();
        publish_segments(&store_path, vec![doc_pending, text_pending]).unwrap();

        let lane =
            TextLane::load_runtime_with_store_path(temp_dir.path(), &test_manifest(), &store_path)
                .unwrap();

        assert_eq!(lane.search("alpha"), vec!["doc-1"]);
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
