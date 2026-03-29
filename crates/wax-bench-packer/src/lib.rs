use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use wax_bench_model::{
    CorpusProfile, DatasetIdentity, DatasetPackManifest, DifficultyDistribution, DirtyProfile,
    EnvironmentConstraints, LanguageShare, LengthBuckets, ManifestChecksums, ManifestFile,
    ManifestGenerator, MetadataProfile, QuerySetEntry, QueryVectorProfile, SegmentTopologyEntry,
    TextProfile, VectorProfile,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackRequest {
    pub source_dir: PathBuf,
    pub out_dir: PathBuf,
    pub tier: String,
    pub variant: String,
}

impl PackRequest {
    pub fn new(
        source_dir: impl Into<PathBuf>,
        out_dir: impl Into<PathBuf>,
        tier: impl Into<String>,
        variant: impl Into<String>,
    ) -> Self {
        Self {
            source_dir: source_dir.into(),
            out_dir: out_dir.into(),
            tier: tier.into(),
            variant: variant.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackError {
    pub message: String,
}

impl PackError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub fn pack_dataset(request: &PackRequest) -> Result<DatasetPackManifest, PackError> {
    let source = load_source_config(&request.source_dir.join("source.json"))?;
    fs::create_dir_all(&request.out_dir)
        .map_err(|_| PackError::new("failed to create output directory"))?;

    let document_bytes = copy_file(
        &request.source_dir.join("docs.ndjson"),
        &request.out_dir.join("docs.ndjson"),
    )?;
    let document_stats = analyze_documents(&document_bytes)?;

    let mut files = vec![build_manifest_file(
        "docs.ndjson",
        "documents",
        "ndjson",
        document_stats.doc_count,
        &document_bytes,
    )];
    let mut query_sets = Vec::new();
    let mut logical_query_hasher = Sha256::new();

    let mut source_query_sets = source.query_sets.clone();
    source_query_sets.sort_by(|left, right| left.name.cmp(&right.name));
    for source_query_set in source_query_sets {
        let query_bytes = copy_file(
            &request.source_dir.join(&source_query_set.path),
            &request.out_dir.join(&source_query_set.path),
        )?;
        let ground_truth_bytes = copy_file(
            &request.source_dir.join(&source_query_set.ground_truth_path),
            &request.out_dir.join(&source_query_set.ground_truth_path),
        )?;

        let query_summary = analyze_query_set(&query_bytes)?;
        logical_query_hasher.update(&query_bytes);

        files.push(build_manifest_file(
            &source_query_set.path,
            "query_set",
            "jsonl",
            query_summary.query_count,
            &query_bytes,
        ));
        files.push(build_manifest_file(
            &source_query_set.ground_truth_path,
            "ground_truth",
            "jsonl",
            non_empty_line_count(&ground_truth_bytes),
            &ground_truth_bytes,
        ));

        query_sets.push(QuerySetEntry {
            query_set_id: format!(
                "{}-{}-{}-{}",
                source.dataset_family, request.tier, source_query_set.name, source.dataset_version
            ),
            path: source_query_set.path,
            ground_truth_path: source_query_set.ground_truth_path,
            query_count: query_summary.query_count,
            classes: query_summary.classes.into_iter().collect(),
            difficulty_distribution: query_summary.difficulty_distribution,
        });
    }

    let vector_profile = build_vector_profile(&source.embedding_spec_id);
    let clean_dataset_id = dataset_id(&source.dataset_family, &request.tier, "clean", &source.dataset_version);
    let dirty_profile = build_dirty_profile(&request.variant, &clean_dataset_id)?;
    let metadata_profile = source.metadata_profile.clone();
    let logical_metadata_checksum = checksum_label(
        &serde_json::to_vec(&metadata_profile)
            .map_err(|_| PackError::new("failed to serialize metadata profile for checksum"))?,
    );
    let query_fingerprint = manifest_query_fingerprint(&request.tier, &request.variant, &clean_dataset_id);
    let logical_query_fingerprint = serde_json::to_vec(&query_fingerprint)
        .map_err(|_| PackError::new("failed to serialize query fingerprint"))?;

    let mut manifest = DatasetPackManifest {
        schema_version: "wax_dataset_pack_v1".to_owned(),
        generated_at: source.generated_at,
        generator: ManifestGenerator {
            name: "wax-bench-packer".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
        },
        identity: DatasetIdentity {
            dataset_id: dataset_id(
                &source.dataset_family,
                &request.tier,
                &request.variant,
                &source.dataset_version,
            ),
            dataset_version: source.dataset_version,
            dataset_family: source.dataset_family,
            dataset_tier: request.tier.clone(),
            variant_id: request.variant.clone(),
            embedding_spec_id: source.embedding_spec_id.clone(),
            embedding_model_version: source.embedding_model_version,
            embedding_model_hash: source.embedding_model_hash,
            corpus_checksum: checksum_label(&document_bytes),
            query_checksum: format!("sha256:{:x}", logical_query_hasher.finalize()),
        },
        environment_constraints: source.environment_constraints,
        corpus: CorpusProfile {
            doc_count: document_stats.doc_count,
            vector_count: if vector_profile.enabled {
                document_stats.doc_count
            } else {
                0
            },
            total_text_bytes: document_stats.total_text_bytes,
            avg_doc_length: document_stats.avg_doc_length,
            median_doc_length: document_stats.median_doc_length,
            p95_doc_length: document_stats.p95_doc_length,
            max_doc_length: document_stats.max_doc_length,
            languages: source.languages,
        },
        text_profile: TextProfile {
            length_buckets: document_stats.length_buckets,
            tokenization_notes: None,
        },
        metadata_profile,
        vector_profile,
        dirty_profile,
        files,
        query_sets,
        checksums: ManifestChecksums {
            manifest_payload_checksum: "sha256:pending".to_owned(),
            logical_documents_checksum: checksum_label(&document_bytes),
            logical_metadata_checksum,
            logical_query_definitions_checksum: checksum_label(&logical_query_fingerprint),
            logical_vector_payload_checksum: None,
            fairness_fingerprint: checksum_label(&logical_query_fingerprint),
        },
    };

    manifest.checksums.manifest_payload_checksum = checksum_label(
        &serde_json::to_vec(&manifest)
            .map_err(|_| PackError::new("failed to serialize manifest checksum payload"))?,
    );

    let manifest_text = serde_json::to_string_pretty(&manifest)
        .map_err(|_| PackError::new("failed to serialize manifest"))?;
    fs::write(request.out_dir.join("manifest.json"), manifest_text)
        .map_err(|_| PackError::new("failed to write manifest"))?;

    validate_manifest(&manifest, &request.out_dir)
        .map_err(|error| PackError::new(error.message))?;

    Ok(manifest)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub message: String,
}

impl ValidationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct SourceConfig {
    dataset_family: String,
    dataset_version: String,
    generated_at: String,
    embedding_spec_id: String,
    embedding_model_version: String,
    embedding_model_hash: String,
    environment_constraints: EnvironmentConstraints,
    languages: Vec<LanguageShare>,
    metadata_profile: MetadataProfile,
    query_sets: Vec<SourceQuerySet>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct SourceQuerySet {
    name: String,
    path: String,
    ground_truth_path: String,
}

#[derive(Debug, Clone, PartialEq)]
struct DocumentStats {
    doc_count: u64,
    total_text_bytes: u64,
    avg_doc_length: f64,
    median_doc_length: u64,
    p95_doc_length: u64,
    max_doc_length: u64,
    length_buckets: LengthBuckets,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QuerySetSummary {
    query_count: u64,
    classes: BTreeSet<String>,
    difficulty_distribution: DifficultyDistribution,
}

pub fn validate_manifest(
    manifest: &DatasetPackManifest,
    pack_root: &Path,
) -> Result<(), ValidationError> {
    validate_constrained_values(manifest)?;
    validate_file_references(manifest)?;
    validate_query_ids(manifest, pack_root)?;
    validate_vector_profile(manifest)?;
    validate_dirty_profile(manifest)?;
    validate_vector_checksum_requirements(manifest)?;
    validate_file_payloads(manifest, pack_root)?;

    Ok(())
}

fn validate_query_ids(
    manifest: &DatasetPackManifest,
    pack_root: &Path,
) -> Result<(), ValidationError> {
    let mut seen_query_set_ids = HashSet::new();
    let mut seen_query_ids = HashSet::new();
    for query_set in &manifest.query_sets {
        if !seen_query_set_ids.insert(query_set.query_set_id.clone()) {
            return Err(ValidationError::new("duplicate query_set_id"));
        }

        let query_ids = load_query_ids(pack_root.join(&query_set.path))?;
        if query_set.query_count != query_ids.len() as u64 {
            return Err(ValidationError::new(
                "query_count must match query file contents",
            ));
        }

        let ground_truth_ids = load_ground_truth_ids(pack_root.join(&query_set.ground_truth_path))?;
        if query_ids.iter().cloned().collect::<HashSet<_>>()
            != ground_truth_ids.into_iter().collect::<HashSet<_>>()
        {
            return Err(ValidationError::new(
                "ground_truth file must align with query ids",
            ));
        }

        for query_id in query_ids {
            if !seen_query_ids.insert(query_id) {
                return Err(ValidationError::new("duplicate query_id"));
            }
        }
    }

    Ok(())
}

fn load_source_config(path: &Path) -> Result<SourceConfig, PackError> {
    let text =
        fs::read_to_string(path).map_err(|_| PackError::new("failed to read source config"))?;
    serde_json::from_str(&text).map_err(|_| PackError::new("failed to parse source config"))
}

fn copy_file(source: &Path, destination: &Path) -> Result<Vec<u8>, PackError> {
    let bytes = fs::read(source).map_err(|_| PackError::new("failed to read source file"))?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .map_err(|_| PackError::new("failed to create destination directory"))?;
    }
    fs::write(destination, &bytes).map_err(|_| PackError::new("failed to write output file"))?;
    Ok(bytes)
}

fn build_manifest_file(
    path: &str,
    kind: &str,
    format: &str,
    record_count: u64,
    bytes: &[u8],
) -> ManifestFile {
    ManifestFile {
        path: path.to_owned(),
        kind: kind.to_owned(),
        format: format.to_owned(),
        record_count,
        checksum: checksum_label(bytes),
    }
}

fn analyze_documents(bytes: &[u8]) -> Result<DocumentStats, PackError> {
    let text = std::str::from_utf8(bytes).map_err(|_| PackError::new("documents must be utf-8"))?;
    let mut lengths = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: DocumentStub =
            serde_json::from_str(line).map_err(|_| PackError::new("documents file contains invalid json"))?;
        lengths.push(record.text.len() as u64);
    }

    lengths.sort_unstable();
    let doc_count = lengths.len() as u64;
    let total_text_bytes = lengths.iter().sum::<u64>();
    let avg_doc_length = if doc_count == 0 {
        0.0
    } else {
        total_text_bytes as f64 / doc_count as f64
    };
    let median_doc_length = percentile_value(&lengths, 0.5);
    let p95_doc_length = percentile_value(&lengths, 0.95);
    let max_doc_length = lengths.last().copied().unwrap_or(0);
    let short_count = lengths.iter().filter(|length| **length <= 64).count() as f64;
    let medium_count = lengths
        .iter()
        .filter(|length| **length > 64 && **length <= 256)
        .count() as f64;
    let long_count = lengths.iter().filter(|length| **length > 256).count() as f64;
    let total_docs = doc_count.max(1) as f64;

    Ok(DocumentStats {
        doc_count,
        total_text_bytes,
        avg_doc_length,
        median_doc_length,
        p95_doc_length,
        max_doc_length,
        length_buckets: LengthBuckets {
            short_ratio: short_count / total_docs,
            medium_ratio: medium_count / total_docs,
            long_ratio: long_count / total_docs,
        },
    })
}

fn analyze_query_set(bytes: &[u8]) -> Result<QuerySetSummary, PackError> {
    let text = std::str::from_utf8(bytes).map_err(|_| PackError::new("query set must be utf-8"))?;
    let mut classes = BTreeSet::new();
    let mut easy = 0;
    let mut medium = 0;
    let mut hard = 0;
    let mut query_count = 0;

    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: QueryDefinitionStub = serde_json::from_str(line)
            .map_err(|_| PackError::new("query_set file contains invalid json"))?;
        query_count += 1;
        classes.insert(record.query_class);
        match record.difficulty.as_str() {
            "easy" => easy += 1,
            "medium" => medium += 1,
            "hard" => hard += 1,
            _ => {}
        }
    }

    Ok(QuerySetSummary {
        query_count,
        classes,
        difficulty_distribution: DifficultyDistribution { easy, medium, hard },
    })
}

fn build_vector_profile(embedding_spec_id: &str) -> VectorProfile {
    VectorProfile {
        enabled: true,
        embedding_dimensions: embedding_dimensions_from_spec_id(embedding_spec_id).unwrap_or(0),
        embedding_dtype: embedding_dtype_from_spec_id(embedding_spec_id).to_owned(),
        distance_metric: distance_metric_from_spec_id(embedding_spec_id).to_owned(),
        query_vectors: QueryVectorProfile {
            precomputed_available: false,
            runtime_embedding_supported: true,
        },
    }
}

fn build_dirty_profile(variant: &str, clean_dataset_id: &str) -> Result<DirtyProfile, PackError> {
    match variant {
        "clean" => Ok(DirtyProfile {
            profile: "clean".to_owned(),
            base_dataset_id: None,
            seed: 0,
            delete_ratio: 0.0,
            update_ratio: 0.0,
            append_ratio: 0.0,
            target_segment_count_range: [1, 1],
            target_segment_topology: vec![SegmentTopologyEntry {
                tier: "large".to_owned(),
                count: 1,
            }],
            target_tombstone_ratio: 0.0,
            compaction_state: "clean".to_owned(),
        }),
        "dirty_light" => Ok(DirtyProfile {
            profile: "dirty_light".to_owned(),
            base_dataset_id: Some(clean_dataset_id.to_owned()),
            seed: 7,
            delete_ratio: 0.05,
            update_ratio: 0.02,
            append_ratio: 0.01,
            target_segment_count_range: [2, 4],
            target_segment_topology: vec![
                SegmentTopologyEntry {
                    tier: "large".to_owned(),
                    count: 1,
                },
                SegmentTopologyEntry {
                    tier: "small".to_owned(),
                    count: 2,
                },
            ],
            target_tombstone_ratio: 0.08,
            compaction_state: "pre_compaction".to_owned(),
        }),
        _ => Err(PackError::new("unsupported variant")),
    }
}

fn dataset_id(dataset_family: &str, tier: &str, variant: &str, dataset_version: &str) -> String {
    format!("{dataset_family}-{tier}-{variant}-{dataset_version}")
}

fn percentile_value(lengths: &[u64], percentile: f64) -> u64 {
    if lengths.is_empty() {
        return 0;
    }

    let index = ((lengths.len() as f64 * percentile).ceil() as usize).saturating_sub(1);
    lengths[index.min(lengths.len() - 1)]
}

fn checksum_label(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn embedding_dtype_from_spec_id(spec_id: &str) -> &str {
    spec_id
        .split('-')
        .find(|part| matches!(*part, "f32" | "i8" | "u8"))
        .unwrap_or("f32")
}

fn distance_metric_from_spec_id(spec_id: &str) -> &str {
    spec_id.rsplit('-').next().unwrap_or("cosine")
}

fn manifest_query_fingerprint(tier: &str, variant: &str, clean_dataset_id: &str) -> serde_json::Value {
    serde_json::json!({
        "tier": tier,
        "variant": variant,
        "clean_dataset_id": clean_dataset_id,
    })
}

fn validate_vector_profile(manifest: &DatasetPackManifest) -> Result<(), ValidationError> {
    if let Some(expected_dimensions) = embedding_dimensions_from_spec_id(
        &manifest.identity.embedding_spec_id,
    ) {
        if manifest.vector_profile.embedding_dimensions != expected_dimensions {
            return Err(ValidationError::new(
                "embedding_dimensions does not match embedding_spec_id",
            ));
        }
    }

    if !manifest.vector_profile.enabled {
        if manifest.corpus.vector_count != 0 {
            return Err(ValidationError::new(
                "vector_count must be 0 when vectors are disabled",
            ));
        }

        if manifest.vector_profile.embedding_dimensions != 0 {
            return Err(ValidationError::new(
                "embedding_dimensions must be 0 when vectors are disabled",
            ));
        }
    }

    Ok(())
}

fn validate_dirty_profile(manifest: &DatasetPackManifest) -> Result<(), ValidationError> {
    let is_clean_profile = manifest.dirty_profile.profile == "clean";
    let has_dirty_behavior = manifest.dirty_profile.delete_ratio > 0.0
        || manifest.dirty_profile.update_ratio > 0.0
        || manifest.dirty_profile.append_ratio > 0.0;

    if manifest.identity.variant_id != manifest.dirty_profile.profile {
        return Err(ValidationError::new(
            "variant_id must match dirty_profile.profile",
        ));
    }

    if is_clean_profile {
        if has_dirty_behavior {
            return Err(ValidationError::new(
                "clean profile must use zero dirty ratios",
            ));
        }

        if manifest.dirty_profile.compaction_state != "clean" {
            return Err(ValidationError::new(
                "clean profile must use compaction_state=clean",
            ));
        }
    } else if manifest.dirty_profile.base_dataset_id.is_none() {
        return Err(ValidationError::new(
            "dirty variants must declare base_dataset_id",
        ));
    }

    Ok(())
}

fn embedding_dimensions_from_spec_id(spec_id: &str) -> Option<u32> {
    let parts: Vec<&str> = spec_id.split('-').collect();
    for window in parts.windows(2) {
        if matches!(window[1], "f32" | "i8" | "u8") {
            if let Ok(dimensions) = window[0].parse::<u32>() {
                return Some(dimensions);
            }
        }
    }

    None
}

fn validate_file_references(manifest: &DatasetPackManifest) -> Result<(), ValidationError> {
    let mut file_paths = HashSet::new();
    for file in &manifest.files {
        if !file_paths.insert(file.path.as_str()) {
            return Err(ValidationError::new("duplicate file path"));
        }

        if !is_pack_relative_path(&file.path) {
            return Err(ValidationError::new(
                "file paths must stay within the dataset pack root",
            ));
        }

        if !file.checksum.starts_with("sha256:") {
            return Err(ValidationError::new("file checksum must use sha256 format"));
        }

        if !matches!(
            file.kind.as_str(),
            "documents"
                | "metadata"
                | "query_set"
                | "ground_truth"
                | "document_vectors"
                | "query_vectors"
                | "prebuilt_store"
        ) {
            return Err(ValidationError::new("invalid file kind"));
        }
    }

    for query_set in &manifest.query_sets {
        if !file_paths.contains(query_set.path.as_str()) {
            return Err(ValidationError::new(
                "query_set path must reference a file entry",
            ));
        }

        if !file_paths.contains(query_set.ground_truth_path.as_str()) {
            return Err(ValidationError::new(
                "ground_truth_path must reference a file entry",
            ));
        }

        let query_file = manifest
            .files
            .iter()
            .find(|file| file.path == query_set.path)
            .expect("query_set path must exist after membership check");
        if query_file.kind != "query_set" {
            return Err(ValidationError::new(
                "query_set path must point to a query_set file",
            ));
        }

        let ground_truth_file = manifest
            .files
            .iter()
            .find(|file| file.path == query_set.ground_truth_path)
            .expect("ground_truth_path must exist after membership check");
        if ground_truth_file.kind != "ground_truth" {
            return Err(ValidationError::new(
                "ground_truth_path must point to a ground_truth file",
            ));
        }
    }

    Ok(())
}

fn validate_file_payloads(
    manifest: &DatasetPackManifest,
    pack_root: &Path,
) -> Result<(), ValidationError> {
    for file in &manifest.files {
        let path = pack_root.join(&file.path);
        let bytes = fs::read(&path)
            .map_err(|_| ValidationError::new("file path must exist in the dataset pack"))?;
        let checksum = format!("sha256:{:x}", Sha256::digest(&bytes));

        if file.checksum != checksum {
            return Err(ValidationError::new(
                "file checksum must match file contents",
            ));
        }
    }

    Ok(())
}

fn validate_constrained_values(manifest: &DatasetPackManifest) -> Result<(), ValidationError> {
    if !matches!(manifest.identity.dataset_tier.as_str(), "small" | "medium" | "large") {
        return Err(ValidationError::new("invalid dataset_tier"));
    }

    if !matches!(
        manifest.vector_profile.embedding_dtype.as_str(),
        "f32" | "i8" | "u8"
    ) {
        return Err(ValidationError::new("invalid embedding_dtype"));
    }

    if !matches!(
        manifest.vector_profile.distance_metric.as_str(),
        "cosine" | "dot" | "l2"
    ) {
        return Err(ValidationError::new("invalid distance_metric"));
    }

    if !matches!(
        manifest.dirty_profile.compaction_state.as_str(),
        "clean" | "pre_compaction" | "post_compaction"
    ) {
        return Err(ValidationError::new("invalid compaction_state"));
    }

    for query_set in &manifest.query_sets {
        for class in &query_set.classes {
            if !matches!(
                class.as_str(),
                "keyword"
                    | "prefix"
                    | "fuzzy_keyword"
                    | "topical"
                    | "vector"
                    | "hybrid"
                    | "metadata_filtered"
                    | "no_hit"
                    | "high_recall"
            ) {
                return Err(ValidationError::new("invalid query class"));
            }
        }
    }

    Ok(())
}

fn validate_vector_checksum_requirements(
    manifest: &DatasetPackManifest,
) -> Result<(), ValidationError> {
    let has_vector_payload = manifest.files.iter().any(|file| file.kind == "document_vectors");
    if has_vector_payload && manifest.checksums.logical_vector_payload_checksum.is_none() {
        return Err(ValidationError::new(
            "vector payload checksum is required when vector payload exists",
        ));
    }

    Ok(())
}

fn load_query_ids(path: PathBuf) -> Result<Vec<String>, ValidationError> {
    let text = fs::read_to_string(&path)
        .map_err(|_| ValidationError::new("query_set file must be readable"))?;
    let mut query_ids = Vec::new();

    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: QueryDefinitionStub = serde_json::from_str(line)
            .map_err(|_| ValidationError::new("query_set file contains invalid json"))?;
        query_ids.push(record.query_id);
    }

    Ok(query_ids)
}

fn load_ground_truth_ids(path: PathBuf) -> Result<Vec<String>, ValidationError> {
    let text = fs::read_to_string(&path)
        .map_err(|_| ValidationError::new("ground_truth file must be readable"))?;
    let mut query_ids = Vec::new();
    let mut seen_query_ids = HashSet::new();

    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: GroundTruthStub = serde_json::from_str(line)
            .map_err(|_| ValidationError::new("ground_truth file contains invalid json"))?;
        if !seen_query_ids.insert(record.query_id.clone()) {
            return Err(ValidationError::new(
                "ground_truth file contains duplicate query_id",
            ));
        }
        query_ids.push(record.query_id);
    }

    Ok(query_ids)
}

fn is_pack_relative_path(path: &str) -> bool {
    let path = Path::new(path);
    !path.is_absolute()
        && path.components().all(|component| {
            matches!(component, Component::Normal(_))
        })
}

#[derive(serde::Deserialize)]
struct QueryDefinitionStub {
    query_id: String,
    query_class: String,
    difficulty: String,
}

#[derive(serde::Deserialize)]
struct GroundTruthStub {
    query_id: String,
}

#[derive(Deserialize)]
struct DocumentStub {
    text: String,
}
fn non_empty_line_count(bytes: &[u8]) -> u64 {
    std::str::from_utf8(bytes)
        .ok()
        .map(|text| text.lines().filter(|line| !line.trim().is_empty()).count() as u64)
        .unwrap_or(0)
}
