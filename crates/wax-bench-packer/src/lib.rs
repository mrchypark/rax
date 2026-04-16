mod artifacts;
mod manifest_builder;
mod payloads;
mod source_loader;
mod validation;

use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use bytemuck::try_cast_slice;
use hnsw_rs::api::AnnT;
use hnsw_rs::prelude::{DistCosine, Hnsw};
use sha2::{Digest, Sha256};
use wax_bench_model::{
    CorpusProfile, DatasetIdentity, DatasetPackManifest, EnvironmentConstraints, LanguageShare,
    ManifestChecksums, ManifestFile, ManifestGenerator, MetadataProfile, QuerySetEntry,
    TextProfile,
};

use crate::artifacts::{
    emit_document_sidecars, emit_query_artifacts, emit_vector_artifacts, QueryArtifactSpec,
};
use crate::manifest_builder::{
    build_dirty_profile, build_vector_profile, checksum_label, dataset_id,
    manifest_query_fingerprint, require_embedding_dimensions, synthetic_embedding_identity,
};
use crate::payloads::build_adhoc_query_files;
use crate::source_loader::{
    analyze_documents, ensure_vector_query_exists, load_document_records_with_offsets,
    load_source_config,
};
use crate::validation::validate_manifest_inner;

const CHECKSUM_BUFFER_BYTES: usize = 64 * 1024;

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
pub struct AdhocPackRequest {
    pub docs_path: PathBuf,
    pub out_dir: PathBuf,
    pub tier: String,
    pub variant: String,
    pub dataset_family: String,
    pub dataset_version: String,
    pub embedding_spec_id: String,
}

impl AdhocPackRequest {
    pub fn new(
        docs_path: impl Into<PathBuf>,
        out_dir: impl Into<PathBuf>,
        tier: impl Into<String>,
    ) -> Self {
        Self {
            docs_path: docs_path.into(),
            out_dir: out_dir.into(),
            tier: tier.into(),
            variant: "clean".to_owned(),
            dataset_family: "adhoc".to_owned(),
            dataset_version: "v1".to_owned(),
            embedding_spec_id: "minilm-l6-384-f32-cosine".to_owned(),
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
    let dimensions = require_embedding_dimensions(&source.embedding_spec_id)?;
    let manifest_embedding = synthetic_embedding_identity(&source.embedding_spec_id);

    let mut source_query_sets = source.query_sets.clone();
    source_query_sets.sort_by(|left, right| left.name.cmp(&right.name));
    ensure_vector_query_exists(&request.source_dir, &source_query_sets)?;

    let documents_path = request.out_dir.join("docs.ndjson");
    copy_file(&request.source_dir.join("docs.ndjson"), &documents_path)?;
    let (document_records, document_offsets) = load_document_records_with_offsets(&documents_path)?;
    let document_stats = analyze_documents(&document_records);
    let documents_checksum = checksum_file(&documents_path)
        .map_err(|_| PackError::new("failed to checksum documents file"))?;

    let mut files = vec![build_manifest_file_from_path(
        "docs.ndjson",
        "documents",
        "ndjson",
        document_stats.doc_count,
        &documents_path,
    )?];
    files.extend(emit_document_sidecars(
        &request.out_dir,
        &document_records,
        &document_offsets,
    )?);
    let mut query_sets = Vec::new();
    let mut logical_query_hasher = Sha256::new();
    let mut vector_payload_hasher = Sha256::new();
    let emitted_vector_artifacts =
        emit_vector_artifacts(&request.out_dir, &document_records, dimensions)?;
    let document_vector_bytes = emitted_vector_artifacts.document_vector_bytes;
    vector_payload_hasher.update(&document_vector_bytes);
    files.extend(emitted_vector_artifacts.manifest_files);

    for source_query_set in source_query_sets {
        let query_bytes = copy_file_bytes(
            &request.source_dir.join(&source_query_set.path),
            &request.out_dir.join(&source_query_set.path),
        )?;
        let ground_truth_bytes = copy_file_bytes(
            &request.source_dir.join(&source_query_set.ground_truth_path),
            &request.out_dir.join(&source_query_set.ground_truth_path),
        )?;
        let qrels_bytes = if let Some(qrels_path) = &source_query_set.qrels_path {
            Some(copy_file_bytes(
                &request.source_dir.join(qrels_path),
                &request.out_dir.join(qrels_path),
            )?)
        } else {
            None
        };

        let emitted_query_artifacts = emit_query_artifacts(
            &request.out_dir,
            QueryArtifactSpec {
                query_path: &source_query_set.path,
                query_bytes: &query_bytes,
                ground_truth_path: &source_query_set.ground_truth_path,
                ground_truth_bytes: &ground_truth_bytes,
                qrels_path: source_query_set.qrels_path.as_deref(),
                qrels_bytes: qrels_bytes.as_deref(),
                query_vector_path: &format!("{}.vectors.jsonl", source_query_set.name),
            },
            dimensions,
        )?;
        let query_summary = emitted_query_artifacts.summary;
        let query_vector_bytes = emitted_query_artifacts.query_vector_bytes;
        logical_query_hasher.update(&query_bytes);
        vector_payload_hasher.update(&query_vector_bytes);
        files.extend(emitted_query_artifacts.manifest_files);

        query_sets.push(QuerySetEntry {
            query_set_id: format!(
                "{}-{}-{}-{}",
                source.dataset_family, request.tier, source_query_set.name, source.dataset_version
            ),
            path: source_query_set.path,
            ground_truth_path: source_query_set.ground_truth_path,
            qrels_path: source_query_set.qrels_path,
            query_count: query_summary.query_count,
            classes: query_summary.classes.into_iter().collect(),
            difficulty_distribution: query_summary.difficulty_distribution,
        });
    }

    let vector_profile = build_vector_profile(&source.embedding_spec_id);
    let clean_dataset_id = dataset_id(
        &source.dataset_family,
        &request.tier,
        "clean",
        &source.dataset_version,
    );
    let dirty_profile = build_dirty_profile(&request.variant, &clean_dataset_id)?;
    let metadata_profile = source.metadata_profile.clone();
    let logical_metadata_checksum = checksum_label(
        &serde_json::to_vec(&metadata_profile)
            .map_err(|_| PackError::new("failed to serialize metadata profile for checksum"))?,
    );
    let query_fingerprint =
        manifest_query_fingerprint(&request.tier, &request.variant, &clean_dataset_id);
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
            embedding_spec_id: manifest_embedding.spec_id,
            embedding_model_version: manifest_embedding.model_version,
            embedding_model_hash: manifest_embedding.model_hash,
            corpus_checksum: documents_checksum.clone(),
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
            logical_documents_checksum: documents_checksum,
            logical_metadata_checksum,
            logical_query_definitions_checksum: checksum_label(&logical_query_fingerprint),
            logical_vector_payload_checksum: Some(format!(
                "sha256:{:x}",
                vector_payload_hasher.finalize()
            )),
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

pub fn pack_adhoc_dataset(request: &AdhocPackRequest) -> Result<DatasetPackManifest, PackError> {
    fs::create_dir_all(&request.out_dir)
        .map_err(|_| PackError::new("failed to create output directory"))?;
    let dimensions = require_embedding_dimensions(&request.embedding_spec_id)?;
    let manifest_embedding = synthetic_embedding_identity(&request.embedding_spec_id);
    let documents_path = request.out_dir.join("docs.ndjson");
    copy_file(&request.docs_path, &documents_path)?;
    let (document_records, document_offsets) = load_document_records_with_offsets(&documents_path)?;
    let document_stats = analyze_documents(&document_records);
    let documents_checksum = checksum_file(&documents_path)
        .map_err(|_| PackError::new("failed to checksum documents file"))?;

    let mut files = vec![build_manifest_file_from_path(
        "docs.ndjson",
        "documents",
        "ndjson",
        document_stats.doc_count,
        &documents_path,
    )?];
    files.extend(emit_document_sidecars(
        &request.out_dir,
        &document_records,
        &document_offsets,
    )?);
    let (query_bytes, ground_truth_bytes) = build_adhoc_query_files(&document_records)?;
    let query_path = "queries/adhoc.jsonl";
    let ground_truth_path = "queries/adhoc-ground-truth.jsonl";
    let emitted_query_artifacts = emit_query_artifacts(
        &request.out_dir,
        QueryArtifactSpec {
            query_path,
            query_bytes: &query_bytes,
            ground_truth_path,
            ground_truth_bytes: &ground_truth_bytes,
            qrels_path: None,
            qrels_bytes: None,
            query_vector_path: "adhoc.vectors.jsonl",
        },
        dimensions,
    )?;
    files.extend(emitted_query_artifacts.manifest_files);

    let emitted_vector_artifacts =
        emit_vector_artifacts(&request.out_dir, &document_records, dimensions)?;
    let document_vector_bytes = emitted_vector_artifacts.document_vector_bytes;
    files.extend(emitted_vector_artifacts.manifest_files);

    let vector_profile = build_vector_profile(&request.embedding_spec_id);
    let clean_dataset_id = dataset_id(
        &request.dataset_family,
        &request.tier,
        "clean",
        &request.dataset_version,
    );
    let dirty_profile = build_dirty_profile(&request.variant, &clean_dataset_id)?;
    let metadata_profile = MetadataProfile {
        facets: Vec::new(),
        selectivity_exemplars: wax_bench_model::SelectivityExemplars {
            broad: "unsupported".to_owned(),
            medium: "unsupported".to_owned(),
            narrow: "unsupported".to_owned(),
            zero_hit: "unsupported".to_owned(),
        },
    };
    let logical_metadata_checksum = checksum_label(
        &serde_json::to_vec(&metadata_profile)
            .map_err(|_| PackError::new("failed to serialize metadata profile for checksum"))?,
    );
    let logical_query_fingerprint = query_bytes.clone();
    let query_summary = emitted_query_artifacts.summary;

    let mut manifest = DatasetPackManifest {
        schema_version: "wax_dataset_pack_v1".to_owned(),
        generated_at: "2026-03-31T00:00:00Z".to_owned(),
        generator: ManifestGenerator {
            name: "wax-bench-packer".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
        },
        identity: DatasetIdentity {
            dataset_id: dataset_id(
                &request.dataset_family,
                &request.tier,
                &request.variant,
                &request.dataset_version,
            ),
            dataset_version: request.dataset_version.clone(),
            dataset_family: request.dataset_family.clone(),
            dataset_tier: request.tier.clone(),
            variant_id: request.variant.clone(),
            embedding_spec_id: manifest_embedding.spec_id,
            embedding_model_version: manifest_embedding.model_version,
            embedding_model_hash: manifest_embedding.model_hash,
            corpus_checksum: documents_checksum.clone(),
            query_checksum: checksum_label(&query_bytes),
        },
        environment_constraints: EnvironmentConstraints {
            min_ram_gb: 1,
            recommended_ram_gb: 2,
            notes: Some("adhoc pack defaults".to_owned()),
        },
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
            languages: vec![LanguageShare {
                code: "und".to_owned(),
                ratio: 1.0,
            }],
        },
        text_profile: TextProfile {
            length_buckets: document_stats.length_buckets,
            tokenization_notes: Some("adhoc defaults".to_owned()),
        },
        metadata_profile,
        vector_profile,
        dirty_profile,
        files,
        query_sets: vec![QuerySetEntry {
            query_set_id: format!(
                "{}-{}-adhoc-{}",
                request.dataset_family, request.tier, request.dataset_version
            ),
            path: query_path.to_owned(),
            ground_truth_path: ground_truth_path.to_owned(),
            qrels_path: None,
            query_count: query_summary.query_count,
            classes: query_summary.classes.into_iter().collect(),
            difficulty_distribution: query_summary.difficulty_distribution,
        }],
        checksums: ManifestChecksums {
            manifest_payload_checksum: "sha256:pending".to_owned(),
            logical_documents_checksum: documents_checksum,
            logical_metadata_checksum,
            logical_query_definitions_checksum: checksum_label(&logical_query_fingerprint),
            logical_vector_payload_checksum: Some(checksum_label(&document_vector_bytes)),
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

pub fn validate_manifest(
    manifest: &DatasetPackManifest,
    pack_root: &Path,
) -> Result<(), ValidationError> {
    validate_manifest_inner(manifest, pack_root)
}

fn copy_file(source: &Path, destination: &Path) -> Result<(), PackError> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .map_err(|_| PackError::new("failed to create destination directory"))?;
    }
    fs::copy(source, destination).map_err(|_| PackError::new("failed to copy source file"))?;
    Ok(())
}

fn copy_file_bytes(source: &Path, destination: &Path) -> Result<Vec<u8>, PackError> {
    copy_file(source, destination)?;
    fs::read(destination).map_err(|_| PackError::new("failed to read copied file"))
}

pub(crate) fn build_manifest_file(
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

fn build_manifest_file_from_path(
    path: &str,
    kind: &str,
    format: &str,
    record_count: u64,
    full_path: &Path,
) -> Result<ManifestFile, PackError> {
    let checksum =
        checksum_file(full_path).map_err(|_| PackError::new("failed to checksum manifest file"))?;
    Ok(ManifestFile {
        path: path.to_owned(),
        kind: kind.to_owned(),
        format: format.to_owned(),
        record_count,
        checksum,
    })
}

pub(crate) fn build_hnsw_vector_sidecar(
    out_dir: &Path,
    vector_bytes: &[u8],
    dimensions: usize,
    doc_count: usize,
) -> Result<(String, Vec<u8>, String, Vec<u8>), PackError> {
    const HNSW_MAX_CONNECTION: usize = 16;
    const HNSW_MAX_LAYER: usize = 16;
    const HNSW_EF_CONSTRUCTION: usize = 64;
    const HNSW_BASENAME: &str = "vector_hnsw";
    let graph_path = out_dir.join(format!("{HNSW_BASENAME}.hnsw.graph"));
    let data_path = out_dir.join(format!("{HNSW_BASENAME}.hnsw.data"));

    if graph_path.exists() {
        fs::remove_file(&graph_path)
            .map_err(|_| PackError::new("failed to clear previous HNSW graph sidecar"))?;
    }
    if data_path.exists() {
        fs::remove_file(&data_path)
            .map_err(|_| PackError::new("failed to clear previous HNSW data sidecar"))?;
    }

    let hnsw = Hnsw::<f32, DistCosine>::new(
        HNSW_MAX_CONNECTION,
        doc_count.max(1),
        HNSW_MAX_LAYER,
        HNSW_EF_CONSTRUCTION,
        DistCosine {},
    );

    for (index, row) in vector_bytes.chunks_exact(dimensions * 4).enumerate() {
        let vector = try_cast_slice::<u8, f32>(row)
            .map_err(|_| PackError::new("document vector payload alignment is invalid"))?;
        hnsw.insert((vector, index));
    }

    let dump_basename = hnsw
        .file_dump(out_dir, HNSW_BASENAME)
        .map_err(|error| PackError::new(error.to_string()))?;
    if dump_basename != HNSW_BASENAME {
        return Err(PackError::new(
            "unexpected HNSW basename returned during sidecar dump",
        ));
    }
    let graph_path = format!("{dump_basename}.hnsw.graph");
    let data_path = format!("{dump_basename}.hnsw.data");
    let graph_bytes = fs::read(out_dir.join(&graph_path))
        .map_err(|_| PackError::new("failed to read HNSW graph sidecar"))?;
    let data_bytes = fs::read(out_dir.join(&data_path))
        .map_err(|_| PackError::new("failed to read HNSW data sidecar"))?;

    Ok((graph_path, graph_bytes, data_path, data_bytes))
}

pub(crate) fn checksum_file(path: &Path) -> Result<String, std::io::Error> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; CHECKSUM_BUFFER_BYTES];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(format!("sha256:{:x}", hasher.finalize()))
}
