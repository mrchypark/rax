use std::fs;

use sha2::{Digest, Sha256};
use tempfile::tempdir;
use wax_bench_model::{ManifestFile, QrelRecord, RankedDocumentHit, RankedQueryResult};
use wax_bench_packer::validate_manifest;
use wax_bench_reducer::{
    compute_search_quality_summary, compute_search_quality_summary_from_paths,
};

#[test]
fn dataset_manifest_validation_accepts_qrels_file_when_declared() {
    let pack_root = tempdir().unwrap();
    fs::create_dir_all(pack_root.path().join("queries")).unwrap();
    fs::copy(
        "fixtures/bench/minimal-dataset-pack/manifest.json",
        pack_root.path().join("manifest.json"),
    )
    .unwrap();
    fs::copy(
        "fixtures/bench/minimal-dataset-pack/docs.ndjson",
        pack_root.path().join("docs.ndjson"),
    )
    .unwrap();
    fs::copy(
        "fixtures/bench/minimal-dataset-pack/queries/core.jsonl",
        pack_root.path().join("queries/core.jsonl"),
    )
    .unwrap();
    fs::copy(
        "fixtures/bench/minimal-dataset-pack/queries/core-ground-truth.jsonl",
        pack_root.path().join("queries/core-ground-truth.jsonl"),
    )
    .unwrap();
    fs::copy(
        "fixtures/bench/minimal-dataset-pack/queries/duplicate.jsonl",
        pack_root.path().join("queries/duplicate.jsonl"),
    )
    .unwrap();

    let mut manifest: wax_bench_model::DatasetPackManifest =
        serde_json::from_str(&fs::read_to_string(pack_root.path().join("manifest.json")).unwrap())
            .unwrap();

    let qrels_bytes = concat!(
        "{\"query_id\":\"q-001\",\"doc_id\":\"doc-001\",\"relevance\":3}\n",
        "{\"query_id\":\"q-002\",\"doc_id\":\"doc-002\",\"relevance\":2}\n",
        "{\"query_id\":\"q-003\",\"doc_id\":\"doc-003\",\"relevance\":3}\n",
    )
    .as_bytes()
    .to_vec();
    fs::write(
        pack_root.path().join("queries/core-qrels.jsonl"),
        &qrels_bytes,
    )
    .unwrap();

    manifest.query_sets[0].qrels_path = Some("queries/core-qrels.jsonl".to_owned());
    manifest.files.push(ManifestFile {
        path: "queries/core-qrels.jsonl".to_owned(),
        kind: "qrels".to_owned(),
        format: "jsonl".to_owned(),
        record_count: 3,
        checksum: format!("sha256:{:x}", Sha256::digest(&qrels_bytes)),
    });

    assert!(validate_manifest(&manifest, pack_root.path()).is_ok());
}

#[test]
fn dataset_manifest_validation_rejects_qrels_missing_query_coverage() {
    let pack_root = tempdir().unwrap();
    fs::create_dir_all(pack_root.path().join("queries")).unwrap();
    fs::copy(
        "fixtures/bench/minimal-dataset-pack/manifest.json",
        pack_root.path().join("manifest.json"),
    )
    .unwrap();
    fs::copy(
        "fixtures/bench/minimal-dataset-pack/docs.ndjson",
        pack_root.path().join("docs.ndjson"),
    )
    .unwrap();
    fs::copy(
        "fixtures/bench/minimal-dataset-pack/queries/core.jsonl",
        pack_root.path().join("queries/core.jsonl"),
    )
    .unwrap();
    fs::copy(
        "fixtures/bench/minimal-dataset-pack/queries/core-ground-truth.jsonl",
        pack_root.path().join("queries/core-ground-truth.jsonl"),
    )
    .unwrap();
    fs::copy(
        "fixtures/bench/minimal-dataset-pack/queries/duplicate.jsonl",
        pack_root.path().join("queries/duplicate.jsonl"),
    )
    .unwrap();

    let mut manifest: wax_bench_model::DatasetPackManifest =
        serde_json::from_str(&fs::read_to_string(pack_root.path().join("manifest.json")).unwrap())
            .unwrap();

    let qrels_bytes = concat!(
        "{\"query_id\":\"q-001\",\"doc_id\":\"doc-001\",\"relevance\":3}\n",
        "{\"query_id\":\"q-002\",\"doc_id\":\"doc-002\",\"relevance\":2}\n",
    )
    .as_bytes()
    .to_vec();
    fs::write(
        pack_root.path().join("queries/core-qrels.jsonl"),
        &qrels_bytes,
    )
    .unwrap();

    manifest.query_sets[0].qrels_path = Some("queries/core-qrels.jsonl".to_owned());
    manifest.files.push(ManifestFile {
        path: "queries/core-qrels.jsonl".to_owned(),
        kind: "qrels".to_owned(),
        format: "jsonl".to_owned(),
        record_count: 2,
        checksum: format!("sha256:{:x}", Sha256::digest(&qrels_bytes)),
    });

    assert_eq!(
        validate_manifest(&manifest, pack_root.path())
            .unwrap_err()
            .message,
        "qrels file must align with query ids"
    );
}

#[test]
fn search_quality_summary_computes_ranked_metrics_from_qrels() {
    let qrels = vec![
        QrelRecord {
            query_id: "q-001".to_owned(),
            doc_id: "doc-001".to_owned(),
            relevance: 3,
        },
        QrelRecord {
            query_id: "q-002".to_owned(),
            doc_id: "doc-002".to_owned(),
            relevance: 2,
        },
    ];
    let results = vec![
        RankedQueryResult {
            query_id: "q-001".to_owned(),
            hits: vec![
                RankedDocumentHit {
                    doc_id: "doc-001".to_owned(),
                },
                RankedDocumentHit {
                    doc_id: "miss-001".to_owned(),
                },
            ],
        },
        RankedQueryResult {
            query_id: "q-002".to_owned(),
            hits: vec![
                RankedDocumentHit {
                    doc_id: "doc-002".to_owned(),
                },
                RankedDocumentHit {
                    doc_id: "miss-002".to_owned(),
                },
            ],
        },
    ];

    let summary = compute_search_quality_summary(&qrels, &results).unwrap();

    assert_eq!(summary.query_count, 2);
    assert_eq!(summary.unrated_hit_count, 2);
    assert_eq!(summary.ndcg_at_10, 1.0);
    assert_eq!(summary.recall_at_100, 1.0);
    assert_eq!(summary.mrr_at_10, 1.0);
    assert_eq!(summary.success_at_1, 1.0);
    assert_eq!(summary.success_at_3, 1.0);
}

#[test]
fn quality_summary_from_paths_rejects_results_missing_query_coverage() {
    let root = tempdir().unwrap();
    let query_set_path = root.path().join("queries.jsonl");
    let qrels_path = root.path().join("qrels.jsonl");
    let results_path = root.path().join("results.json");

    fs::write(
        &query_set_path,
        concat!(
            "{\"query_id\":\"q-001\",\"query_class\":\"keyword\",\"difficulty\":\"easy\"}\n",
            "{\"query_id\":\"q-002\",\"query_class\":\"keyword\",\"difficulty\":\"easy\"}\n",
        ),
    )
    .unwrap();
    fs::write(
        &qrels_path,
        concat!(
            "{\"query_id\":\"q-001\",\"doc_id\":\"doc-001\",\"relevance\":3}\n",
            "{\"query_id\":\"q-002\",\"doc_id\":\"doc-002\",\"relevance\":2}\n",
        ),
    )
    .unwrap();
    fs::write(
        &results_path,
        "[{\"query_id\":\"q-001\",\"hits\":[{\"doc_id\":\"doc-001\"}]}]",
    )
    .unwrap();

    assert_eq!(
        compute_search_quality_summary_from_paths(&query_set_path, &qrels_path, &results_path)
            .unwrap_err()
            .message,
        "results file must align with query ids"
    );
}
