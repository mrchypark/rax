use std::fs;
use std::path::Path;

use tempfile::tempdir;
use wax_bench_model::DatasetPackManifest;
use wax_bench_packer::{pack_dataset, PackRequest};

#[test]
fn dataset_packer_produces_byte_stable_manifest_for_same_source_and_config() {
    let source = Path::new("fixtures/bench/source/minimal");
    let out_a = tempdir().unwrap();
    let out_b = tempdir().unwrap();

    pack_dataset(&PackRequest::new(source, out_a.path(), "small", "clean")).unwrap();
    pack_dataset(&PackRequest::new(source, out_b.path(), "small", "clean")).unwrap();

    let manifest_a = fs::read_to_string(out_a.path().join("manifest.json")).unwrap();
    let manifest_b = fs::read_to_string(out_b.path().join("manifest.json")).unwrap();

    assert_eq!(manifest_a, manifest_b);
}

#[test]
fn dataset_packer_emits_expected_clean_and_dirty_metadata() {
    let source = Path::new("fixtures/bench/source/minimal");
    let clean_out = tempdir().unwrap();
    let dirty_out = tempdir().unwrap();

    pack_dataset(&PackRequest::new(source, clean_out.path(), "small", "clean")).unwrap();
    pack_dataset(&PackRequest::new(source, dirty_out.path(), "small", "dirty_light")).unwrap();

    let clean = read_manifest(clean_out.path());
    let dirty = read_manifest(dirty_out.path());

    assert_eq!(clean.identity.variant_id, "clean");
    assert_eq!(clean.dirty_profile.profile, "clean");
    assert_eq!(clean.dirty_profile.delete_ratio, 0.0);

    assert_eq!(dirty.identity.variant_id, "dirty_light");
    assert_eq!(dirty.dirty_profile.profile, "dirty_light");
    assert!(dirty.dirty_profile.base_dataset_id.is_some());
    assert!(dirty.dirty_profile.delete_ratio > 0.0);
}

#[test]
fn dataset_packer_keeps_query_set_ids_stable_across_variants() {
    let source = Path::new("fixtures/bench/source/minimal");
    let clean_out = tempdir().unwrap();
    let dirty_out = tempdir().unwrap();

    pack_dataset(&PackRequest::new(source, clean_out.path(), "small", "clean")).unwrap();
    pack_dataset(&PackRequest::new(source, dirty_out.path(), "small", "dirty_light")).unwrap();

    let clean = read_manifest(clean_out.path());
    let dirty = read_manifest(dirty_out.path());

    let clean_ids: Vec<_> = clean
        .query_sets
        .iter()
        .map(|query_set| query_set.query_set_id.clone())
        .collect();
    let dirty_ids: Vec<_> = dirty
        .query_sets
        .iter()
        .map(|query_set| query_set.query_set_id.clone())
        .collect();

    assert_eq!(clean_ids, dirty_ids);
}

fn read_manifest(out_dir: &Path) -> DatasetPackManifest {
    let text = fs::read_to_string(out_dir.join("manifest.json")).unwrap();
    serde_json::from_str(&text).unwrap()
}
