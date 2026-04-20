use std::fs;

use tempfile::tempdir;
use wax_v2_multimodal::{
    MultimodalAssetKind, MultimodalAssetQuery, MultimodalIngestSession, NewMultimodalAssetImport,
};

#[test]
fn multimodal_ingest_session_imports_image_asset_with_stable_descriptor() {
    let dataset_dir = tempdir().unwrap();
    let image_path = dataset_dir.path().join("hero.png");
    fs::write(&image_path, [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a]).unwrap();

    let mut session = MultimodalIngestSession::open(dataset_dir.path()).unwrap();
    let asset = session
        .import_asset(
            NewMultimodalAssetImport::new(
                "image:hero",
                MultimodalAssetKind::Image,
                &image_path,
                "bootstrap-test",
                1_717_171_721_000,
            )
            .with_media_type("image/png"),
        )
        .unwrap();

    assert_eq!(asset.asset_id, "image:hero");
    assert_eq!(asset.kind, MultimodalAssetKind::Image);
    assert_eq!(asset.media_type.as_deref(), Some("image/png"));
    assert_eq!(asset.byte_length, 8);
    assert!(dataset_dir.path().join(&asset.stored_relative_path).exists());
}

#[test]
fn multimodal_ingest_session_reopens_and_lists_imported_assets() {
    let dataset_dir = tempdir().unwrap();
    let image_path = dataset_dir.path().join("poster.jpg");
    fs::write(&image_path, [0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10]).unwrap();

    let mut initial = MultimodalIngestSession::open(dataset_dir.path()).unwrap();
    initial
        .import_asset(
            NewMultimodalAssetImport::new(
                "image:poster",
                MultimodalAssetKind::Image,
                &image_path,
                "bootstrap-test",
                1_717_171_722_000,
            )
            .with_media_type("image/jpeg"),
        )
        .unwrap();
    initial.close().unwrap();

    let mut reopened = MultimodalIngestSession::open(dataset_dir.path()).unwrap();
    let asset = reopened
        .asset(MultimodalAssetQuery::asset_id("image:poster"))
        .unwrap()
        .unwrap();
    assert_eq!(asset.asset_id, "image:poster");
    assert_eq!(asset.media_type.as_deref(), Some("image/jpeg"));

    let assets = reopened.list_assets().unwrap();
    assert_eq!(assets.len(), 1);
    assert_eq!(assets[0].asset_id, "image:poster");
}
