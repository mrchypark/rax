use std::fs;

use tempfile::tempdir;
use wax_v2_multimodal::{
    BootstrapImageMetadata, MultimodalAssetKind, MultimodalIngestSession, NewMultimodalAssetImport,
    PhotoAssetQuery,
};

#[test]
fn photorag_lists_only_image_assets_through_typed_image_api() {
    let dataset_dir = tempdir().unwrap();
    let image_path = dataset_dir.path().join("cover.png");
    let video_path = dataset_dir.path().join("clip.mp4");
    fs::write(&image_path, [0x89, b'P', b'N', b'G']).unwrap();
    fs::write(&video_path, [0x00, 0x00, 0x00, 0x18]).unwrap();

    let mut session = MultimodalIngestSession::open(dataset_dir.path()).unwrap();
    session
        .import_asset(NewMultimodalAssetImport::new(
            "image:cover",
            MultimodalAssetKind::Image,
            &image_path,
            "bootstrap-test",
            1_717_171_723_000,
        ))
        .unwrap();
    session
        .import_asset(NewMultimodalAssetImport::new(
            "video:clip",
            MultimodalAssetKind::Video,
            &video_path,
            "bootstrap-test",
            1_717_171_724_000,
        ))
        .unwrap();

    let photo_assets = session.list_photo_assets().unwrap();
    assert_eq!(photo_assets.len(), 1);
    assert_eq!(photo_assets[0].asset_id, "image:cover");
    assert_eq!(photo_assets[0].stored_relative_path.ends_with(".png"), true);
}

#[test]
fn photorag_reads_bootstrap_image_metadata_after_reopen() {
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
                1_717_171_725_000,
            )
            .with_media_type("image/jpeg")
            .with_image_metadata(
                BootstrapImageMetadata::new()
                    .with_dimensions(1600, 900)
                    .with_captured_at_ms(1_717_171_700_000),
            ),
        )
        .unwrap();
    initial.close().unwrap();

    let mut reopened = MultimodalIngestSession::open(dataset_dir.path()).unwrap();
    let photo_asset = reopened
        .photo_asset(PhotoAssetQuery::asset_id("image:poster"))
        .unwrap()
        .unwrap();

    assert_eq!(photo_asset.asset_id, "image:poster");
    assert_eq!(photo_asset.media_type.as_deref(), Some("image/jpeg"));
    assert_eq!(photo_asset.image_metadata.as_ref().unwrap().width_px, Some(1600));
    assert_eq!(photo_asset.image_metadata.as_ref().unwrap().height_px, Some(900));
    assert_eq!(
        photo_asset.image_metadata.as_ref().unwrap().captured_at_ms,
        Some(1_717_171_700_000)
    );
}
