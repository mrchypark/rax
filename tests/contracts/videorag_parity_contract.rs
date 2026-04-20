use std::fs;

use tempfile::tempdir;
use wax_v2_multimodal::{
    BootstrapVideoMetadata, MultimodalAssetKind, MultimodalIngestSession, NewMultimodalAssetImport,
    VideoAssetQuery,
};

#[test]
fn videorag_lists_only_video_assets_through_typed_video_api() {
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
            1_717_171_726_000,
        ))
        .unwrap();
    session
        .import_asset(NewMultimodalAssetImport::new(
            "video:clip",
            MultimodalAssetKind::Video,
            &video_path,
            "bootstrap-test",
            1_717_171_727_000,
        ))
        .unwrap();

    let video_assets = session.list_video_assets().unwrap();
    assert_eq!(video_assets.len(), 1);
    assert_eq!(video_assets[0].asset_id, "video:clip");
    assert!(video_assets[0].stored_relative_path.ends_with(".mp4"));
}

#[test]
fn videorag_reads_bootstrap_video_metadata_after_reopen() {
    let dataset_dir = tempdir().unwrap();
    let video_path = dataset_dir.path().join("scene.mov");
    fs::write(&video_path, [0x00, 0x00, 0x00, 0x14, b'f', b't', b'y', b'p']).unwrap();

    let mut initial = MultimodalIngestSession::open(dataset_dir.path()).unwrap();
    initial
        .import_asset(
            NewMultimodalAssetImport::new(
                "video:scene",
                MultimodalAssetKind::Video,
                &video_path,
                "bootstrap-test",
                1_717_171_728_000,
            )
            .with_media_type("video/quicktime")
            .with_video_metadata(
                BootstrapVideoMetadata::new()
                    .with_duration_ms(12_345)
                    .with_frame_dimensions(1920, 1080)
                    .with_frame_rate_milli_fps(29_970),
            ),
        )
        .unwrap();
    initial.close().unwrap();

    let mut reopened = MultimodalIngestSession::open(dataset_dir.path()).unwrap();
    let video_asset = reopened
        .video_asset(VideoAssetQuery::asset_id("video:scene"))
        .unwrap()
        .unwrap();

    assert_eq!(video_asset.asset_id, "video:scene");
    assert_eq!(video_asset.media_type.as_deref(), Some("video/quicktime"));
    assert_eq!(
        video_asset.video_metadata.as_ref().unwrap().duration_ms,
        Some(12_345)
    );
    assert_eq!(
        video_asset.video_metadata.as_ref().unwrap().frame_width_px,
        Some(1920)
    );
    assert_eq!(
        video_asset.video_metadata.as_ref().unwrap().frame_height_px,
        Some(1080)
    );
    assert_eq!(
        video_asset.video_metadata.as_ref().unwrap().frame_rate_milli_fps,
        Some(29_970)
    );
}
