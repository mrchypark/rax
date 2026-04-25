use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MULTIMODAL_METADATA_FILE_NAME: &str = "multimodal-assets.ndjson";
const MULTIMODAL_ASSET_DIR_NAME: &str = "multimodal-assets";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MultimodalAssetKind {
    Image,
    Video,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultimodalAssetProvenance {
    pub source: String,
    pub imported_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultimodalAsset {
    pub asset_id: String,
    pub kind: MultimodalAssetKind,
    pub original_file_name: String,
    pub media_type: Option<String>,
    pub byte_length: u64,
    pub sha256_hex: String,
    pub stored_relative_path: String,
    #[serde(default)]
    pub image_metadata: Option<BootstrapImageMetadata>,
    #[serde(default)]
    pub video_metadata: Option<BootstrapVideoMetadata>,
    pub provenance: MultimodalAssetProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BootstrapImageMetadata {
    pub width_px: Option<u32>,
    pub height_px: Option<u32>,
    pub captured_at_ms: Option<u64>,
}

impl BootstrapImageMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_dimensions(mut self, width_px: u32, height_px: u32) -> Self {
        self.width_px = Some(width_px);
        self.height_px = Some(height_px);
        self
    }

    pub fn with_captured_at_ms(mut self, captured_at_ms: u64) -> Self {
        self.captured_at_ms = Some(captured_at_ms);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BootstrapVideoMetadata {
    pub duration_ms: Option<u64>,
    pub frame_width_px: Option<u32>,
    pub frame_height_px: Option<u32>,
    pub frame_rate_milli_fps: Option<u32>,
}

impl BootstrapVideoMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_duration_ms(mut self, duration_ms: u64) -> Self {
        self.duration_ms = Some(duration_ms);
        self
    }

    pub fn with_frame_dimensions(mut self, frame_width_px: u32, frame_height_px: u32) -> Self {
        self.frame_width_px = Some(frame_width_px);
        self.frame_height_px = Some(frame_height_px);
        self
    }

    pub fn with_frame_rate_milli_fps(mut self, frame_rate_milli_fps: u32) -> Self {
        self.frame_rate_milli_fps = Some(frame_rate_milli_fps);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhotoAsset {
    pub asset_id: String,
    pub original_file_name: String,
    pub media_type: Option<String>,
    pub byte_length: u64,
    pub stored_relative_path: String,
    pub image_metadata: Option<BootstrapImageMetadata>,
    pub provenance: MultimodalAssetProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoAsset {
    pub asset_id: String,
    pub original_file_name: String,
    pub media_type: Option<String>,
    pub byte_length: u64,
    pub stored_relative_path: String,
    pub video_metadata: Option<BootstrapVideoMetadata>,
    pub provenance: MultimodalAssetProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMultimodalAssetImport {
    asset_id: String,
    kind: MultimodalAssetKind,
    source_path: PathBuf,
    media_type: Option<String>,
    image_metadata: Option<BootstrapImageMetadata>,
    video_metadata: Option<BootstrapVideoMetadata>,
    provenance: MultimodalAssetProvenance,
}

impl NewMultimodalAssetImport {
    pub fn new(
        asset_id: impl Into<String>,
        kind: MultimodalAssetKind,
        source_path: impl Into<PathBuf>,
        source: impl Into<String>,
        imported_at_ms: u64,
    ) -> Self {
        Self {
            asset_id: asset_id.into(),
            kind,
            source_path: source_path.into(),
            media_type: None,
            image_metadata: None,
            video_metadata: None,
            provenance: MultimodalAssetProvenance {
                source: source.into(),
                imported_at_ms,
            },
        }
    }

    pub fn with_media_type(mut self, media_type: impl Into<String>) -> Self {
        self.media_type = Some(media_type.into());
        self
    }

    pub fn with_image_metadata(mut self, image_metadata: BootstrapImageMetadata) -> Self {
        self.image_metadata = Some(image_metadata);
        self
    }

    pub fn with_video_metadata(mut self, video_metadata: BootstrapVideoMetadata) -> Self {
        self.video_metadata = Some(video_metadata);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultimodalAssetQuery {
    asset_id: String,
}

impl MultimodalAssetQuery {
    pub fn asset_id(asset_id: impl Into<String>) -> Self {
        Self {
            asset_id: asset_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhotoAssetQuery {
    asset_id: String,
}

impl PhotoAssetQuery {
    pub fn asset_id(asset_id: impl Into<String>) -> Self {
        Self {
            asset_id: asset_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoAssetQuery {
    asset_id: String,
}

impl VideoAssetQuery {
    pub fn asset_id(asset_id: impl Into<String>) -> Self {
        Self {
            asset_id: asset_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultimodalError {
    Io(String),
    Json(String),
    InvalidRequest(String),
}

impl fmt::Display for MultimodalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) | Self::Json(message) | Self::InvalidRequest(message) => {
                write!(f, "{message}")
            }
        }
    }
}

pub struct MultimodalIngestSession {
    root: PathBuf,
    metadata_path: PathBuf,
    assets: Vec<MultimodalAsset>,
    closed: bool,
}

impl MultimodalIngestSession {
    pub fn open(root: &Path) -> Result<Self, MultimodalError> {
        fs::create_dir_all(root).map_err(io_error)?;
        fs::create_dir_all(root.join(MULTIMODAL_ASSET_DIR_NAME)).map_err(io_error)?;

        let metadata_path = root.join(MULTIMODAL_METADATA_FILE_NAME);
        if !metadata_path.exists() {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&metadata_path)
                .map_err(io_error)?;
        }

        let assets = load_assets(&metadata_path)?;
        Ok(Self {
            root: root.to_path_buf(),
            metadata_path,
            assets,
            closed: false,
        })
    }

    pub fn import_asset(
        &mut self,
        new_asset: NewMultimodalAssetImport,
    ) -> Result<MultimodalAsset, MultimodalError> {
        self.ensure_open()?;
        validate_import_request(&new_asset)?;

        let mut metadata_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.metadata_path)
            .map_err(io_error)?;
        metadata_file.lock_exclusive().map_err(io_error)?;

        let mut assets = load_assets_from_file(&mut metadata_file)?;
        if assets
            .iter()
            .any(|asset| asset.asset_id == new_asset.asset_id)
        {
            return Err(MultimodalError::InvalidRequest(format!(
                "multimodal asset id already exists: {}",
                new_asset.asset_id
            )));
        }

        let original_file_name = new_asset
            .source_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                MultimodalError::InvalidRequest(
                    "multimodal source path must have a valid UTF-8 file name".to_owned(),
                )
            })?
            .to_owned();
        let temp_relative_path = format!(
            "{MULTIMODAL_ASSET_DIR_NAME}/{}.importing",
            sanitize_asset_id(&new_asset.asset_id)
        );
        let temp_absolute_path = self.root.join(&temp_relative_path);
        let (byte_length, sha256_hex) =
            copy_asset_and_hash(&new_asset.source_path, &temp_absolute_path)?;
        let stored_file_name = format!(
            "{}-{}{}",
            sanitize_asset_id(&new_asset.asset_id),
            &sha256_hex[..12],
            file_extension_suffix(&new_asset.source_path)
        );
        let stored_relative_path = format!("{MULTIMODAL_ASSET_DIR_NAME}/{stored_file_name}");
        let stored_absolute_path = self.root.join(&stored_relative_path);
        fs::rename(&temp_absolute_path, &stored_absolute_path).map_err(io_error)?;

        let asset = MultimodalAsset {
            asset_id: new_asset.asset_id,
            kind: new_asset.kind,
            original_file_name,
            media_type: new_asset.media_type,
            byte_length,
            sha256_hex,
            stored_relative_path,
            image_metadata: new_asset.image_metadata,
            video_metadata: new_asset.video_metadata,
            provenance: new_asset.provenance,
        };

        append_asset_to_file(&mut metadata_file, &asset)?;
        metadata_file.unlock().map_err(io_error)?;
        assets.push(asset.clone());
        self.assets = assets;
        Ok(asset)
    }

    pub fn asset(
        &mut self,
        query: MultimodalAssetQuery,
    ) -> Result<Option<MultimodalAsset>, MultimodalError> {
        self.ensure_open()?;
        Ok(self
            .assets
            .iter()
            .find(|asset| asset.asset_id == query.asset_id)
            .cloned())
    }

    pub fn list_assets(&mut self) -> Result<Vec<MultimodalAsset>, MultimodalError> {
        self.ensure_open()?;
        Ok(self.assets.clone())
    }

    pub fn photo_asset(
        &mut self,
        query: PhotoAssetQuery,
    ) -> Result<Option<PhotoAsset>, MultimodalError> {
        self.ensure_open()?;
        Ok(self
            .assets
            .iter()
            .find(|asset| {
                asset.asset_id == query.asset_id && asset.kind == MultimodalAssetKind::Image
            })
            .map(photo_asset_from_multimodal))
    }

    pub fn list_photo_assets(&mut self) -> Result<Vec<PhotoAsset>, MultimodalError> {
        self.ensure_open()?;
        Ok(self
            .assets
            .iter()
            .filter(|asset| asset.kind == MultimodalAssetKind::Image)
            .map(photo_asset_from_multimodal)
            .collect())
    }

    pub fn video_asset(
        &mut self,
        query: VideoAssetQuery,
    ) -> Result<Option<VideoAsset>, MultimodalError> {
        self.ensure_open()?;
        Ok(self
            .assets
            .iter()
            .find(|asset| {
                asset.asset_id == query.asset_id && asset.kind == MultimodalAssetKind::Video
            })
            .map(video_asset_from_multimodal))
    }

    pub fn list_video_assets(&mut self) -> Result<Vec<VideoAsset>, MultimodalError> {
        self.ensure_open()?;
        Ok(self
            .assets
            .iter()
            .filter(|asset| asset.kind == MultimodalAssetKind::Video)
            .map(video_asset_from_multimodal)
            .collect())
    }

    pub fn close(&mut self) -> Result<(), MultimodalError> {
        self.closed = true;
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), MultimodalError> {
        if self.closed {
            return Err(MultimodalError::InvalidRequest(
                "multimodal ingest session is already closed".to_owned(),
            ));
        }
        Ok(())
    }
}

fn load_assets(path: &Path) -> Result<Vec<MultimodalAsset>, MultimodalError> {
    let mut file = OpenOptions::new().read(true).open(path).map_err(io_error)?;
    load_assets_from_file(&mut file)
}

fn load_assets_from_file(file: &mut File) -> Result<Vec<MultimodalAsset>, MultimodalError> {
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    BufReader::new(file)
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some(other),
        })
        .map(|line| {
            let line = line.map_err(io_error)?;
            serde_json::from_str(&line).map_err(json_error)
        })
        .collect()
}

fn append_asset_to_file(file: &mut File, asset: &MultimodalAsset) -> Result<(), MultimodalError> {
    let encoded = serde_json::to_string(asset).map_err(json_error)?;
    writeln!(file, "{encoded}").map_err(io_error)?;
    file.flush().map_err(io_error)?;
    Ok(())
}

fn validate_import_request(request: &NewMultimodalAssetImport) -> Result<(), MultimodalError> {
    if request.asset_id.is_empty() {
        return Err(MultimodalError::InvalidRequest(
            "multimodal asset id cannot be empty".to_owned(),
        ));
    }
    if request.provenance.source.is_empty() {
        return Err(MultimodalError::InvalidRequest(
            "multimodal provenance source cannot be empty".to_owned(),
        ));
    }
    if let Some(media_type) = request.media_type.as_ref() {
        if media_type.is_empty() {
            return Err(MultimodalError::InvalidRequest(
                "multimodal media type cannot be empty".to_owned(),
            ));
        }
    }
    if request.kind != MultimodalAssetKind::Image && request.image_metadata.is_some() {
        return Err(MultimodalError::InvalidRequest(
            "bootstrap image metadata is only valid for image assets".to_owned(),
        ));
    }
    if request.kind != MultimodalAssetKind::Video && request.video_metadata.is_some() {
        return Err(MultimodalError::InvalidRequest(
            "bootstrap video metadata is only valid for video assets".to_owned(),
        ));
    }

    let metadata = fs::metadata(&request.source_path).map_err(io_error)?;
    if !metadata.is_file() {
        return Err(MultimodalError::InvalidRequest(format!(
            "multimodal source path is not a file: {}",
            request.source_path.display()
        )));
    }
    Ok(())
}

fn sanitize_asset_id(asset_id: &str) -> String {
    asset_id
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => ch,
            _ => '_',
        })
        .collect()
}

fn file_extension_suffix(path: &Path) -> String {
    path.extension()
        .and_then(|ext| ext.to_str())
        .filter(|ext| !ext.is_empty())
        .map(|ext| format!(".{ext}"))
        .unwrap_or_else(|| ".bin".to_owned())
}

fn sha256_digest_hex(digest: &[u8]) -> String {
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push(hex_nibble(byte >> 4));
        hex.push(hex_nibble(byte & 0x0f));
    }
    hex
}

fn copy_asset_and_hash(
    source_path: &Path,
    destination_path: &Path,
) -> Result<(u64, String), MultimodalError> {
    let mut source = std::fs::File::open(source_path).map_err(io_error)?;
    let mut destination = std::fs::File::create(destination_path).map_err(io_error)?;
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    let mut buffer = [0u8; 64 * 1024];

    loop {
        let read = source.read(&mut buffer).map_err(io_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        destination.write_all(&buffer[..read]).map_err(io_error)?;
        total = total.checked_add(read as u64).ok_or_else(|| {
            MultimodalError::InvalidRequest("multimodal asset length overflow".to_owned())
        })?;
    }
    destination.flush().map_err(io_error)?;

    Ok((total, sha256_digest_hex(hasher.finalize().as_slice())))
}

fn photo_asset_from_multimodal(asset: &MultimodalAsset) -> PhotoAsset {
    PhotoAsset {
        asset_id: asset.asset_id.clone(),
        original_file_name: asset.original_file_name.clone(),
        media_type: asset.media_type.clone(),
        byte_length: asset.byte_length,
        stored_relative_path: asset.stored_relative_path.clone(),
        image_metadata: asset.image_metadata.clone(),
        provenance: asset.provenance.clone(),
    }
}

fn video_asset_from_multimodal(asset: &MultimodalAsset) -> VideoAsset {
    VideoAsset {
        asset_id: asset.asset_id.clone(),
        original_file_name: asset.original_file_name.clone(),
        media_type: asset.media_type.clone(),
        byte_length: asset.byte_length,
        stored_relative_path: asset.stored_relative_path.clone(),
        video_metadata: asset.video_metadata.clone(),
        provenance: asset.provenance.clone(),
    }
}

fn hex_nibble(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => unreachable!(),
    }
}

fn io_error(error: std::io::Error) -> MultimodalError {
    MultimodalError::Io(error.to_string())
}

fn json_error(error: serde_json::Error) -> MultimodalError {
    MultimodalError::Json(error.to_string())
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{
        sha256_digest_hex, BootstrapImageMetadata, BootstrapVideoMetadata, MultimodalAssetKind,
        MultimodalAssetQuery, MultimodalIngestSession, NewMultimodalAssetImport, PhotoAssetQuery,
        VideoAssetQuery,
    };

    #[test]
    fn multimodal_ingest_session_round_trips_imported_assets() {
        let root = tempdir().unwrap();
        let source_path = root.path().join("frame.png");
        std::fs::write(&source_path, [1_u8, 2, 3, 4]).unwrap();

        let mut session = MultimodalIngestSession::open(root.path()).unwrap();
        let imported = session
            .import_asset(
                NewMultimodalAssetImport::new(
                    "image:frame",
                    MultimodalAssetKind::Image,
                    &source_path,
                    "bootstrap-test",
                    100,
                )
                .with_media_type("image/png"),
            )
            .unwrap();
        assert_eq!(imported.byte_length, 4);
        let expected_sha = sha256_digest_hex(&Sha256::digest([1_u8, 2, 3, 4]));
        assert_eq!(imported.sha256_hex, expected_sha);
        session.close().unwrap();

        let mut reopened = MultimodalIngestSession::open(root.path()).unwrap();
        let asset = reopened
            .asset(MultimodalAssetQuery::asset_id("image:frame"))
            .unwrap()
            .unwrap();
        assert_eq!(asset.media_type.as_deref(), Some("image/png"));
        assert!(root.path().join(asset.stored_relative_path).exists());
    }

    #[test]
    fn multimodal_ingest_session_rejects_duplicate_asset_ids() {
        let root = tempdir().unwrap();
        let source_path = root.path().join("frame.jpg");
        std::fs::write(&source_path, [1_u8, 2, 3, 4]).unwrap();

        let mut session = MultimodalIngestSession::open(root.path()).unwrap();
        session
            .import_asset(NewMultimodalAssetImport::new(
                "image:duplicate",
                MultimodalAssetKind::Image,
                &source_path,
                "bootstrap-test",
                100,
            ))
            .unwrap();

        let error = session
            .import_asset(NewMultimodalAssetImport::new(
                "image:duplicate",
                MultimodalAssetKind::Image,
                &source_path,
                "bootstrap-test",
                101,
            ))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("multimodal asset id already exists"));
    }

    #[test]
    fn multimodal_ingest_session_lists_only_photo_assets() {
        let root = tempdir().unwrap();
        let image_path = root.path().join("frame.png");
        let video_path = root.path().join("clip.mp4");
        std::fs::write(&image_path, [1_u8, 2, 3, 4]).unwrap();
        std::fs::write(&video_path, [4_u8, 3, 2, 1]).unwrap();

        let mut session = MultimodalIngestSession::open(root.path()).unwrap();
        session
            .import_asset(NewMultimodalAssetImport::new(
                "image:frame",
                MultimodalAssetKind::Image,
                &image_path,
                "bootstrap-test",
                100,
            ))
            .unwrap();
        session
            .import_asset(NewMultimodalAssetImport::new(
                "video:clip",
                MultimodalAssetKind::Video,
                &video_path,
                "bootstrap-test",
                101,
            ))
            .unwrap();

        let photo_assets = session.list_photo_assets().unwrap();
        assert_eq!(photo_assets.len(), 1);
        assert_eq!(photo_assets[0].asset_id, "image:frame");
    }

    #[test]
    fn multimodal_ingest_session_round_trips_photo_metadata() {
        let root = tempdir().unwrap();
        let image_path = root.path().join("poster.jpg");
        std::fs::write(&image_path, [1_u8, 2, 3, 4]).unwrap();

        let mut session = MultimodalIngestSession::open(root.path()).unwrap();
        session
            .import_asset(
                NewMultimodalAssetImport::new(
                    "image:poster",
                    MultimodalAssetKind::Image,
                    &image_path,
                    "bootstrap-test",
                    100,
                )
                .with_image_metadata(
                    BootstrapImageMetadata::new()
                        .with_dimensions(800, 600)
                        .with_captured_at_ms(90),
                ),
            )
            .unwrap();
        session.close().unwrap();

        let mut reopened = MultimodalIngestSession::open(root.path()).unwrap();
        let photo = reopened
            .photo_asset(PhotoAssetQuery::asset_id("image:poster"))
            .unwrap()
            .unwrap();
        assert_eq!(photo.image_metadata.as_ref().unwrap().width_px, Some(800));
        assert_eq!(photo.image_metadata.as_ref().unwrap().height_px, Some(600));
        assert_eq!(
            photo.image_metadata.as_ref().unwrap().captured_at_ms,
            Some(90)
        );

        let raw = reopened
            .asset(MultimodalAssetQuery::asset_id("image:poster"))
            .unwrap()
            .unwrap();
        assert_eq!(raw.image_metadata.as_ref().unwrap().width_px, Some(800));
    }

    #[test]
    fn multimodal_ingest_session_lists_only_video_assets() {
        let root = tempdir().unwrap();
        let image_path = root.path().join("frame.png");
        let video_path = root.path().join("clip.mp4");
        std::fs::write(&image_path, [1_u8, 2, 3, 4]).unwrap();
        std::fs::write(&video_path, [4_u8, 3, 2, 1]).unwrap();

        let mut session = MultimodalIngestSession::open(root.path()).unwrap();
        session
            .import_asset(NewMultimodalAssetImport::new(
                "image:frame",
                MultimodalAssetKind::Image,
                &image_path,
                "bootstrap-test",
                100,
            ))
            .unwrap();
        session
            .import_asset(NewMultimodalAssetImport::new(
                "video:clip",
                MultimodalAssetKind::Video,
                &video_path,
                "bootstrap-test",
                101,
            ))
            .unwrap();

        let video_assets = session.list_video_assets().unwrap();
        assert_eq!(video_assets.len(), 1);
        assert_eq!(video_assets[0].asset_id, "video:clip");
    }

    #[test]
    fn multimodal_ingest_session_round_trips_video_metadata() {
        let root = tempdir().unwrap();
        let video_path = root.path().join("scene.mov");
        std::fs::write(&video_path, [1_u8, 2, 3, 4]).unwrap();

        let mut session = MultimodalIngestSession::open(root.path()).unwrap();
        session
            .import_asset(
                NewMultimodalAssetImport::new(
                    "video:scene",
                    MultimodalAssetKind::Video,
                    &video_path,
                    "bootstrap-test",
                    100,
                )
                .with_video_metadata(
                    BootstrapVideoMetadata::new()
                        .with_duration_ms(3_000)
                        .with_frame_dimensions(1920, 1080)
                        .with_frame_rate_milli_fps(24_000),
                ),
            )
            .unwrap();
        session.close().unwrap();

        let mut reopened = MultimodalIngestSession::open(root.path()).unwrap();
        let video = reopened
            .video_asset(VideoAssetQuery::asset_id("video:scene"))
            .unwrap()
            .unwrap();
        assert_eq!(
            video.video_metadata.as_ref().unwrap().duration_ms,
            Some(3_000)
        );
        assert_eq!(
            video.video_metadata.as_ref().unwrap().frame_width_px,
            Some(1920)
        );
        assert_eq!(
            video.video_metadata.as_ref().unwrap().frame_height_px,
            Some(1080)
        );
        assert_eq!(
            video.video_metadata.as_ref().unwrap().frame_rate_milli_fps,
            Some(24_000)
        );
    }
}
