use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
#[cfg(not(unix))]
use std::path::Component;
use std::path::Path;
#[cfg(any(not(unix), target_os = "macos"))]
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
#[cfg(test)]
use std::thread::ThreadId;

#[cfg(not(unix))]
use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use fs2::FileExt;
use memmap2::{Mmap, MmapOptions};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
};

const FILE_MAGIC: &[u8; 8] = b"RAXSTORE";
const MANIFEST_MAGIC: &[u8; 8] = b"RAXMANIF";
const OBJECT_MAGIC: &[u8; 4] = b"RXOB";
const FORMAT_VERSION: u32 = 1;
const SUPERBLOCK_CHECKSUM_OFFSET: usize = 64;
const SUPERBLOCK_CHECKSUM_LENGTH: usize = 32;
const MANIFEST_HEADER_LENGTH: usize = 24;
const SEGMENT_DESCRIPTOR_LENGTH: usize = 128;
const MAX_SEGMENT_DESCRIPTOR_COUNT: usize = 65_536;
const MAX_MANIFEST_PAYLOAD_LENGTH: usize =
    MANIFEST_HEADER_LENGTH + (MAX_SEGMENT_DESCRIPTOR_COUNT * SEGMENT_DESCRIPTOR_LENGTH);
const MAX_MANIFEST_OBJECT_LENGTH: usize = OBJECT_HEADER_LENGTH + MAX_MANIFEST_PAYLOAD_LENGTH;
const OBJECT_HEADER_LENGTH: usize = 64;
const OBJECT_VERSION: u16 = 1;
const DEFAULT_OBJECT_ALIGNMENT: u64 = 4096;
const STORE_PUBLISH_LOCK_BUSY_MESSAGE: &str = "store publish lock is busy; retry";
const MAX_SEGMENT_OBJECT_LENGTH: u64 = 8 * 1024 * 1024 * 1024;
const MAX_ACTIVE_SEGMENT_BYTES: u64 = 512 * 1024 * 1024;

pub const SUPERBLOCK_SIZE: usize = 128;
static TEMP_STORE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
type CreateEmptyStoreFailHook = (ThreadId, Box<dyn FnOnce() -> CoreError + Send>);
#[cfg(test)]
static CREATE_EMPTY_STORE_FAIL_HOOKS: OnceLock<Mutex<Vec<CreateEmptyStoreFailHook>>> =
    OnceLock::new();
#[cfg(test)]
type CreateEmptyStorePrePublishHook = (ThreadId, Box<dyn FnOnce() -> Result<(), CoreError> + Send>);
#[cfg(test)]
static CREATE_EMPTY_STORE_PRE_PUBLISH_HOOKS: OnceLock<Mutex<Vec<CreateEmptyStorePrePublishHook>>> =
    OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    Io(String),
    AlreadyExists(String),
    TemporaryNameExhausted(String),
    InvalidMagic {
        context: &'static str,
    },
    InvalidVersion(u32),
    UnexpectedLength {
        context: &'static str,
        expected_at_least: usize,
        actual: usize,
    },
    ChecksumMismatch {
        context: &'static str,
    },
    InvalidManifest(String),
    PublishPreconditionFailed(String),
    UnknownSegmentKind(u16),
    NoValidSuperblock,
}

impl From<std::io::Error> for CoreError {
    fn from(error: std::io::Error) -> Self {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            return Self::AlreadyExists(error.to_string());
        }
        Self::Io(error.to_string())
    }
}

impl CoreError {
    pub fn is_already_exists(&self) -> bool {
        matches!(self, Self::AlreadyExists(_))
    }
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    Doc,
    Txt,
    Vec,
}

impl SegmentKind {
    fn as_code(self) -> u16 {
        match self {
            Self::Doc => 1,
            Self::Txt => 2,
            Self::Vec => 3,
        }
    }

    fn from_code(code: u16) -> Result<Self, CoreError> {
        match code {
            1 => Ok(Self::Doc),
            2 => Ok(Self::Txt),
            3 => Ok(Self::Vec),
            _ => Err(CoreError::UnknownSegmentKind(code)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDescriptor {
    pub family: SegmentKind,
    pub family_version: u16,
    pub flags: u32,
    pub object_offset: u64,
    pub object_length: u64,
    pub segment_generation: u64,
    pub doc_id_start: u64,
    pub doc_id_end_exclusive: u64,
    pub min_timestamp_ms: u64,
    pub max_timestamp_ms: u64,
    pub live_items: u64,
    pub tombstoned_items: u64,
    pub backend_id: u64,
    pub backend_aux: u64,
    pub object_checksum: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSegmentDescriptor {
    pub family: SegmentKind,
    pub family_version: u16,
    pub flags: u32,
    pub doc_id_start: u64,
    pub doc_id_end_exclusive: u64,
    pub min_timestamp_ms: u64,
    pub max_timestamp_ms: u64,
    pub live_items: u64,
    pub tombstoned_items: u64,
    pub backend_id: u64,
    pub backend_aux: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSegmentWrite {
    pub descriptor: PendingSegmentDescriptor,
    pub object_bytes: Vec<u8>,
}

impl PendingSegmentDescriptor {
    fn publish(
        &self,
        object_offset: u64,
        object_length: u64,
        segment_generation: u64,
        object_checksum: [u8; 32],
    ) -> SegmentDescriptor {
        SegmentDescriptor {
            family: self.family,
            family_version: self.family_version,
            flags: self.flags,
            object_offset,
            object_length,
            segment_generation,
            doc_id_start: self.doc_id_start,
            doc_id_end_exclusive: self.doc_id_end_exclusive,
            min_timestamp_ms: self.min_timestamp_ms,
            max_timestamp_ms: self.max_timestamp_ms,
            live_items: self.live_items,
            tombstoned_items: self.tombstoned_items,
            backend_id: self.backend_id,
            backend_aux: self.backend_aux,
            object_checksum,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectType {
    Manifest = 1,
    DocSegment = 2,
    TxtSegment = 3,
    VecSegment = 4,
}

impl ObjectType {
    fn as_code(self) -> u16 {
        self as u16
    }

    fn from_code(code: u16) -> Result<Self, CoreError> {
        match code {
            1 => Ok(Self::Manifest),
            2 => Ok(Self::DocSegment),
            3 => Ok(Self::TxtSegment),
            4 => Ok(Self::VecSegment),
            _ => Err(CoreError::InvalidManifest(format!(
                "unknown object type: {code}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveManifest {
    pub generation: u64,
    pub segments: Vec<SegmentDescriptor>,
}

impl ActiveManifest {
    pub fn encode(&self) -> Result<Vec<u8>, CoreError> {
        if self.segments.len() > MAX_SEGMENT_DESCRIPTOR_COUNT {
            return Err(CoreError::InvalidManifest(format!(
                "manifest segment count exceeds maximum {MAX_SEGMENT_DESCRIPTOR_COUNT}"
            )));
        }
        let segment_count = u32::try_from(self.segments.len()).map_err(|_| {
            CoreError::InvalidManifest("manifest segment count exceeds u32::MAX".to_owned())
        })?;
        let encoded_length = self
            .segments
            .len()
            .checked_mul(SEGMENT_DESCRIPTOR_LENGTH)
            .and_then(|length| length.checked_add(MANIFEST_HEADER_LENGTH))
            .ok_or_else(|| {
                CoreError::InvalidManifest("manifest encoded length overflow".to_owned())
            })?;
        let mut bytes = Vec::with_capacity(encoded_length);
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.generation.to_le_bytes());
        bytes.extend_from_slice(&segment_count.to_le_bytes());

        for segment in &self.segments {
            bytes.extend_from_slice(&segment.family.as_code().to_le_bytes());
            bytes.extend_from_slice(&segment.family_version.to_le_bytes());
            bytes.extend_from_slice(&segment.flags.to_le_bytes());
            bytes.extend_from_slice(&segment.object_offset.to_le_bytes());
            bytes.extend_from_slice(&segment.object_length.to_le_bytes());
            bytes.extend_from_slice(&segment.segment_generation.to_le_bytes());
            bytes.extend_from_slice(&segment.doc_id_start.to_le_bytes());
            bytes.extend_from_slice(&segment.doc_id_end_exclusive.to_le_bytes());
            bytes.extend_from_slice(&segment.min_timestamp_ms.to_le_bytes());
            bytes.extend_from_slice(&segment.max_timestamp_ms.to_le_bytes());
            bytes.extend_from_slice(&segment.live_items.to_le_bytes());
            bytes.extend_from_slice(&segment.tombstoned_items.to_le_bytes());
            bytes.extend_from_slice(&segment.backend_id.to_le_bytes());
            bytes.extend_from_slice(&segment.backend_aux.to_le_bytes());
            bytes.extend_from_slice(&segment.object_checksum);
        }

        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CoreError> {
        if bytes.len() < MANIFEST_HEADER_LENGTH {
            return Err(CoreError::UnexpectedLength {
                context: "manifest",
                expected_at_least: MANIFEST_HEADER_LENGTH,
                actual: bytes.len(),
            });
        }
        if &bytes[..8] != MANIFEST_MAGIC {
            return Err(CoreError::InvalidMagic {
                context: "manifest",
            });
        }

        let version = read_u32(bytes, 8);
        if version != FORMAT_VERSION {
            return Err(CoreError::InvalidVersion(version));
        }

        let generation = read_u64(bytes, 12);
        let segment_count = read_u32(bytes, 20) as usize;
        let expected_length = segment_count
            .checked_mul(SEGMENT_DESCRIPTOR_LENGTH)
            .and_then(|length| length.checked_add(MANIFEST_HEADER_LENGTH))
            .ok_or_else(|| CoreError::InvalidManifest("manifest length overflow".to_owned()))?;
        if bytes.len() != expected_length {
            return Err(CoreError::InvalidManifest(format!(
                "manifest length mismatch: expected {expected_length} bytes, found {}",
                bytes.len()
            )));
        }

        let mut segments = Vec::with_capacity(segment_count);
        let mut cursor = MANIFEST_HEADER_LENGTH;
        for _ in 0..segment_count {
            let family = SegmentKind::from_code(read_u16(bytes, cursor))?;
            let family_version = read_u16(bytes, cursor + 2);
            let flags = read_u32(bytes, cursor + 4);
            let object_offset = read_u64(bytes, cursor + 8);
            let object_length = read_u64(bytes, cursor + 16);
            let segment_generation = read_u64(bytes, cursor + 24);
            let doc_id_start = read_u64(bytes, cursor + 32);
            let doc_id_end_exclusive = read_u64(bytes, cursor + 40);
            let min_timestamp_ms = read_u64(bytes, cursor + 48);
            let max_timestamp_ms = read_u64(bytes, cursor + 56);
            let live_items = read_u64(bytes, cursor + 64);
            let tombstoned_items = read_u64(bytes, cursor + 72);
            let backend_id = read_u64(bytes, cursor + 80);
            let backend_aux = read_u64(bytes, cursor + 88);
            let mut object_checksum = [0; 32];
            object_checksum.copy_from_slice(&bytes[cursor + 96..cursor + 128]);
            segments.push(SegmentDescriptor {
                family,
                family_version,
                flags,
                object_offset,
                object_length,
                segment_generation,
                doc_id_start,
                doc_id_end_exclusive,
                min_timestamp_ms,
                max_timestamp_ms,
                live_items,
                tombstoned_items,
                backend_id,
                backend_aux,
                object_checksum,
            });
            cursor += SEGMENT_DESCRIPTOR_LENGTH;
        }

        validate_segments(generation, &segments)?;

        Ok(Self {
            generation,
            segments,
        })
    }

    pub fn checksum(bytes: &[u8]) -> [u8; 32] {
        sha256(bytes)
    }
}

fn validate_segments(
    manifest_generation: u64,
    segments: &[SegmentDescriptor],
) -> Result<(), CoreError> {
    let mut total_object_bytes = 0u64;
    for segment in segments {
        if segment.object_length == 0 {
            return Err(CoreError::InvalidManifest(
                "segment descriptor object length must be non-zero".to_owned(),
            ));
        }
        total_object_bytes = total_object_bytes
            .checked_add(segment.object_length)
            .ok_or_else(|| {
                CoreError::InvalidManifest("active segment object byte total overflow".to_owned())
            })?;
        if total_object_bytes > MAX_ACTIVE_SEGMENT_BYTES {
            return Err(CoreError::InvalidManifest(format!(
                "active segment object bytes exceed maximum {MAX_ACTIVE_SEGMENT_BYTES}"
            )));
        }
        if segment.segment_generation > manifest_generation {
            return Err(CoreError::InvalidManifest(
                "segment descriptor generation must not exceed manifest generation".to_owned(),
            ));
        }
        if segment.doc_id_start > segment.doc_id_end_exclusive {
            return Err(CoreError::InvalidManifest(
                "segment descriptor doc_id range is invalid".to_owned(),
            ));
        }
        if segment.min_timestamp_ms > segment.max_timestamp_ms && segment.live_items > 0 {
            return Err(CoreError::InvalidManifest(
                "segment descriptor timestamp range is invalid".to_owned(),
            ));
        }
    }

    let mut families = segments
        .iter()
        .map(|segment| segment.family)
        .collect::<Vec<_>>();
    families.sort_by_key(|family| family.as_code());
    for pair in families.windows(2) {
        if pair[0] == pair[1] {
            return Err(CoreError::InvalidManifest(
                "active manifest accepts at most one segment per family".to_owned(),
            ));
        }
    }

    for pair in segments.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        let left_key = (
            left.family.as_code(),
            left.object_offset,
            left.segment_generation,
        );
        let right_key = (
            right.family.as_code(),
            right.object_offset,
            right.segment_generation,
        );
        if left_key > right_key {
            return Err(CoreError::InvalidManifest(
                "segment descriptors must be sorted by family, object_offset, segment_generation"
                    .to_owned(),
            ));
        }
    }

    let mut by_offset = segments.iter().collect::<Vec<_>>();
    by_offset.sort_by_key(|segment| (segment.object_offset, segment.object_length));
    for pair in by_offset.windows(2) {
        let left = pair[0];
        let right = pair[1];
        let left_end = left
            .object_offset
            .checked_add(left.object_length)
            .ok_or_else(|| {
                CoreError::InvalidManifest("segment object range overflow".to_owned())
            })?;
        if left_end > right.object_offset {
            return Err(CoreError::InvalidManifest(
                "segment object ranges must not overlap".to_owned(),
            ));
        }
    }

    Ok(())
}

fn validate_pending_segments(pending_segments: &[PendingSegmentWrite]) -> Result<(), CoreError> {
    let mut families = pending_segments
        .iter()
        .map(|segment| segment.descriptor.family)
        .collect::<Vec<_>>();
    families.sort_by_key(|family| family.as_code());
    for pair in families.windows(2) {
        if pair[0] == pair[1] {
            return Err(CoreError::InvalidManifest(
                "publish_segments accepts at most one pending segment per family".to_owned(),
            ));
        }
    }

    for pending_segment in pending_segments {
        let descriptor = &pending_segment.descriptor;
        if pending_segment.object_bytes.is_empty() {
            return Err(CoreError::InvalidManifest(
                "pending segment object payload must be non-empty".to_owned(),
            ));
        }
        let encoded_length = (OBJECT_HEADER_LENGTH as u64)
            .checked_add(pending_segment.object_bytes.len() as u64)
            .ok_or_else(|| {
                CoreError::InvalidManifest("segment object length overflow".to_owned())
            })?;
        if encoded_length > MAX_SEGMENT_OBJECT_LENGTH {
            return Err(CoreError::InvalidManifest(
                "pending segment object exceeds maximum supported segment size".to_owned(),
            ));
        }
        if descriptor.doc_id_start > descriptor.doc_id_end_exclusive {
            return Err(CoreError::InvalidManifest(
                "pending segment descriptor doc_id range is invalid".to_owned(),
            ));
        }
        if descriptor.min_timestamp_ms > descriptor.max_timestamp_ms && descriptor.live_items > 0 {
            return Err(CoreError::InvalidManifest(
                "pending segment descriptor timestamp range is invalid".to_owned(),
            ));
        }
    }

    Ok(())
}

fn validate_active_segment_object_byte_budget(
    retained_segments: &[SegmentDescriptor],
    pending_segments: &[PendingSegmentWrite],
) -> Result<(), CoreError> {
    let mut total_object_bytes = retained_segments.iter().try_fold(0u64, |total, segment| {
        total.checked_add(segment.object_length).ok_or_else(|| {
            CoreError::InvalidManifest("active segment object byte total overflow".to_owned())
        })
    })?;
    for pending_segment in pending_segments {
        let encoded_length = (OBJECT_HEADER_LENGTH as u64)
            .checked_add(pending_segment.object_bytes.len() as u64)
            .ok_or_else(|| {
                CoreError::InvalidManifest("segment object length overflow".to_owned())
            })?;
        total_object_bytes = total_object_bytes
            .checked_add(encoded_length)
            .ok_or_else(|| {
                CoreError::InvalidManifest("active segment object byte total overflow".to_owned())
            })?;
        if total_object_bytes > MAX_ACTIVE_SEGMENT_BYTES {
            return Err(CoreError::InvalidManifest(format!(
                "active segment object bytes exceed maximum {MAX_ACTIVE_SEGMENT_BYTES}"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Superblock {
    pub generation: u64,
    pub active_manifest_offset: u64,
    pub active_manifest_length: u32,
    pub manifest_checksum: [u8; 32],
}

impl Superblock {
    pub fn new(
        generation: u64,
        active_manifest_offset: u64,
        active_manifest_length: u32,
        manifest_checksum: [u8; 32],
    ) -> Self {
        Self {
            generation,
            active_manifest_offset,
            active_manifest_length,
            manifest_checksum,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![0; SUPERBLOCK_SIZE];
        bytes[..8].copy_from_slice(FILE_MAGIC);
        bytes[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.generation.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.active_manifest_offset.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.active_manifest_length.to_le_bytes());
        bytes[32..64].copy_from_slice(&self.manifest_checksum);
        let checksum = sha256(&bytes[..SUPERBLOCK_CHECKSUM_OFFSET]);
        bytes[SUPERBLOCK_CHECKSUM_OFFSET..SUPERBLOCK_CHECKSUM_OFFSET + SUPERBLOCK_CHECKSUM_LENGTH]
            .copy_from_slice(&checksum);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CoreError> {
        if bytes.len() < SUPERBLOCK_SIZE {
            return Err(CoreError::UnexpectedLength {
                context: "superblock",
                expected_at_least: SUPERBLOCK_SIZE,
                actual: bytes.len(),
            });
        }
        if &bytes[..8] != FILE_MAGIC {
            return Err(CoreError::InvalidMagic {
                context: "superblock",
            });
        }

        let version = read_u32(bytes, 8);
        if version != FORMAT_VERSION {
            return Err(CoreError::InvalidVersion(version));
        }

        let expected_checksum = sha256(&bytes[..SUPERBLOCK_CHECKSUM_OFFSET]);
        let actual_checksum = &bytes
            [SUPERBLOCK_CHECKSUM_OFFSET..SUPERBLOCK_CHECKSUM_OFFSET + SUPERBLOCK_CHECKSUM_LENGTH];
        if expected_checksum != actual_checksum {
            return Err(CoreError::ChecksumMismatch {
                context: "superblock",
            });
        }

        let generation = read_u64(bytes, 12);
        let active_manifest_offset = read_u64(bytes, 20);
        let active_manifest_length = read_u32(bytes, 28);
        let mut manifest_checksum = [0; 32];
        manifest_checksum.copy_from_slice(&bytes[32..64]);

        Ok(Self {
            generation,
            active_manifest_offset,
            active_manifest_length,
            manifest_checksum,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenedStore {
    pub superblock: Superblock,
    pub manifest: ActiveManifest,
    pub highest_observed_generation: u64,
    pub active_superblock_offset: u64,
}

#[derive(Debug)]
pub struct SegmentObject {
    backing: SegmentObjectBacking,
    payload_range: Range<usize>,
}

#[derive(Debug)]
enum SegmentObjectBacking {
    Mapped(Mmap),
}

impl SegmentObject {
    pub fn as_slice(&self) -> &[u8] {
        let bytes = match &self.backing {
            SegmentObjectBacking::Mapped(bytes) => bytes.as_ref(),
        };
        &bytes[self.payload_range.clone()]
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }
}

impl std::ops::Deref for SegmentObject {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

pub fn create_empty_store(path: &Path) -> Result<(), CoreError> {
    create_empty_store_and_open(path).map(|_| ())
}

pub fn create_empty_store_and_open(path: &Path) -> Result<std::fs::File, CoreError> {
    let mut temp = create_new_temporary_store_file(path)?;
    let temp_identity = file_identity_from_file(&temp.file)?;
    let result = create_empty_store_from_file(&mut temp.file)
        .and_then(|()| run_create_empty_store_pre_publish_hook())
        .and_then(|()| publish_temporary_store_file(&temp));
    match result {
        Ok(file) => {
            if let Err(error) = remove_failed_created_store_file(&temp, Some(&temp_identity)) {
                let _ = remove_published_target_file(&temp, &temp_identity);
                return Err(error);
            }
            #[cfg(target_os = "macos")]
            let mut file = {
                let reopened_file = match open_leaf_at(
                    &temp.parent,
                    &temp.target_name,
                    SecureOpenMode::ReadWrite,
                ) {
                    Ok(file) => file,
                    Err(error) => {
                        let _ = remove_published_target_file(&temp, &temp_identity);
                        return Err(error);
                    }
                };
                drop(file);
                let actual_identity = match file_identity_from_file(&reopened_file) {
                    Ok(identity) => identity,
                    Err(error) => {
                        let _ = remove_published_target_file(&temp, &temp_identity);
                        return Err(CoreError::from(error));
                    }
                };
                if !file_identities_match(&actual_identity, &temp_identity) {
                    let _ = remove_published_target_file(&temp, &temp_identity);
                    return Err(CoreError::InvalidManifest(
                        "published store target identity changed before final open".to_owned(),
                    ));
                }
                reopened_file
            };
            #[cfg(not(target_os = "macos"))]
            let mut file = file;
            if let Err(error) = file.seek(SeekFrom::Start(0)) {
                let _ = remove_published_target_file(&temp, &temp_identity);
                return Err(CoreError::from(error));
            }
            Ok(file)
        }
        Err(error) => {
            let _ = remove_failed_created_store_file(&temp, Some(&temp_identity));
            Err(error)
        }
    }
}

pub fn create_empty_store_from_file(file: &mut std::fs::File) -> Result<(), CoreError> {
    if file.metadata()?.len() != 0 {
        return Err(CoreError::InvalidManifest(
            "new store file must be empty".to_owned(),
        ));
    }

    let manifest = ActiveManifest {
        generation: 0,
        segments: Vec::new(),
    };
    let manifest_bytes = manifest.encode()?;
    let manifest_object = encode_object(
        ObjectType::Manifest,
        manifest.generation,
        DEFAULT_OBJECT_ALIGNMENT,
        &manifest_bytes,
    );
    let manifest_offset = align_up((SUPERBLOCK_SIZE * 2) as u64, DEFAULT_OBJECT_ALIGNMENT)?;
    let superblock = Superblock::new(
        manifest.generation,
        manifest_offset,
        manifest_object.len() as u32,
        ActiveManifest::checksum(&manifest_bytes),
    );
    let encoded_superblock = superblock.encode();
    file.seek(SeekFrom::Start(0))?;
    write_zero_padding(file, manifest_offset)?;
    file.write_all(&manifest_object)?;
    #[cfg(test)]
    run_create_empty_store_fail_hook()?;
    file.flush()?;
    file.sync_all()?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&encoded_superblock)?;
    file.write_all(&encoded_superblock)?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

fn remove_failed_created_store_file(
    temp: &TemporaryStoreFile,
    expected_identity: Option<&FileIdentity>,
) -> Result<(), CoreError> {
    if expected_identity
        .is_some_and(|expected_identity| !temporary_store_identity_matches(temp, expected_identity))
    {
        return Ok(());
    }
    #[cfg(unix)]
    {
        let temp_name = nul_terminated_name(&temp.temp_name)?;
        let result =
            unsafe { libc::unlinkat(temp.parent.as_raw_fd(), temp_name.as_ptr().cast(), 0) };
        match result {
            0 => {
                let _ = temp.parent.sync_all();
                Ok(())
            }
            _ => {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(CoreError::from(error))
                }
            }
        }
    }
    #[cfg(not(unix))]
    match temp.parent.remove_file(Path::new(&temp.temp_name)) {
        Ok(()) => sync_cap_dir(&temp.parent),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CoreError::from(error)),
    }
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct FileIdentity {
    dev: u64,
    ino: u64,
}

#[cfg(windows)]
#[derive(Clone, Copy)]
struct FileIdentity {
    volume_serial_number: u32,
    file_index: u64,
}

#[cfg(not(any(unix, windows)))]
#[derive(Clone, Copy)]
struct FileIdentity;

#[cfg(unix)]
fn file_identity_from_file(file: &std::fs::File) -> Result<FileIdentity, std::io::Error> {
    let metadata = file.metadata()?;
    Ok(FileIdentity {
        dev: metadata.dev(),
        ino: metadata.ino(),
    })
}

#[cfg(windows)]
fn file_identity_from_file(file: &std::fs::File) -> Result<FileIdentity, std::io::Error> {
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    let result =
        unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), info.as_mut_ptr()) };
    if result == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let info = unsafe { info.assume_init() };
    Ok(FileIdentity {
        volume_serial_number: info.dwVolumeSerialNumber,
        file_index: ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64,
    })
}

#[cfg(not(any(unix, windows)))]
fn file_identity_from_file(_file: &std::fs::File) -> Result<FileIdentity, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "stable file identity is unsupported on this platform",
    ))
}

#[cfg(unix)]
fn temporary_store_identity_matches(temp: &TemporaryStoreFile, expected: &FileIdentity) -> bool {
    child_identity_matches(&temp.parent, &temp.temp_name, expected)
}

#[cfg(unix)]
fn child_identity_matches(parent: &std::fs::File, name: &OsStr, expected: &FileIdentity) -> bool {
    child_identity(parent, name)
        .ok()
        .flatten()
        .is_some_and(|actual| file_identities_match(&actual, expected))
}

#[cfg(unix)]
fn child_identity(
    parent: &std::fs::File,
    name: &OsStr,
) -> Result<Option<FileIdentity>, std::io::Error> {
    let Ok(name) = nul_terminated_name(name) else {
        return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
    };
    let mut actual = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr().cast(),
            actual.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        return if error.kind() == std::io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error)
        };
    }
    let actual = unsafe { actual.assume_init() };
    Ok(Some(FileIdentity {
        dev: actual.st_dev as u64,
        ino: actual.st_ino,
    }))
}

#[cfg(unix)]
fn file_identities_match(actual: &FileIdentity, expected: &FileIdentity) -> bool {
    actual.dev == expected.dev && actual.ino == expected.ino
}

#[cfg(windows)]
fn temporary_store_identity_matches(temp: &TemporaryStoreFile, expected: &FileIdentity) -> bool {
    open_file_at_cap_dir_without_hardlink_check(
        &temp.parent,
        &temp.temp_name,
        NonUnixSecureOpenMode::ReadOnly,
    )
    .map_err(|error| std::io::Error::other(error.to_string()))
    .and_then(|file| file_identity_from_file(&file))
    .map(|actual| {
        actual.volume_serial_number == expected.volume_serial_number
            && actual.file_index == expected.file_index
    })
    .unwrap_or(false)
}

#[cfg(windows)]
fn file_identities_match(actual: &FileIdentity, expected: &FileIdentity) -> bool {
    actual.volume_serial_number == expected.volume_serial_number
        && actual.file_index == expected.file_index
}

#[cfg(not(any(unix, windows)))]
fn temporary_store_identity_matches(temp: &TemporaryStoreFile, expected: &FileIdentity) -> bool {
    let _ = (temp, expected);
    false
}

#[cfg(not(any(unix, windows)))]
fn file_identities_match(actual: &FileIdentity, expected: &FileIdentity) -> bool {
    let _ = (actual, expected);
    false
}

struct TemporaryStoreFile {
    file: OpenOptionsFile,
    #[cfg(unix)]
    parent: OpenOptionsFile,
    #[cfg(not(unix))]
    parent: cap_std::fs::Dir,
    #[cfg(unix)]
    temp_name: OsString,
    #[cfg(not(unix))]
    temp_name: OsString,
    #[cfg(unix)]
    target_name: OsString,
    #[cfg(not(unix))]
    target_name: OsString,
}

#[cfg(unix)]
fn create_new_temporary_store_file(path: &Path) -> Result<TemporaryStoreFile, CoreError> {
    let (parent, target_name) = secure_parent_dir_and_leaf(path, true)?;
    for _ in 0..16 {
        let temp_name = temporary_store_name(&target_name);
        match open_leaf_at(&parent, &temp_name, SecureOpenMode::CreateNewWrite) {
            Ok(file) => {
                return Ok(TemporaryStoreFile {
                    file,
                    parent,
                    temp_name,
                    target_name,
                });
            }
            Err(error) if error.is_already_exists() => continue,
            Err(error) => return Err(error),
        }
    }
    Err(CoreError::TemporaryNameExhausted(format!(
        "temporary store path for {} already exists",
        path.display()
    )))
}

#[cfg(not(unix))]
fn create_new_temporary_store_file(path: &Path) -> Result<TemporaryStoreFile, CoreError> {
    let (parent, target_name) = secure_cap_parent_dir_and_leaf(path, true)?;
    for _ in 0..16 {
        let temp_name = temporary_store_name(&target_name);
        match create_new_store_file_at_cap_dir(&parent, &temp_name) {
            Ok(file) => {
                return Ok(TemporaryStoreFile {
                    file,
                    parent,
                    temp_name,
                    target_name,
                });
            }
            Err(error) if error.is_already_exists() => continue,
            Err(error) => return Err(error),
        }
    }
    Err(CoreError::TemporaryNameExhausted(format!(
        "temporary store path for {} already exists",
        path.display()
    )))
}

#[cfg(unix)]
fn temporary_store_name(file_name: &OsStr) -> OsString {
    let counter = TEMP_STORE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut name = Vec::new();
    name.push(b'.');
    name.extend_from_slice(file_name.as_bytes());
    name.extend_from_slice(format!(".create-{}-{counter}.tmp", std::process::id()).as_bytes());
    OsString::from_vec(name)
}

#[cfg(not(unix))]
fn temporary_store_name(file_name: &OsStr) -> OsString {
    let counter = TEMP_STORE_COUNTER.fetch_add(1, Ordering::Relaxed);
    OsString::from(format!(
        ".{file_name}.create-{}-{counter}.tmp",
        std::process::id(),
        file_name = file_name.to_string_lossy()
    ))
}

fn publish_temporary_store_file(temp: &TemporaryStoreFile) -> Result<OpenOptionsFile, CoreError> {
    let expected_identity = file_identity_from_file(&temp.file)?;
    if !temporary_store_identity_matches(temp, &expected_identity) {
        return Err(CoreError::Io(
            "temporary store file identity did not match initialized temp file".to_owned(),
        ));
    }
    #[cfg(not(unix))]
    {
        temp.parent
            .hard_link(
                Path::new(&temp.temp_name),
                &temp.parent,
                Path::new(&temp.target_name),
            )
            .map_err(CoreError::from)?;
        let publish_result = (|| {
            sync_cap_dir(&temp.parent)?;
            if !temporary_target_identity_matches(temp, &expected_identity) {
                return Err(CoreError::InvalidManifest(
                    "published store target identity changed before open".to_owned(),
                ));
            }
            let mut file = open_file_at_cap_dir(
                &temp.parent,
                &temp.target_name,
                NonUnixSecureOpenMode::ReadWrite,
            )?;
            let actual_identity = file_identity_from_file(&file)?;
            if !file_identities_match(&actual_identity, &expected_identity) {
                return Err(CoreError::InvalidManifest(
                    "published store target identity changed before open".to_owned(),
                ));
            }
            file.seek(SeekFrom::Start(0))?;
            Ok(file)
        })();
        match publish_result {
            Ok(file) => Ok(file),
            Err(error) => {
                let _ = remove_published_target_file(temp, &expected_identity);
                Err(error)
            }
        }
    }
    #[cfg(unix)]
    {
        let temp_name = nul_terminated_name(&temp.temp_name)?;
        let target_name = nul_terminated_name(&temp.target_name)?;
        let result = unsafe {
            libc::linkat(
                temp.parent.as_raw_fd(),
                temp_name.as_ptr().cast(),
                temp.parent.as_raw_fd(),
                target_name.as_ptr().cast(),
                0,
            )
        };
        if result != 0 {
            return Err(CoreError::from(std::io::Error::last_os_error()));
        }
        let publish_result = (|| {
            temp.parent.sync_all()?;
            let mut file =
                open_leaf_at(&temp.parent, &temp.target_name, SecureOpenMode::ReadWrite)?;
            let actual_identity = file_identity_from_file(&file)?;
            if !file_identities_match(&actual_identity, &expected_identity) {
                return Err(CoreError::InvalidManifest(
                    "published store target identity changed before open".to_owned(),
                ));
            }
            file.seek(SeekFrom::Start(0))?;
            Ok(file)
        })();
        match publish_result {
            Ok(file) => Ok(file),
            Err(error) => {
                let _ = remove_published_target_file(temp, &expected_identity);
                Err(error)
            }
        }
    }
}

#[cfg(not(unix))]
fn temporary_target_identity_matches(temp: &TemporaryStoreFile, expected: &FileIdentity) -> bool {
    open_file_at_cap_dir_without_hardlink_check(
        &temp.parent,
        &temp.target_name,
        NonUnixSecureOpenMode::ReadOnly,
    )
    .map_err(|error| std::io::Error::other(error.to_string()))
    .and_then(|file| file_identity_from_file(&file))
    .map(|actual| file_identities_match(&actual, expected))
    .unwrap_or(false)
}

fn remove_published_target_file(
    temp: &TemporaryStoreFile,
    expected_identity: &FileIdentity,
) -> Result<(), CoreError> {
    #[cfg(unix)]
    {
        if !child_identity_matches(&temp.parent, &temp.target_name, expected_identity) {
            return Ok(());
        }
        let target_name = nul_terminated_name(&temp.target_name)?;
        let result =
            unsafe { libc::unlinkat(temp.parent.as_raw_fd(), target_name.as_ptr().cast(), 0) };
        match result {
            0 => {
                let _ = temp.parent.sync_all();
                Ok(())
            }
            _ => {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(CoreError::from(error))
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let Ok(target) = open_file_at_cap_dir_without_hardlink_check(
            &temp.parent,
            &temp.target_name,
            NonUnixSecureOpenMode::ReadOnly,
        ) else {
            return Ok(());
        };
        let actual_identity = file_identity_from_file(&target)?;
        if !file_identities_match(&actual_identity, expected_identity) {
            return Ok(());
        }
        temp.parent.remove_file(Path::new(&temp.target_name))?;
        sync_cap_dir(&temp.parent)
    }
}

fn verify_path_still_matches_file(
    path: &Path,
    expected_identity: &FileIdentity,
) -> Result<(), CoreError> {
    let file = open_store_file_read(path)?;
    let actual_identity = file_identity_from_file(&file)?;
    if file_identities_match(&actual_identity, expected_identity) {
        Ok(())
    } else {
        Err(CoreError::InvalidManifest(
            "store path changed during publish".to_owned(),
        ))
    }
}

fn verify_requested_path_generation(
    path: &Path,
    expected_identity: &FileIdentity,
    expected_generation: u64,
) -> Result<(), CoreError> {
    let mut file = open_store_file_read(path)?;
    let actual_identity = file_identity_from_file(&file)?;
    if !file_identities_match(&actual_identity, expected_identity) {
        return Err(CoreError::InvalidManifest(
            "store path changed during publish".to_owned(),
        ));
    }
    let opened = open_store_from_file(&mut file, SegmentValidation::Full)?;
    if opened.manifest.generation != expected_generation {
        return Err(CoreError::InvalidManifest(
            "published generation was not active at requested store path".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn set_create_empty_store_fail_hook(hook: impl FnOnce() -> CoreError + Send + 'static) {
    let current_thread = std::thread::current().id();
    let mut guard = CREATE_EMPTY_STORE_FAIL_HOOKS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("create empty store fail hook mutex poisoned");
    guard.retain(|(thread_id, _)| *thread_id != current_thread);
    guard.push((current_thread, Box::new(hook)));
}

#[cfg(test)]
fn run_create_empty_store_fail_hook() -> Result<(), CoreError> {
    let hook = {
        let mut guard = CREATE_EMPTY_STORE_FAIL_HOOKS
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .expect("create empty store fail hook mutex poisoned");
        guard
            .iter()
            .position(|(thread_id, _)| *thread_id == std::thread::current().id())
            .map(|index| guard.swap_remove(index).1)
    };
    if let Some(hook) = hook {
        return Err(hook());
    }
    Ok(())
}

#[cfg(test)]
fn set_create_empty_store_pre_publish_hook(
    hook: impl FnOnce() -> Result<(), CoreError> + Send + 'static,
) {
    let current_thread = std::thread::current().id();
    let mut guard = CREATE_EMPTY_STORE_PRE_PUBLISH_HOOKS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("create empty store pre-publish hook mutex poisoned");
    guard.retain(|(thread_id, _)| *thread_id != current_thread);
    guard.push((current_thread, Box::new(hook)));
}

#[cfg(test)]
fn run_create_empty_store_pre_publish_hook() -> Result<(), CoreError> {
    let Some(hooks) = CREATE_EMPTY_STORE_PRE_PUBLISH_HOOKS.get() else {
        return Ok(());
    };
    let current_thread = std::thread::current().id();
    let hook = {
        let mut guard = hooks
            .lock()
            .expect("create empty store pre-publish hook mutex poisoned");
        let Some(index) = guard
            .iter()
            .position(|(thread_id, _)| *thread_id == current_thread)
        else {
            return Ok(());
        };
        guard.swap_remove(index).1
    };
    hook()
}

#[cfg(not(test))]
fn run_create_empty_store_pre_publish_hook() -> Result<(), CoreError> {
    Ok(())
}

fn try_lock_exclusive_for_publish(file: &std::fs::File) -> Result<(), CoreError> {
    file.try_lock_exclusive().map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            CoreError::PublishPreconditionFailed(STORE_PUBLISH_LOCK_BUSY_MESSAGE.to_owned())
        } else {
            CoreError::from(error)
        }
    })
}

pub fn open_store(path: &Path) -> Result<OpenedStore, CoreError> {
    let mut file = open_store_file_read(path)?;
    open_store_from_file(&mut file, SegmentValidation::Full)
}

pub fn open_store_shallow(path: &Path) -> Result<OpenedStore, CoreError> {
    let mut file = open_store_file_read(path)?;
    open_store_from_file(&mut file, SegmentValidation::Shallow)
}

pub fn open_file_read_no_symlinks(path: &Path) -> Result<std::fs::File, CoreError> {
    open_store_file_read(path)
}

pub fn open_file_readwrite_no_symlinks(path: &Path) -> Result<std::fs::File, CoreError> {
    open_store_file_readwrite(path)
}

pub fn read_file_no_symlinks(path: &Path) -> Result<Vec<u8>, CoreError> {
    let mut file = open_file_read_no_symlinks(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[derive(Clone, Copy)]
enum SegmentValidation {
    Full,
    Shallow,
}

fn open_store_from_file(
    file: &mut OpenOptionsFile,
    segment_validation: SegmentValidation,
) -> Result<OpenedStore, CoreError> {
    let file_length = file.metadata()?.len();
    if file_length < (SUPERBLOCK_SIZE * 2) as u64 {
        return Err(CoreError::UnexpectedLength {
            context: "store",
            expected_at_least: SUPERBLOCK_SIZE * 2,
            actual: file_length as usize,
        });
    }
    file.seek(SeekFrom::Start(0))?;
    let mut superblock_bytes = [0u8; SUPERBLOCK_SIZE * 2];
    file.read_exact(&mut superblock_bytes)?;

    let candidate_a = Superblock::decode(&superblock_bytes[..SUPERBLOCK_SIZE])
        .ok()
        .map(|superblock| SuperblockCandidate {
            superblock,
            offset: 0,
        });
    let candidate_b = Superblock::decode(&superblock_bytes[SUPERBLOCK_SIZE..SUPERBLOCK_SIZE * 2])
        .ok()
        .map(|superblock| SuperblockCandidate {
            superblock,
            offset: SUPERBLOCK_SIZE as u64,
        });
    let Some(candidates) = ordered_superblock_candidates(candidate_a, candidate_b) else {
        return Err(CoreError::NoValidSuperblock);
    };

    let highest_observed_generation = candidates
        .iter()
        .map(|candidate| candidate.superblock.generation)
        .max()
        .unwrap_or(0);
    let mut last_error = None;
    for candidate in candidates {
        match open_store_from_superblock(
            file,
            file_length,
            candidate,
            highest_observed_generation,
            segment_validation,
        ) {
            Ok(opened) => return Ok(opened),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or(CoreError::NoValidSuperblock))
}

pub fn publish_segment(
    path: &Path,
    pending: PendingSegmentDescriptor,
    object_bytes: &[u8],
) -> Result<OpenedStore, CoreError> {
    publish_segments(
        path,
        vec![PendingSegmentWrite {
            descriptor: pending,
            object_bytes: object_bytes.to_vec(),
        }],
    )
}

pub fn publish_segments(
    path: &Path,
    pending_segments: Vec<PendingSegmentWrite>,
) -> Result<OpenedStore, CoreError> {
    let published_families = pending_segments
        .iter()
        .map(|segment| segment.descriptor.family)
        .collect::<Vec<_>>();
    publish_segments_with_precondition(path, pending_segments, |manifest| {
        if manifest
            .segments
            .iter()
            .any(|segment| published_families.contains(&segment.family))
        {
            return Err(CoreError::PublishPreconditionFailed(
                "publish_segments refuses to replace an existing segment family without an explicit precondition"
                    .to_owned(),
            ));
        }
        Ok(())
    })
}

pub fn publish_segments_with_precondition<F>(
    path: &Path,
    pending_segments: Vec<PendingSegmentWrite>,
    precondition: F,
) -> Result<OpenedStore, CoreError>
where
    F: FnOnce(&ActiveManifest) -> Result<(), CoreError>,
{
    publish_segments_replacing_families_with_precondition(path, pending_segments, &[], precondition)
}

pub fn publish_segments_replacing_families_with_precondition<F>(
    path: &Path,
    pending_segments: Vec<PendingSegmentWrite>,
    removed_families: &[SegmentKind],
    precondition: F,
) -> Result<OpenedStore, CoreError>
where
    F: FnOnce(&ActiveManifest) -> Result<(), CoreError>,
{
    if pending_segments.is_empty() {
        return Err(CoreError::InvalidManifest(
            "publish_segments requires at least one pending segment".to_owned(),
        ));
    }
    validate_pending_segments(&pending_segments)?;

    let mut file = open_store_file_readwrite(path)?;
    let opened_file_identity = file_identity_from_file(&file)?;
    try_lock_exclusive_for_publish(&file)?;
    let original_len = file.metadata()?.len();
    let mut commit_started = false;
    let mut commit_synced = false;
    let mut commit_generation = None;

    let publish_result = (|| {
        verify_path_still_matches_file(path, &opened_file_identity)?;
        let opened = open_store_from_file(&mut file, SegmentValidation::Full)?;
        precondition(&opened.manifest)?;
        verify_path_still_matches_file(path, &opened_file_identity)?;
        let publish_superblock_offset = inactive_superblock_offset(opened.active_superblock_offset);
        let new_generation = next_generation_for_superblock_offset(
            opened.highest_observed_generation,
            publish_superblock_offset,
        )?;
        let published_families = pending_segments
            .iter()
            .map(|segment| segment.descriptor.family)
            .collect::<Vec<_>>();
        let mut segments = opened
            .manifest
            .segments
            .into_iter()
            .filter(|segment| {
                !published_families.contains(&segment.family)
                    && !removed_families.contains(&segment.family)
            })
            .collect::<Vec<_>>();
        let published_segment_count = segments
            .len()
            .checked_add(pending_segments.len())
            .ok_or_else(|| {
                CoreError::InvalidManifest("manifest segment count overflow".to_owned())
            })?;
        if published_segment_count > MAX_SEGMENT_DESCRIPTOR_COUNT {
            return Err(CoreError::InvalidManifest(format!(
                "manifest segment count exceeds maximum {MAX_SEGMENT_DESCRIPTOR_COUNT}"
            )));
        }
        validate_active_segment_object_byte_budget(&segments, &pending_segments)?;
        for pending_segment in pending_segments {
            let object_type = object_type_for_family(pending_segment.descriptor.family);
            let appended_object = append_object(
                &mut file,
                object_type,
                new_generation,
                DEFAULT_OBJECT_ALIGNMENT,
                &pending_segment.object_bytes,
            )?;
            let published_segment = pending_segment.descriptor.publish(
                appended_object.offset,
                appended_object.length,
                new_generation,
                appended_object.payload_checksum,
            );
            segments.push(published_segment);
        }
        segments.sort_by_key(|segment| {
            (
                segment.family.as_code(),
                segment.object_offset,
                segment.segment_generation,
            )
        });
        validate_segments(new_generation, &segments)?;

        let manifest = ActiveManifest {
            generation: new_generation,
            segments,
        };
        let manifest_bytes = manifest.encode()?;
        let appended_manifest = append_object(
            &mut file,
            ObjectType::Manifest,
            new_generation,
            DEFAULT_OBJECT_ALIGNMENT,
            &manifest_bytes,
        )?;
        file.flush()?;
        file.sync_all()?;

        let superblock = Superblock::new(
            new_generation,
            appended_manifest.offset,
            appended_manifest.length as u32,
            ActiveManifest::checksum(&manifest_bytes),
        );
        commit_started = true;
        commit_generation = Some(new_generation);
        file.seek(SeekFrom::Start(publish_superblock_offset))?;
        file.write_all(&superblock.encode())?;
        file.flush()?;
        file.sync_all()?;
        commit_synced = true;

        let opened = open_store_from_file(&mut file, SegmentValidation::Full)?;
        let mut path_file = open_store_file_read(path)?;
        let path_identity = file_identity_from_file(&path_file)?;
        if !file_identities_match(&path_identity, &opened_file_identity) {
            return Err(CoreError::InvalidManifest(
                "store path changed during publish".to_owned(),
            ));
        }
        let path_opened = open_store_from_file(&mut path_file, SegmentValidation::Full)?;
        if path_opened.manifest.generation != opened.manifest.generation {
            return Err(CoreError::InvalidManifest(
                "published generation was not active at requested store path".to_owned(),
            ));
        }
        Ok(opened)
    })();

    match publish_result {
        Ok(opened) if !commit_started || Some(opened.manifest.generation) == commit_generation => {
            Ok(opened)
        }
        Ok(opened) => Err(CoreError::InvalidManifest(format!(
            "published generation {:?} was not active after commit; reopened generation {}",
            commit_generation, opened.manifest.generation
        ))),
        Err(error) if commit_started && commit_synced => {
            if let Some(committed_generation) = commit_generation {
                if let Ok(opened) = open_store_from_file(&mut file, SegmentValidation::Full) {
                    if opened.manifest.generation == committed_generation
                        && verify_requested_path_generation(
                            path,
                            &opened_file_identity,
                            committed_generation,
                        )
                        .is_ok()
                    {
                        return Ok(opened);
                    }
                }
            }
            Err(error)
        }
        Err(error) if commit_started => Err(error),
        Err(error) => {
            if file.metadata()?.len() > original_len {
                file.set_len(original_len)?;
                file.seek(SeekFrom::Start(original_len))?;
                file.sync_all()?;
            }
            Err(error)
        }
    }
}

pub fn read_segment_object(
    path: &Path,
    descriptor: &SegmentDescriptor,
) -> Result<Vec<u8>, CoreError> {
    Ok(map_segment_object(path, descriptor)?.to_vec())
}

/// Reads a persisted segment object and returns a validated payload view.
///
/// Safety invariant: callers only receive a shared payload view after this function has
/// validated that the descriptor range stays within the current file length and that the payload
/// checksum matches the manifest descriptor. The object bytes are owned by the returned value, so
/// callers do not hold file descriptors or mmap handles after this function returns.
pub fn map_segment_object(
    path: &Path,
    descriptor: &SegmentDescriptor,
) -> Result<SegmentObject, CoreError> {
    map_segment_object_with_payload_validation(path, descriptor, true)
}

/// Reads a persisted segment object and returns a payload view after validating only descriptor
/// bounds and the object header checksum contract.
///
/// This is intended for already-opened read-only snapshot paths that have explicitly chosen fast
/// header validation over rescanning large immutable payloads.
pub fn map_segment_object_shallow(
    path: &Path,
    descriptor: &SegmentDescriptor,
) -> Result<SegmentObject, CoreError> {
    map_segment_object_with_payload_validation(path, descriptor, false)
}

fn map_segment_object_with_payload_validation(
    path: &Path,
    descriptor: &SegmentDescriptor,
    validate_payload: bool,
) -> Result<SegmentObject, CoreError> {
    let mut file = open_store_file_read(path)?;
    let file_length = file.metadata()?.len();
    let object_end = descriptor
        .object_offset
        .checked_add(descriptor.object_length)
        .ok_or_else(|| CoreError::InvalidManifest("segment object range overflow".to_owned()))?;
    if object_end > file_length {
        return Err(CoreError::InvalidManifest(
            "segment object range extends past end of file".to_owned(),
        ));
    }
    if !descriptor
        .object_offset
        .is_multiple_of(DEFAULT_OBJECT_ALIGNMENT)
    {
        return Err(CoreError::InvalidManifest(
            "segment object offset must use store object alignment".to_owned(),
        ));
    }
    if descriptor.object_length > MAX_SEGMENT_OBJECT_LENGTH {
        return Err(CoreError::InvalidManifest(
            "segment object length exceeds maximum supported segment size".to_owned(),
        ));
    }
    let mut object_header = [0u8; OBJECT_HEADER_LENGTH];
    file.seek(SeekFrom::Start(descriptor.object_offset))?;
    file.read_exact(&mut object_header)?;
    validate_object_header_length(
        &object_header,
        object_type_for_family(descriptor.family),
        descriptor.segment_generation,
        descriptor.object_length,
    )?;

    let object_length = usize::try_from(descriptor.object_length).map_err(|_| {
        CoreError::InvalidManifest("segment object length exceeds addressable memory".to_owned())
    })?;
    let map_offset = align_down(descriptor.object_offset, mmap_allocation_granularity());
    let map_prefix = usize::try_from(descriptor.object_offset - map_offset).map_err(|_| {
        CoreError::InvalidManifest(
            "segment object mmap prefix exceeds addressable memory".to_owned(),
        )
    })?;
    let map_length = map_prefix.checked_add(object_length).ok_or_else(|| {
        CoreError::InvalidManifest("segment object mmap range overflow".to_owned())
    })?;
    let map_end = map_offset.checked_add(map_length as u64).ok_or_else(|| {
        CoreError::InvalidManifest("segment object mmap range overflow".to_owned())
    })?;
    if map_end > file_length {
        return Err(CoreError::InvalidManifest(
            "segment object range extends past end of file".to_owned(),
        ));
    }
    // SAFETY: the mmap is read-only, aligned to the host allocation granularity, and callers only
    // receive slices after descriptor bounds and the header checksum contract are validated. Store
    // writers must append immutable objects and publish by atomically switching superblocks.
    let mapped = unsafe {
        MmapOptions::new()
            .offset(map_offset)
            .len(map_length)
            .map(&file)?
    };
    let object_end = map_prefix + object_length;
    let object_bytes = &mapped[map_prefix..object_end];
    let decoded = if validate_payload {
        decode_object_payload(
            object_bytes,
            object_type_for_family(descriptor.family),
            descriptor.segment_generation,
        )?
    } else {
        decode_object_payload_header(
            object_bytes,
            object_type_for_family(descriptor.family),
            descriptor.segment_generation,
        )?
    };
    if decoded.payload_checksum != descriptor.object_checksum {
        return Err(CoreError::ChecksumMismatch {
            context: "segment object",
        });
    }
    Ok(SegmentObject {
        backing: SegmentObjectBacking::Mapped(mapped),
        payload_range: map_prefix + decoded.payload_range.start
            ..map_prefix + decoded.payload_range.end,
    })
}

fn ordered_superblock_candidates(
    left: Option<SuperblockCandidate>,
    right: Option<SuperblockCandidate>,
) -> Option<Vec<SuperblockCandidate>> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if left.superblock.generation >= right.superblock.generation {
                Some(vec![left, right])
            } else {
                Some(vec![right, left])
            }
        }
        (Some(left), None) => Some(vec![left]),
        (None, Some(right)) => Some(vec![right]),
        (None, None) => None,
    }
}

fn inactive_superblock_offset(active_offset: u64) -> u64 {
    if active_offset == 0 {
        SUPERBLOCK_SIZE as u64
    } else {
        0
    }
}

fn next_generation_for_superblock_offset(
    highest_observed_generation: u64,
    superblock_offset: u64,
) -> Result<u64, CoreError> {
    let wants_even = superblock_offset == 0;
    let mut generation = highest_observed_generation
        .checked_add(1)
        .ok_or_else(|| CoreError::InvalidManifest("manifest generation overflow".to_owned()))?;
    if generation.is_multiple_of(2) != wants_even {
        generation = generation
            .checked_add(1)
            .ok_or_else(|| CoreError::InvalidManifest("manifest generation overflow".to_owned()))?;
    }
    Ok(generation)
}

#[derive(Clone)]
struct SuperblockCandidate {
    superblock: Superblock,
    offset: u64,
}

fn open_store_from_superblock(
    file: &mut OpenOptionsFile,
    file_length: u64,
    active_candidate: SuperblockCandidate,
    highest_observed_generation: u64,
    segment_validation: SegmentValidation,
) -> Result<OpenedStore, CoreError> {
    let active_superblock = active_candidate.superblock;
    let manifest_offset = active_superblock.active_manifest_offset;
    let manifest_length = active_superblock.active_manifest_length as u64;
    if manifest_length > MAX_MANIFEST_OBJECT_LENGTH as u64 {
        return Err(CoreError::InvalidManifest(format!(
            "active manifest object length exceeds supported bound: {manifest_length} > {MAX_MANIFEST_OBJECT_LENGTH}"
        )));
    }
    let manifest_end = manifest_offset
        .checked_add(manifest_length)
        .ok_or_else(|| CoreError::InvalidManifest("manifest offset overflow".to_owned()))?;

    if manifest_end > file_length {
        return Err(CoreError::InvalidManifest(
            "manifest range extends past end of file".to_owned(),
        ));
    }

    let mut manifest_object = vec![0u8; manifest_length as usize];
    file.seek(SeekFrom::Start(manifest_offset))?;
    file.read_exact(&mut manifest_object)?;
    let decoded_manifest = decode_object_payload(
        &manifest_object,
        ObjectType::Manifest,
        active_superblock.generation,
    )?;
    if decoded_manifest.payload_checksum != active_superblock.manifest_checksum {
        return Err(CoreError::ChecksumMismatch {
            context: "manifest",
        });
    }

    let manifest_bytes = &manifest_object[decoded_manifest.payload_range.clone()];
    let manifest = ActiveManifest::decode(manifest_bytes)?;
    if manifest.generation != active_superblock.generation {
        return Err(CoreError::InvalidManifest(
            "manifest generation does not match active superblock".to_owned(),
        ));
    }
    if matches!(segment_validation, SegmentValidation::Full) {
        validate_active_segment_objects(file, file_length, &manifest.segments)?;
    }

    Ok(OpenedStore {
        superblock: active_superblock,
        manifest,
        highest_observed_generation,
        active_superblock_offset: active_candidate.offset,
    })
}

fn validate_active_segment_objects(
    file: &mut OpenOptionsFile,
    file_length: u64,
    segments: &[SegmentDescriptor],
) -> Result<(), CoreError> {
    for segment in segments {
        validate_active_segment_object(file, file_length, segment)?;
    }
    Ok(())
}

fn validate_active_segment_object(
    file: &mut OpenOptionsFile,
    file_length: u64,
    descriptor: &SegmentDescriptor,
) -> Result<(), CoreError> {
    let object_end = descriptor
        .object_offset
        .checked_add(descriptor.object_length)
        .ok_or_else(|| CoreError::InvalidManifest("segment object range overflow".to_owned()))?;
    if object_end > file_length {
        return Err(CoreError::InvalidManifest(
            "segment object range extends past end of file".to_owned(),
        ));
    }
    if !descriptor
        .object_offset
        .is_multiple_of(DEFAULT_OBJECT_ALIGNMENT)
    {
        return Err(CoreError::InvalidManifest(
            "segment object offset must use store object alignment".to_owned(),
        ));
    }
    if descriptor.object_length > MAX_SEGMENT_OBJECT_LENGTH {
        return Err(CoreError::InvalidManifest(
            "segment object length exceeds maximum supported segment size".to_owned(),
        ));
    }

    let mut object_header = [0u8; OBJECT_HEADER_LENGTH];
    file.seek(SeekFrom::Start(descriptor.object_offset))?;
    file.read_exact(&mut object_header)?;
    validate_object_header_length(
        &object_header,
        object_type_for_family(descriptor.family),
        descriptor.segment_generation,
        descriptor.object_length,
    )?;
    let mut payload_checksum = [0u8; 32];
    payload_checksum.copy_from_slice(&object_header[32..64]);
    if payload_checksum != descriptor.object_checksum {
        return Err(CoreError::ChecksumMismatch {
            context: "segment object",
        });
    }

    let mut hasher = Sha256::new();
    let mut remaining = descriptor
        .object_length
        .checked_sub(OBJECT_HEADER_LENGTH as u64)
        .ok_or_else(|| CoreError::InvalidManifest("segment object length underflow".to_owned()))?;
    let mut buffer = [0u8; 64 * 1024];
    while remaining > 0 {
        let chunk_len = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..chunk_len])?;
        hasher.update(&buffer[..chunk_len]);
        remaining -= chunk_len as u64;
    }
    let digest = hasher.finalize();
    let mut actual_checksum = [0u8; 32];
    actual_checksum.copy_from_slice(&digest);
    if actual_checksum != descriptor.object_checksum {
        return Err(CoreError::ChecksumMismatch {
            context: "segment object",
        });
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(bytes);
    let mut checksum = [0; 32];
    checksum.copy_from_slice(&digest);
    checksum
}

fn align_up(value: u64, alignment: u64) -> Result<u64, CoreError> {
    if alignment == 0 {
        return Ok(value);
    }
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or_else(|| CoreError::InvalidManifest("file offset overflow".to_owned()))
    }
}

fn align_down(value: u64, alignment: u64) -> u64 {
    if alignment == 0 {
        value
    } else {
        value - (value % alignment)
    }
}

fn mmap_allocation_granularity() -> u64 {
    #[cfg(unix)]
    {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size > 0 {
            return page_size as u64;
        }
    }

    default_mmap_allocation_granularity()
}

fn default_mmap_allocation_granularity() -> u64 {
    #[cfg(windows)]
    {
        65_536
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        16_384
    }
    #[cfg(not(any(windows, target_os = "macos", target_os = "ios")))]
    {
        DEFAULT_OBJECT_ALIGNMENT
    }
}

fn write_zero_padding(file: &mut OpenOptionsFile, target_offset: u64) -> Result<(), CoreError> {
    let current_offset = file.seek(SeekFrom::End(0))?;
    if target_offset < current_offset {
        return Err(CoreError::InvalidManifest(
            "target offset moved backwards".to_owned(),
        ));
    }
    let mut padding = target_offset - current_offset;
    if padding > 0 {
        let zeroes = [0u8; DEFAULT_OBJECT_ALIGNMENT as usize];
        while padding > 0 {
            let chunk = padding.min(zeroes.len() as u64);
            file.write_all(&zeroes[..chunk as usize])?;
            padding -= chunk;
        }
    }
    Ok(())
}

type OpenOptionsFile = std::fs::File;

#[cfg(unix)]
fn open_store_file_read(path: &Path) -> Result<OpenOptionsFile, CoreError> {
    open_path_no_symlinks(path, SecureOpenMode::ReadOnly)
}

#[cfg(unix)]
fn open_store_file_readwrite(path: &Path) -> Result<OpenOptionsFile, CoreError> {
    open_path_no_symlinks(path, SecureOpenMode::ReadWrite)
}

#[cfg(not(unix))]
fn open_store_file_read(path: &Path) -> Result<OpenOptionsFile, CoreError> {
    open_path_no_symlinks_nonunix(path, NonUnixSecureOpenMode::ReadOnly)
}

#[cfg(not(unix))]
fn open_store_file_readwrite(path: &Path) -> Result<OpenOptionsFile, CoreError> {
    open_path_no_symlinks_nonunix(path, NonUnixSecureOpenMode::ReadWrite)
}

#[cfg(unix)]
#[derive(Clone, Copy)]
enum SecureOpenMode {
    ReadOnly,
    ReadWrite,
    CreateNewWrite,
}

#[cfg(unix)]
fn open_path_no_symlinks(path: &Path, mode: SecureOpenMode) -> Result<OpenOptionsFile, CoreError> {
    if unix_fd_file_number(path)?.is_some() {
        return Err(CoreError::InvalidManifest(
            "direct file-descriptor store paths are unsupported; use a containing fd directory plus file name".to_owned(),
        ));
    }
    let (dir, leaf) =
        secure_parent_dir_and_leaf(path, matches!(mode, SecureOpenMode::CreateNewWrite))?;
    open_leaf_at(&dir, &leaf, mode)
}

#[cfg(unix)]
fn secure_parent_dir_and_leaf(
    path: &Path,
    create_missing: bool,
) -> Result<(OpenOptionsFile, OsString), CoreError> {
    #[cfg(target_os = "macos")]
    let macos_private_alias_path = macos_private_alias_path(path);
    #[cfg(target_os = "macos")]
    let path = macos_private_alias_path.as_deref().unwrap_or(path);
    if matches!(
        path.components().next_back(),
        Some(std::path::Component::CurDir)
    ) {
        return Err(CoreError::InvalidManifest(
            "store path must include a file name".to_owned(),
        ));
    }
    let (mut dir, normal_components) =
        if let Some(fd_relative) = proc_self_fd_dir_and_relative_components(path)? {
            fd_relative
        } else if path.is_absolute() {
            let mut components = path.components();
            match components.next() {
                Some(std::path::Component::RootDir) => {}
                _ => {
                    return Err(CoreError::InvalidManifest(
                        "absolute path must start at filesystem root".to_owned(),
                    ));
                }
            }
            (
                open_root_dir()?,
                normal_components_from_path(path, components)?,
            )
        } else {
            (
                open_current_dir()?,
                normal_components_from_path(path, path.components())?,
            )
        };

    if normal_components.is_empty() {
        return Err(CoreError::InvalidManifest(
            "store path must include a file name".to_owned(),
        ));
    }
    for (index, name) in normal_components.iter().enumerate() {
        if index + 1 == normal_components.len() {
            return Ok((dir, (*name).to_owned()));
        }
        dir = open_dir_at(&dir, name, create_missing)?;
    }
    unreachable!("non-empty normal component list should return from loop")
}

#[cfg(unix)]
fn unix_fd_file_number(path: &Path) -> Result<Option<libc::c_int>, CoreError> {
    #[cfg(target_os = "linux")]
    if let Some(fd) = linux_proc_self_fd_file_number(path)? {
        return Ok(Some(fd));
    }
    dev_fd_file_number(path)
}

#[cfg(target_os = "linux")]
fn linux_proc_self_fd_file_number(path: &Path) -> Result<Option<libc::c_int>, CoreError> {
    fd_file_number_with_prefix(path, &["proc", "self", "fd"], "/proc/self/fd")
}

#[cfg(unix)]
fn dev_fd_file_number(path: &Path) -> Result<Option<libc::c_int>, CoreError> {
    fd_file_number_with_prefix(path, &["dev", "fd"], "/dev/fd")
}

#[cfg(unix)]
fn fd_file_number_with_prefix(
    path: &Path,
    prefix: &[&str],
    label: &'static str,
) -> Result<Option<libc::c_int>, CoreError> {
    let mut components = path.components();
    if !matches!(components.next(), Some(std::path::Component::RootDir)) {
        return Ok(None);
    }
    for expected in prefix {
        match components.next() {
            Some(std::path::Component::Normal(component)) if component == OsStr::new(expected) => {}
            _ => return Ok(None),
        }
    }
    let Some(std::path::Component::Normal(fd)) = components.next() else {
        return Ok(None);
    };
    if components.next().is_some() {
        return Ok(None);
    }
    let fd = fd.to_str().ok_or_else(|| {
        CoreError::InvalidManifest(format!("{label} component must be valid UTF-8"))
    })?;
    if fd.is_empty() || !fd.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(CoreError::InvalidManifest(format!(
            "{label} component must be a non-negative file descriptor"
        )));
    }
    fd.parse::<libc::c_int>()
        .map(Some)
        .map_err(|_| CoreError::InvalidManifest(format!("{label} component is out of range")))
}

#[cfg(unix)]
fn normal_components_from_path<'a>(
    path: &Path,
    components: impl Iterator<Item = std::path::Component<'a>>,
) -> Result<Vec<&'a OsStr>, CoreError> {
    let mut normal_components = Vec::new();
    for component in components {
        match component {
            std::path::Component::Normal(name) => normal_components.push(name),
            std::path::Component::CurDir => {}
            _ => {
                return Err(CoreError::InvalidManifest(format!(
                    "store path {} must not contain parent or prefix components",
                    path.display()
                )));
            }
        }
    }
    Ok(normal_components)
}

#[cfg(all(unix, target_os = "linux"))]
fn proc_self_fd_dir_and_relative_components(
    path: &Path,
) -> Result<Option<(OpenOptionsFile, Vec<&OsStr>)>, CoreError> {
    let mut components = path.components();
    if !matches!(components.next(), Some(std::path::Component::RootDir)) {
        return Ok(None);
    }
    for expected in ["proc", "self", "fd"] {
        match components.next() {
            Some(std::path::Component::Normal(component)) if component == OsStr::new(expected) => {}
            _ => return Ok(None),
        }
    }
    let Some(std::path::Component::Normal(fd)) = components.next() else {
        return Ok(None);
    };
    let fd = fd.to_str().ok_or_else(|| {
        CoreError::InvalidManifest("/proc/self/fd component must be valid UTF-8".to_owned())
    })?;
    if fd.is_empty() || !fd.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(CoreError::InvalidManifest(
            "/proc/self/fd component must be a non-negative file descriptor".to_owned(),
        ));
    }
    let fd = fd.parse::<libc::c_int>().map_err(|_| {
        CoreError::InvalidManifest("/proc/self/fd component is out of range".to_owned())
    })?;
    Ok(Some((
        duplicate_dir_fd(fd)?,
        normal_components_from_path(path, components)?,
    )))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn proc_self_fd_dir_and_relative_components(
    _path: &Path,
) -> Result<Option<(OpenOptionsFile, Vec<&OsStr>)>, CoreError> {
    Ok(None)
}

#[cfg(all(unix, target_os = "linux"))]
fn duplicate_dir_fd(fd: libc::c_int) -> Result<OpenOptionsFile, CoreError> {
    if fd < 0 {
        return Err(CoreError::InvalidManifest(
            "/proc/self/fd component must be a non-negative file descriptor".to_owned(),
        ));
    }
    let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    file_from_dir_fd(duplicated)
}

#[cfg(all(unix, target_os = "linux"))]
fn file_from_dir_fd(fd: libc::c_int) -> Result<OpenOptionsFile, CoreError> {
    if fd < 0 {
        return Err(CoreError::from(std::io::Error::last_os_error()));
    }
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(CoreError::from(error));
    }
    let stat = unsafe { stat.assume_init() };
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFDIR {
        unsafe {
            libc::close(fd);
        }
        return Err(CoreError::InvalidManifest(
            "/proc/self/fd root must resolve to a directory".to_owned(),
        ));
    }
    Ok(unsafe { OpenOptionsFile::from_raw_fd(fd) })
}

#[cfg(unix)]
fn open_root_dir() -> Result<OpenOptionsFile, CoreError> {
    let fd = unsafe {
        libc::open(
            c"/".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    file_from_fd(fd)
}

#[cfg(unix)]
fn open_current_dir() -> Result<OpenOptionsFile, CoreError> {
    let fd = unsafe {
        libc::open(
            c".".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    file_from_fd(fd)
}

#[cfg(all(unix, target_os = "macos"))]
fn macos_private_alias_path(path: &Path) -> Option<PathBuf> {
    if let Ok(rest) = path.strip_prefix("/tmp") {
        Some(Path::new("/private/tmp").join(rest))
    } else if let Ok(rest) = path.strip_prefix("/var") {
        Some(Path::new("/private/var").join(rest))
    } else {
        None
    }
}

#[cfg(unix)]
fn open_dir_at(
    parent: &OpenOptionsFile,
    name: &OsStr,
    create_missing: bool,
) -> Result<OpenOptionsFile, CoreError> {
    match open_dir_at_existing(parent, name) {
        Ok(dir) => Ok(dir),
        Err(error) if create_missing && error.kind() == std::io::ErrorKind::NotFound => {
            let name = nul_terminated_name(name)?;
            let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr().cast(), 0o700) };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(CoreError::from(error));
                }
            }
            parent.sync_all()?;
            open_dir_at_existing(parent, OsStr::from_bytes(&name[..name.len() - 1]))
                .map_err(CoreError::from)
        }
        Err(error) => Err(CoreError::from(error)),
    }
}

#[cfg(unix)]
fn open_dir_at_existing(
    parent: &OpenOptionsFile,
    name: &OsStr,
) -> Result<OpenOptionsFile, std::io::Error> {
    let name = nul_terminated_name_io(name)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr().cast(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    file_from_fd_io(fd)
}

#[cfg(unix)]
fn open_leaf_at(
    parent: &OpenOptionsFile,
    name: &OsStr,
    mode: SecureOpenMode,
) -> Result<OpenOptionsFile, CoreError> {
    let name = nul_terminated_name(name)?;
    let flags = match mode {
        SecureOpenMode::ReadOnly => {
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK
        }
        SecureOpenMode::ReadWrite => {
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK
        }
        SecureOpenMode::CreateNewWrite => {
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK
        }
    };
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr().cast(), flags, 0o600) };
    let file = file_from_regular_fd_at(parent, OsStr::from_bytes(&name[..name.len() - 1]), fd)?;
    if matches!(mode, SecureOpenMode::CreateNewWrite) {
        parent.sync_all()?;
    }
    Ok(file)
}

#[cfg(unix)]
fn cleanup_temporary_store_links_at(
    parent: &OpenOptionsFile,
    name: &OsStr,
    target_stat: &libc::stat,
) -> Result<(), CoreError> {
    let allow_ascii_case_match =
        !directory_contains_exact_store_entry_at(parent, name, target_stat)?;
    let dir_fd = open_directory_cursor_fd_at(parent)?;
    let dir = unsafe { libc::fdopendir(dir_fd) };
    if dir.is_null() {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(dir_fd);
        }
        return Err(CoreError::from(error));
    }
    loop {
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
            break;
        }
        let entry_name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if !is_recoverable_temporary_store_link(entry_name, name.as_bytes(), allow_ascii_case_match)
        {
            continue;
        }
        let mut entry_name_nul = entry_name.to_vec();
        entry_name_nul.push(0);
        let mut entry_stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let stat_result = unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                entry_name_nul.as_ptr().cast(),
                entry_stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if stat_result != 0 {
            continue;
        }
        let entry_stat = unsafe { entry_stat.assume_init() };
        if entry_stat.st_dev == target_stat.st_dev
            && entry_stat.st_ino == target_stat.st_ino
            && (entry_stat.st_mode & libc::S_IFMT) == libc::S_IFREG
        {
            let _ =
                unsafe { libc::unlinkat(parent.as_raw_fd(), entry_name_nul.as_ptr().cast(), 0) };
        }
    }
    let close_result = unsafe { libc::closedir(dir) };
    if close_result != 0 {
        return Err(CoreError::from(std::io::Error::last_os_error()));
    }
    parent.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn is_recoverable_temporary_store_link(
    entry_name: &[u8],
    target_name: &[u8],
    allow_ascii_case_match: bool,
) -> bool {
    entry_name != target_name
        && is_core_temporary_store_link(entry_name, target_name, allow_ascii_case_match)
}

#[cfg(unix)]
fn is_core_temporary_store_link(
    entry_name: &[u8],
    target_name: &[u8],
    allow_ascii_case_match: bool,
) -> bool {
    let Some(rest) = entry_name.strip_prefix(b".") else {
        return false;
    };
    let Some(rest) = rest.strip_suffix(b".tmp") else {
        return false;
    };
    if rest.len() <= target_name.len() + b".create-".len() {
        return false;
    }
    let candidate_target = &rest[..target_name.len()];
    let target_matches = candidate_target == target_name
        || (allow_ascii_case_match && candidate_target.eq_ignore_ascii_case(target_name));
    if !target_matches {
        return false;
    }
    let Some(suffix) = rest[target_name.len()..].strip_prefix(b".create-") else {
        return false;
    };
    is_pid_counter_suffix(suffix)
}

#[cfg(unix)]
fn is_pid_counter_suffix(suffix: &[u8]) -> bool {
    is_pid_counter_suffix_bytes(suffix)
}

fn is_pid_counter_suffix_bytes(suffix: &[u8]) -> bool {
    let Some(separator_index) = suffix.iter().position(|byte| *byte == b'-') else {
        return false;
    };
    let pid = &suffix[..separator_index];
    let counter = &suffix[separator_index + 1..];
    !pid.is_empty()
        && !counter.is_empty()
        && pid.iter().all(u8::is_ascii_digit)
        && counter.iter().all(u8::is_ascii_digit)
}

#[cfg(unix)]
fn directory_contains_exact_store_entry_at(
    parent: &OpenOptionsFile,
    name: &OsStr,
    target_stat: &libc::stat,
) -> Result<bool, CoreError> {
    let target_name = name.as_bytes();
    let dir_fd = open_directory_cursor_fd_at(parent)?;
    let dir = unsafe { libc::fdopendir(dir_fd) };
    if dir.is_null() {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(dir_fd);
        }
        return Err(CoreError::from(error));
    }
    let mut found = false;
    loop {
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
            break;
        }
        let entry_name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if entry_name != target_name {
            continue;
        }
        let mut entry_name_nul = entry_name.to_vec();
        entry_name_nul.push(0);
        let mut entry_stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let stat_result = unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                entry_name_nul.as_ptr().cast(),
                entry_stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if stat_result != 0 {
            continue;
        }
        let entry_stat = unsafe { entry_stat.assume_init() };
        found = entry_stat.st_dev == target_stat.st_dev
            && entry_stat.st_ino == target_stat.st_ino
            && (entry_stat.st_mode & libc::S_IFMT) == libc::S_IFREG;
        break;
    }
    let close_result = unsafe { libc::closedir(dir) };
    if close_result != 0 {
        return Err(CoreError::from(std::io::Error::last_os_error()));
    }
    Ok(found)
}

#[cfg(unix)]
fn open_directory_cursor_fd_at(parent: &OpenOptionsFile) -> Result<libc::c_int, CoreError> {
    let dot = b".\0";
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            dot.as_ptr().cast(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(CoreError::from(std::io::Error::last_os_error()));
    }
    Ok(fd)
}

#[cfg(unix)]
fn nul_terminated_name(name: &OsStr) -> Result<Vec<u8>, CoreError> {
    nul_terminated_name_io(name).map_err(CoreError::from)
}

#[cfg(unix)]
fn nul_terminated_name_io(name: &OsStr) -> Result<Vec<u8>, std::io::Error> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path component must be non-empty and must not contain NUL",
        ));
    }
    let mut name = bytes.to_vec();
    name.push(0);
    Ok(name)
}

#[cfg(unix)]
fn file_from_fd(fd: libc::c_int) -> Result<OpenOptionsFile, CoreError> {
    file_from_fd_io(fd).map_err(CoreError::from)
}

#[cfg(unix)]
fn file_from_regular_fd_at(
    parent: &OpenOptionsFile,
    name: &OsStr,
    fd: libc::c_int,
) -> Result<OpenOptionsFile, CoreError> {
    if fd < 0 {
        return Err(CoreError::from(std::io::Error::last_os_error()));
    }
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(CoreError::from(error));
    }
    let mut stat = unsafe { stat.assume_init() };
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
        unsafe {
            libc::close(fd);
        }
        return Err(CoreError::InvalidManifest(
            "store path must resolve to a regular file".to_owned(),
        ));
    }
    let file = unsafe { OpenOptionsFile::from_raw_fd(fd) };
    if stat.st_nlink != 1 {
        cleanup_temporary_store_links_at(parent, name, &stat)?;
        let result = unsafe { libc::fstat(file.as_raw_fd(), &mut stat) };
        if result != 0 {
            return Err(CoreError::from(std::io::Error::last_os_error()));
        }
    }
    if stat.st_nlink != 1 {
        return Err(CoreError::InvalidManifest(
            "store files and lock files must not be hard-linked".to_owned(),
        ));
    }
    Ok(file)
}

#[cfg(unix)]
fn file_from_fd_io(fd: libc::c_int) -> Result<OpenOptionsFile, std::io::Error> {
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { OpenOptionsFile::from_raw_fd(fd) })
}

#[cfg(not(unix))]
#[derive(Clone, Copy)]
enum NonUnixSecureOpenMode {
    ReadOnly,
    ReadWrite,
    CreateNewReadWrite,
}

#[cfg(not(unix))]
fn open_path_no_symlinks_nonunix(
    path: &Path,
    mode: NonUnixSecureOpenMode,
) -> Result<OpenOptionsFile, CoreError> {
    let (dir, name) = secure_cap_parent_dir_and_leaf(
        path,
        matches!(mode, NonUnixSecureOpenMode::CreateNewReadWrite),
    )?;
    open_file_at_cap_dir(&dir, &name, mode)
}

#[cfg(not(unix))]
fn secure_cap_parent_dir_and_leaf(
    path: &Path,
    create_missing: bool,
) -> Result<(cap_std::fs::Dir, OsString), CoreError> {
    let (base, relative) = capability_base_and_relative(path)?;
    let mut dir = cap_std::fs::Dir::open_ambient_dir(base, cap_std::ambient_authority())?;
    let mut components = relative.components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(CoreError::InvalidManifest(
                "store path must not contain parent or prefix components".to_owned(),
            ));
        };
        if components.peek().is_some() {
            dir = match dir.open_dir_nofollow(Path::new(name)) {
                Ok(dir) => dir,
                Err(error) if create_missing && error.kind() == std::io::ErrorKind::NotFound => {
                    dir.create_dir(Path::new(name))?;
                    sync_cap_dir(&dir)?;
                    dir.open_dir_nofollow(Path::new(name))?
                }
                Err(error) => return Err(CoreError::from(error)),
            };
            continue;
        }

        return Ok((dir, name.to_owned()));
    }
    Err(CoreError::InvalidManifest(
        "store path must include a file name".to_owned(),
    ))
}

#[cfg(not(unix))]
fn create_new_store_file_at_cap_dir(
    dir: &cap_std::fs::Dir,
    name: &OsStr,
) -> Result<OpenOptionsFile, CoreError> {
    open_file_at_cap_dir(dir, name, NonUnixSecureOpenMode::CreateNewReadWrite)
}

#[cfg(not(unix))]
fn open_file_at_cap_dir(
    dir: &cap_std::fs::Dir,
    name: &OsStr,
    mode: NonUnixSecureOpenMode,
) -> Result<OpenOptionsFile, CoreError> {
    open_file_at_cap_dir_inner(dir, name, mode, true)
}

#[cfg(not(unix))]
fn open_file_at_cap_dir_without_hardlink_check(
    dir: &cap_std::fs::Dir,
    name: &OsStr,
    mode: NonUnixSecureOpenMode,
) -> Result<OpenOptionsFile, CoreError> {
    open_file_at_cap_dir_inner(dir, name, mode, false)
}

#[cfg(not(unix))]
fn open_file_at_cap_dir_inner(
    dir: &cap_std::fs::Dir,
    name: &OsStr,
    mode: NonUnixSecureOpenMode,
    reject_hard_links: bool,
) -> Result<OpenOptionsFile, CoreError> {
    let mut options = cap_std::fs::OpenOptions::new();
    match mode {
        NonUnixSecureOpenMode::ReadOnly => {
            options.read(true);
        }
        NonUnixSecureOpenMode::ReadWrite => {
            options.read(true).write(true);
        }
        NonUnixSecureOpenMode::CreateNewReadWrite => {
            options.read(true).write(true).create_new(true);
        }
    }
    options.follow(FollowSymlinks::No);
    let file = dir.open_with(Path::new(name), &options)?;
    if !file.metadata()?.is_file() {
        return Err(CoreError::InvalidManifest(
            "store path must resolve to a regular file".to_owned(),
        ));
    }
    let file = file.into_std();
    if reject_hard_links {
        cleanup_non_unix_temporary_store_links(dir, name, &file)?;
        reject_non_unix_hard_linked_file(&file)?;
    }
    if matches!(mode, NonUnixSecureOpenMode::CreateNewReadWrite) {
        sync_cap_dir(dir)?;
    }
    Ok(file)
}

#[cfg(all(not(unix), windows))]
fn reject_non_unix_hard_linked_file(file: &OpenOptionsFile) -> Result<(), CoreError> {
    if windows_file_link_count(file)? != 1 {
        return Err(CoreError::InvalidManifest(
            "store files and lock files must not be hard-linked".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(all(not(unix), not(windows)))]
fn reject_non_unix_hard_linked_file(_file: &OpenOptionsFile) -> Result<(), CoreError> {
    Err(CoreError::Io(
        "hard-link validation is unsupported on this platform".to_owned(),
    ))
}

#[cfg(all(not(unix), windows))]
fn cleanup_non_unix_temporary_store_links(
    dir: &cap_std::fs::Dir,
    name: &OsStr,
    target_file: &OpenOptionsFile,
) -> Result<(), CoreError> {
    if windows_file_link_count(target_file)? <= 1 {
        return Ok(());
    }
    let target_identity = file_identity_from_file(target_file)?;
    let target_name = name.to_string_lossy();
    let prefix = format!(".{target_name}.create-");
    let allow_ascii_case_match =
        !directory_contains_exact_store_entry_non_unix(dir, name, &target_identity)?;
    for entry in dir.entries()? {
        let entry = entry?;
        let entry_name = entry.file_name();
        if entry_name == name {
            continue;
        }
        let entry_name_lossy = entry_name.to_string_lossy();
        let matches_prefix = entry_name_lossy.starts_with(&prefix)
            || (allow_ascii_case_match
                && entry_name_lossy
                    .to_ascii_lowercase()
                    .starts_with(&prefix.to_ascii_lowercase()));
        if !matches_prefix || !entry_name_lossy.ends_with(".tmp") {
            continue;
        }
        let suffix = &entry_name_lossy[prefix.len()..entry_name_lossy.len() - ".tmp".len()];
        if !is_pid_counter_suffix_bytes(suffix.as_bytes()) {
            continue;
        }
        let Ok(candidate) = open_file_at_cap_dir_without_hardlink_check(
            dir,
            &entry_name,
            NonUnixSecureOpenMode::ReadOnly,
        ) else {
            continue;
        };
        let Ok(candidate_identity) = file_identity_from_file(&candidate) else {
            continue;
        };
        if file_identities_match(&candidate_identity, &target_identity) {
            entry.remove_file()?;
        }
    }
    sync_cap_dir(dir)?;
    Ok(())
}

#[cfg(all(not(unix), windows))]
fn directory_contains_exact_store_entry_non_unix(
    dir: &cap_std::fs::Dir,
    name: &OsStr,
    target_identity: &FileIdentity,
) -> Result<bool, CoreError> {
    for entry in dir.entries()? {
        let entry = entry?;
        if entry.file_name() != name {
            continue;
        }
        let Ok(candidate) =
            open_file_at_cap_dir_without_hardlink_check(dir, name, NonUnixSecureOpenMode::ReadOnly)
        else {
            continue;
        };
        let candidate_identity = file_identity_from_file(&candidate)?;
        return Ok(file_identities_match(&candidate_identity, target_identity));
    }
    Ok(false)
}

#[cfg(all(not(unix), not(windows)))]
fn cleanup_non_unix_temporary_store_links(
    _dir: &cap_std::fs::Dir,
    _name: &OsStr,
    _target_file: &OpenOptionsFile,
) -> Result<(), CoreError> {
    Ok(())
}

#[cfg(windows)]
fn windows_file_link_count(file: &std::fs::File) -> Result<u32, CoreError> {
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    let result =
        unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), info.as_mut_ptr()) };
    if result == 0 {
        return Err(CoreError::from(std::io::Error::last_os_error()));
    }
    Ok(unsafe { info.assume_init() }.nNumberOfLinks)
}

#[cfg(not(unix))]
fn sync_cap_dir(dir: &cap_std::fs::Dir) -> Result<(), CoreError> {
    dir.try_clone()?.into_std_file().sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn capability_base_and_relative(path: &Path) -> Result<(PathBuf, PathBuf), CoreError> {
    let mut components = path.components().peekable();
    let mut base = PathBuf::new();
    match components.peek().copied() {
        Some(Component::Prefix(prefix)) => {
            base.push(prefix.as_os_str());
            components.next();
            if matches!(components.peek(), Some(Component::RootDir)) {
                base.push(Component::RootDir.as_os_str());
                components.next();
            } else {
                return Err(CoreError::InvalidManifest(
                    "store path prefix must be absolute".to_owned(),
                ));
            }
        }
        Some(Component::RootDir) => {
            base.push(Component::RootDir.as_os_str());
            components.next();
        }
        _ => {
            base = std::env::current_dir()?;
        }
    }

    let mut relative = PathBuf::new();
    for component in components {
        let Component::Normal(name) = component else {
            return Err(CoreError::InvalidManifest(
                "store path must not contain parent or prefix components".to_owned(),
            ));
        };
        relative.push(name);
    }
    Ok((base, relative))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppendedObject {
    offset: u64,
    length: u64,
    payload_checksum: [u8; 32],
}

fn append_object(
    file: &mut OpenOptionsFile,
    object_type: ObjectType,
    logical_generation: u64,
    alignment: u64,
    payload: &[u8],
) -> Result<AppendedObject, CoreError> {
    let current_end = file.seek(SeekFrom::End(0))?;
    let object_offset = align_up(current_end, alignment.max(DEFAULT_OBJECT_ALIGNMENT))?;
    write_zero_padding(file, object_offset)?;
    let payload_checksum = sha256(payload);
    let header = encode_object_header(
        object_type,
        logical_generation,
        alignment,
        payload.len() as u64,
        payload_checksum,
    );
    file.write_all(&header)?;
    file.write_all(payload)?;
    Ok(AppendedObject {
        offset: object_offset,
        length: (header.len() + payload.len()) as u64,
        payload_checksum,
    })
}

fn encode_object(
    object_type: ObjectType,
    logical_generation: u64,
    alignment: u64,
    payload: &[u8],
) -> Vec<u8> {
    let payload_checksum = sha256(payload);
    let mut bytes = Vec::from(encode_object_header(
        object_type,
        logical_generation,
        alignment,
        payload.len() as u64,
        payload_checksum,
    ));
    bytes.extend_from_slice(payload);
    bytes
}

fn encode_object_header(
    object_type: ObjectType,
    logical_generation: u64,
    alignment: u64,
    payload_length: u64,
    payload_checksum: [u8; 32],
) -> [u8; OBJECT_HEADER_LENGTH] {
    let mut header = [0u8; OBJECT_HEADER_LENGTH];
    header[..4].copy_from_slice(OBJECT_MAGIC);
    header[4..6].copy_from_slice(&object_type.as_code().to_le_bytes());
    header[6..8].copy_from_slice(&OBJECT_VERSION.to_le_bytes());
    header[8..16].copy_from_slice(&payload_length.to_le_bytes());
    header[16..24].copy_from_slice(&logical_generation.to_le_bytes());
    header[24..32].copy_from_slice(&alignment.to_le_bytes());
    header[32..64].copy_from_slice(&payload_checksum);
    header
}

#[derive(Debug)]
struct DecodedObject {
    payload_range: Range<usize>,
    payload_checksum: [u8; 32],
}

fn decode_object_payload(
    object_bytes: &[u8],
    expected_type: ObjectType,
    expected_generation: u64,
) -> Result<DecodedObject, CoreError> {
    let decoded = decode_object_payload_header(object_bytes, expected_type, expected_generation)?;
    let payload_checksum = sha256(&object_bytes[decoded.payload_range.clone()]);
    if payload_checksum != decoded.payload_checksum {
        return Err(CoreError::ChecksumMismatch { context: "object" });
    }
    Ok(decoded)
}

fn decode_object_payload_header(
    object_bytes: &[u8],
    expected_type: ObjectType,
    expected_generation: u64,
) -> Result<DecodedObject, CoreError> {
    if object_bytes.len() < OBJECT_HEADER_LENGTH {
        return Err(CoreError::UnexpectedLength {
            context: "object",
            expected_at_least: OBJECT_HEADER_LENGTH,
            actual: object_bytes.len(),
        });
    }
    if &object_bytes[..4] != OBJECT_MAGIC {
        return Err(CoreError::InvalidMagic { context: "object" });
    }
    let object_type = ObjectType::from_code(read_u16(object_bytes, 4))?;
    if object_type != expected_type {
        return Err(CoreError::InvalidManifest(
            "object type does not match expected family".to_owned(),
        ));
    }
    let version = read_u16(object_bytes, 6);
    if version != OBJECT_VERSION {
        return Err(CoreError::InvalidVersion(version as u32));
    }
    let object_length = usize::try_from(read_u64(object_bytes, 8)).map_err(|_| {
        CoreError::InvalidManifest("object length exceeds addressable memory".to_owned())
    })?;
    let logical_generation = read_u64(object_bytes, 16);
    if logical_generation != expected_generation {
        return Err(CoreError::InvalidManifest(
            "object generation does not match expected generation".to_owned(),
        ));
    }
    let payload_end = OBJECT_HEADER_LENGTH
        .checked_add(object_length)
        .ok_or_else(|| CoreError::InvalidManifest("object length overflow".to_owned()))?;
    if payload_end != object_bytes.len() {
        return Err(CoreError::InvalidManifest(
            "object payload length does not match descriptor object length".to_owned(),
        ));
    }
    let payload_range = OBJECT_HEADER_LENGTH..payload_end;
    let mut expected_checksum = [0u8; 32];
    expected_checksum.copy_from_slice(&object_bytes[32..64]);
    Ok(DecodedObject {
        payload_range,
        payload_checksum: expected_checksum,
    })
}

fn validate_object_header_length(
    object_header: &[u8; OBJECT_HEADER_LENGTH],
    expected_type: ObjectType,
    expected_generation: u64,
    descriptor_object_length: u64,
) -> Result<(), CoreError> {
    if &object_header[..4] != OBJECT_MAGIC {
        return Err(CoreError::InvalidMagic { context: "object" });
    }
    let object_type = ObjectType::from_code(read_u16(object_header, 4))?;
    if object_type != expected_type {
        return Err(CoreError::InvalidManifest(
            "object type does not match expected family".to_owned(),
        ));
    }
    let version = read_u16(object_header, 6);
    if version != OBJECT_VERSION {
        return Err(CoreError::InvalidVersion(version as u32));
    }
    let object_length = read_u64(object_header, 8);
    let logical_generation = read_u64(object_header, 16);
    if logical_generation != expected_generation {
        return Err(CoreError::InvalidManifest(
            "object generation does not match expected generation".to_owned(),
        ));
    }
    let expected_total = (OBJECT_HEADER_LENGTH as u64)
        .checked_add(object_length)
        .ok_or_else(|| CoreError::InvalidManifest("object length overflow".to_owned()))?;
    if expected_total != descriptor_object_length {
        return Err(CoreError::InvalidManifest(
            "descriptor object length does not match encoded object length".to_owned(),
        ));
    }
    Ok(())
}

fn object_type_for_family(family: SegmentKind) -> ObjectType {
    match family {
        SegmentKind::Doc => ObjectType::DocSegment,
        SegmentKind::Txt => ObjectType::TxtSegment,
        SegmentKind::Vec => ObjectType::VecSegment,
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("u16 slice"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("u32 slice"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("u64 slice"))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs::OpenOptions;

    use fs2::FileExt;
    use tempfile::tempdir;

    use crate::{
        align_up, create_empty_store, decode_object_payload, default_mmap_allocation_granularity,
        map_segment_object, map_segment_object_shallow, open_store, publish_segment,
        publish_segments, publish_segments_with_precondition, read_segment_object,
        set_create_empty_store_fail_hook, set_create_empty_store_pre_publish_hook,
        write_zero_padding, ActiveManifest, CoreError, ObjectType, PendingSegmentDescriptor,
        PendingSegmentWrite, SegmentDescriptor, SegmentKind, SegmentObjectBacking, Superblock,
        DEFAULT_OBJECT_ALIGNMENT, FORMAT_VERSION, MANIFEST_HEADER_LENGTH, MANIFEST_MAGIC,
        MAX_ACTIVE_SEGMENT_BYTES, MAX_MANIFEST_OBJECT_LENGTH, OBJECT_HEADER_LENGTH, OBJECT_MAGIC,
        SEGMENT_DESCRIPTOR_LENGTH, STORE_PUBLISH_LOCK_BUSY_MESSAGE, SUPERBLOCK_SIZE,
    };

    fn replace_segment(
        path: &std::path::Path,
        descriptor: PendingSegmentDescriptor,
        object_bytes: &[u8],
    ) -> Result<crate::OpenedStore, CoreError> {
        publish_segments_with_precondition(
            path,
            vec![PendingSegmentWrite {
                descriptor,
                object_bytes: object_bytes.to_vec(),
            }],
            |_| Ok(()),
        )
    }

    fn test_pending_descriptor(family: SegmentKind) -> PendingSegmentDescriptor {
        PendingSegmentDescriptor {
            family,
            family_version: 1,
            flags: 0,
            doc_id_start: 0,
            doc_id_end_exclusive: 1,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
            live_items: 1,
            tombstoned_items: 0,
            backend_id: 0,
            backend_aux: 0,
        }
    }

    fn test_segment_descriptor(family: SegmentKind, object_length: u64) -> SegmentDescriptor {
        SegmentDescriptor {
            family,
            family_version: 1,
            flags: 0,
            object_offset: DEFAULT_OBJECT_ALIGNMENT,
            object_length,
            segment_generation: 1,
            doc_id_start: 0,
            doc_id_end_exclusive: 1,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
            live_items: 1,
            tombstoned_items: 0,
            backend_id: 0,
            backend_aux: 0,
            object_checksum: [0; 32],
        }
    }

    #[test]
    fn superblock_round_trips_with_checksum_when_encoded() {
        let manifest = ActiveManifest {
            generation: 7,
            segments: vec![SegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                object_offset: 4096,
                object_length: 128,
                segment_generation: 7,
                doc_id_start: 10,
                doc_id_end_exclusive: 12,
                min_timestamp_ms: 1000,
                max_timestamp_ms: 2000,
                live_items: 2,
                tombstoned_items: 0,
                backend_id: 17,
                backend_aux: 0,
                object_checksum: [9; 32],
            }],
        };
        let manifest_bytes = manifest.encode().expect("manifest should encode");
        let manifest_checksum = ActiveManifest::checksum(&manifest_bytes);
        let superblock = Superblock::new(7, 8192, manifest_bytes.len() as u32, manifest_checksum);

        let encoded = superblock.encode();
        let decoded = Superblock::decode(&encoded).expect("superblock should decode");

        assert_eq!(decoded, superblock);
    }

    #[test]
    fn active_segment_byte_budget_counts_pending_segments_before_append() {
        let retained = vec![test_segment_descriptor(
            SegmentKind::Doc,
            MAX_ACTIVE_SEGMENT_BYTES,
        )];
        let pending = vec![PendingSegmentWrite {
            descriptor: test_pending_descriptor(SegmentKind::Txt),
            object_bytes: vec![1],
        }];

        let error = crate::validate_active_segment_object_byte_budget(&retained, &pending)
            .expect_err("pending segment should exceed active budget");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message)
                if message.contains("active segment object bytes exceed maximum")
        ));
    }

    #[test]
    fn publish_missing_store_does_not_create_sidecar_parent() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("missing").join("store.rax");

        let error = publish_segment(&path, test_pending_descriptor(SegmentKind::Doc), b"segment")
            .expect_err("publish should not create a missing store");

        assert!(matches!(
            error,
            CoreError::Io(_) | CoreError::InvalidManifest(_)
        ));
        assert!(!temp_dir.path().join("missing").exists());
    }

    #[test]
    fn publish_fails_if_target_path_is_replaced_after_open() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("store.rax");
        let moved = temp_dir.path().join("store.old");
        create_empty_store(&path).expect("store should be created");

        let error = publish_segments_with_precondition(
            &path,
            vec![PendingSegmentWrite {
                descriptor: test_pending_descriptor(SegmentKind::Doc),
                object_bytes: b"segment".to_vec(),
            }],
            {
                let path = path.clone();
                let moved = moved.clone();
                move |_| {
                    std::fs::rename(&path, &moved).map_err(CoreError::from)?;
                    create_empty_store(&path)?;
                    Ok(())
                }
            },
        )
        .expect_err("publish should fail when requested path is replaced");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message) if message == "store path changed during publish"
        ));
        assert_eq!(open_store(&path).unwrap().manifest.generation, 0);
        assert_eq!(open_store(&moved).unwrap().manifest.generation, 0);
    }

    #[test]
    fn next_generation_targets_inactive_superblock_slot_after_fallback() {
        assert_eq!(
            crate::next_generation_for_superblock_offset(2, SUPERBLOCK_SIZE as u64).unwrap(),
            3
        );
        assert_eq!(
            crate::next_generation_for_superblock_offset(2, 0).unwrap(),
            4
        );
    }

    #[test]
    fn manifest_round_trips_full_segment_descriptor_shape() {
        let manifest = ActiveManifest {
            generation: 9,
            segments: vec![SegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0x10,
                object_offset: 16384,
                object_length: 512,
                segment_generation: 9,
                doc_id_start: 100,
                doc_id_end_exclusive: 140,
                min_timestamp_ms: 1_000,
                max_timestamp_ms: 9_000,
                live_items: 37,
                tombstoned_items: 2,
                backend_id: 11,
                backend_aux: 99,
                object_checksum: [3; 32],
            }],
        };

        let encoded = manifest.encode().expect("manifest should encode");
        let decoded = ActiveManifest::decode(&encoded).expect("manifest should decode");

        assert_eq!(decoded, manifest);
    }

    #[test]
    fn manifest_encodes_descriptor_checksum_at_offset_96() {
        let checksum = [0xab; 32];
        let manifest = ActiveManifest {
            generation: 9,
            segments: vec![SegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0x10,
                object_offset: 16384,
                object_length: 512,
                segment_generation: 9,
                doc_id_start: 100,
                doc_id_end_exclusive: 140,
                min_timestamp_ms: 1_000,
                max_timestamp_ms: 9_000,
                live_items: 37,
                tombstoned_items: 2,
                backend_id: 11,
                backend_aux: 99,
                object_checksum: checksum,
            }],
        };

        let encoded = manifest.encode().expect("manifest should encode");
        let descriptor_start = MANIFEST_HEADER_LENGTH;

        assert_eq!(
            encoded.len(),
            MANIFEST_HEADER_LENGTH + SEGMENT_DESCRIPTOR_LENGTH
        );
        assert_eq!(
            &encoded[descriptor_start + 88..descriptor_start + 96],
            &99_u64.to_le_bytes()
        );
        assert_eq!(
            &encoded[descriptor_start + 96..descriptor_start + 128],
            checksum.as_slice()
        );
    }

    #[test]
    fn manifest_encode_writes_full_descriptor_length() {
        let manifest = ActiveManifest {
            generation: 9,
            segments: vec![SegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0x10,
                object_offset: 16384,
                object_length: 512,
                segment_generation: 9,
                doc_id_start: 100,
                doc_id_end_exclusive: 140,
                min_timestamp_ms: 1_000,
                max_timestamp_ms: 9_000,
                live_items: 37,
                tombstoned_items: 2,
                backend_id: 11,
                backend_aux: 99,
                object_checksum: [0xab; 32],
            }],
        };

        let encoded = manifest.encode().expect("manifest should encode");

        assert_eq!(
            encoded.len() - MANIFEST_HEADER_LENGTH,
            SEGMENT_DESCRIPTOR_LENGTH
        );
        ActiveManifest::decode(&encoded).expect("manifest should decode");
    }

    #[test]
    fn manifest_rejects_invalid_doc_id_ranges() {
        let manifest = ActiveManifest {
            generation: 9,
            segments: vec![SegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                object_offset: 16384,
                object_length: 512,
                segment_generation: 9,
                doc_id_start: 140,
                doc_id_end_exclusive: 100,
                min_timestamp_ms: 1_000,
                max_timestamp_ms: 9_000,
                live_items: 37,
                tombstoned_items: 2,
                backend_id: 11,
                backend_aux: 99,
                object_checksum: [3; 32],
            }],
        };

        let encoded = manifest.encode().expect("manifest should encode");
        let error =
            ActiveManifest::decode(&encoded).expect_err("manifest should reject invalid ranges");

        assert!(matches!(error, CoreError::InvalidManifest(message) if message.contains("doc_id")));
    }

    #[test]
    fn manifest_rejects_segment_generation_newer_than_manifest() {
        let manifest = ActiveManifest {
            generation: 1,
            segments: vec![SegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                object_offset: 16384,
                object_length: 512,
                segment_generation: 2,
                doc_id_start: 100,
                doc_id_end_exclusive: 140,
                min_timestamp_ms: 1_000,
                max_timestamp_ms: 9_000,
                live_items: 37,
                tombstoned_items: 2,
                backend_id: 11,
                backend_aux: 99,
                object_checksum: [3; 32],
            }],
        };

        let encoded = manifest.encode().expect("manifest should encode");
        let error =
            ActiveManifest::decode(&encoded).expect_err("future segment generation should fail");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message)
                if message.contains("must not exceed manifest generation")
        ));
    }

    #[test]
    fn manifest_decode_rejects_implausible_segment_count_before_allocation() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MANIFEST_MAGIC);
        encoded.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        encoded.extend_from_slice(&1_u64.to_le_bytes());
        encoded.extend_from_slice(&u32::MAX.to_le_bytes());

        let error = ActiveManifest::decode(&encoded).expect_err("manifest length should fail");

        assert!(matches!(error, CoreError::InvalidManifest(message) if message.contains("length")));
    }

    #[test]
    fn manifest_encode_rejects_segment_count_above_decode_bound() {
        let segment = SegmentDescriptor {
            family: SegmentKind::Doc,
            family_version: 1,
            flags: 0,
            object_offset: 4096,
            object_length: 128,
            segment_generation: 1,
            doc_id_start: 0,
            doc_id_end_exclusive: 1,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
            live_items: 1,
            tombstoned_items: 0,
            backend_id: 0,
            backend_aux: 0,
            object_checksum: [1; 32],
        };
        let manifest = ActiveManifest {
            generation: 1,
            segments: vec![segment; crate::MAX_SEGMENT_DESCRIPTOR_COUNT + 1],
        };

        let error = manifest
            .encode()
            .expect_err("oversized manifest should fail");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message) if message.contains("segment count")
        ));
    }

    #[test]
    fn object_decode_rejects_unaddressable_payload_length_before_indexing() {
        let mut bytes = [0u8; OBJECT_HEADER_LENGTH];
        bytes[..4].copy_from_slice(OBJECT_MAGIC);
        bytes[4..6].copy_from_slice(&ObjectType::DocSegment.as_code().to_le_bytes());
        bytes[6..8].copy_from_slice(&1u16.to_le_bytes());
        bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        bytes[16..24].copy_from_slice(&1u64.to_le_bytes());

        let error = decode_object_payload(&bytes, ObjectType::DocSegment, 1)
            .expect_err("object length should fail");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message)
                if message.contains("object length")
                    || message.contains("addressable memory")
        ));
    }

    #[test]
    fn mmap_allocation_granularity_fallback_is_safe_for_target() {
        let fallback = default_mmap_allocation_granularity();

        assert!(fallback >= DEFAULT_OBJECT_ALIGNMENT);
        assert!(fallback.is_multiple_of(DEFAULT_OBJECT_ALIGNMENT));
    }

    #[test]
    fn map_segment_object_rejects_unaligned_descriptor_offsets() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("unaligned.rax");
        std::fs::write(&path, vec![0u8; 128]).expect("seed file");

        let descriptor = SegmentDescriptor {
            family: SegmentKind::Doc,
            family_version: 1,
            flags: 0,
            object_offset: 1,
            object_length: 64,
            segment_generation: 1,
            doc_id_start: 0,
            doc_id_end_exclusive: 1,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
            live_items: 1,
            tombstoned_items: 0,
            backend_id: 0,
            backend_aux: 0,
            object_checksum: [0; 32],
        };

        let error = map_segment_object(&path, &descriptor).expect_err("offset should be rejected");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message) if message.contains("object alignment")
        ));
    }

    #[test]
    fn create_empty_store_opens_with_zero_segments() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("empty.rax");

        create_empty_store(&path).expect("store should be created");
        let opened = open_store(&path).expect("store should reopen");
        let bytes = std::fs::read(&path).expect("store bytes");

        assert_eq!(opened.manifest.generation, 0);
        assert!(opened.manifest.segments.is_empty());
        assert!(opened
            .superblock
            .active_manifest_offset
            .is_multiple_of(4096));
        assert_eq!(
            &bytes[opened.superblock.active_manifest_offset as usize
                ..opened.superblock.active_manifest_offset as usize + 4],
            OBJECT_MAGIC
        );
    }

    #[test]
    fn create_empty_store_rejects_existing_path() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("empty.rax");
        std::fs::write(&path, b"existing").expect("seed existing file");

        let error = create_empty_store(&path).expect_err("existing store path should fail");
        assert!(matches!(error, CoreError::AlreadyExists(_)));
    }

    #[test]
    fn create_empty_store_reports_temp_name_exhaustion_distinctly() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("agent.rax");
        let start_counter = 1_000_000_000_u64;
        let file_name = path.file_name().unwrap().to_string_lossy();
        for counter in start_counter..start_counter + 1024 {
            std::fs::write(
                temp_dir.path().join(format!(
                    ".{file_name}.create-{}-{counter}.tmp",
                    std::process::id()
                )),
                b"stale temp",
            )
            .expect("seed stale temp name");
        }
        crate::TEMP_STORE_COUNTER.store(start_counter, std::sync::atomic::Ordering::Relaxed);

        let error = create_empty_store(&path).expect_err("temp name exhaustion should fail");

        assert!(matches!(error, CoreError::TemporaryNameExhausted(_)));
        assert!(!path.exists());
    }

    #[test]
    fn create_empty_store_publish_does_not_replace_racing_existing_path() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("race.rax");
        let mut temp = crate::create_new_temporary_store_file(&path).expect("temp store");
        crate::create_empty_store_from_file(&mut temp.file).expect("initialize temp store");
        std::fs::write(&path, b"winner").expect("seed racing winner");

        let error =
            crate::publish_temporary_store_file(&temp).expect_err("publish race should fail");

        assert!(matches!(error, CoreError::AlreadyExists(_)));
        assert_eq!(std::fs::read(&path).unwrap(), b"winner");
        let _ = crate::remove_failed_created_store_file(&temp, None);
    }

    #[test]
    fn create_empty_store_rejects_temp_path_swap_before_publish() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("agent.rax");
        set_create_empty_store_pre_publish_hook({
            let temp_dir = temp_dir.path().to_path_buf();
            move || {
                let temp_path = std::fs::read_dir(&temp_dir)
                    .map_err(CoreError::from)?
                    .map(|entry| entry.map(|entry| entry.path()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(CoreError::from)?
                    .into_iter()
                    .find(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| {
                                name.starts_with(".agent.rax.create-") && name.ends_with(".tmp")
                            })
                    })
                    .ok_or_else(|| CoreError::Io("test temp file was not created".to_owned()))?;
                let replacement_path = temp_dir.join("replacement.rax");
                crate::create_empty_store(&replacement_path)?;
                std::fs::remove_file(&temp_path).map_err(CoreError::from)?;
                std::fs::hard_link(&replacement_path, &temp_path).map_err(CoreError::from)?;
                Ok(())
            }
        });

        let error = crate::create_empty_store_and_open(&path)
            .expect_err("temp path replacement must not publish successfully");

        assert!(matches!(error, CoreError::Io(message) if message.contains("identity")));
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn create_empty_store_removes_published_target_after_temp_hard_link_poisoning() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("agent.rax");
        set_create_empty_store_pre_publish_hook({
            let temp_dir = temp_dir.path().to_path_buf();
            move || {
                let temp_path = std::fs::read_dir(&temp_dir)
                    .map_err(CoreError::from)?
                    .map(|entry| entry.map(|entry| entry.path()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(CoreError::from)?
                    .into_iter()
                    .find(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| {
                                name.starts_with(".agent.rax.create-") && name.ends_with(".tmp")
                            })
                    })
                    .ok_or_else(|| CoreError::Io("test temp file was not created".to_owned()))?;
                std::fs::hard_link(&temp_path, temp_dir.join("extra-hardlink"))
                    .map_err(CoreError::from)?;
                Ok(())
            }
        });

        let error = crate::create_empty_store_and_open(&path)
            .expect_err("extra hard link must fail publish");

        assert!(
            matches!(error, CoreError::InvalidManifest(message) if message.contains("hard-linked"))
        );
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn create_empty_store_publish_uses_open_parent_after_path_swap() {
        let temp_dir = tempdir().expect("tempdir");
        let parent = temp_dir.path().join("parent");
        let moved_parent = temp_dir.path().join("moved-parent");
        let outside = temp_dir.path().join("outside");
        std::fs::create_dir(&parent).expect("parent");
        std::fs::create_dir(&outside).expect("outside");
        let path = parent.join("race.rax");
        let mut temp = crate::create_new_temporary_store_file(&path).expect("temp store");
        crate::create_empty_store_from_file(&mut temp.file).expect("initialize temp store");

        std::fs::rename(&parent, &moved_parent).expect("move checked parent");
        std::os::unix::fs::symlink(&outside, &parent).expect("swap parent path to symlink");

        crate::publish_temporary_store_file(&temp).expect("fd-relative publish");

        assert!(moved_parent.join("race.rax").exists());
        assert!(!outside.join("race.rax").exists());
        let opened = open_store(&moved_parent.join("race.rax")).expect("published store opens");
        assert_eq!(opened.manifest.generation, 0);
        let _ = crate::remove_failed_created_store_file(&temp, None);
    }

    #[test]
    fn create_empty_store_removes_partial_file_after_initialization_failure() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("partial.rax");
        set_create_empty_store_fail_hook(|| CoreError::Io("injected failure".to_owned()));

        let error = create_empty_store(&path).expect_err("injected write failure should fail");

        assert!(matches!(error, CoreError::Io(message) if message == "injected failure"));
        assert!(!path.exists());
        assert!(std::fs::read_dir(temp_dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn create_empty_store_failure_cleanup_does_not_remove_replacement_temp_file() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("partial.rax");
        let temp_root = temp_dir.path().to_owned();
        set_create_empty_store_fail_hook(move || {
            let temp_path = std::fs::read_dir(&temp_root)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| {
                    path.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with(".partial.rax.create-")
                })
                .expect("created temp store path");
            std::fs::remove_file(&temp_path).expect("remove original temp path");
            std::fs::write(&temp_path, b"replacement").expect("write replacement temp path");
            CoreError::Io("injected failure".to_owned())
        });

        let error = create_empty_store(&path).expect_err("injected write failure should fail");

        assert!(matches!(error, CoreError::Io(message) if message == "injected failure"));
        assert!(!path.exists());
        let files = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 1);
        assert_eq!(std::fs::read(&files[0]).unwrap(), b"replacement");
    }

    #[cfg(unix)]
    #[test]
    fn open_store_recovers_leftover_first_create_temp_hard_link() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("empty.rax");
        create_empty_store(&path).expect("store should be created");
        let leftover_temp = temp_dir.path().join(format!(
            ".empty.rax.create-{}-987654321.tmp",
            std::process::id()
        ));
        std::fs::hard_link(&path, &leftover_temp).expect("leftover temp link");

        let opened = open_store(&path).expect("store should reopen after temp cleanup");

        assert_eq!(opened.manifest.generation, 0);
        assert!(!leftover_temp.exists());
    }

    #[cfg(unix)]
    #[test]
    fn open_store_recovers_non_utf8_core_temp_hard_link() {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};

        let temp_dir = tempdir().expect("tempdir");
        let file_name = OsStr::from_bytes(b"bad\xff.rax");
        let path = temp_dir.path().join(file_name);
        if let Err(error) = create_empty_store(&path) {
            if matches!(error, CoreError::Io(ref message) if message.contains("Illegal byte sequence"))
            {
                return;
            }
            panic!("store should be created: {error:?}");
        }
        let leftover_temp = temp_dir.path().join(crate::temporary_store_name(file_name));
        std::fs::hard_link(&path, &leftover_temp).expect("leftover temp link");

        let opened = open_store(&path).expect("store should reopen after temp cleanup");

        assert_eq!(opened.manifest.generation, 0);
        assert!(!leftover_temp.exists());
        assert!(crate::temporary_store_name(file_name)
            .into_vec()
            .starts_with(b".bad\xff.rax.create-"));
    }

    #[cfg(unix)]
    #[test]
    fn open_store_recovers_core_temp_for_target_containing_create_marker() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("agent.create-prod.rax");
        create_empty_store(&path).expect("store should be created");
        let leftover_temp = temp_dir.path().join(format!(
            ".agent.create-prod.rax.create-{}-987654321.tmp",
            std::process::id()
        ));
        std::fs::hard_link(&path, &leftover_temp).expect("leftover temp link");

        let opened = open_store(&path).expect("store should reopen after temp cleanup");

        assert_eq!(opened.manifest.generation, 0);
        assert!(!leftover_temp.exists());
    }

    #[cfg(unix)]
    #[test]
    fn open_store_recovers_case_mismatched_core_temp_hard_link_on_casefolded_volume() {
        let temp_dir = tempdir().expect("tempdir");
        let actual_path = temp_dir.path().join("Agent.rax");
        let requested_path = temp_dir.path().join("agent.rax");
        create_empty_store(&actual_path).expect("store should be created");
        if !requested_path.exists() {
            return;
        }
        let leftover_temp = temp_dir.path().join(format!(
            ".Agent.rax.create-{}-987654321.tmp",
            std::process::id()
        ));
        std::fs::hard_link(&actual_path, &leftover_temp).expect("leftover temp link");

        let opened = open_store(&requested_path)
            .expect("casefolded open should recover mismatched-case temp cleanup");

        assert_eq!(opened.manifest.generation, 0);
        assert!(!leftover_temp.exists());
    }

    #[cfg(unix)]
    #[test]
    fn open_store_rejects_case_mismatched_core_temp_on_case_sensitive_volume() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("agent.rax");
        create_empty_store(&path).expect("store should be created");
        let leftover_temp = temp_dir.path().join(format!(
            ".Agent.rax.create-{}-987654321.tmp",
            std::process::id()
        ));
        std::fs::hard_link(&path, &leftover_temp).expect("case-mismatched temp-like link");

        let error = open_store(&path).expect_err("case-mismatched hard link should be rejected");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message)
                if message == "store files and lock files must not be hard-linked"
        ));
        assert!(leftover_temp.exists());
    }

    #[cfg(unix)]
    #[test]
    fn open_store_rejects_broad_core_like_hard_link() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("agent.rax");
        create_empty_store(&path).expect("store should be created");
        let non_generated_link = temp_dir.path().join(".agent.rax.create-manual-stale.tmp");
        std::fs::hard_link(&path, &non_generated_link).expect("non-generated hard link");

        let error = open_store(&path).expect_err("non-generated hard link should be rejected");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message)
                if message == "store files and lock files must not be hard-linked"
        ));
        assert!(non_generated_link.exists());
    }

    #[cfg(unix)]
    #[test]
    fn open_store_does_not_unlink_temp_like_store_name() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join(".agent.rax.create-1-2.tmp");
        create_empty_store(&path).expect("store should be created");

        let opened = open_store(&path).expect("temp-like store name should open");

        assert_eq!(opened.manifest.generation, 0);
        assert!(path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn open_store_rejects_symlink_leaf_paths() {
        let temp_dir = tempdir().expect("tempdir");
        let target = temp_dir.path().join("target.rax");
        let link = temp_dir.path().join("link.rax");
        create_empty_store(&target).expect("store should be created");
        std::os::unix::fs::symlink(&target, &link).expect("symlink should be created");

        let error = open_store(&link).expect_err("symlink store path should fail");

        assert!(matches!(error, CoreError::Io(_)));
    }

    #[cfg(unix)]
    #[test]
    fn open_store_rejects_symlink_parent_components() {
        let temp_dir = tempdir().expect("tempdir");
        let real_dir = temp_dir.path().join("real");
        let link_dir = temp_dir.path().join("link");
        std::fs::create_dir(&real_dir).expect("real dir");
        std::os::unix::fs::symlink(&real_dir, &link_dir).expect("parent symlink");
        let real_store = real_dir.join("store.rax");
        create_empty_store(&real_store).expect("real store");

        let error = open_store(&link_dir.join("store.rax"))
            .expect_err("symlink parent component must be rejected");

        assert!(matches!(error, CoreError::Io(_)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn open_store_accepts_proc_self_fd_directory_roots_without_following_path_symlinks() {
        use std::os::fd::AsRawFd;

        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("store.rax");
        create_empty_store(&path).expect("store should be created");
        let root_dir = std::fs::File::open(temp_dir.path()).expect("root dir");
        let fd_path =
            std::path::PathBuf::from(format!("/proc/self/fd/{}/store.rax", root_dir.as_raw_fd()));

        let opened = open_store(&fd_path).expect("fd-relative store should open");

        assert_eq!(opened.manifest.generation, 0);
    }

    #[cfg(unix)]
    #[test]
    fn create_empty_store_rejects_parent_dir_components_before_resolution() {
        let temp_dir = tempdir().expect("tempdir");
        let base = temp_dir.path().join("base");
        let outside = temp_dir.path().join("outside");
        std::fs::create_dir(&base).expect("base dir");
        std::fs::create_dir(&outside).expect("outside dir");

        let error = create_empty_store(&base.join("..").join("outside").join("store.rax"))
            .expect_err("parent dir component must be rejected");

        assert!(matches!(error, CoreError::InvalidManifest(_)));
        assert!(!outside.join("store.rax").exists());
    }

    #[cfg(unix)]
    #[test]
    fn read_file_no_symlinks_rejects_fifo_leaf_without_blocking() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let temp_dir = tempdir().expect("tempdir");
        let fifo = temp_dir.path().join("pipe.rax");
        let fifo_name = CString::new(fifo.as_os_str().as_bytes()).expect("fifo path");
        let result = unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) };
        assert_eq!(result, 0, "mkfifo should succeed");

        let error = crate::read_file_no_symlinks(&fifo).expect_err("fifo leaf should be rejected");

        assert!(
            matches!(error, CoreError::InvalidManifest(message) if message.contains("regular file"))
        );
    }

    #[test]
    fn open_store_rejects_corrupt_superblock_checksum() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("corrupt.rax");

        create_empty_store(&path).expect("store should be created");

        let mut bytes = std::fs::read(&path).expect("store bytes");
        bytes[0] ^= 0xFF;
        bytes[SUPERBLOCK_SIZE] ^= 0xFF;
        std::fs::write(&path, bytes).expect("rewrite store");

        let error = open_store(&path).expect_err("open should fail");
        assert!(matches!(error, CoreError::NoValidSuperblock));
    }

    #[test]
    fn open_store_rejects_oversized_manifest_length_before_allocation() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("oversized-manifest.rax");

        create_empty_store(&path).expect("store should be created");
        let mut bytes = std::fs::read(&path).expect("store bytes");
        let superblock = Superblock::decode(&bytes[..SUPERBLOCK_SIZE]).expect("superblock");
        let oversized = Superblock::new(
            superblock.generation,
            superblock.active_manifest_offset,
            (MAX_MANIFEST_OBJECT_LENGTH + 1) as u32,
            superblock.manifest_checksum,
        )
        .encode();
        bytes[..SUPERBLOCK_SIZE].copy_from_slice(&oversized);
        bytes[SUPERBLOCK_SIZE..SUPERBLOCK_SIZE * 2].copy_from_slice(&oversized);
        std::fs::write(&path, bytes).expect("rewrite store");

        let error = open_store(&path).expect_err("open should fail before allocation");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message)
                if message.contains("active manifest object length exceeds supported bound")
        ));
    }

    #[test]
    fn publish_segment_appends_object_and_reopens_with_new_manifest_generation() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("published.rax");
        let object_bytes = b"doc-segment-object";

        create_empty_store(&path).expect("store should be created");
        let opened = publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 2,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 2,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            object_bytes,
        )
        .expect("segment should publish");

        assert_eq!(opened.manifest.generation, 1);
        assert_eq!(opened.manifest.segments.len(), 1);
        assert_eq!(opened.manifest.segments[0].family, SegmentKind::Doc);
        assert_eq!(
            opened.manifest.segments[0].object_length,
            (OBJECT_HEADER_LENGTH + object_bytes.len()) as u64
        );

        let reopened = open_store(&path).expect("store should reopen");
        let read_back =
            read_segment_object(&path, &reopened.manifest.segments[0]).expect("object bytes");
        let bytes = std::fs::read(&path).expect("store bytes");

        assert_eq!(reopened.manifest.generation, 1);
        assert_eq!(read_back, object_bytes);
        assert!(reopened.manifest.segments[0]
            .object_offset
            .is_multiple_of(4096));
        assert!(reopened
            .superblock
            .active_manifest_offset
            .is_multiple_of(4096));
        assert_eq!(
            &bytes[reopened.manifest.segments[0].object_offset as usize
                ..reopened.manifest.segments[0].object_offset as usize + 4],
            OBJECT_MAGIC
        );
        assert_eq!(
            &bytes[reopened.superblock.active_manifest_offset as usize
                ..reopened.superblock.active_manifest_offset as usize + 4],
            OBJECT_MAGIC
        );
    }

    #[test]
    fn map_segment_object_returns_mapped_payload_view() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("mapped.rax");

        create_empty_store(&path).expect("store should be created");
        let opened = publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"mapped-payload",
        )
        .expect("segment should publish");

        let object =
            map_segment_object(&path, &opened.manifest.segments[0]).expect("mapped object");

        assert!(matches!(&object.backing, SegmentObjectBacking::Mapped(_)));
        assert_eq!(object.as_slice(), b"mapped-payload");
    }

    #[test]
    fn map_segment_object_rejects_corrupt_payload_bytes() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("corrupt-payload.rax");

        create_empty_store(&path).expect("store should be created");
        let opened = publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"mapped-payload",
        )
        .expect("segment should publish");
        let descriptor = opened.manifest.segments[0].clone();
        let mut bytes = std::fs::read(&path).expect("store bytes");
        let payload_start = descriptor.object_offset as usize + OBJECT_HEADER_LENGTH;
        bytes[payload_start] ^= 0xFF;
        std::fs::write(&path, bytes).expect("rewrite store");

        let error =
            map_segment_object(&path, &descriptor).expect_err("payload checksum should fail");

        assert!(matches!(error, CoreError::ChecksumMismatch { .. }));
        assert!(map_segment_object_shallow(&path, &descriptor).is_ok());
    }

    #[test]
    fn opened_store_snapshot_remains_stable_after_later_publish() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("snapshot.rax");

        create_empty_store(&path).expect("store should be created");
        let generation_one = publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"segment-one",
        )
        .expect("first publish");
        let snapshot = generation_one.clone();

        let generation_two = replace_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 1,
                doc_id_end_exclusive: 2,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"segment-two",
        )
        .expect("second publish");

        assert_eq!(snapshot.manifest.generation, 1);
        assert_eq!(snapshot.manifest.segments.len(), 1);
        assert_eq!(generation_two.manifest.generation, 2);
        assert_eq!(generation_two.manifest.segments.len(), 1);
        assert_eq!(
            read_segment_object(&path, &snapshot.manifest.segments[0]).expect("snapshot object"),
            b"segment-one"
        );
        assert_eq!(
            read_segment_object(&path, &generation_two.manifest.segments[0])
                .expect("latest object"),
            b"segment-two"
        );
    }

    #[test]
    fn publish_segments_with_precondition_rejects_before_generation_advance() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("precondition.rax");

        create_empty_store(&path).expect("store should be created");
        let error = publish_segments_with_precondition(
            &path,
            vec![PendingSegmentWrite {
                descriptor: PendingSegmentDescriptor {
                    family: SegmentKind::Doc,
                    family_version: 1,
                    flags: 0,
                    doc_id_start: 0,
                    doc_id_end_exclusive: 1,
                    min_timestamp_ms: 0,
                    max_timestamp_ms: 0,
                    live_items: 1,
                    tombstoned_items: 0,
                    backend_id: 0,
                    backend_aux: 0,
                },
                object_bytes: b"rejected-segment".to_vec(),
            }],
            |manifest| {
                assert_eq!(manifest.generation, 0);
                Err(CoreError::PublishPreconditionFailed(
                    "document generation changed".to_owned(),
                ))
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            CoreError::PublishPreconditionFailed(message)
                if message.contains("document generation changed")
        ));
        let reopened = open_store(&path).expect("store should reopen");
        assert_eq!(reopened.manifest.generation, 0);
        assert!(reopened.manifest.segments.is_empty());
    }

    #[test]
    fn publish_segments_without_precondition_refuses_existing_family_replacement() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("replace-needs-precondition.rax");

        create_empty_store(&path).expect("store should be created");
        publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"doc-one",
        )
        .expect("first publish");

        let error = publish_segments(
            &path,
            vec![PendingSegmentWrite {
                descriptor: PendingSegmentDescriptor {
                    family: SegmentKind::Doc,
                    family_version: 1,
                    flags: 0,
                    doc_id_start: 1,
                    doc_id_end_exclusive: 2,
                    min_timestamp_ms: 0,
                    max_timestamp_ms: 0,
                    live_items: 1,
                    tombstoned_items: 0,
                    backend_id: 0,
                    backend_aux: 0,
                },
                object_bytes: b"doc-two".to_vec(),
            }],
        )
        .expect_err("replacement should require explicit precondition");

        assert!(matches!(
            error,
            CoreError::PublishPreconditionFailed(message) if message.contains("explicit precondition")
        ));
    }

    #[test]
    fn publish_segments_rejects_duplicate_pending_families() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("duplicate-pending-family.rax");
        let pending_doc = PendingSegmentDescriptor {
            family: SegmentKind::Doc,
            family_version: 1,
            flags: 0,
            doc_id_start: 0,
            doc_id_end_exclusive: 1,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
            live_items: 1,
            tombstoned_items: 0,
            backend_id: 0,
            backend_aux: 0,
        };

        create_empty_store(&path).expect("store should be created");
        let error = publish_segments_with_precondition(
            &path,
            vec![
                PendingSegmentWrite {
                    descriptor: pending_doc.clone(),
                    object_bytes: b"doc-one".to_vec(),
                },
                PendingSegmentWrite {
                    descriptor: PendingSegmentDescriptor {
                        doc_id_start: 1,
                        doc_id_end_exclusive: 2,
                        ..pending_doc
                    },
                    object_bytes: b"doc-two".to_vec(),
                },
            ],
            |_| Ok(()),
        )
        .expect_err("duplicate pending families should fail");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message)
                if message.contains("at most one pending segment per family")
        ));
        let reopened = open_store(&path).expect("store should reopen");
        assert_eq!(reopened.manifest.generation, 0);
        assert!(reopened.manifest.segments.is_empty());
    }

    #[test]
    fn publish_segment_replaces_only_the_published_family() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("family-replace.rax");

        create_empty_store(&path).expect("store should be created");
        publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"doc-one",
        )
        .expect("first doc publish");
        publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Txt,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"text-one",
        )
        .expect("text publish");
        let opened = replace_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 1,
                doc_id_end_exclusive: 2,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"doc-two",
        )
        .expect("second doc publish");

        assert_eq!(opened.manifest.segments.len(), 2);
        assert_eq!(
            opened
                .manifest
                .segments
                .iter()
                .filter(|segment| segment.family == SegmentKind::Doc)
                .count(),
            1
        );
        assert_eq!(
            opened
                .manifest
                .segments
                .iter()
                .filter(|segment| segment.family == SegmentKind::Txt)
                .count(),
            1
        );
    }

    #[test]
    fn open_store_uses_latest_valid_generation_after_multiple_publishes() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("reopen.rax");

        create_empty_store(&path).expect("store should be created");
        publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"segment-one",
        )
        .expect("first publish");
        let generation_two = replace_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 1,
                doc_id_end_exclusive: 2,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"segment-two",
        )
        .expect("second publish");

        let reopened = open_store(&path).expect("reopen latest");
        assert_eq!(reopened.manifest.generation, 2);
        assert_eq!(reopened.manifest, generation_two.manifest);

        let mut bytes = std::fs::read(&path).expect("store bytes");
        bytes[0] ^= 0xFF;
        std::fs::write(&path, bytes).expect("rewrite store");

        let fallback = open_store(&path).expect("fallback reopen");
        assert_eq!(fallback.manifest.generation, 1);
        assert_eq!(fallback.manifest.segments.len(), 1);
        assert_eq!(
            read_segment_object(&path, &fallback.manifest.segments[0]).expect("fallback object"),
            b"segment-one"
        );
    }

    #[test]
    fn open_store_falls_back_when_latest_manifest_object_is_corrupt() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("manifest-fallback.rax");

        create_empty_store(&path).expect("store should be created");
        publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"segment-one",
        )
        .expect("first publish");
        let generation_two = replace_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 1,
                doc_id_end_exclusive: 2,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"segment-two",
        )
        .expect("second publish");

        let mut bytes = std::fs::read(&path).expect("store bytes");
        let manifest_offset = generation_two.superblock.active_manifest_offset as usize;
        bytes[manifest_offset + OBJECT_HEADER_LENGTH] ^= 0xFF;
        std::fs::write(&path, bytes).expect("rewrite store");

        let fallback = open_store(&path).expect("fallback reopen");
        assert_eq!(fallback.manifest.generation, 1);
        assert_eq!(fallback.manifest.segments.len(), 1);
        assert_eq!(
            read_segment_object(&path, &fallback.manifest.segments[0]).expect("fallback object"),
            b"segment-one"
        );

        let recovered = replace_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 2,
                doc_id_end_exclusive: 3,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"segment-three",
        )
        .expect("publish after fallback");
        assert_eq!(recovered.manifest.generation, 4);
        assert_eq!(
            read_segment_object(&path, &recovered.manifest.segments[0]).expect("recovered object"),
            b"segment-three"
        );
    }

    #[test]
    fn open_store_falls_back_when_latest_segment_object_is_corrupt() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("segment-fallback.rax");

        create_empty_store(&path).expect("store should be created");
        publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"segment-one",
        )
        .expect("first publish");
        let generation_two = replace_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 1,
                doc_id_end_exclusive: 2,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"segment-two",
        )
        .expect("second publish");

        let latest_segment = &generation_two.manifest.segments[0];
        let mut bytes = std::fs::read(&path).expect("store bytes");
        let payload_offset = (latest_segment.object_offset as usize) + OBJECT_HEADER_LENGTH;
        bytes[payload_offset] ^= 0xFF;
        std::fs::write(&path, bytes).expect("rewrite store");

        let fallback = open_store(&path).expect("fallback reopen");
        assert_eq!(fallback.manifest.generation, 1);
        assert_eq!(fallback.manifest.segments.len(), 1);
        assert_eq!(
            read_segment_object(&path, &fallback.manifest.segments[0]).expect("fallback object"),
            b"segment-one"
        );
    }

    #[test]
    fn manifest_rejects_segments_with_identical_object_offsets() {
        let descriptor = SegmentDescriptor {
            family: SegmentKind::Doc,
            family_version: 1,
            flags: 0,
            object_offset: 4_096,
            object_length: 128,
            segment_generation: 1,
            doc_id_start: 0,
            doc_id_end_exclusive: 1,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
            live_items: 1,
            tombstoned_items: 0,
            backend_id: 0,
            backend_aux: 0,
            object_checksum: [1u8; 32],
        };
        let manifest = ActiveManifest {
            generation: 1,
            segments: vec![
                descriptor.clone(),
                SegmentDescriptor {
                    family: SegmentKind::Txt,
                    object_checksum: [2u8; 32],
                    ..descriptor
                },
            ],
        };

        let encoded = manifest.encode().expect("manifest should encode");
        let error = ActiveManifest::decode(&encoded).expect_err("manifest should fail");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message)
                if message.contains("must not overlap")
        ));
    }

    #[test]
    fn manifest_rejects_multiple_active_segments_for_same_family() {
        let descriptor = SegmentDescriptor {
            family: SegmentKind::Doc,
            family_version: 1,
            flags: 0,
            object_offset: 4_096,
            object_length: 128,
            segment_generation: 1,
            doc_id_start: 0,
            doc_id_end_exclusive: 1,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
            live_items: 1,
            tombstoned_items: 0,
            backend_id: 0,
            backend_aux: 0,
            object_checksum: [1u8; 32],
        };
        let manifest = ActiveManifest {
            generation: 1,
            segments: vec![
                descriptor.clone(),
                SegmentDescriptor {
                    object_offset: 8_192,
                    object_checksum: [2u8; 32],
                    ..descriptor
                },
            ],
        };

        let encoded = manifest.encode().expect("manifest should encode");
        let error = ActiveManifest::decode(&encoded).expect_err("manifest should fail");

        assert!(matches!(
            error,
            CoreError::InvalidManifest(message)
                if message.contains("at most one segment per family")
        ));
    }

    #[test]
    fn write_zero_padding_handles_targets_larger_than_default_alignment() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("padding.bin");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("open file");

        write_zero_padding(&mut file, DEFAULT_OBJECT_ALIGNMENT * 3 + 17).expect("write padding");

        let bytes = std::fs::read(&path).expect("read padding");
        assert_eq!(bytes.len() as u64, DEFAULT_OBJECT_ALIGNMENT * 3 + 17);
        assert!(bytes.iter().all(|byte| *byte == 0));
    }

    #[test]
    fn align_up_rejects_file_offset_overflow() {
        let error = align_up(u64::MAX, DEFAULT_OBJECT_ALIGNMENT).expect_err("overflow");
        assert!(
            matches!(error, CoreError::InvalidManifest(message) if message.contains("overflow"))
        );
    }

    #[test]
    fn publish_segments_holds_exclusive_file_lock_while_mutating_store() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("locked.rax");
        create_empty_store(&path).expect("create store");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open store");
        file.lock_exclusive().expect("take exclusive lock");

        let error = publish_segment(
            &path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: 1,
                flags: 0,
                doc_id_start: 0,
                doc_id_end_exclusive: 1,
                min_timestamp_ms: 0,
                max_timestamp_ms: 0,
                live_items: 1,
                tombstoned_items: 0,
                backend_id: 0,
                backend_aux: 0,
            },
            b"locked-segment",
        )
        .expect_err("publish should fail while another writer holds the lock");

        assert!(matches!(
            error,
            CoreError::PublishPreconditionFailed(message)
                if message == STORE_PUBLISH_LOCK_BUSY_MESSAGE
        ));
    }
}
