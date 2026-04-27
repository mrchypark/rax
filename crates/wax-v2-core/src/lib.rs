use std::fmt;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;

use fs2::FileExt;
use memmap2::{Mmap, MmapOptions};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

const FILE_MAGIC: &[u8; 8] = b"RAXWAXV2";
const MANIFEST_MAGIC: &[u8; 8] = b"RAXMANI1";
const OBJECT_MAGIC: &[u8; 4] = b"WXOB";
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

pub const SUPERBLOCK_SIZE: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    Io(String),
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
        Self::Io(error.to_string())
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

        validate_segments(&segments)?;

        Ok(Self {
            generation,
            segments,
        })
    }

    pub fn checksum(bytes: &[u8]) -> [u8; 32] {
        sha256(bytes)
    }
}

fn validate_segments(segments: &[SegmentDescriptor]) -> Result<(), CoreError> {
    for segment in segments {
        if segment.object_length == 0 {
            return Err(CoreError::InvalidManifest(
                "segment descriptor object length must be non-zero".to_owned(),
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
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
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

    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    let mut file = open_no_follow(&mut options, path)?;
    let encoded_superblock = superblock.encode();
    file.write_all(&encoded_superblock)?;
    file.write_all(&encoded_superblock)?;
    write_zero_padding(&mut file, manifest_offset)?;
    file.write_all(&manifest_object)?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

pub fn open_store(path: &Path) -> Result<OpenedStore, CoreError> {
    let mut options = OpenOptions::new();
    options.read(true);
    let mut file = open_no_follow(&mut options, path)?;
    open_store_from_file(&mut file)
}

fn open_store_from_file(file: &mut OpenOptionsFile) -> Result<OpenedStore, CoreError> {
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

    let candidate_a = Superblock::decode(&superblock_bytes[..SUPERBLOCK_SIZE]).ok();
    let candidate_b =
        Superblock::decode(&superblock_bytes[SUPERBLOCK_SIZE..SUPERBLOCK_SIZE * 2]).ok();
    let Some(candidates) = ordered_superblock_candidates(candidate_a, candidate_b) else {
        return Err(CoreError::NoValidSuperblock);
    };

    let mut last_error = None;
    for candidate in candidates {
        match open_store_from_superblock(file, file_length, candidate) {
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
    publish_segments_with_precondition(path, pending_segments, |_| Ok(()))
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

    let mut options = OpenOptions::new();
    options.read(true).write(true);
    let mut file = open_no_follow(&mut options, path)?;
    file.try_lock_exclusive()?;

    let opened = open_store_from_file(&mut file)?;
    precondition(&opened.manifest)?;
    let new_generation = opened
        .manifest
        .generation
        .checked_add(1)
        .ok_or_else(|| CoreError::InvalidManifest("manifest generation overflow".to_owned()))?;
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
    validate_segments(&segments)?;

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
    let superblock_offset = if new_generation % 2 == 0 {
        0
    } else {
        SUPERBLOCK_SIZE as u64
    };
    file.seek(SeekFrom::Start(superblock_offset))?;
    file.write_all(&superblock.encode())?;
    file.flush()?;
    file.sync_all()?;

    open_store_from_file(&mut file)
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
    let mut options = OpenOptions::new();
    options.read(true);
    let file = open_no_follow(&mut options, path)?;
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
    if descriptor
        .object_offset
        .checked_rem(DEFAULT_OBJECT_ALIGNMENT)
        != Some(0)
    {
        return Err(CoreError::InvalidManifest(
            "segment object offset must use store object alignment".to_owned(),
        ));
    }

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
    let decoded = decode_object_payload_header(
        &mapped[map_prefix..object_end],
        object_type_for_family(descriptor.family),
        descriptor.segment_generation,
    )?;
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
    left: Option<Superblock>,
    right: Option<Superblock>,
) -> Option<Vec<Superblock>> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if left.generation >= right.generation {
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

fn open_store_from_superblock(
    file: &mut OpenOptionsFile,
    file_length: u64,
    active_superblock: Superblock,
) -> Result<OpenedStore, CoreError> {
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

    Ok(OpenedStore {
        superblock: active_superblock,
        manifest,
    })
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

fn open_no_follow(options: &mut OpenOptions, path: &Path) -> Result<OpenOptionsFile, CoreError> {
    #[cfg(unix)]
    {
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options.open(path).map_err(CoreError::from)
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
    if payload_end > object_bytes.len() {
        return Err(CoreError::InvalidManifest(
            "object payload extends past end of object".to_owned(),
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
    use std::fs::OpenOptions;

    use fs2::FileExt;
    use tempfile::tempdir;

    use crate::{
        align_up, create_empty_store, decode_object_payload, default_mmap_allocation_granularity,
        map_segment_object, open_store, publish_segment, publish_segments_with_precondition,
        read_segment_object, write_zero_padding, ActiveManifest, CoreError, ObjectType,
        PendingSegmentDescriptor, PendingSegmentWrite, SegmentDescriptor, SegmentKind,
        SegmentObjectBacking, Superblock, DEFAULT_OBJECT_ALIGNMENT, FORMAT_VERSION,
        MANIFEST_HEADER_LENGTH, MANIFEST_MAGIC, MAX_MANIFEST_OBJECT_LENGTH, OBJECT_HEADER_LENGTH,
        OBJECT_MAGIC, SEGMENT_DESCRIPTOR_LENGTH, SUPERBLOCK_SIZE,
    };

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
    fn manifest_round_trips_full_segment_descriptor_shape() {
        let manifest = ActiveManifest {
            generation: 3,
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
            generation: 3,
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
            generation: 3,
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
            generation: 3,
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
        assert_eq!(fallback % DEFAULT_OBJECT_ALIGNMENT, 0);
    }

    #[test]
    fn map_segment_object_rejects_unaligned_descriptor_offsets() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("unaligned.wax");
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
        let path = temp_dir.path().join("empty.wax");

        create_empty_store(&path).expect("store should be created");
        let opened = open_store(&path).expect("store should reopen");
        let bytes = std::fs::read(&path).expect("store bytes");

        assert_eq!(opened.manifest.generation, 0);
        assert!(opened.manifest.segments.is_empty());
        assert_eq!(opened.superblock.active_manifest_offset % 4096, 0);
        assert_eq!(
            &bytes[opened.superblock.active_manifest_offset as usize
                ..opened.superblock.active_manifest_offset as usize + 4],
            OBJECT_MAGIC
        );
    }

    #[test]
    fn create_empty_store_rejects_existing_path() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("empty.wax");
        std::fs::write(&path, b"existing").expect("seed existing file");

        let error = create_empty_store(&path).expect_err("existing store path should fail");
        assert!(matches!(error, CoreError::Io(_)));
    }

    #[cfg(unix)]
    #[test]
    fn open_store_rejects_symlink_leaf_paths() {
        let temp_dir = tempdir().expect("tempdir");
        let target = temp_dir.path().join("target.wax");
        let link = temp_dir.path().join("link.wax");
        create_empty_store(&target).expect("store should be created");
        std::os::unix::fs::symlink(&target, &link).expect("symlink should be created");

        let error = open_store(&link).expect_err("symlink store path should fail");

        assert!(matches!(error, CoreError::Io(_)));
    }

    #[test]
    fn open_store_rejects_corrupt_superblock_checksum() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("corrupt.wax");

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
        let path = temp_dir.path().join("oversized-manifest.wax");

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
        let path = temp_dir.path().join("published.wax");
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
        assert_eq!(reopened.manifest.segments[0].object_offset % 4096, 0);
        assert_eq!(reopened.superblock.active_manifest_offset % 4096, 0);
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
        let path = temp_dir.path().join("mapped.wax");

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
    fn opened_store_snapshot_remains_stable_after_later_publish() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("snapshot.wax");

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

        let generation_two = publish_segment(
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
        let path = temp_dir.path().join("precondition.wax");

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
    fn publish_segment_replaces_only_the_published_family() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("family-replace.wax");

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
        let opened = publish_segment(
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
        let path = temp_dir.path().join("reopen.wax");

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
        let generation_two = publish_segment(
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
        let path = temp_dir.path().join("manifest-fallback.wax");

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
        let generation_two = publish_segment(
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
        let path = temp_dir.path().join("locked.wax");
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

        assert!(matches!(error, CoreError::Io(_)));
    }
}
