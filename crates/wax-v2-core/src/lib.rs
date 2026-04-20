use std::fmt;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use sha2::{Digest, Sha256};

const FILE_MAGIC: &[u8; 8] = b"RAXWAXV2";
const MANIFEST_MAGIC: &[u8; 8] = b"RAXMANI1";
const OBJECT_MAGIC: &[u8; 4] = b"WXOB";
const FORMAT_VERSION: u32 = 1;
const SUPERBLOCK_CHECKSUM_OFFSET: usize = 64;
const SUPERBLOCK_CHECKSUM_LENGTH: usize = 32;
const MANIFEST_HEADER_LENGTH: usize = 24;
const SEGMENT_DESCRIPTOR_LENGTH: usize = 128;
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
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            MANIFEST_HEADER_LENGTH + self.segments.len() * SEGMENT_DESCRIPTOR_LENGTH,
        );
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.generation.to_le_bytes());
        bytes.extend_from_slice(&(self.segments.len() as u32).to_le_bytes());

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

        bytes
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
        let expected_length = MANIFEST_HEADER_LENGTH + segment_count * SEGMENT_DESCRIPTOR_LENGTH;
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

        let left_end = left
            .object_offset
            .checked_add(left.object_length)
            .ok_or_else(|| {
                CoreError::InvalidManifest("segment object range overflow".to_owned())
            })?;
        if left.object_offset < right.object_offset && left_end > right.object_offset {
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

pub fn create_empty_store(path: &Path) -> Result<(), CoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let manifest = ActiveManifest {
        generation: 0,
        segments: Vec::new(),
    };
    let manifest_bytes = manifest.encode();
    let manifest_object = encode_object(
        ObjectType::Manifest,
        manifest.generation,
        DEFAULT_OBJECT_ALIGNMENT,
        &manifest_bytes,
    );
    let manifest_offset = align_up((SUPERBLOCK_SIZE * 2) as u64, DEFAULT_OBJECT_ALIGNMENT);
    let superblock = Superblock::new(
        manifest.generation,
        manifest_offset,
        manifest_object.len() as u32,
        ActiveManifest::checksum(&manifest_bytes),
    );

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
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
    let mut bytes = Vec::new();
    OpenOptions::new()
        .read(true)
        .open(path)?
        .read_to_end(&mut bytes)?;

    if bytes.len() < SUPERBLOCK_SIZE * 2 {
        return Err(CoreError::UnexpectedLength {
            context: "store",
            expected_at_least: SUPERBLOCK_SIZE * 2,
            actual: bytes.len(),
        });
    }

    let candidate_a = Superblock::decode(&bytes[..SUPERBLOCK_SIZE]).ok();
    let candidate_b = Superblock::decode(&bytes[SUPERBLOCK_SIZE..SUPERBLOCK_SIZE * 2]).ok();
    let Some(candidates) = ordered_superblock_candidates(candidate_a, candidate_b) else {
        return Err(CoreError::NoValidSuperblock);
    };

    let mut last_error = None;
    for candidate in candidates {
        match open_store_from_superblock(&bytes, candidate) {
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
    if pending_segments.is_empty() {
        return Err(CoreError::InvalidManifest(
            "publish_segments requires at least one pending segment".to_owned(),
        ));
    }

    let opened = open_store(path)?;
    let new_generation = opened
        .manifest
        .generation
        .checked_add(1)
        .ok_or_else(|| CoreError::InvalidManifest("manifest generation overflow".to_owned()))?;

    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut segments = opened.manifest.segments.clone();
    for pending_segment in pending_segments {
        let object_type = object_type_for_family(pending_segment.descriptor.family);
        let (object_offset, object_length) = append_object(
            &mut file,
            object_type,
            new_generation,
            DEFAULT_OBJECT_ALIGNMENT,
            &pending_segment.object_bytes,
        )?;
        let published_segment = pending_segment.descriptor.publish(
            object_offset,
            object_length,
            new_generation,
            sha256(&pending_segment.object_bytes),
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
    let manifest_bytes = manifest.encode();
    let (manifest_offset, manifest_length) = append_object(
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
        manifest_offset,
        manifest_length as u32,
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

    open_store(path)
}

pub fn read_segment_object(
    path: &Path,
    descriptor: &SegmentDescriptor,
) -> Result<Vec<u8>, CoreError> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let file_length = file.seek(SeekFrom::End(0))?;
    let object_end = descriptor
        .object_offset
        .checked_add(descriptor.object_length)
        .ok_or_else(|| CoreError::InvalidManifest("segment object range overflow".to_owned()))?;
    if object_end > file_length {
        return Err(CoreError::InvalidManifest(
            "segment object range extends past end of file".to_owned(),
        ));
    }

    let mut bytes = vec![0u8; descriptor.object_length as usize];
    file.seek(SeekFrom::Start(descriptor.object_offset))?;
    file.read_exact(&mut bytes)?;
    let payload = decode_object(
        &bytes,
        object_type_for_family(descriptor.family),
        descriptor.segment_generation,
    )?;
    if sha256(&payload) != descriptor.object_checksum {
        return Err(CoreError::ChecksumMismatch {
            context: "segment object",
        });
    }
    Ok(payload)
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
    bytes: &[u8],
    active_superblock: Superblock,
) -> Result<OpenedStore, CoreError> {
    let manifest_offset = active_superblock.active_manifest_offset as usize;
    let manifest_length = active_superblock.active_manifest_length as usize;
    let manifest_end = manifest_offset
        .checked_add(manifest_length)
        .ok_or_else(|| CoreError::InvalidManifest("manifest offset overflow".to_owned()))?;

    if manifest_end > bytes.len() {
        return Err(CoreError::InvalidManifest(
            "manifest range extends past end of file".to_owned(),
        ));
    }

    let manifest_object = &bytes[manifest_offset..manifest_end];
    let manifest_bytes = decode_object(
        manifest_object,
        ObjectType::Manifest,
        active_superblock.generation,
    )?;
    let manifest_checksum = ActiveManifest::checksum(&manifest_bytes);
    if manifest_checksum != active_superblock.manifest_checksum {
        return Err(CoreError::ChecksumMismatch {
            context: "manifest",
        });
    }

    let manifest = ActiveManifest::decode(&manifest_bytes)?;
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

fn align_up(value: u64, alignment: u64) -> u64 {
    if alignment == 0 {
        return value;
    }
    let remainder = value % alignment;
    if remainder == 0 {
        value
    } else {
        value + (alignment - remainder)
    }
}

fn write_zero_padding(file: &mut OpenOptionsFile, target_offset: u64) -> Result<(), CoreError> {
    let current_offset = file.seek(SeekFrom::End(0))?;
    if target_offset < current_offset {
        return Err(CoreError::InvalidManifest(
            "target offset moved backwards".to_owned(),
        ));
    }
    let padding = (target_offset - current_offset) as usize;
    if padding > 0 {
        file.write_all(&vec![0u8; padding])?;
    }
    Ok(())
}

type OpenOptionsFile = std::fs::File;

fn append_object(
    file: &mut OpenOptionsFile,
    object_type: ObjectType,
    logical_generation: u64,
    alignment: u64,
    payload: &[u8],
) -> Result<(u64, u64), CoreError> {
    let current_end = file.seek(SeekFrom::End(0))?;
    let object_offset = align_up(current_end, alignment.max(DEFAULT_OBJECT_ALIGNMENT));
    write_zero_padding(file, object_offset)?;
    let object_bytes = encode_object(object_type, logical_generation, alignment, payload);
    file.write_all(&object_bytes)?;
    Ok((object_offset, object_bytes.len() as u64))
}

fn encode_object(
    object_type: ObjectType,
    logical_generation: u64,
    alignment: u64,
    payload: &[u8],
) -> Vec<u8> {
    let mut bytes = vec![0u8; OBJECT_HEADER_LENGTH];
    bytes[..4].copy_from_slice(OBJECT_MAGIC);
    bytes[4..6].copy_from_slice(&object_type.as_code().to_le_bytes());
    bytes[6..8].copy_from_slice(&OBJECT_VERSION.to_le_bytes());
    bytes[8..16].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes[16..24].copy_from_slice(&logical_generation.to_le_bytes());
    bytes[24..32].copy_from_slice(&alignment.to_le_bytes());
    bytes[32..64].copy_from_slice(&sha256(payload));
    bytes.extend_from_slice(payload);
    bytes
}

fn decode_object(
    object_bytes: &[u8],
    expected_type: ObjectType,
    expected_generation: u64,
) -> Result<Vec<u8>, CoreError> {
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
    let object_length = read_u64(object_bytes, 8) as usize;
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
    let payload = object_bytes[OBJECT_HEADER_LENGTH..payload_end].to_vec();
    let mut expected_checksum = [0u8; 32];
    expected_checksum.copy_from_slice(&object_bytes[32..64]);
    if sha256(&payload) != expected_checksum {
        return Err(CoreError::ChecksumMismatch { context: "object" });
    }
    Ok(payload)
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
    use tempfile::tempdir;

    use crate::{
        create_empty_store, open_store, publish_segment, read_segment_object, ActiveManifest,
        CoreError, PendingSegmentDescriptor, SegmentDescriptor, SegmentKind, Superblock,
        OBJECT_HEADER_LENGTH, OBJECT_MAGIC, SUPERBLOCK_SIZE,
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
        let manifest_bytes = manifest.encode();
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

        let encoded = manifest.encode();
        let decoded = ActiveManifest::decode(&encoded).expect("manifest should decode");

        assert_eq!(decoded, manifest);
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

        let encoded = manifest.encode();
        let error =
            ActiveManifest::decode(&encoded).expect_err("manifest should reject invalid ranges");

        assert!(matches!(error, CoreError::InvalidManifest(message) if message.contains("doc_id")));
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
        assert_eq!(generation_two.manifest.segments.len(), 2);
        assert_eq!(
            read_segment_object(&path, &snapshot.manifest.segments[0]).expect("snapshot object"),
            b"segment-one"
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
}
