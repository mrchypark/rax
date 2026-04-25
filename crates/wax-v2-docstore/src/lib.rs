use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use wax_bench_model::DatasetPackManifest;
use wax_v2_core::{OpenedStore, PendingSegmentDescriptor, PendingSegmentWrite, SegmentKind};

const DOC_SEGMENT_MAGIC: &[u8; 4] = b"WXDG";
const DOC_SEGMENT_MAJOR: u16 = 1;
const DOC_SEGMENT_MINOR: u16 = 1;
const DOC_SEGMENT_HEADER_LENGTH: usize = 88;
const DOC_ROW_LENGTH: usize = 56;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocstoreError {
    Io(String),
    MissingDocumentsFile,
    Json(String),
    InvalidDocument(String),
}

impl From<std::io::Error> for DocstoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<serde_json::Error> for DocstoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, serde::Serialize)]
pub struct DocIdBinding {
    pub wax_doc_id: u64,
    pub external_doc_id: String,
}

impl DocIdBinding {
    pub fn new(wax_doc_id: u64, external_doc_id: impl Into<String>) -> Self {
        Self {
            wax_doc_id,
            external_doc_id: external_doc_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocIdMap {
    bindings: Vec<DocIdBinding>,
    by_external_doc_id: HashMap<String, u64>,
    by_wax_doc_id: HashMap<u64, String>,
}

impl DocIdMap {
    pub fn from_document_order(document_ids: &[String]) -> Result<Self, DocstoreError> {
        let bindings = document_ids
            .iter()
            .enumerate()
            .map(|(index, document_id)| DocIdBinding::new(index as u64, document_id.clone()))
            .collect();
        Self::from_bindings(bindings)
    }

    pub fn from_bindings(bindings: Vec<DocIdBinding>) -> Result<Self, DocstoreError> {
        let mut by_external_doc_id = HashMap::with_capacity(bindings.len());
        let mut by_wax_doc_id = HashMap::with_capacity(bindings.len());

        for binding in &bindings {
            if by_external_doc_id
                .insert(binding.external_doc_id.clone(), binding.wax_doc_id)
                .is_some()
            {
                return Err(DocstoreError::InvalidDocument(
                    "duplicate external doc_id binding".to_owned(),
                ));
            }
            if by_wax_doc_id
                .insert(binding.wax_doc_id, binding.external_doc_id.clone())
                .is_some()
            {
                return Err(DocstoreError::InvalidDocument(
                    "duplicate wax_doc_id binding".to_owned(),
                ));
            }
        }

        Ok(Self {
            bindings,
            by_external_doc_id,
            by_wax_doc_id,
        })
    }

    pub fn bindings(&self) -> &[DocIdBinding] {
        &self.bindings
    }

    pub fn wax_doc_id(&self, external_doc_id: &str) -> Option<u64> {
        self.by_external_doc_id.get(external_doc_id).copied()
    }

    pub fn external_doc_id(&self, wax_doc_id: u64) -> Option<&str> {
        self.by_wax_doc_id.get(&wax_doc_id).map(String::as_str)
    }

    pub fn encode_json(&self) -> Result<Vec<u8>, DocstoreError> {
        serde_json::to_vec(&self.bindings).map_err(Into::into)
    }

    pub fn decode_json(bytes: &[u8]) -> Result<Self, DocstoreError> {
        let bindings = serde_json::from_slice(bytes)?;
        Self::from_bindings(bindings)
    }

    pub fn next_wax_doc_id(&self) -> u64 {
        self.bindings
            .iter()
            .map(|binding| binding.wax_doc_id)
            .max()
            .map(|value| value + 1)
            .unwrap_or(0)
    }

    pub fn extend_to_cover_document_order(
        &self,
        document_ids: &[String],
    ) -> Result<Self, DocstoreError> {
        let mut bindings = self.bindings.clone();
        let mut next_wax_doc_id = self.next_wax_doc_id();
        let mut seen = HashSet::with_capacity(document_ids.len());

        for document_id in document_ids {
            if !seen.insert(document_id.clone()) {
                return Err(DocstoreError::InvalidDocument(
                    "duplicate external doc_id in current document order".to_owned(),
                ));
            }
            if self.wax_doc_id(document_id).is_none() {
                bindings.push(DocIdBinding::new(next_wax_doc_id, document_id.clone()));
                next_wax_doc_id += 1;
            }
        }

        bindings.sort_by_key(|binding| binding.wax_doc_id);
        Self::from_bindings(bindings)
    }

    pub fn bindings_for_external_doc_ids_sorted(
        &self,
        document_ids: &[String],
    ) -> Result<Vec<DocIdBinding>, DocstoreError> {
        let mut seen = HashSet::with_capacity(document_ids.len());
        let mut bindings = Vec::with_capacity(document_ids.len());

        for document_id in document_ids {
            if !seen.insert(document_id.clone()) {
                return Err(DocstoreError::InvalidDocument(
                    "duplicate external doc_id in current document order".to_owned(),
                ));
            }
            let wax_doc_id = self.wax_doc_id(document_id).ok_or_else(|| {
                DocstoreError::InvalidDocument(format!(
                    "missing wax doc id binding for {document_id}"
                ))
            })?;
            bindings.push(DocIdBinding::new(wax_doc_id, document_id.clone()));
        }

        bindings.sort_by_key(|binding| binding.wax_doc_id);
        Ok(bindings)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectionRef {
    packed: u64,
}

impl SectionRef {
    pub fn new(offset: u32, length: u32) -> Self {
        Self {
            packed: ((offset as u64) << 32) | length as u64,
        }
    }

    pub fn offset(self) -> u32 {
        (self.packed >> 32) as u32
    }

    pub fn length(self) -> u32 {
        self.packed as u32
    }

    fn packed(self) -> u64 {
        self.packed
    }

    fn from_packed(packed: u64) -> Self {
        Self { packed }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocRow {
    pub doc_id: u64,
    pub timestamp_ms: u64,
    pub flags: u32,
    pub payload_offset: u64,
    pub payload_length: u64,
    pub metadata_ref: SectionRef,
    pub preview_ref: SectionRef,
}

impl DocRow {
    pub const FLAG_TOMBSTONE: u32 = 1;

    pub fn is_tombstone(&self) -> bool {
        self.flags & Self::FLAG_TOMBSTONE != 0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocSegmentRecord {
    pub row: DocRow,
    pub payload: Vec<u8>,
    pub metadata: Value,
    pub preview: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryDocSegment {
    pub doc_id_map: DocIdMap,
    pub records: Vec<DocSegmentRecord>,
}

impl BinaryDocSegment {
    pub fn encode(&self) -> Result<Vec<u8>, DocstoreError> {
        validate_doc_id_order(&self.records)?;

        let mut payload_section = Vec::new();
        let mut encoded_rows = Vec::with_capacity(self.records.len() * DOC_ROW_LENGTH);
        let mut metadata_entries = Vec::with_capacity(self.records.len());
        let mut preview_entries = Vec::with_capacity(self.records.len());

        for record in &self.records {
            let expected_payload_offset = payload_section.len() as u64;
            let expected_payload_length = record.payload.len() as u64;
            if record.row.payload_offset != expected_payload_offset {
                return Err(DocstoreError::InvalidDocument(format!(
                    "payload_offset mismatch for doc_id {}",
                    record.row.doc_id
                )));
            }
            if record.row.payload_length != expected_payload_length {
                return Err(DocstoreError::InvalidDocument(format!(
                    "payload_length mismatch for doc_id {}",
                    record.row.doc_id
                )));
            }
            payload_section.extend_from_slice(&record.payload);

            metadata_entries.push((
                record.row.metadata_ref,
                serde_json::to_vec(&record.metadata)?,
            ));
            preview_entries.push((
                record.row.preview_ref,
                record
                    .preview
                    .as_ref()
                    .map(|preview| preview.as_bytes().to_vec())
                    .unwrap_or_default(),
            ));

            encoded_rows.extend_from_slice(&record.row.doc_id.to_le_bytes());
            encoded_rows.extend_from_slice(&record.row.timestamp_ms.to_le_bytes());
            encoded_rows.extend_from_slice(&record.row.flags.to_le_bytes());
            encoded_rows.extend_from_slice(&0u32.to_le_bytes());
            encoded_rows.extend_from_slice(&record.row.payload_offset.to_le_bytes());
            encoded_rows.extend_from_slice(&record.row.payload_length.to_le_bytes());
            encoded_rows.extend_from_slice(&record.row.metadata_ref.packed().to_le_bytes());
            encoded_rows.extend_from_slice(&record.row.preview_ref.packed().to_le_bytes());
        }

        let metadata_section = build_ref_section(&metadata_entries, "metadata")?;
        let preview_section = build_ref_section(&preview_entries, "preview")?;
        let payload_bytes_offset = DOC_SEGMENT_HEADER_LENGTH as u64;
        let metadata_bytes_offset = payload_bytes_offset + payload_section.len() as u64;
        let preview_bytes_offset = metadata_bytes_offset + metadata_section.len() as u64;
        let binding_section = self.doc_id_map.encode_json()?;
        let binding_bytes_offset = preview_bytes_offset + preview_section.len() as u64;
        let row_table_offset = binding_bytes_offset + binding_section.len() as u64;

        let mut body = Vec::with_capacity(
            payload_section.len()
                + metadata_section.len()
                + preview_section.len()
                + binding_section.len()
                + encoded_rows.len(),
        );
        body.extend_from_slice(&payload_section);
        body.extend_from_slice(&metadata_section);
        body.extend_from_slice(&preview_section);
        body.extend_from_slice(&binding_section);
        body.extend_from_slice(&encoded_rows);
        let checksum = sha256(&body);

        let mut bytes = Vec::with_capacity(DOC_SEGMENT_HEADER_LENGTH + body.len());
        bytes.extend_from_slice(DOC_SEGMENT_MAGIC);
        bytes.extend_from_slice(&DOC_SEGMENT_MAJOR.to_le_bytes());
        bytes.extend_from_slice(&DOC_SEGMENT_MINOR.to_le_bytes());
        bytes.extend_from_slice(&(self.records.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&payload_bytes_offset.to_le_bytes());
        bytes.extend_from_slice(&metadata_bytes_offset.to_le_bytes());
        bytes.extend_from_slice(&preview_bytes_offset.to_le_bytes());
        bytes.extend_from_slice(&binding_bytes_offset.to_le_bytes());
        bytes.extend_from_slice(&row_table_offset.to_le_bytes());
        bytes.extend_from_slice(&checksum);
        bytes.extend_from_slice(&body);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DocstoreError> {
        if bytes.len() < DOC_SEGMENT_HEADER_LENGTH {
            return Err(DocstoreError::InvalidDocument(format!(
                "doc segment too short: expected at least {DOC_SEGMENT_HEADER_LENGTH} bytes"
            )));
        }
        if &bytes[..4] != DOC_SEGMENT_MAGIC {
            return Err(DocstoreError::InvalidDocument(
                "doc segment magic mismatch".to_owned(),
            ));
        }
        let major = read_u16(bytes, 4);
        let minor = read_u16(bytes, 6);
        if major != DOC_SEGMENT_MAJOR || minor > DOC_SEGMENT_MINOR {
            return Err(DocstoreError::InvalidDocument(
                "unsupported doc segment version".to_owned(),
            ));
        }

        let row_count = read_u64(bytes, 8) as usize;
        let payload_bytes_offset = read_u64(bytes, 16) as usize;
        let metadata_bytes_offset = read_u64(bytes, 24) as usize;
        let preview_bytes_offset = read_u64(bytes, 32) as usize;
        let (binding_bytes_offset, row_table_offset, checksum_start, checksum_end, header_length) =
            if minor == 0 {
                (
                    preview_bytes_offset,
                    read_u64(bytes, 40) as usize,
                    48,
                    80,
                    80,
                )
            } else {
                (
                    read_u64(bytes, 40) as usize,
                    read_u64(bytes, 48) as usize,
                    56,
                    88,
                    88,
                )
            };
        if payload_bytes_offset != header_length
            || payload_bytes_offset > metadata_bytes_offset
            || metadata_bytes_offset > preview_bytes_offset
            || preview_bytes_offset > binding_bytes_offset
            || binding_bytes_offset > row_table_offset
            || preview_bytes_offset > row_table_offset
            || row_table_offset > bytes.len()
        {
            return Err(DocstoreError::InvalidDocument(
                "doc segment section offsets are invalid".to_owned(),
            ));
        }

        let mut contents_checksum = [0u8; 32];
        contents_checksum.copy_from_slice(&bytes[checksum_start..checksum_end]);
        let body = &bytes[header_length..];
        if sha256(body) != contents_checksum {
            return Err(DocstoreError::InvalidDocument(
                "doc segment checksum mismatch".to_owned(),
            ));
        }

        let row_table_length = row_count.checked_mul(DOC_ROW_LENGTH).ok_or_else(|| {
            DocstoreError::InvalidDocument("row table length overflow".to_owned())
        })?;
        let row_table_end = row_table_offset
            .checked_add(row_table_length)
            .ok_or_else(|| DocstoreError::InvalidDocument("row table range overflow".to_owned()))?;
        if row_table_end != bytes.len() {
            return Err(DocstoreError::InvalidDocument(
                "doc segment row table length mismatch".to_owned(),
            ));
        }

        let payload_section = &bytes[payload_bytes_offset..metadata_bytes_offset];
        let metadata_section = &bytes[metadata_bytes_offset..preview_bytes_offset];
        let preview_section = &bytes[preview_bytes_offset..binding_bytes_offset];
        let binding_section = &bytes[binding_bytes_offset..row_table_offset];
        let row_table = &bytes[row_table_offset..row_table_end];

        let mut records = Vec::with_capacity(row_count);
        let mut previous_doc_id = None;
        for row_bytes in row_table.chunks_exact(DOC_ROW_LENGTH) {
            let row = DocRow {
                doc_id: read_u64(row_bytes, 0),
                timestamp_ms: read_u64(row_bytes, 8),
                flags: read_u32(row_bytes, 16),
                payload_offset: read_u64(row_bytes, 24),
                payload_length: read_u64(row_bytes, 32),
                metadata_ref: SectionRef::from_packed(read_u64(row_bytes, 40)),
                preview_ref: SectionRef::from_packed(read_u64(row_bytes, 48)),
            };

            if previous_doc_id.is_some_and(|previous| previous >= row.doc_id) {
                return Err(DocstoreError::InvalidDocument(
                    "doc rows must be sorted by doc_id".to_owned(),
                ));
            }
            previous_doc_id = Some(row.doc_id);

            let payload = read_section_bytes(
                payload_section,
                row.payload_offset,
                row.payload_length,
                "payload",
            )?
            .to_vec();
            let metadata_bytes = read_section_bytes(
                metadata_section,
                row.metadata_ref.offset() as u64,
                row.metadata_ref.length() as u64,
                "metadata",
            )?;
            let metadata = serde_json::from_slice(metadata_bytes)?;
            let preview_bytes = read_section_bytes(
                preview_section,
                row.preview_ref.offset() as u64,
                row.preview_ref.length() as u64,
                "preview",
            )?;
            let preview = if preview_bytes.is_empty() {
                None
            } else {
                Some(
                    String::from_utf8(preview_bytes.to_vec())
                        .map_err(|error| DocstoreError::InvalidDocument(error.to_string()))?,
                )
            };

            records.push(DocSegmentRecord {
                row,
                payload,
                metadata,
                preview,
            });
        }

        let doc_id_map = if minor == 0 {
            doc_id_map_from_records(&records)?
        } else {
            DocIdMap::decode_json(binding_section)?
        };

        Ok(Self {
            doc_id_map,
            records,
        })
    }
}

#[derive(Debug)]
pub struct Docstore {
    source: DocstoreSource,
}

#[derive(Debug)]
enum DocstoreSource {
    DatasetPack {
        documents_path: PathBuf,
        offset_index: Option<HashMap<String, DocumentOffsetEntry>>,
    },
    Store {
        segment: StoreDocSegment,
    },
}

#[derive(Debug)]
struct StoreDocSegment {
    minor: u16,
    bytes: Arc<wax_v2_core::SegmentObject>,
    payload_range: Range<usize>,
    rows_by_wax_doc_id: Vec<StoreDocEntry>,
    ordered_doc_ids: Vec<String>,
    entries_by_external_doc_id: HashMap<String, StoreDocEntry>,
    doc_id_map: Option<DocIdMap>,
}

#[derive(Debug, Clone)]
struct StoreDocEntry {
    row: DocRow,
}

impl StoreDocSegment {
    fn open(bytes: wax_v2_core::SegmentObject) -> Result<Self, DocstoreError> {
        let bytes = Arc::new(bytes);
        if bytes.len() < DOC_SEGMENT_HEADER_LENGTH {
            return Err(DocstoreError::InvalidDocument(format!(
                "doc segment too short: expected at least {DOC_SEGMENT_HEADER_LENGTH} bytes"
            )));
        }
        if &bytes[..4] != DOC_SEGMENT_MAGIC {
            return Err(DocstoreError::InvalidDocument(
                "doc segment magic mismatch".to_owned(),
            ));
        }
        let major = read_u16(bytes.as_ref(), 4);
        let minor = read_u16(bytes.as_ref(), 6);
        if major != DOC_SEGMENT_MAJOR || minor > DOC_SEGMENT_MINOR {
            return Err(DocstoreError::InvalidDocument(
                "unsupported doc segment version".to_owned(),
            ));
        }

        let row_count = read_u64(bytes.as_ref(), 8) as usize;
        let payload_bytes_offset = read_u64(bytes.as_ref(), 16) as usize;
        let metadata_bytes_offset = read_u64(bytes.as_ref(), 24) as usize;
        let preview_bytes_offset = read_u64(bytes.as_ref(), 32) as usize;
        let (binding_bytes_offset, row_table_offset, checksum_start, checksum_end, header_length) =
            if minor == 0 {
                (
                    preview_bytes_offset,
                    read_u64(bytes.as_ref(), 40) as usize,
                    48,
                    80,
                    80,
                )
            } else {
                (
                    read_u64(bytes.as_ref(), 40) as usize,
                    read_u64(bytes.as_ref(), 48) as usize,
                    56,
                    88,
                    88,
                )
            };
        if payload_bytes_offset != header_length
            || payload_bytes_offset > metadata_bytes_offset
            || metadata_bytes_offset > preview_bytes_offset
            || preview_bytes_offset > binding_bytes_offset
            || binding_bytes_offset > row_table_offset
            || row_table_offset > bytes.len()
        {
            return Err(DocstoreError::InvalidDocument(
                "doc segment section offsets are invalid".to_owned(),
            ));
        }

        let mut contents_checksum = [0u8; 32];
        contents_checksum.copy_from_slice(&bytes[checksum_start..checksum_end]);
        if sha256(&bytes[header_length..]) != contents_checksum {
            return Err(DocstoreError::InvalidDocument(
                "doc segment checksum mismatch".to_owned(),
            ));
        }

        let row_table_length = row_count.checked_mul(DOC_ROW_LENGTH).ok_or_else(|| {
            DocstoreError::InvalidDocument("row table length overflow".to_owned())
        })?;
        let row_table_end = row_table_offset
            .checked_add(row_table_length)
            .ok_or_else(|| DocstoreError::InvalidDocument("row table range overflow".to_owned()))?;
        if row_table_end != bytes.len() {
            return Err(DocstoreError::InvalidDocument(
                "doc segment row table length mismatch".to_owned(),
            ));
        }

        let binding_section = &bytes[binding_bytes_offset..row_table_offset];
        let row_table = &bytes[row_table_offset..row_table_end];
        let mut previous_doc_id = None;
        let doc_id_map = if minor == 0 {
            None
        } else {
            Some(DocIdMap::decode_json(binding_section)?)
        };
        let mut rows_by_wax_doc_id = Vec::with_capacity(row_count);
        let mut ordered_doc_ids = Vec::with_capacity(row_count);
        let mut entries_by_external_doc_id = HashMap::with_capacity(row_count);

        for row_bytes in row_table.chunks_exact(DOC_ROW_LENGTH) {
            let row = DocRow {
                doc_id: read_u64(row_bytes, 0),
                timestamp_ms: read_u64(row_bytes, 8),
                flags: read_u32(row_bytes, 16),
                payload_offset: read_u64(row_bytes, 24),
                payload_length: read_u64(row_bytes, 32),
                metadata_ref: SectionRef::from_packed(read_u64(row_bytes, 40)),
                preview_ref: SectionRef::from_packed(read_u64(row_bytes, 48)),
            };

            if previous_doc_id.is_some_and(|previous| previous >= row.doc_id) {
                return Err(DocstoreError::InvalidDocument(
                    "doc rows must be sorted by doc_id".to_owned(),
                ));
            }
            previous_doc_id = Some(row.doc_id);

            if let Some(doc_id_map) = doc_id_map.as_ref() {
                let external_doc_id = doc_id_map
                    .external_doc_id(row.doc_id)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        DocstoreError::InvalidDocument(format!(
                            "missing external doc_id binding for wax_doc_id {}",
                            row.doc_id
                        ))
                    })?;
                if entries_by_external_doc_id
                    .insert(external_doc_id.clone(), StoreDocEntry { row: row.clone() })
                    .is_some()
                {
                    return Err(DocstoreError::InvalidDocument(
                        "duplicate external doc_id in store segment".to_owned(),
                    ));
                }
                ordered_doc_ids.push(external_doc_id);
            }

            rows_by_wax_doc_id.push(StoreDocEntry { row });
        }

        Ok(Self {
            minor,
            bytes,
            payload_range: payload_bytes_offset..metadata_bytes_offset,
            rows_by_wax_doc_id,
            ordered_doc_ids,
            entries_by_external_doc_id,
            doc_id_map,
        })
    }

    fn load_documents_by_id(
        &self,
        target_doc_ids: &[String],
    ) -> Result<HashMap<String, Value>, DocstoreError> {
        if self.minor == 0 {
            return self.load_documents_by_id_minor_v0(target_doc_ids);
        }

        let mut documents = HashMap::new();
        for doc_id in target_doc_ids {
            let Some(entry) = self.entries_by_external_doc_id.get(doc_id) else {
                continue;
            };
            let (_, value) = self.parse_payload_document(entry)?;
            documents.insert(doc_id.clone(), value);
        }
        Ok(documents)
    }

    fn ordered_documents(&self) -> Result<Vec<(String, Value)>, DocstoreError> {
        if self.minor == 0 {
            return self
                .rows_by_wax_doc_id
                .iter()
                .map(|entry| self.parse_payload_document(entry))
                .collect();
        }

        let documents = self.load_documents_by_id(&self.ordered_doc_ids)?;
        self.ordered_doc_ids
            .iter()
            .map(|doc_id| {
                let document = documents.get(doc_id).cloned().ok_or_else(|| {
                    DocstoreError::InvalidDocument(format!(
                        "store-backed document missing for doc_id {doc_id}"
                    ))
                })?;
                Ok((doc_id.clone(), document))
            })
            .collect()
    }

    fn load_document_ids(&self) -> Result<Vec<String>, DocstoreError> {
        if self.minor == 0 {
            return self
                .rows_by_wax_doc_id
                .iter()
                .map(|entry| self.parse_payload_doc_id(entry))
                .collect();
        }

        Ok(self.ordered_doc_ids.clone())
    }

    fn build_doc_id_map(&self) -> Result<DocIdMap, DocstoreError> {
        if let Some(doc_id_map) = self.doc_id_map.as_ref() {
            return Ok(doc_id_map.clone());
        }

        let bindings = self
            .rows_by_wax_doc_id
            .iter()
            .map(|entry| {
                self.parse_payload_doc_id(entry)
                    .map(|external_doc_id| DocIdBinding::new(entry.row.doc_id, external_doc_id))
            })
            .collect::<Result<Vec<_>, _>>()?;
        DocIdMap::from_bindings(bindings)
    }

    fn load_documents_by_id_minor_v0(
        &self,
        target_doc_ids: &[String],
    ) -> Result<HashMap<String, Value>, DocstoreError> {
        let mut remaining = target_doc_ids
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let mut documents = HashMap::new();

        for entry in &self.rows_by_wax_doc_id {
            let (external_doc_id, value) = self.parse_payload_document(entry)?;
            if remaining.remove(external_doc_id.as_str()) {
                documents.insert(external_doc_id, value);
                if remaining.is_empty() {
                    break;
                }
            }
        }

        Ok(documents)
    }

    fn parse_payload_doc_id(&self, entry: &StoreDocEntry) -> Result<String, DocstoreError> {
        let payload = self.payload_bytes(entry)?;
        let payload_text = std::str::from_utf8(payload)
            .map_err(|error| DocstoreError::InvalidDocument(error.to_string()))?;
        parse_document_id(payload_text, "store-backed document payload")
            .map(Cow::into_owned)
            .map_err(DocstoreError::InvalidDocument)
    }

    fn parse_payload_document(
        &self,
        entry: &StoreDocEntry,
    ) -> Result<(String, Value), DocstoreError> {
        let payload = self.payload_bytes(entry)?;
        let value: Value = serde_json::from_slice(payload)?;
        let object = value.as_object().ok_or_else(|| {
            DocstoreError::InvalidDocument("document line must be a json object".to_owned())
        })?;
        let external_doc_id = object
            .get("doc_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                DocstoreError::InvalidDocument(
                    "store-backed document payload missing doc_id".to_owned(),
                )
            })?
            .to_owned();
        Ok((external_doc_id, Value::Object(object.clone())))
    }

    fn payload_bytes<'a>(&'a self, entry: &StoreDocEntry) -> Result<&'a [u8], DocstoreError> {
        read_section_bytes(
            &self.bytes[self.payload_range.clone()],
            entry.row.payload_offset,
            entry.row.payload_length,
            "payload",
        )
    }
}

impl Docstore {
    pub fn open(mount_root: &Path, manifest: &DatasetPackManifest) -> Result<Self, DocstoreError> {
        let store_path = mount_root.join("store.wax");
        if store_path.exists() {
            let opened = wax_v2_core::open_store(&store_path)
                .map_err(|error| DocstoreError::InvalidDocument(error.to_string()))?;
            if let Some(descriptor) = opened
                .manifest
                .segments
                .iter()
                .filter(|segment| segment.family == SegmentKind::Doc)
                .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
            {
                return Self::open_store_segment(&store_path, descriptor);
            }
        }

        Self::open_dataset_pack(mount_root, manifest)
    }

    pub fn open_dataset_pack(
        mount_root: &Path,
        manifest: &DatasetPackManifest,
    ) -> Result<Self, DocstoreError> {
        let documents_path = manifest
            .files
            .iter()
            .find(|file| file.kind == "documents")
            .map(|file| mount_root.join(&file.path))
            .ok_or(DocstoreError::MissingDocumentsFile)?;

        let offset_index = manifest
            .files
            .iter()
            .find(|file| file.kind == "document_offsets")
            .map(|file| mount_root.join(&file.path))
            .filter(|path| path.exists())
            .map(|path| load_document_offset_index(&path))
            .transpose()?;

        Ok(Self {
            source: DocstoreSource::DatasetPack {
                documents_path,
                offset_index,
            },
        })
    }

    pub fn load_documents_by_id(
        &self,
        target_doc_ids: &[String],
    ) -> Result<HashMap<String, Value>, DocstoreError> {
        match &self.source {
            DocstoreSource::DatasetPack {
                documents_path,
                offset_index,
            } => {
                if let Some(index) = offset_index {
                    return load_documents_by_id_from_offsets(
                        documents_path,
                        index,
                        target_doc_ids,
                    );
                }

                let mut remaining = target_doc_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<HashSet<_>>();
                let mut documents = HashMap::new();
                let file = File::open(documents_path)?;
                let reader = BufReader::new(file);
                for line in reader.lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let doc_id = parse_document_id(&line, "document line")
                        .map(Cow::into_owned)
                        .map_err(DocstoreError::InvalidDocument)?;
                    if remaining.remove(doc_id.as_str()) {
                        let value: Value = serde_json::from_str(&line)?;
                        let object = value.as_object().ok_or_else(|| {
                            DocstoreError::InvalidDocument(
                                "document line must be a json object".to_owned(),
                            )
                        })?;
                        documents.insert(doc_id, Value::Object(object.clone()));
                        if remaining.is_empty() {
                            break;
                        }
                    }
                }
                Ok(documents)
            }
            DocstoreSource::Store { segment } => segment.load_documents_by_id(target_doc_ids),
        }
    }

    pub fn load_document_ids(&self) -> Result<Vec<String>, DocstoreError> {
        match &self.source {
            DocstoreSource::DatasetPack { documents_path, .. } => {
                load_document_ids_from_documents(documents_path)
            }
            DocstoreSource::Store { segment } => segment.load_document_ids(),
        }
    }

    pub fn build_doc_id_map(&self) -> Result<DocIdMap, DocstoreError> {
        match &self.source {
            DocstoreSource::DatasetPack { .. } => {
                let document_ids = self.load_document_ids()?;
                DocIdMap::from_document_order(&document_ids)
            }
            DocstoreSource::Store { segment } => segment.build_doc_id_map(),
        }
    }

    pub fn build_binary_doc_segment(&self) -> Result<BinaryDocSegment, DocstoreError> {
        let ordered_documents = match &self.source {
            DocstoreSource::DatasetPack { documents_path, .. } => {
                let file = File::open(documents_path)?;
                let reader = BufReader::new(file);
                reader
                    .lines()
                    .filter_map(|line| match line {
                        Ok(line) if line.trim().is_empty() => None,
                        other => Some(other),
                    })
                    .map(|line| {
                        let line = line?;
                        let document: Value = serde_json::from_str(&line)?;
                        let object = document.as_object().ok_or_else(|| {
                            DocstoreError::InvalidDocument(
                                "document line must be a json object".to_owned(),
                            )
                        })?;
                        let external_doc_id = object
                            .get("doc_id")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                DocstoreError::InvalidDocument(
                                    "document line missing doc_id".to_owned(),
                                )
                            })?
                            .to_owned();
                        Ok((external_doc_id, document))
                    })
                    .collect::<Result<Vec<_>, DocstoreError>>()?
            }
            DocstoreSource::Store { segment } => segment.ordered_documents()?,
        };

        let document_ids = ordered_documents
            .iter()
            .map(|(external_doc_id, _)| external_doc_id.clone())
            .collect::<Vec<_>>();
        let doc_id_map = self
            .build_doc_id_map()?
            .extend_to_cover_document_order(&document_ids)?;
        let active_bindings = doc_id_map.bindings_for_external_doc_ids_sorted(&document_ids)?;
        let documents_by_external_id = ordered_documents.into_iter().collect::<HashMap<_, _>>();
        let mut records = Vec::new();
        let mut payload_offset = 0u64;
        let mut metadata_offset = 0u32;
        let mut preview_offset = 0u32;

        for binding in active_bindings {
            let external_doc_id = binding.external_doc_id;
            let wax_doc_id = binding.wax_doc_id;
            let document = documents_by_external_id
                .get(&external_doc_id)
                .cloned()
                .ok_or_else(|| {
                    DocstoreError::InvalidDocument(format!(
                        "missing document payload for {external_doc_id}"
                    ))
                })?;
            let object = document.as_object().ok_or_else(|| {
                DocstoreError::InvalidDocument("document line must be a json object".to_owned())
            })?;
            let payload = serde_json::to_vec(&document)?;
            let metadata = object
                .get("metadata")
                .cloned()
                .unwrap_or_else(|| Value::Object(Default::default()));
            let metadata_bytes = serde_json::to_vec(&metadata)?;
            let preview = object
                .get("text")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let preview_length = preview.as_ref().map(|value| value.len()).unwrap_or(0);

            records.push(DocSegmentRecord {
                row: DocRow {
                    doc_id: wax_doc_id,
                    timestamp_ms: object
                        .get("timestamp_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    flags: 0,
                    payload_offset,
                    payload_length: payload.len() as u64,
                    metadata_ref: SectionRef::new(metadata_offset, metadata_bytes.len() as u32),
                    preview_ref: SectionRef::new(preview_offset, preview_length as u32),
                },
                payload,
                metadata,
                preview,
            });

            payload_offset += records.last().expect("record").payload.len() as u64;
            metadata_offset = metadata_offset
                .checked_add(metadata_bytes.len() as u32)
                .ok_or_else(|| {
                    DocstoreError::InvalidDocument("metadata offset overflow".to_owned())
                })?;
            preview_offset = preview_offset
                .checked_add(preview_length as u32)
                .ok_or_else(|| {
                    DocstoreError::InvalidDocument("preview offset overflow".to_owned())
                })?;
        }

        Ok(BinaryDocSegment {
            doc_id_map,
            records,
        })
    }

    pub fn publish_to_store(&self, store_path: &Path) -> Result<OpenedStore, DocstoreError> {
        let prepared = self.prepare_segment_for_store(store_path)?;
        wax_v2_core::publish_segment(store_path, prepared.descriptor, &prepared.object_bytes)
            .map_err(|error| DocstoreError::InvalidDocument(error.to_string()))
    }

    pub fn prepare_segment_for_store(
        &self,
        store_path: &Path,
    ) -> Result<PendingSegmentWrite, DocstoreError> {
        let segment = if let Some(persisted_map) = load_persisted_doc_id_map_from_store(store_path)?
        {
            self.build_binary_doc_segment_with_doc_id_map(persisted_map)?
        } else {
            self.build_binary_doc_segment()?
        };
        let object_bytes = segment.encode()?;
        let descriptor = pending_doc_segment_descriptor(&segment);
        Ok(PendingSegmentWrite {
            descriptor,
            object_bytes,
        })
    }

    fn build_binary_doc_segment_with_doc_id_map(
        &self,
        doc_id_map: DocIdMap,
    ) -> Result<BinaryDocSegment, DocstoreError> {
        match &self.source {
            DocstoreSource::DatasetPack {
                documents_path,
                offset_index: _,
            } => {
                let file = File::open(documents_path)?;
                let reader = BufReader::new(file);
                let ordered_documents = reader
                    .lines()
                    .filter_map(|line| match line {
                        Ok(line) if line.trim().is_empty() => None,
                        other => Some(other),
                    })
                    .map(|line| {
                        let line = line?;
                        let document: Value = serde_json::from_str(&line)?;
                        let object = document.as_object().ok_or_else(|| {
                            DocstoreError::InvalidDocument(
                                "document line must be a json object".to_owned(),
                            )
                        })?;
                        let external_doc_id = object
                            .get("doc_id")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                DocstoreError::InvalidDocument(
                                    "document line missing doc_id".to_owned(),
                                )
                            })?
                            .to_owned();
                        Ok((external_doc_id, document))
                    })
                    .collect::<Result<Vec<_>, DocstoreError>>()?;
                build_binary_doc_segment_from_documents(ordered_documents, doc_id_map)
            }
            DocstoreSource::Store { segment } => {
                build_binary_doc_segment_from_documents(segment.ordered_documents()?, doc_id_map)
            }
        }
    }

    fn open_store_segment(
        store_path: &Path,
        descriptor: &wax_v2_core::SegmentDescriptor,
    ) -> Result<Self, DocstoreError> {
        let bytes = wax_v2_core::map_segment_object(store_path, descriptor)
            .map_err(|error| DocstoreError::InvalidDocument(error.to_string()))?;
        let segment = StoreDocSegment::open(bytes)?;

        Ok(Self {
            source: DocstoreSource::Store { segment },
        })
    }
}

pub fn validate_store_segment_against_dataset_pack(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<(), DocstoreError> {
    let store_path = mount_root.join("store.wax");
    if !store_path.exists() {
        return Ok(());
    }

    let opened = wax_v2_core::open_store(&store_path)
        .map_err(|error| DocstoreError::InvalidDocument(error.to_string()))?;
    let Some(descriptor) = opened
        .manifest
        .segments
        .iter()
        .filter(|segment| segment.family == SegmentKind::Doc)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
    else {
        return Ok(());
    };

    let store_docstore = Docstore::open_store_segment(&store_path, descriptor)?;
    validate_store_docstore_against_dataset_pack(mount_root, manifest, &store_docstore)
}

fn validate_store_docstore_against_dataset_pack(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
    store_docstore: &Docstore,
) -> Result<(), DocstoreError> {
    let documents_exist = manifest
        .files
        .iter()
        .find(|file| file.kind == "documents")
        .map(|file| mount_root.join(&file.path))
        .is_some_and(|path| path.exists());
    if !documents_exist {
        return Ok(());
    }

    let dataset_docstore = match Docstore::open_dataset_pack(mount_root, manifest) {
        Ok(docstore) => docstore,
        Err(DocstoreError::MissingDocumentsFile) => return Ok(()),
        Err(error) => return Err(error),
    };

    let dataset_doc_ids = dataset_docstore.load_document_ids()?;
    let store_doc_ids = store_docstore.load_document_ids()?;
    if dataset_doc_ids != store_doc_ids {
        return Err(DocstoreError::InvalidDocument(
            "store doc segment does not match mounted dataset document ids".to_owned(),
        ));
    }

    for doc_id in &dataset_doc_ids {
        let dataset_documents =
            dataset_docstore.load_documents_by_id(std::slice::from_ref(doc_id))?;
        let store_documents = store_docstore.load_documents_by_id(std::slice::from_ref(doc_id))?;
        if dataset_documents.get(doc_id) != store_documents.get(doc_id) {
            return Err(DocstoreError::InvalidDocument(format!(
                "store doc segment does not match mounted dataset payload for {doc_id}"
            )));
        }
    }

    Ok(())
}

fn build_binary_doc_segment_from_documents(
    ordered_documents: Vec<(String, Value)>,
    doc_id_map: DocIdMap,
) -> Result<BinaryDocSegment, DocstoreError> {
    let document_ids = ordered_documents
        .iter()
        .map(|(external_doc_id, _)| external_doc_id.clone())
        .collect::<Vec<_>>();
    let doc_id_map = doc_id_map.extend_to_cover_document_order(&document_ids)?;
    let active_bindings = doc_id_map.bindings_for_external_doc_ids_sorted(&document_ids)?;
    let documents_by_external_id = ordered_documents.into_iter().collect::<HashMap<_, _>>();

    let mut records = Vec::new();
    let mut payload_offset = 0u64;
    let mut metadata_offset = 0u32;
    let mut preview_offset = 0u32;

    for binding in active_bindings {
        let external_doc_id = binding.external_doc_id;
        let wax_doc_id = binding.wax_doc_id;
        let document = documents_by_external_id
            .get(&external_doc_id)
            .cloned()
            .ok_or_else(|| {
                DocstoreError::InvalidDocument(format!(
                    "missing document payload for {external_doc_id}"
                ))
            })?;
        let object = document.as_object().ok_or_else(|| {
            DocstoreError::InvalidDocument("document line must be a json object".to_owned())
        })?;
        let payload = serde_json::to_vec(&document)?;
        let metadata = object
            .get("metadata")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));
        let metadata_bytes = serde_json::to_vec(&metadata)?;
        let preview = object
            .get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let preview_length = preview.as_ref().map(|value| value.len()).unwrap_or(0);

        records.push(DocSegmentRecord {
            row: DocRow {
                doc_id: wax_doc_id,
                timestamp_ms: object
                    .get("timestamp_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                flags: 0,
                payload_offset,
                payload_length: payload.len() as u64,
                metadata_ref: SectionRef::new(metadata_offset, metadata_bytes.len() as u32),
                preview_ref: SectionRef::new(preview_offset, preview_length as u32),
            },
            payload,
            metadata,
            preview,
        });

        payload_offset += records.last().expect("record").payload.len() as u64;
        metadata_offset = metadata_offset
            .checked_add(metadata_bytes.len() as u32)
            .ok_or_else(|| DocstoreError::InvalidDocument("metadata offset overflow".to_owned()))?;
        preview_offset = preview_offset
            .checked_add(preview_length as u32)
            .ok_or_else(|| DocstoreError::InvalidDocument("preview offset overflow".to_owned()))?;
    }

    Ok(BinaryDocSegment {
        doc_id_map,
        records,
    })
}

fn load_persisted_doc_id_map_from_store(
    store_path: &Path,
) -> Result<Option<DocIdMap>, DocstoreError> {
    if !store_path.exists() {
        return Ok(None);
    }
    let opened = wax_v2_core::open_store(store_path)
        .map_err(|error| DocstoreError::InvalidDocument(error.to_string()))?;
    let Some(descriptor) = opened
        .manifest
        .segments
        .iter()
        .filter(|segment| segment.family == SegmentKind::Doc)
        .max_by_key(|segment| (segment.segment_generation, segment.object_offset))
    else {
        return Ok(None);
    };
    let bytes = wax_v2_core::map_segment_object(store_path, descriptor)
        .map_err(|error| DocstoreError::InvalidDocument(error.to_string()))?;
    let segment = StoreDocSegment::open(bytes)?;
    Ok(Some(segment.build_doc_id_map()?))
}

pub fn load_document_ids_from_documents(path: &Path) -> Result<Vec<String>, DocstoreError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    reader
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some(other),
        })
        .map(|line| {
            let line = line?;
            parse_document_id(&line, "document line")
                .map(Cow::into_owned)
                .map_err(DocstoreError::InvalidDocument)
        })
        .collect()
}

pub fn prepare_raw_documents_segment(
    store_path: &Path,
    ordered_documents: Vec<(String, Value)>,
) -> Result<PendingSegmentWrite, DocstoreError> {
    let doc_id_map = load_persisted_doc_id_map_from_store(store_path)?.unwrap_or_else(|| {
        DocIdMap::from_bindings(Vec::new()).expect("empty doc id map should be valid")
    });
    let segment = build_binary_doc_segment_from_documents(ordered_documents, doc_id_map)?;
    let object_bytes = segment.encode()?;
    let descriptor = pending_doc_segment_descriptor(&segment);
    Ok(PendingSegmentWrite {
        descriptor,
        object_bytes,
    })
}

pub fn parse_document_id<'a>(line: &'a str, context: &str) -> Result<Cow<'a, str>, String> {
    let record: DocumentIdOnlyRecord<'a> =
        serde_json::from_str(line).map_err(|error| error.to_string())?;
    let Some(doc_id) = record.doc_id else {
        return Err(format!("{context} missing doc_id"));
    };
    if doc_id.is_empty() {
        return Err(format!("{context} missing doc_id"));
    }
    Ok(doc_id)
}

fn load_document_offset_index(
    path: &Path,
) -> Result<HashMap<String, DocumentOffsetEntry>, DocstoreError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut index = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: DocumentOffsetRecord = serde_json::from_str(&line)?;
        index.insert(
            record.doc_id,
            DocumentOffsetEntry {
                offset: record.offset,
                length: record.length,
            },
        );
    }

    Ok(index)
}

fn load_documents_by_id_from_offsets(
    path: &Path,
    offset_index: &HashMap<String, DocumentOffsetEntry>,
    target_doc_ids: &[String],
) -> Result<HashMap<String, Value>, DocstoreError> {
    let mut file = File::open(path)?;
    let mut documents = HashMap::new();

    for doc_id in target_doc_ids {
        let Some(entry) = offset_index.get(doc_id.as_str()) else {
            continue;
        };
        file.seek(SeekFrom::Start(entry.offset))?;
        let mut buffer = vec![0u8; entry.length as usize];
        file.read_exact(&mut buffer)?;
        let line = std::str::from_utf8(&buffer)
            .map_err(|error| DocstoreError::InvalidDocument(error.to_string()))?;
        let value: Value = serde_json::from_str(line.trim_end())?;
        let object = value.as_object().ok_or_else(|| {
            DocstoreError::InvalidDocument("document line must be a json object".to_owned())
        })?;
        documents.insert(doc_id.clone(), Value::Object(object.clone()));
    }

    Ok(documents)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentOffsetEntry {
    offset: u64,
    length: u64,
}

#[derive(Debug, Deserialize)]
struct DocumentOffsetRecord {
    doc_id: String,
    offset: u64,
    length: u64,
}

#[derive(Debug, Deserialize)]
struct DocumentIdOnlyRecord<'a> {
    #[serde(borrow)]
    doc_id: Option<Cow<'a, str>>,
}

fn validate_doc_id_order(records: &[DocSegmentRecord]) -> Result<(), DocstoreError> {
    for pair in records.windows(2) {
        if pair[0].row.doc_id >= pair[1].row.doc_id {
            return Err(DocstoreError::InvalidDocument(
                "doc rows must be sorted by doc_id".to_owned(),
            ));
        }
    }
    Ok(())
}

fn doc_id_map_from_records(records: &[DocSegmentRecord]) -> Result<DocIdMap, DocstoreError> {
    let mut bindings = Vec::with_capacity(records.len());
    for record in records {
        let value: Value = serde_json::from_slice(&record.payload)?;
        let object = value.as_object().ok_or_else(|| {
            DocstoreError::InvalidDocument("document line must be a json object".to_owned())
        })?;
        let external_doc_id = object
            .get("doc_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                DocstoreError::InvalidDocument("store-backed document missing doc_id".to_owned())
            })?
            .to_owned();
        bindings.push(DocIdBinding::new(record.row.doc_id, external_doc_id));
    }
    bindings.sort_by_key(|binding| binding.wax_doc_id);
    DocIdMap::from_bindings(bindings)
}

fn build_ref_section(
    entries: &[(SectionRef, Vec<u8>)],
    label: &str,
) -> Result<Vec<u8>, DocstoreError> {
    let mut sorted_ranges = entries
        .iter()
        .map(|(reference, bytes)| {
            let start = reference.offset() as usize;
            let end = start
                .checked_add(reference.length() as usize)
                .ok_or_else(|| {
                    DocstoreError::InvalidDocument(format!("{label} ref range overflow"))
                })?;
            if reference.length() as usize != bytes.len() {
                return Err(DocstoreError::InvalidDocument(format!(
                    "{label} ref length does not match encoded bytes"
                )));
            }
            Ok((start, end))
        })
        .collect::<Result<Vec<_>, DocstoreError>>()?;
    sorted_ranges.sort_unstable_by_key(|(start, end)| (*start, *end));

    let mut section_length = 0usize;
    let mut previous_end = 0usize;
    for (index, (start, end)) in sorted_ranges.iter().copied().enumerate() {
        if index > 0 && start < previous_end {
            return Err(DocstoreError::InvalidDocument(format!(
                "{label} refs must not overlap"
            )));
        }
        section_length = section_length.max(end);
        previous_end = end;
    }

    let mut section = vec![0u8; section_length];
    for (reference, bytes) in entries {
        let start = reference.offset() as usize;
        let end = start + reference.length() as usize;
        section[start..end].copy_from_slice(bytes);
    }

    Ok(section)
}

fn read_section_bytes<'a>(
    section: &'a [u8],
    offset: u64,
    length: u64,
    label: &str,
) -> Result<&'a [u8], DocstoreError> {
    let offset = usize::try_from(offset)
        .map_err(|_| DocstoreError::InvalidDocument(format!("{label} offset overflow")))?;
    let length = usize::try_from(length)
        .map_err(|_| DocstoreError::InvalidDocument(format!("{label} length overflow")))?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| DocstoreError::InvalidDocument(format!("{label} range overflow")))?;
    if end > section.len() {
        return Err(DocstoreError::InvalidDocument(format!(
            "{label} range extends past section bounds"
        )));
    }
    Ok(&section[offset..end])
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

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(bytes);
    let mut checksum = [0u8; 32];
    checksum.copy_from_slice(&digest);
    checksum
}

fn pending_doc_segment_descriptor(segment: &BinaryDocSegment) -> PendingSegmentDescriptor {
    let doc_id_start = segment
        .records
        .first()
        .map(|record| record.row.doc_id)
        .unwrap_or(0);
    let doc_id_end_exclusive = segment
        .records
        .last()
        .map(|record| record.row.doc_id + 1)
        .unwrap_or(0);
    let min_timestamp_ms = segment
        .records
        .iter()
        .map(|record| record.row.timestamp_ms)
        .min()
        .unwrap_or(0);
    let max_timestamp_ms = segment
        .records
        .iter()
        .map(|record| record.row.timestamp_ms)
        .max()
        .unwrap_or(0);
    let tombstoned_items = segment
        .records
        .iter()
        .filter(|record| record.row.is_tombstone())
        .count() as u64;

    PendingSegmentDescriptor {
        family: SegmentKind::Doc,
        family_version: 1,
        flags: 0,
        doc_id_start,
        doc_id_end_exclusive,
        min_timestamp_ms,
        max_timestamp_ms,
        live_items: segment.records.len() as u64 - tombstoned_items,
        tombstoned_items,
        backend_id: 0,
        backend_aux: 0,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;
    use wax_bench_model::DatasetPackManifest;
    use wax_v2_core::{
        create_empty_store, open_store, publish_segment, read_segment_object,
        PendingSegmentDescriptor, SegmentKind,
    };

    use crate::{
        parse_document_id, read_u64, sha256, BinaryDocSegment, DocIdBinding, DocIdMap, DocRow,
        DocSegmentRecord, Docstore, DocstoreError, DocstoreSource, SectionRef,
        DOC_SEGMENT_HEADER_LENGTH, DOC_SEGMENT_MAGIC, DOC_SEGMENT_MAJOR,
    };

    #[test]
    fn parse_document_id_handles_whitespace_and_escaped_quotes() {
        let line = "  {\"text\":\"escaped\",\"doc_id\":\"doc-\\\"001\"}  ";
        let doc_id = parse_document_id(line, "document line").unwrap();

        assert_eq!(doc_id.as_ref(), "doc-\"001");
    }

    #[test]
    fn open_dataset_pack_loads_documents_by_id_from_offsets() {
        let temp_dir = tempdir().unwrap();
        let docs_path = temp_dir.path().join("docs.ndjson");
        let offsets_path = temp_dir.path().join("document-offsets.jsonl");
        let doc_lines = [
            "{\"doc_id\":\"doc-001\",\"text\":\"alpha\",\"metadata\":{\"kind\":\"note\"}}\n",
            "{\"doc_id\":\"doc-002\",\"text\":\"beta\",\"metadata\":{\"kind\":\"task\"}}\n",
        ];
        let docs_content = doc_lines.concat();
        fs::write(&docs_path, &docs_content).unwrap();

        let offset_second = doc_lines[0].len() as u64;
        let length_second = doc_lines[1].len() as u64;
        fs::write(
            &offsets_path,
            format!(
                "{{\"doc_id\":\"doc-002\",\"offset\":{offset_second},\"length\":{length_second}}}\n"
            ),
        )
        .unwrap();

        let manifest = test_manifest(true);
        let docstore = Docstore::open_dataset_pack(temp_dir.path(), &manifest).unwrap();
        let documents = docstore
            .load_documents_by_id(&["doc-002".to_owned()])
            .unwrap();

        assert_eq!(
            docstore.load_document_ids().unwrap(),
            vec!["doc-001", "doc-002"]
        );
        assert_eq!(documents.len(), 1);
        assert_eq!(documents["doc-002"]["text"], "beta");
    }

    #[test]
    fn open_dataset_pack_rejects_missing_documents_file() {
        let temp_dir = tempdir().unwrap();
        let manifest = test_manifest(false);

        let error = Docstore::open_dataset_pack(temp_dir.path(), &manifest).unwrap_err();

        assert_eq!(error, DocstoreError::MissingDocumentsFile);
    }

    #[test]
    fn doc_id_map_assigns_numeric_ids_in_document_file_order() {
        let temp_dir = tempdir().unwrap();
        let docs_path = temp_dir.path().join("docs.ndjson");
        let offsets_path = temp_dir.path().join("document-offsets.jsonl");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-900\",\"text\":\"first\"}\n",
                "{\"doc_id\":\"doc-010\",\"text\":\"second\"}\n",
                "{\"doc_id\":\"doc-500\",\"text\":\"third\"}\n",
            ),
        )
        .unwrap();
        fs::write(&offsets_path, "").unwrap();

        let manifest = test_manifest(true);
        let docstore = Docstore::open_dataset_pack(temp_dir.path(), &manifest).unwrap();

        let doc_id_map = docstore.build_doc_id_map().unwrap();

        assert_eq!(
            doc_id_map.bindings(),
            &[
                DocIdBinding::new(0, "doc-900"),
                DocIdBinding::new(1, "doc-010"),
                DocIdBinding::new(2, "doc-500"),
            ]
        );
        assert_eq!(doc_id_map.wax_doc_id("doc-010"), Some(1));
        assert_eq!(doc_id_map.external_doc_id(2), Some("doc-500"));
    }

    #[test]
    fn doc_id_map_round_trips_persisted_bindings() {
        let doc_id_map = DocIdMap::from_bindings(vec![
            DocIdBinding::new(0, "doc-001"),
            DocIdBinding::new(1, "doc-002"),
        ])
        .unwrap();

        let encoded = doc_id_map.encode_json().unwrap();
        let decoded = DocIdMap::decode_json(&encoded).unwrap();

        assert_eq!(decoded, doc_id_map);
        assert_eq!(decoded.external_doc_id(1), Some("doc-002"));
    }

    #[test]
    fn doc_id_map_changes_when_document_file_order_changes() {
        let first =
            DocIdMap::from_document_order(&["doc-a".to_owned(), "doc-b".to_owned()]).unwrap();
        let second =
            DocIdMap::from_document_order(&["doc-b".to_owned(), "doc-a".to_owned()]).unwrap();

        assert_ne!(first, second);
        assert_eq!(first.wax_doc_id("doc-a"), Some(0));
        assert_eq!(second.wax_doc_id("doc-a"), Some(1));
    }

    #[test]
    fn doc_id_map_rejects_duplicate_external_doc_ids() {
        let error = DocIdMap::from_bindings(vec![
            DocIdBinding::new(1, "doc-001"),
            DocIdBinding::new(2, "doc-001"),
        ])
        .expect_err("duplicate source ids should fail");

        assert!(
            matches!(error, DocstoreError::InvalidDocument(message) if message.contains("duplicate"))
        );
    }

    #[test]
    fn publish_dataset_pack_to_store_creates_manifest_visible_doc_segment() {
        let temp_dir = tempdir().unwrap();
        let docs_path = temp_dir.path().join("docs.ndjson");
        let offsets_path = temp_dir.path().join("document-offsets.jsonl");
        let store_path = temp_dir.path().join("store.wax");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-900\",\"text\":\"first\",\"metadata\":{\"kind\":\"note\"}}\n",
                "{\"doc_id\":\"doc-010\",\"text\":\"second\",\"metadata\":{\"kind\":\"task\"}}\n",
            ),
        )
        .unwrap();
        fs::write(&offsets_path, "").unwrap();
        create_empty_store(&store_path).unwrap();

        let manifest = test_manifest(true);
        let docstore = Docstore::open_dataset_pack(temp_dir.path(), &manifest).unwrap();

        let published = docstore.publish_to_store(&store_path).unwrap();
        assert_eq!(published.manifest.generation, 1);
        assert_eq!(published.manifest.segments.len(), 1);
        assert_eq!(published.manifest.segments[0].family, SegmentKind::Doc);
        assert_eq!(published.manifest.segments[0].doc_id_start, 0);
        assert_eq!(published.manifest.segments[0].doc_id_end_exclusive, 2);
        assert_eq!(published.manifest.segments[0].live_items, 2);

        let reopened = open_store(&store_path).unwrap();
        let bytes = read_segment_object(&store_path, &reopened.manifest.segments[0]).unwrap();
        let decoded = BinaryDocSegment::decode(&bytes).unwrap();

        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].row.doc_id, 0);
        assert_eq!(decoded.records[1].row.doc_id, 1);
        assert_eq!(decoded.records[0].preview.as_deref(), Some("first"));
        assert_eq!(decoded.records[1].preview.as_deref(), Some("second"));
    }

    #[test]
    fn open_prefers_manifest_visible_doc_segment_when_docs_sidecar_is_missing() {
        let temp_dir = tempdir().unwrap();
        let docs_path = temp_dir.path().join("docs.ndjson");
        let offsets_path = temp_dir.path().join("document-offsets.jsonl");
        let store_path = temp_dir.path().join("store.wax");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-900\",\"text\":\"first\",\"metadata\":{\"kind\":\"note\"}}\n",
                "{\"doc_id\":\"doc-010\",\"text\":\"second\",\"metadata\":{\"kind\":\"task\"}}\n",
            ),
        )
        .unwrap();
        fs::write(&offsets_path, "").unwrap();
        create_empty_store(&store_path).unwrap();

        let manifest = test_manifest(true);
        let dataset_docstore = Docstore::open_dataset_pack(temp_dir.path(), &manifest).unwrap();
        dataset_docstore.publish_to_store(&store_path).unwrap();
        fs::remove_file(&docs_path).unwrap();
        fs::remove_file(&offsets_path).unwrap();

        let reopened = Docstore::open(temp_dir.path(), &manifest).unwrap();
        let documents = reopened
            .load_documents_by_id(&["doc-010".to_owned()])
            .unwrap();

        assert_eq!(
            reopened.load_document_ids().unwrap(),
            vec!["doc-900", "doc-010"]
        );
        assert_eq!(documents["doc-010"]["text"], "second");
    }

    #[test]
    fn open_store_segment_keeps_lazy_store_backing() {
        let temp_dir = tempdir().unwrap();
        let docs_path = temp_dir.path().join("docs.ndjson");
        let offsets_path = temp_dir.path().join("document-offsets.jsonl");
        let store_path = temp_dir.path().join("store.wax");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-900\",\"text\":\"first\",\"metadata\":{\"kind\":\"note\"}}\n",
                "{\"doc_id\":\"doc-010\",\"text\":\"second\",\"metadata\":{\"kind\":\"task\"}}\n",
            ),
        )
        .unwrap();
        fs::write(&offsets_path, "").unwrap();
        create_empty_store(&store_path).unwrap();

        let manifest = test_manifest(true);
        let dataset_docstore = Docstore::open_dataset_pack(temp_dir.path(), &manifest).unwrap();
        dataset_docstore.publish_to_store(&store_path).unwrap();
        fs::remove_file(&docs_path).unwrap();
        fs::remove_file(&offsets_path).unwrap();

        let reopened = Docstore::open(temp_dir.path(), &manifest).unwrap();

        assert!(matches!(
            &reopened.source,
            DocstoreSource::Store { segment }
                if segment.ordered_doc_ids == vec!["doc-900".to_owned(), "doc-010".to_owned()]
        ));
    }

    #[test]
    fn open_store_segment_uses_binding_section_for_doc_ids_in_minor_v1() {
        let temp_dir = tempdir().unwrap();
        let docs_path = temp_dir.path().join("docs.ndjson");
        let offsets_path = temp_dir.path().join("document-offsets.jsonl");
        let store_path = temp_dir.path().join("store.wax");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-900\",\"text\":\"first\",\"metadata\":{\"kind\":\"note\"}}\n",
                "{\"doc_id\":\"doc-010\",\"text\":\"second\",\"metadata\":{\"kind\":\"task\"}}\n",
            ),
        )
        .unwrap();
        fs::write(&offsets_path, "").unwrap();
        create_empty_store(&store_path).unwrap();

        let manifest = test_manifest(true);
        let dataset_docstore = Docstore::open_dataset_pack(temp_dir.path(), &manifest).unwrap();
        let published = dataset_docstore.publish_to_store(&store_path).unwrap();
        let descriptor = published.manifest.segments[0].clone();
        let mut bytes = read_segment_object(&store_path, &descriptor).unwrap();
        let replaced = bytes
            .windows(b"doc-900".len())
            .position(|window| window == b"doc-900")
            .unwrap();
        bytes[replaced..replaced + 7].copy_from_slice(b"doc-999");
        let checksum = sha256(&bytes[88..]);
        bytes[56..88].copy_from_slice(&checksum);
        publish_segment(
            &store_path,
            PendingSegmentDescriptor {
                family: SegmentKind::Doc,
                family_version: descriptor.family_version,
                flags: descriptor.flags,
                doc_id_start: descriptor.doc_id_start,
                doc_id_end_exclusive: descriptor.doc_id_end_exclusive,
                min_timestamp_ms: descriptor.min_timestamp_ms,
                max_timestamp_ms: descriptor.max_timestamp_ms,
                live_items: descriptor.live_items,
                tombstoned_items: descriptor.tombstoned_items,
                backend_id: descriptor.backend_id,
                backend_aux: descriptor.backend_aux,
            },
            &bytes,
        )
        .unwrap();
        fs::remove_file(&docs_path).unwrap();
        fs::remove_file(&offsets_path).unwrap();

        let reopened = Docstore::open(temp_dir.path(), &manifest).unwrap();

        assert_eq!(
            reopened.load_document_ids().unwrap(),
            vec!["doc-900", "doc-010"]
        );
    }

    #[test]
    fn open_store_segment_defers_minor_v0_payload_parsing_until_lookup() {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join("store.wax");
        create_empty_store(&store_path).unwrap();
        let first_payload = br#"{"doc_id":"doc-001","text":"alpha"}"#.to_vec();
        let second_payload = br#"{"doc_id":"doc-002","text":"unterminated""#.to_vec();
        let first_payload_len = first_payload.len() as u64;
        let second_payload_len = second_payload.len() as u64;

        let segment = BinaryDocSegment {
            doc_id_map: test_doc_id_map(&[(0, "doc-001"), (1, "doc-002")]),
            records: vec![
                DocSegmentRecord {
                    row: DocRow {
                        doc_id: 0,
                        timestamp_ms: 0,
                        flags: 0,
                        payload_offset: 0,
                        payload_length: first_payload_len,
                        metadata_ref: SectionRef::new(0, 2),
                        preview_ref: SectionRef::new(0, 5),
                    },
                    payload: first_payload,
                    metadata: json!({}),
                    preview: Some("alpha".to_owned()),
                },
                DocSegmentRecord {
                    row: DocRow {
                        doc_id: 1,
                        timestamp_ms: 0,
                        flags: 0,
                        payload_offset: first_payload_len,
                        payload_length: second_payload_len,
                        metadata_ref: SectionRef::new(2, 2),
                        preview_ref: SectionRef::new(5, 4),
                    },
                    payload: second_payload,
                    metadata: json!({}),
                    preview: Some("beta".to_owned()),
                },
            ],
        };
        let bytes = encode_minor_v0_segment(&segment).unwrap();
        publish_segment(
            &store_path,
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
            &bytes,
        )
        .unwrap();

        let manifest = test_manifest(false);
        let reopened = Docstore::open(temp_dir.path(), &manifest).unwrap();
        let documents = reopened
            .load_documents_by_id(&["doc-001".to_owned()])
            .unwrap();

        assert_eq!(documents["doc-001"]["text"], "alpha");
    }

    #[test]
    fn validate_store_segment_rejects_manifest_visible_doc_segment_when_mounted_docs_do_not_match()
    {
        let temp_dir = tempdir().unwrap();
        let docs_path = temp_dir.path().join("docs.ndjson");
        let offsets_path = temp_dir.path().join("document-offsets.jsonl");
        let store_path = temp_dir.path().join("store.wax");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-900\",\"text\":\"first\",\"metadata\":{\"kind\":\"note\"}}\n",
                "{\"doc_id\":\"doc-010\",\"text\":\"second\",\"metadata\":{\"kind\":\"task\"}}\n",
            ),
        )
        .unwrap();
        fs::write(&offsets_path, "").unwrap();
        create_empty_store(&store_path).unwrap();

        let manifest = test_manifest(true);
        let dataset_docstore = Docstore::open_dataset_pack(temp_dir.path(), &manifest).unwrap();
        dataset_docstore.publish_to_store(&store_path).unwrap();

        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-900\",\"text\":\"first changed\",\"metadata\":{\"kind\":\"note\"}}\n",
                "{\"doc_id\":\"doc-010\",\"text\":\"second\",\"metadata\":{\"kind\":\"task\"}}\n",
            ),
        )
        .unwrap();

        let error = crate::validate_store_segment_against_dataset_pack(temp_dir.path(), &manifest)
            .unwrap_err();

        assert!(matches!(
            error,
            DocstoreError::InvalidDocument(message)
                if message.contains("does not match mounted dataset")
        ));
    }

    #[test]
    fn binary_doc_segment_round_trips_records_with_tombstones() {
        let metadata_note = serde_json::to_vec(&json!({"kind":"note"})).unwrap();
        let metadata_deleted = serde_json::to_vec(&json!({"kind":"deleted"})).unwrap();
        let segment = BinaryDocSegment {
            doc_id_map: test_doc_id_map(&[(10, "doc-010"), (11, "doc-011")]),
            records: vec![
                DocSegmentRecord {
                    row: DocRow {
                        doc_id: 10,
                        timestamp_ms: 1_000,
                        flags: 0,
                        payload_offset: 0,
                        payload_length: 5,
                        metadata_ref: SectionRef::new(0, metadata_note.len() as u32),
                        preview_ref: SectionRef::new(0, 5),
                    },
                    payload: b"alpha".to_vec(),
                    metadata: json!({"kind":"note"}),
                    preview: Some("alpha".to_owned()),
                },
                DocSegmentRecord {
                    row: DocRow {
                        doc_id: 11,
                        timestamp_ms: 2_000,
                        flags: DocRow::FLAG_TOMBSTONE,
                        payload_offset: 5,
                        payload_length: 0,
                        metadata_ref: SectionRef::new(
                            metadata_note.len() as u32,
                            metadata_deleted.len() as u32,
                        ),
                        preview_ref: SectionRef::new(5, 0),
                    },
                    payload: Vec::new(),
                    metadata: json!({"kind":"deleted"}),
                    preview: None,
                },
            ],
        };

        let bytes = segment.encode().expect("segment should encode");
        let decoded = BinaryDocSegment::decode(&bytes).expect("segment should decode");

        assert_eq!(decoded, segment);
        assert!(decoded.records[1].row.is_tombstone());
    }

    #[test]
    fn binary_doc_segment_decode_uses_metadata_and_preview_refs() {
        let metadata_alpha = serde_json::to_vec(&json!({"kind":"alpha"})).unwrap();
        let metadata_beta = serde_json::to_vec(&json!({"kind":"beta"})).unwrap();
        let preview_alpha = b"alpha".to_vec();
        let preview_beta = b"beta".to_vec();
        let segment = BinaryDocSegment {
            doc_id_map: test_doc_id_map(&[(10, "doc-010"), (11, "doc-011")]),
            records: vec![
                DocSegmentRecord {
                    row: DocRow {
                        doc_id: 10,
                        timestamp_ms: 1_000,
                        flags: 0,
                        payload_offset: 0,
                        payload_length: 5,
                        metadata_ref: SectionRef::new(
                            metadata_beta.len() as u32,
                            metadata_alpha.len() as u32,
                        ),
                        preview_ref: SectionRef::new(
                            preview_beta.len() as u32,
                            preview_alpha.len() as u32,
                        ),
                    },
                    payload: b"alpha".to_vec(),
                    metadata: json!({"kind":"alpha"}),
                    preview: Some("alpha".to_owned()),
                },
                DocSegmentRecord {
                    row: DocRow {
                        doc_id: 11,
                        timestamp_ms: 2_000,
                        flags: 0,
                        payload_offset: 5,
                        payload_length: 4,
                        metadata_ref: SectionRef::new(0, metadata_beta.len() as u32),
                        preview_ref: SectionRef::new(0, preview_beta.len() as u32),
                    },
                    payload: b"beta".to_vec(),
                    metadata: json!({"kind":"beta"}),
                    preview: Some("beta".to_owned()),
                },
            ],
        };

        let mut bytes = segment.encode().expect("segment should encode");
        let metadata_bytes_offset = read_u64(&bytes, 24) as usize;
        let preview_bytes_offset = read_u64(&bytes, 32) as usize;
        let binding_bytes_offset = read_u64(&bytes, 40) as usize;
        bytes[metadata_bytes_offset..preview_bytes_offset]
            .copy_from_slice(&[metadata_beta.as_slice(), metadata_alpha.as_slice()].concat());
        bytes[preview_bytes_offset..binding_bytes_offset]
            .copy_from_slice(&[preview_beta.as_slice(), preview_alpha.as_slice()].concat());
        let checksum = sha256(&bytes[DOC_SEGMENT_HEADER_LENGTH..]);
        bytes[56..88].copy_from_slice(&checksum);

        let decoded = BinaryDocSegment::decode(&bytes).expect("segment should decode via refs");

        assert_eq!(decoded, segment);
    }

    #[test]
    fn binary_doc_segment_rejects_out_of_bounds_metadata_refs() {
        let metadata_note = serde_json::to_vec(&json!({"kind":"note"})).unwrap();
        let segment = BinaryDocSegment {
            doc_id_map: test_doc_id_map(&[(10, "doc-010")]),
            records: vec![DocSegmentRecord {
                row: DocRow {
                    doc_id: 10,
                    timestamp_ms: 1_000,
                    flags: 0,
                    payload_offset: 0,
                    payload_length: 5,
                    metadata_ref: SectionRef::new(0, metadata_note.len() as u32),
                    preview_ref: SectionRef::new(0, 5),
                },
                payload: b"alpha".to_vec(),
                metadata: json!({"kind":"note"}),
                preview: Some("alpha".to_owned()),
            }],
        };

        let mut bytes = segment.encode().expect("segment should encode");
        let row_table_offset = read_u64(&bytes, 48) as usize;
        let bad_ref = SectionRef::new(999, 10).packed().to_le_bytes();
        bytes[row_table_offset + 40..row_table_offset + 48].copy_from_slice(&bad_ref);
        let checksum = sha256(&bytes[DOC_SEGMENT_HEADER_LENGTH..]);
        bytes[56..88].copy_from_slice(&checksum);

        let error = BinaryDocSegment::decode(&bytes).expect_err("segment should reject bad refs");

        assert!(
            matches!(error, DocstoreError::InvalidDocument(message) if message.contains("metadata"))
        );
    }

    #[test]
    fn binary_doc_segment_rejects_unsorted_doc_ids() {
        let segment = BinaryDocSegment {
            doc_id_map: test_doc_id_map(&[(10, "doc-010"), (11, "doc-011")]),
            records: vec![
                DocSegmentRecord {
                    row: DocRow {
                        doc_id: 11,
                        timestamp_ms: 2_000,
                        flags: 0,
                        payload_offset: 0,
                        payload_length: 4,
                        metadata_ref: SectionRef::new(0, 2),
                        preview_ref: SectionRef::new(0, 4),
                    },
                    payload: b"beta".to_vec(),
                    metadata: json!({}),
                    preview: Some("beta".to_owned()),
                },
                DocSegmentRecord {
                    row: DocRow {
                        doc_id: 10,
                        timestamp_ms: 1_000,
                        flags: 0,
                        payload_offset: 4,
                        payload_length: 5,
                        metadata_ref: SectionRef::new(2, 2),
                        preview_ref: SectionRef::new(4, 5),
                    },
                    payload: b"alpha".to_vec(),
                    metadata: json!({}),
                    preview: Some("alpha".to_owned()),
                },
            ],
        };

        let error = segment
            .encode()
            .expect_err("segment should reject unsorted rows");

        assert!(
            matches!(error, DocstoreError::InvalidDocument(message) if message.contains("sorted"))
        );
    }

    fn test_doc_id_map(bindings: &[(u64, &str)]) -> DocIdMap {
        DocIdMap::from_bindings(
            bindings
                .iter()
                .map(|(wax_doc_id, external_doc_id)| {
                    DocIdBinding::new(*wax_doc_id, (*external_doc_id).to_owned())
                })
                .collect(),
        )
        .unwrap()
    }

    fn encode_minor_v0_segment(segment: &BinaryDocSegment) -> Result<Vec<u8>, DocstoreError> {
        let minor_v1 = segment.encode()?;
        let payload_bytes_offset = read_u64(&minor_v1, 16) as usize;
        let metadata_bytes_offset = read_u64(&minor_v1, 24) as usize;
        let preview_bytes_offset = read_u64(&minor_v1, 32) as usize;
        let binding_bytes_offset = read_u64(&minor_v1, 40) as usize;
        let row_table_offset = read_u64(&minor_v1, 48) as usize;
        let row_count = read_u64(&minor_v1, 8);

        let payload_section = &minor_v1[payload_bytes_offset..metadata_bytes_offset];
        let metadata_section = &minor_v1[metadata_bytes_offset..preview_bytes_offset];
        let preview_section = &minor_v1[preview_bytes_offset..binding_bytes_offset];
        let row_table = &minor_v1[row_table_offset..];
        let minor_v0_row_table_offset =
            80 + payload_section.len() + metadata_section.len() + preview_section.len();

        let mut body = Vec::with_capacity(
            payload_section.len()
                + metadata_section.len()
                + preview_section.len()
                + row_table.len(),
        );
        body.extend_from_slice(payload_section);
        body.extend_from_slice(metadata_section);
        body.extend_from_slice(preview_section);
        body.extend_from_slice(row_table);
        let checksum = sha256(&body);

        let mut bytes = Vec::with_capacity(80 + body.len());
        bytes.extend_from_slice(DOC_SEGMENT_MAGIC);
        bytes.extend_from_slice(&DOC_SEGMENT_MAJOR.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&row_count.to_le_bytes());
        bytes.extend_from_slice(&(80u64).to_le_bytes());
        bytes.extend_from_slice(&(80u64 + payload_section.len() as u64).to_le_bytes());
        bytes.extend_from_slice(
            &(80u64 + payload_section.len() as u64 + metadata_section.len() as u64).to_le_bytes(),
        );
        bytes.extend_from_slice(&(minor_v0_row_table_offset as u64).to_le_bytes());
        bytes.extend_from_slice(&checksum);
        bytes.extend_from_slice(&body);
        Ok(bytes)
    }

    fn test_manifest(include_documents: bool) -> DatasetPackManifest {
        let mut files = vec![
            json!({"path":"document-offsets.jsonl","kind":"document_offsets","format":"jsonl","record_count":1,"checksum":"sha256:offsets"}),
        ];
        if include_documents {
            files.insert(
                0,
                json!({"path":"docs.ndjson","kind":"documents","format":"ndjson","record_count":2,"checksum":"sha256:docs"}),
            );
        }
        serde_json::from_value(json!({
            "schema_version": "wax_dataset_pack_v1",
            "generated_at": "2026-04-19T00:00:00Z",
            "generator": {"name":"test","version":"0.1.0"},
            "identity": {
                "dataset_id":"knowledge-small-clean-v1",
                "dataset_version":"v1",
                "dataset_family":"knowledge",
                "dataset_tier":"small",
                "variant_id":"clean",
                "embedding_spec_id":"minilm-l6-384-f32-cosine",
                "embedding_model_version":"test",
                "embedding_model_hash":"sha256:model",
                "corpus_checksum":"sha256:corpus",
                "query_checksum":"sha256:query"
            },
            "environment_constraints": {"min_ram_gb":1,"recommended_ram_gb":1},
            "corpus": {
                "doc_count":2,
                "vector_count":2,
                "total_text_bytes":9,
                "avg_doc_length":4.5,
                "median_doc_length":4,
                "p95_doc_length":5,
                "max_doc_length":5,
                "languages":[{"code":"en","ratio":1.0}]
            },
            "text_profile": {
                "length_buckets":{"short_ratio":1.0,"medium_ratio":0.0,"long_ratio":0.0}
            },
            "metadata_profile": {
                "facets":[],
                "selectivity_exemplars":{
                    "broad":"*",
                    "medium":"kind = note",
                    "narrow":"kind = task",
                    "zero_hit":"kind = missing"
                }
            },
            "vector_profile": {
                "enabled": true,
                "embedding_dimensions": 384,
                "embedding_dtype":"f32",
                "distance_metric":"cosine",
                "query_vectors":{"precomputed_available":true,"runtime_embedding_supported":false}
            },
            "dirty_profile": {
                "profile":"clean",
                "seed":0,
                "delete_ratio":0.0,
                "update_ratio":0.0,
                "append_ratio":0.0,
                "target_segment_count_range":[1,1],
                "target_segment_topology":[],
                "target_tombstone_ratio":0.0,
                "compaction_state":"clean"
            },
            "files": files,
            "query_sets": [],
            "checksums": {
                "manifest_payload_checksum":"sha256:manifest",
                "logical_documents_checksum":"sha256:documents",
                "logical_metadata_checksum":"sha256:meta",
                "logical_query_definitions_checksum":"sha256:logical-query",
                "logical_vector_payload_checksum":"sha256:vector",
                "fairness_fingerprint":"sha256:fair"
            }
        }))
        .unwrap()
    }
}
