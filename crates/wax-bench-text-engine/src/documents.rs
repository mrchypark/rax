use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::Value;
use wax_bench_model::DatasetPackManifest;

pub(crate) fn load_document_ids_from_documents(path: &Path) -> Result<Vec<String>, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let reader = BufReader::new(file);
    reader
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some(other),
        })
        .map(|line| {
            let line = line.map_err(|error| error.to_string())?;
            parse_document_id(&line, "document line").map(Cow::into_owned)
        })
        .collect()
}

pub(crate) fn materialize_document_previews(
    documents: &mut HashMap<String, Value>,
    path: &Path,
    offset_index: Option<&HashMap<String, DocumentOffsetEntry>>,
    doc_ids: &[String],
) -> Result<(), String> {
    let missing = doc_ids
        .iter()
        .filter(|doc_id| !documents.contains_key(doc_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        let loaded = load_documents_by_id(path, offset_index, &missing)?;
        for (doc_id, document) in loaded {
            documents.insert(doc_id, document);
        }
    }
    for doc_id in doc_ids {
        let document = documents
            .get(doc_id)
            .ok_or_else(|| format!("document missing for hit doc_id: {doc_id}"))?;
        document
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("text missing for hit doc_id: {doc_id}"))?;
    }
    Ok(())
}

pub(crate) fn load_documents_by_id(
    path: &Path,
    offset_index: Option<&HashMap<String, DocumentOffsetEntry>>,
    target_doc_ids: &[String],
) -> Result<HashMap<String, Value>, String> {
    if let Some(index) = offset_index {
        return load_documents_by_id_from_offsets(path, index, target_doc_ids);
    }

    let mut remaining = target_doc_ids
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let mut documents = HashMap::new();
    let file = File::open(path).map_err(|error| error.to_string())?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line.map_err(|error| error.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let doc_id = parse_document_id(&line, "document line")?;
        if remaining.remove(doc_id.as_ref()) {
            let value: Value = serde_json::from_str(&line).map_err(|error| error.to_string())?;
            let object = value
                .as_object()
                .ok_or_else(|| "document line must be a json object".to_owned())?;
            documents.insert(doc_id.into_owned(), Value::Object(object.clone()));
            if remaining.is_empty() {
                break;
            }
        }
    }
    Ok(documents)
}

pub(crate) fn document_offsets_path(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Option<PathBuf> {
    manifest
        .files
        .iter()
        .find(|file| file.kind == "document_offsets")
        .map(|file| mount_root.join(&file.path))
}

pub(crate) fn load_document_offset_index(
    path: &Path,
) -> Result<HashMap<String, DocumentOffsetEntry>, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let reader = BufReader::new(file);
    let mut index = HashMap::new();

    for line in reader.lines() {
        let line = line.map_err(|error| error.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let record: DocumentOffsetRecord =
            serde_json::from_str(&line).map_err(|error| error.to_string())?;
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

pub(crate) fn parse_document_id<'a>(line: &'a str, context: &str) -> Result<Cow<'a, str>, String> {
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

fn load_documents_by_id_from_offsets(
    path: &Path,
    offset_index: &HashMap<String, DocumentOffsetEntry>,
    target_doc_ids: &[String],
) -> Result<HashMap<String, Value>, String> {
    let mut file = File::open(path).map_err(|error| error.to_string())?;
    let mut documents = HashMap::new();

    for doc_id in target_doc_ids {
        let Some(entry) = offset_index.get(doc_id.as_str()) else {
            continue;
        };
        file.seek(SeekFrom::Start(entry.offset))
            .map_err(|error| error.to_string())?;
        let mut buffer = vec![0u8; entry.length as usize];
        file.read_exact(&mut buffer)
            .map_err(|error| error.to_string())?;
        let line = std::str::from_utf8(&buffer).map_err(|error| error.to_string())?;
        let value: Value =
            serde_json::from_str(line.trim_end()).map_err(|error| error.to_string())?;
        let object = value
            .as_object()
            .ok_or_else(|| "document line must be a json object".to_owned())?;
        documents.insert(doc_id.clone(), Value::Object(object.clone()));
    }

    Ok(documents)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DocumentOffsetEntry {
    pub(crate) offset: u64,
    pub(crate) length: u64,
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
