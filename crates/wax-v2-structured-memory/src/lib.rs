use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const STRUCTURED_MEMORY_FILE_NAME: &str = "structured-memory.ndjson";
const ENTITY_KIND_PREDICATE: &str = "__entity_kind";
const ENTITY_ALIAS_PREDICATE: &str = "__entity_alias";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StructuredMemoryStatus {
    Asserted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredMemoryProvenance {
    pub source: String,
    pub asserted_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredMemoryRecord {
    pub record_id: u64,
    pub subject: String,
    pub predicate: String,
    pub value: serde_json::Value,
    pub status: StructuredMemoryStatus,
    pub provenance: StructuredMemoryProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredEntity {
    pub entity_id: String,
    pub kind: String,
    pub aliases: Vec<String>,
    pub provenance: StructuredMemoryProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewStructuredEntity {
    entity_id: String,
    kind: String,
    aliases: Vec<String>,
    provenance: StructuredMemoryProvenance,
}

impl NewStructuredEntity {
    pub fn new(
        entity_id: impl Into<String>,
        kind: impl Into<String>,
        source: impl Into<String>,
        asserted_at_ms: u64,
    ) -> Self {
        Self {
            entity_id: entity_id.into(),
            kind: kind.into(),
            aliases: Vec::new(),
            provenance: StructuredMemoryProvenance {
                source: source.into(),
                asserted_at_ms,
            },
        }
    }

    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.aliases.push(alias.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredEntityQuery {
    entity_id: String,
}

impl StructuredEntityQuery {
    pub fn entity_id(entity_id: impl Into<String>) -> Self {
        Self {
            entity_id: entity_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewStructuredMemoryRecord {
    subject: String,
    predicate: String,
    value: serde_json::Value,
    provenance: StructuredMemoryProvenance,
}

impl NewStructuredMemoryRecord {
    pub fn fact(
        subject: impl Into<String>,
        predicate: impl Into<String>,
        value: serde_json::Value,
        source: impl Into<String>,
        asserted_at_ms: u64,
    ) -> Self {
        Self {
            subject: subject.into(),
            predicate: predicate.into(),
            value,
            provenance: StructuredMemoryProvenance {
                source: source.into(),
                asserted_at_ms,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredFact {
    pub fact_id: u64,
    pub subject: String,
    pub predicate: String,
    pub value: serde_json::Value,
    pub status: StructuredMemoryStatus,
    pub provenance: StructuredMemoryProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewStructuredFact {
    subject: String,
    predicate: String,
    value: serde_json::Value,
    provenance: StructuredMemoryProvenance,
}

impl NewStructuredFact {
    pub fn new(
        subject: impl Into<String>,
        predicate: impl Into<String>,
        value: serde_json::Value,
        source: impl Into<String>,
        asserted_at_ms: u64,
    ) -> Self {
        Self {
            subject: subject.into(),
            predicate: predicate.into(),
            value,
            provenance: StructuredMemoryProvenance {
                source: source.into(),
                asserted_at_ms,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredMemoryQuery {
    subject: Option<String>,
    predicate: Option<String>,
}

impl StructuredMemoryQuery {
    pub fn subject(subject: impl Into<String>) -> Self {
        Self {
            subject: Some(subject.into()),
            predicate: None,
        }
    }

    pub fn with_predicate(mut self, predicate: impl Into<String>) -> Self {
        self.predicate = Some(predicate.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredFactQuery {
    subject: Option<String>,
    predicate: Option<String>,
}

impl StructuredFactQuery {
    pub fn subject(subject: impl Into<String>) -> Self {
        Self {
            subject: Some(subject.into()),
            predicate: None,
        }
    }

    pub fn with_predicate(mut self, predicate: impl Into<String>) -> Self {
        self.predicate = Some(predicate.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StructuredMemoryError {
    Io(String),
    Json(String),
    InvalidRequest(String),
}

impl fmt::Display for StructuredMemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) | Self::Json(message) | Self::InvalidRequest(message) => {
                write!(f, "{message}")
            }
        }
    }
}

pub struct StructuredMemorySession {
    storage_path: PathBuf,
    records: Vec<StructuredMemoryRecord>,
    next_record_id: u64,
    closed: bool,
}

impl StructuredMemorySession {
    pub fn open(root: &Path) -> Result<Self, StructuredMemoryError> {
        fs::create_dir_all(root).map_err(io_error)?;
        let storage_path = root.join(STRUCTURED_MEMORY_FILE_NAME);
        if !storage_path.exists() {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&storage_path)
                .map_err(io_error)?;
        }

        let records = load_records(&storage_path)?;
        let next_record_id = records
            .last()
            .map(|record| record.record_id + 1)
            .unwrap_or(0);

        Ok(Self {
            storage_path,
            records,
            next_record_id,
            closed: false,
        })
    }

    pub fn record(
        &mut self,
        new_record: NewStructuredMemoryRecord,
    ) -> Result<StructuredMemoryRecord, StructuredMemoryError> {
        self.ensure_open()?;
        validate_record_inputs(&new_record)?;

        let record = StructuredMemoryRecord {
            record_id: self.next_record_id,
            subject: new_record.subject,
            predicate: new_record.predicate,
            value: new_record.value,
            status: StructuredMemoryStatus::Asserted,
            provenance: new_record.provenance,
        };

        let encoded = serde_json::to_string(&record).map_err(json_error)?;
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.storage_path)
            .map_err(io_error)?;
        writeln!(file, "{encoded}").map_err(io_error)?;
        file.flush().map_err(io_error)?;

        self.records.push(record.clone());
        self.next_record_id += 1;
        Ok(record)
    }

    pub fn upsert_entity(
        &mut self,
        new_entity: NewStructuredEntity,
    ) -> Result<StructuredEntity, StructuredMemoryError> {
        self.ensure_open()?;
        validate_entity_inputs(&new_entity)?;

        let entity_id = new_entity.entity_id.clone();
        let current = self.entity(StructuredEntityQuery::entity_id(entity_id.clone()))?;

        if current.as_ref().map(|entity| entity.kind.as_str()) != Some(new_entity.kind.as_str()) {
            self.record(NewStructuredMemoryRecord::fact(
                entity_id.clone(),
                ENTITY_KIND_PREDICATE,
                serde_json::json!(new_entity.kind),
                new_entity.provenance.source.clone(),
                new_entity.provenance.asserted_at_ms,
            ))?;
        }

        let existing_aliases = current
            .as_ref()
            .map(|entity| entity.aliases.as_slice())
            .unwrap_or(&[]);
        for alias in new_entity.aliases {
            if !existing_aliases.iter().any(|existing| existing == &alias) {
                self.record(NewStructuredMemoryRecord::fact(
                    entity_id.clone(),
                    ENTITY_ALIAS_PREDICATE,
                    serde_json::json!(alias),
                    new_entity.provenance.source.clone(),
                    new_entity.provenance.asserted_at_ms,
                ))?;
            }
        }

        self.entity(StructuredEntityQuery::entity_id(entity_id))?
            .ok_or_else(|| {
                StructuredMemoryError::InvalidRequest(
                    "structured memory entity upsert did not persist entity metadata".to_owned(),
                )
            })
    }

    pub fn entity(
        &mut self,
        query: StructuredEntityQuery,
    ) -> Result<Option<StructuredEntity>, StructuredMemoryError> {
        self.ensure_open()?;

        let mut kind: Option<(String, StructuredMemoryProvenance)> = None;
        let mut aliases = Vec::new();

        for record in self
            .records
            .iter()
            .filter(|record| record.subject == query.entity_id)
        {
            match record.predicate.as_str() {
                ENTITY_KIND_PREDICATE => {
                    let kind_value = record.value.as_str().ok_or_else(|| {
                        StructuredMemoryError::Json(
                            "structured memory entity kind must be a string".to_owned(),
                        )
                    })?;
                    kind = Some((kind_value.to_owned(), record.provenance.clone()));
                }
                ENTITY_ALIAS_PREDICATE => {
                    let alias_value = record.value.as_str().ok_or_else(|| {
                        StructuredMemoryError::Json(
                            "structured memory entity alias must be a string".to_owned(),
                        )
                    })?;
                    if !aliases.iter().any(|alias| alias == alias_value) {
                        aliases.push(alias_value.to_owned());
                    }
                }
                _ => {}
            }
        }

        Ok(kind.map(|(kind, provenance)| StructuredEntity {
            entity_id: query.entity_id,
            kind,
            aliases,
            provenance,
        }))
    }

    pub fn assert_fact(
        &mut self,
        new_fact: NewStructuredFact,
    ) -> Result<StructuredFact, StructuredMemoryError> {
        self.ensure_open()?;
        validate_fact_inputs(&new_fact)?;

        let record = self.record(NewStructuredMemoryRecord {
            subject: new_fact.subject,
            predicate: new_fact.predicate,
            value: new_fact.value,
            provenance: new_fact.provenance,
        })?;
        structured_fact_from_record(record)
    }

    pub fn facts(
        &mut self,
        query: StructuredFactQuery,
    ) -> Result<Vec<StructuredFact>, StructuredMemoryError> {
        self.ensure_open()?;
        self.query(StructuredMemoryQuery {
            subject: query.subject,
            predicate: query.predicate,
        })?
        .into_iter()
        .filter(|record| !is_reserved_entity_predicate(record.predicate.as_str()))
        .map(structured_fact_from_record)
        .collect()
    }

    pub fn query(
        &mut self,
        query: StructuredMemoryQuery,
    ) -> Result<Vec<StructuredMemoryRecord>, StructuredMemoryError> {
        self.ensure_open()?;
        Ok(self
            .records
            .iter()
            .filter(|record| {
                query
                    .subject
                    .as_ref()
                    .is_none_or(|subject| &record.subject == subject)
                    && query
                        .predicate
                        .as_ref()
                        .is_none_or(|predicate| &record.predicate == predicate)
            })
            .cloned()
            .collect())
    }

    pub fn close(&mut self) -> Result<(), StructuredMemoryError> {
        self.closed = true;
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), StructuredMemoryError> {
        if self.closed {
            return Err(StructuredMemoryError::InvalidRequest(
                "structured memory session is already closed".to_owned(),
            ));
        }
        Ok(())
    }
}

fn load_records(path: &Path) -> Result<Vec<StructuredMemoryRecord>, StructuredMemoryError> {
    BufReader::new(OpenOptions::new().read(true).open(path).map_err(io_error)?)
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

fn validate_record_inputs(record: &NewStructuredMemoryRecord) -> Result<(), StructuredMemoryError> {
    if record.subject.is_empty() {
        return Err(StructuredMemoryError::InvalidRequest(
            "structured memory record subject cannot be empty".to_owned(),
        ));
    }
    if record.predicate.is_empty() {
        return Err(StructuredMemoryError::InvalidRequest(
            "structured memory record predicate cannot be empty".to_owned(),
        ));
    }
    if record.provenance.source.is_empty() {
        return Err(StructuredMemoryError::InvalidRequest(
            "structured memory provenance source cannot be empty".to_owned(),
        ));
    }
    Ok(())
}

fn validate_entity_inputs(entity: &NewStructuredEntity) -> Result<(), StructuredMemoryError> {
    if entity.entity_id.is_empty() {
        return Err(StructuredMemoryError::InvalidRequest(
            "structured memory entity id cannot be empty".to_owned(),
        ));
    }
    if entity.kind.is_empty() {
        return Err(StructuredMemoryError::InvalidRequest(
            "structured memory entity kind cannot be empty".to_owned(),
        ));
    }
    if entity.aliases.iter().any(|alias| alias.is_empty()) {
        return Err(StructuredMemoryError::InvalidRequest(
            "structured memory entity aliases cannot be empty".to_owned(),
        ));
    }
    if entity.provenance.source.is_empty() {
        return Err(StructuredMemoryError::InvalidRequest(
            "structured memory provenance source cannot be empty".to_owned(),
        ));
    }
    Ok(())
}

fn validate_fact_inputs(fact: &NewStructuredFact) -> Result<(), StructuredMemoryError> {
    if is_reserved_entity_predicate(fact.predicate.as_str()) {
        return Err(StructuredMemoryError::InvalidRequest(
            "structured memory fact predicate is reserved for entity bootstrap metadata"
                .to_owned(),
        ));
    }

    validate_record_inputs(&NewStructuredMemoryRecord {
        subject: fact.subject.clone(),
        predicate: fact.predicate.clone(),
        value: fact.value.clone(),
        provenance: fact.provenance.clone(),
    })
}

fn structured_fact_from_record(
    record: StructuredMemoryRecord,
) -> Result<StructuredFact, StructuredMemoryError> {
    if is_reserved_entity_predicate(record.predicate.as_str()) {
        return Err(StructuredMemoryError::InvalidRequest(
            "structured memory entity bootstrap metadata is not exposed as a fact".to_owned(),
        ));
    }

    Ok(StructuredFact {
        fact_id: record.record_id,
        subject: record.subject,
        predicate: record.predicate,
        value: record.value,
        status: record.status,
        provenance: record.provenance,
    })
}

fn is_reserved_entity_predicate(predicate: &str) -> bool {
    matches!(predicate, ENTITY_KIND_PREDICATE | ENTITY_ALIAS_PREDICATE)
}

fn io_error(error: std::io::Error) -> StructuredMemoryError {
    StructuredMemoryError::Io(error.to_string())
}

fn json_error(error: serde_json::Error) -> StructuredMemoryError {
    StructuredMemoryError::Json(error.to_string())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        NewStructuredEntity, NewStructuredFact, NewStructuredMemoryRecord, StructuredEntityQuery,
        StructuredFactQuery, StructuredMemoryQuery, StructuredMemorySession,
    };

    #[test]
    fn structured_memory_query_filters_by_subject_and_predicate() {
        let root = tempdir().unwrap();
        let mut session = StructuredMemorySession::open(root.path()).unwrap();
        session
            .record(NewStructuredMemoryRecord::fact(
                "person:alice",
                "name",
                serde_json::json!("Alice"),
                "bootstrap-test",
                100,
            ))
            .unwrap();
        session
            .record(NewStructuredMemoryRecord::fact(
                "person:alice",
                "works_at",
                serde_json::json!("company:acme"),
                "bootstrap-test",
                200,
            ))
            .unwrap();

        let results = session
            .query(StructuredMemoryQuery::subject("person:alice").with_predicate("works_at"))
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].predicate, "works_at");
    }

    #[test]
    fn structured_memory_open_reloads_records_from_bootstrap_file() {
        let root = tempdir().unwrap();
        {
            let mut session = StructuredMemorySession::open(root.path()).unwrap();
            session
                .record(NewStructuredMemoryRecord::fact(
                    "person:alice",
                    "name",
                    serde_json::json!("Alice"),
                    "bootstrap-test",
                    100,
                ))
                .unwrap();
        }

        let mut reopened = StructuredMemorySession::open(root.path()).unwrap();
        let results = reopened
            .query(StructuredMemoryQuery::subject("person:alice"))
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record_id, 0);
    }

    #[test]
    fn structured_memory_entity_round_trips_kind_and_aliases() {
        let root = tempdir().unwrap();
        let mut session = StructuredMemorySession::open(root.path()).unwrap();
        session
            .upsert_entity(
                NewStructuredEntity::new("person:alice", "person", "bootstrap-test", 100)
                    .with_alias("Alice")
                    .with_alias("Alice Kim"),
            )
            .unwrap();
        session.close().unwrap();

        let mut reopened = StructuredMemorySession::open(root.path()).unwrap();
        let entity = reopened
            .entity(StructuredEntityQuery::entity_id("person:alice"))
            .unwrap()
            .unwrap();

        assert_eq!(entity.kind, "person");
        assert_eq!(entity.aliases, vec!["Alice", "Alice Kim"]);
    }

    #[test]
    fn structured_memory_fact_queries_hide_entity_metadata_records() {
        let root = tempdir().unwrap();
        let mut session = StructuredMemorySession::open(root.path()).unwrap();
        session
            .upsert_entity(NewStructuredEntity::new(
                "person:alice",
                "person",
                "bootstrap-test",
                100,
            ))
            .unwrap();
        session
            .assert_fact(NewStructuredFact::new(
                "person:alice",
                "works_at",
                serde_json::json!("company:acme"),
                "bootstrap-test",
                200,
            ))
            .unwrap();

        let facts = session
            .facts(StructuredFactQuery::subject("person:alice"))
            .unwrap();

        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].predicate, "works_at");
    }
}
