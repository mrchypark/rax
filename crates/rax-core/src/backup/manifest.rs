use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    pub snapshot_id: String,
    pub base_snapshot_id: Option<String>,
    pub generation: u64,
    pub wal_start_seq: u64,
    pub wal_end_seq: u64,
    pub changed_segments: Vec<String>,
}

pub fn full_manifest(snapshot_id: &str, generation: u64, segments: Vec<String>) -> BackupManifest {
    BackupManifest {
        snapshot_id: snapshot_id.to_string(),
        base_snapshot_id: None,
        generation,
        wal_start_seq: 0,
        wal_end_seq: 0,
        changed_segments: segments,
    }
}

pub fn incremental_manifest(
    base_snapshot_id: &str,
    snapshot_id: &str,
    generation: u64,
    wal_start_seq: u64,
    wal_end_seq: u64,
    changed_segments: Vec<String>,
) -> BackupManifest {
    BackupManifest {
        snapshot_id: snapshot_id.to_string(),
        base_snapshot_id: Some(base_snapshot_id.to_string()),
        generation,
        wal_start_seq,
        wal_end_seq,
        changed_segments,
    }
}
