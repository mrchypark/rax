use crate::backup::manifest::BackupManifest;

pub fn restore_pitr(manifests: &[BackupManifest], target_wal_seq: u64) -> Option<String> {
    manifests
        .iter()
        .filter(|m| m.wal_end_seq <= target_wal_seq)
        .max_by_key(|m| m.wal_end_seq)
        .map(|m| m.snapshot_id.clone())
}
