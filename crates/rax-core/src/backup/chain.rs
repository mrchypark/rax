use crate::backup::manifest::BackupManifest;

pub fn verify_chain(manifests: &[BackupManifest]) -> bool {
    if manifests.is_empty() {
        return true;
    }

    for i in 1..manifests.len() {
        let prev = &manifests[i - 1];
        let cur = &manifests[i];
        if cur.base_snapshot_id.as_deref() != Some(prev.snapshot_id.as_str()) {
            return false;
        }
        if cur.wal_start_seq > cur.wal_end_seq {
            return false;
        }
    }
    true
}

pub fn interrupted_chain_detected(manifests: &[BackupManifest]) -> bool {
    !verify_chain(manifests)
}
