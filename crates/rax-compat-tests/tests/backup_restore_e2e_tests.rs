use std::sync::Arc;

use object_store::memory::InMemory;
use rax_core::backup::export::BackupExporter;
use rax_core::backup::manifest::{full_manifest, incremental_manifest};
use rax_core::backup::restore::restore_incremental;

#[tokio::test]
async fn backup_restore_e2e_memory_backend_round_trip() {
    let store = Arc::new(InMemory::new());
    let exporter = BackupExporter::new(store, "backup");

    let base = full_manifest("snap-1", 1, vec!["seg-a".to_string()]);
    let inc = incremental_manifest("snap-1", "snap-2", 2, 10, 15, vec!["seg-b".to_string()]);

    exporter.export_manifest("base.json", &base).await.unwrap();
    exporter.export_manifest("inc.json", &inc).await.unwrap();

    let state = restore_incremental(&[base, inc]).unwrap();
    assert_eq!(state.applied_snapshots, vec!["snap-1".to_string(), "snap-2".to_string()]);
}
