use std::sync::Arc;

use object_store::memory::InMemory;
use rax_core::backup::export::BackupExporter;
use rax_core::backup::manifest::full_manifest;

#[tokio::test]
async fn backup_exporter_writes_manifest_to_object_store_memory_backend() {
    let store = Arc::new(InMemory::new());
    let exporter = BackupExporter::new(store, "backup");
    let manifest = full_manifest("snap-1", 1, vec!["seg-a".to_string()]);

    exporter
        .export_manifest("manifest.json", &manifest)
        .await
        .unwrap();
    let json = exporter.read_manifest_json("manifest.json").await.unwrap();

    assert!(json.contains("snap-1"));
    assert!(json.contains("seg-a"));
}
