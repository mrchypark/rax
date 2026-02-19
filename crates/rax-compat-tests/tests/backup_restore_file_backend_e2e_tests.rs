use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::local::LocalFileSystem;
use rax_core::backup::export::BackupExporter;
use rax_core::backup::manifest::{full_manifest, incremental_manifest};
use rax_core::backup::restore::restore_incremental;

fn unique_temp_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rax-local-backup-e2e-{}-{nonce}",
        std::process::id()
    ))
}

#[tokio::test]
async fn backup_restore_e2e_local_filesystem_backend_round_trip() {
    let root = unique_temp_dir();
    fs::create_dir_all(&root).unwrap();

    let store = Arc::new(LocalFileSystem::new_with_prefix(&root).unwrap());
    let exporter = BackupExporter::new(store, "backup");

    let base = full_manifest("snap-10", 10, vec!["seg-a".to_string()]);
    let inc = incremental_manifest(
        "snap-10",
        "snap-11",
        11,
        100,
        111,
        vec!["seg-b".to_string()],
    );

    exporter.export_manifest("base.json", &base).await.unwrap();
    exporter.export_manifest("inc.json", &inc).await.unwrap();

    let base_json = exporter.read_manifest_json("base.json").await.unwrap();
    assert!(base_json.contains("snap-10"));
    assert!(base_json.contains("seg-a"));

    let path = root.join("backup").join("inc.json");
    let inc_json = fs::read_to_string(path).unwrap();
    assert!(inc_json.contains("snap-11"));
    assert!(inc_json.contains("seg-b"));

    let state = restore_incremental(&[base, inc]).unwrap();
    assert_eq!(
        state.applied_snapshots,
        vec!["snap-10".to_string(), "snap-11".to_string()]
    );

    fs::remove_dir_all(&root).unwrap();
}
