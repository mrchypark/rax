use rax_core::backup::chain::verify_chain;
use rax_core::backup::manifest::{full_manifest, incremental_manifest};

#[test]
fn incremental_backup_contains_only_changed_segments_and_wal_range() {
    let base = full_manifest("snap-1", 1, vec!["seg-a".to_string(), "seg-b".to_string()]);
    let inc = incremental_manifest("snap-1", "snap-2", 2, 100, 130, vec!["seg-b".to_string()]);

    assert_eq!(inc.changed_segments, vec!["seg-b".to_string()]);
    assert_eq!(inc.wal_start_seq, 100);
    assert_eq!(inc.wal_end_seq, 130);
    assert!(verify_chain(&[base, inc]));
}
