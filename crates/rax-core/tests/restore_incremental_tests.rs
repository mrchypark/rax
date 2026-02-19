use rax_core::backup::manifest::{full_manifest, incremental_manifest};
use rax_core::backup::restore::restore_incremental;

#[test]
fn incremental_restore_applies_ordered_chain() {
    let base = full_manifest("snap-1", 1, vec!["seg-a".to_string()]);
    let inc = incremental_manifest("snap-1", "snap-2", 2, 10, 20, vec!["seg-b".to_string()]);
    let state = restore_incremental(&[base, inc]).unwrap();
    assert_eq!(state.applied_snapshots, vec!["snap-1".to_string(), "snap-2".to_string()]);
}
