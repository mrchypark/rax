use rax_core::backup::manifest::full_manifest;
use rax_core::backup::restore::restore_full;

#[test]
fn full_restore_uses_base_manifest() {
    let base = full_manifest("snap-1", 1, vec!["seg-a".to_string()]);
    let state = restore_full(&base);
    assert_eq!(state.applied_snapshots, vec!["snap-1".to_string()]);
}
