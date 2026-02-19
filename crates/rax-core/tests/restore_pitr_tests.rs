use rax_core::backup::manifest::{full_manifest, incremental_manifest};
use rax_core::backup::pitr::restore_pitr;

#[test]
fn restore_pitr_stops_at_target_timestamp_and_produces_expected_state() {
    let mut base = full_manifest("snap-1", 1, vec!["seg-a".to_string()]);
    base.wal_end_seq = 100;
    let inc = incremental_manifest("snap-1", "snap-2", 2, 101, 150, vec!["seg-b".to_string()]);

    let chosen = restore_pitr(&[base, inc], 120).unwrap();
    assert_eq!(chosen, "snap-1".to_string());
}
