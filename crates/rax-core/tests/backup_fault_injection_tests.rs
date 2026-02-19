use rax_core::backup::chain::interrupted_chain_detected;
use rax_core::backup::manifest::{full_manifest, incremental_manifest};

#[test]
fn interrupted_incremental_backup_is_detected_and_recoverable() {
    let base = full_manifest("snap-1", 1, vec!["seg-a".to_string()]);
    let broken = incremental_manifest("wrong-base", "snap-2", 2, 11, 20, vec!["seg-b".to_string()]);

    assert!(interrupted_chain_detected(&[base.clone(), broken]));

    let repaired = incremental_manifest("snap-1", "snap-2", 2, 11, 20, vec!["seg-b".to_string()]);
    assert!(!interrupted_chain_detected(&[base, repaired]));
}
