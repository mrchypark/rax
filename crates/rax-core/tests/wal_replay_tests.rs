use rax_core::wal::entry::WALEntry;
use rax_core::wal::replay::replay_pending_puts;
use rax_core::wal::ring::WALRing;

#[test]
fn wal_replay_recovers_pending_put() {
    let mut ring = WALRing::new(16);
    ring.append(WALEntry::PutFrame {
        frame_id: 10,
        payload: vec![9],
    });
    ring.append(WALEntry::PutFrame {
        frame_id: 11,
        payload: vec![8],
    });

    let pending = replay_pending_puts(ring.records(), 1);
    assert_eq!(pending.get(&11), Some(&vec![8]));
}
