use rax_core::wal::entry::WALEntry;
use rax_core::wal::ring::WALRing;

#[test]
fn wal_compaction_removes_committed_records() {
    let mut ring = WALRing::new(16);
    ring.append(WALEntry::PutFrame {
        frame_id: 1,
        payload: vec![1],
    });
    ring.append(WALEntry::PutFrame {
        frame_id: 2,
        payload: vec![2],
    });
    ring.append(WALEntry::DeleteFrame { frame_id: 1 });

    let removed = ring.compact(2);
    assert_eq!(removed, 2);
    assert_eq!(ring.records().len(), 1);
    assert_eq!(ring.records()[0].sequence, 3);
}
