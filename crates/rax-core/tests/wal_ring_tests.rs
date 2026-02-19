use rax_core::wal::entry::WALEntry;
use rax_core::wal::ring::WALRing;

#[test]
fn wal_ring_appends_and_reads_in_order() {
    let mut ring = WALRing::new(2);
    ring.append(WALEntry::PutFrame {
        frame_id: 1,
        payload: vec![1],
    });
    ring.append(WALEntry::PutFrame {
        frame_id: 2,
        payload: vec![2],
    });
    ring.append(WALEntry::DeleteFrame { frame_id: 1 });

    let records = ring.records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].sequence, 2);
    assert_eq!(records[1].sequence, 3);
}
