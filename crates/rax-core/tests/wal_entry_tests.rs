use rax_core::wal::entry::WALEntry;

#[test]
fn wal_entry_round_trip_codec() {
    let entry = WALEntry::PutFrame {
        frame_id: 7,
        payload: vec![1, 2, 3],
    };

    let encoded = entry.encode();
    let decoded = WALEntry::decode(&encoded).unwrap();
    assert_eq!(decoded, entry);
}
