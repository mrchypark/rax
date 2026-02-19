use std::collections::HashMap;

use crate::wal::entry::WALEntry;
use crate::wal::ring::WALRecord;

pub fn replay_pending_puts(records: &[WALRecord], committed_sequence: u64) -> HashMap<u64, Vec<u8>> {
    let mut out: HashMap<u64, Vec<u8>> = HashMap::new();
    for record in records {
        if record.sequence <= committed_sequence {
            continue;
        }
        match &record.entry {
            WALEntry::PutFrame { frame_id, payload } => {
                out.insert(*frame_id, payload.clone());
            }
            WALEntry::DeleteFrame { frame_id } => {
                out.remove(frame_id);
            }
        }
    }
    out
}
