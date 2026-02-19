use crate::wal::entry::WALEntry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WALRecord {
    pub sequence: u64,
    pub entry: WALEntry,
}

#[derive(Debug, Clone)]
pub struct WALRing {
    capacity: usize,
    next_sequence: u64,
    records: Vec<WALRecord>,
}

impl WALRing {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_sequence: 1,
            records: Vec::new(),
        }
    }

    pub fn append(&mut self, entry: WALEntry) -> u64 {
        let seq = self.next_sequence;
        self.next_sequence += 1;
        self.records.push(WALRecord {
            sequence: seq,
            entry,
        });
        if self.records.len() > self.capacity {
            let overflow = self.records.len() - self.capacity;
            self.records.drain(0..overflow);
        }
        seq
    }

    pub fn records(&self) -> &[WALRecord] {
        &self.records
    }
}
