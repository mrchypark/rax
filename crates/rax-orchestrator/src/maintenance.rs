use std::collections::HashSet;

#[derive(Default)]
pub struct SurrogateMaintenance {
    stale: HashSet<u64>,
}

impl SurrogateMaintenance {
    pub fn mark_stale(&mut self, id: u64) {
        self.stale.insert(id);
    }

    pub fn rebuild(&mut self) -> usize {
        let count = self.stale.len();
        self.stale.clear();
        count
    }
}
