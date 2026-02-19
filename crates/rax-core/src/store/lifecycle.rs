use std::collections::HashMap;

use crate::frame::{FrameMeta, FrameStatus};

#[derive(Default)]
pub struct LifecycleStore {
    next_id: u64,
    payloads: HashMap<u64, Vec<u8>>,
    metas: HashMap<u64, FrameMeta>,
}

impl LifecycleStore {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            payloads: HashMap::new(),
            metas: HashMap::new(),
        }
    }

    pub fn put(&mut self, payload: Vec<u8>, timestamp: u64) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.payloads.insert(id, payload);
        self.metas.insert(id, FrameMeta::active(id, timestamp));
        id
    }

    pub fn delete(&mut self, id: u64) {
        if let Some(meta) = self.metas.get_mut(&id) {
            meta.status = FrameStatus::Deleted;
        }
        self.payloads.remove(&id);
    }

    pub fn supersede(&mut self, superseded_id: u64, superseding_id: u64) {
        if let Some(old) = self.metas.get_mut(&superseded_id) {
            old.superseded_by = Some(superseding_id);
        }
        if let Some(newer) = self.metas.get_mut(&superseding_id) {
            newer.supersedes = Some(superseded_id);
        }
    }

    pub fn get_visible(&self, id: u64) -> Option<&FrameMeta> {
        self.metas.get(&id).and_then(|m| {
            if m.status == FrameStatus::Active && m.superseded_by.is_none() {
                Some(m)
            } else {
                None
            }
        })
    }

    pub fn timeline(&self, include_superseded: bool) -> Vec<FrameMeta> {
        let mut out: Vec<FrameMeta> = self
            .metas
            .values()
            .filter(|m| {
                m.status == FrameStatus::Active && (include_superseded || m.superseded_by.is_none())
            })
            .cloned()
            .collect();
        out.sort_by_key(|m| m.timestamp);
        out
    }

    pub fn meta(&self, id: u64) -> Option<&FrameMeta> {
        self.metas.get(&id)
    }
}
