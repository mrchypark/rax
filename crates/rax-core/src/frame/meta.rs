#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameStatus {
    Active,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameMeta {
    pub id: u64,
    pub status: FrameStatus,
    pub supersedes: Option<u64>,
    pub superseded_by: Option<u64>,
    pub timestamp: u64,
}

impl FrameMeta {
    pub fn active(id: u64, timestamp: u64) -> Self {
        Self {
            id,
            status: FrameStatus::Active,
            supersedes: None,
            superseded_by: None,
            timestamp,
        }
    }
}
