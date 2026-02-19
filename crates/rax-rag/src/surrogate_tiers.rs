#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SurrogateTier {
    Tiny,
    Short,
    Long,
}

pub fn select_tier(token_count: usize) -> SurrogateTier {
    if token_count <= 16 {
        SurrogateTier::Tiny
    } else if token_count <= 64 {
        SurrogateTier::Short
    } else {
        SurrogateTier::Long
    }
}
