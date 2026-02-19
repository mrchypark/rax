#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSetRewriteReport {
    pub before_count: usize,
    pub after_count: usize,
}

pub fn rewrite_live_set(
    logical_ids: &[u64],
    superseded_ids: &[u64],
) -> (Vec<u64>, LiveSetRewriteReport) {
    let superseded: std::collections::HashSet<u64> = superseded_ids.iter().copied().collect();
    let mut rewritten: Vec<u64> = logical_ids
        .iter()
        .copied()
        .filter(|id| !superseded.contains(id))
        .collect();
    rewritten.sort_unstable();

    (
        rewritten.clone(),
        LiveSetRewriteReport {
            before_count: logical_ids.len(),
            after_count: rewritten.len(),
        },
    )
}
