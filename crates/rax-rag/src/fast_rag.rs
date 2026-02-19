use crate::token_counter::count_tokens;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextChunk {
    pub id: u64,
    pub text: String,
    pub importance: i32,
}

pub fn build_context(mut chunks: Vec<ContextChunk>, token_budget: usize) -> Vec<ContextChunk> {
    chunks.sort_by(|a, b| b.importance.cmp(&a.importance).then_with(|| a.id.cmp(&b.id)));

    let mut used = 0usize;
    let mut out = Vec::new();
    for c in chunks {
        let tokens = count_tokens(&c.text);
        if used + tokens > token_budget {
            continue;
        }
        used += tokens;
        out.push(c);
    }
    out
}
