#[derive(Debug, Clone, PartialEq)]
pub struct SearchRequest {
    pub query: String,
    pub structured_weight: f32,
    pub semantic_weight: f32,
    pub temporal_weight: f32,
}

impl SearchRequest {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            structured_weight: 1.0,
            semantic_weight: 1.0,
            temporal_weight: 0.5,
        }
    }
}
