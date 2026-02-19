#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryMode {
    Constraint,
    Semantic,
}

pub fn classify_query(query: &str) -> QueryMode {
    if query.contains(':') || query.contains("=") {
        QueryMode::Constraint
    } else {
        QueryMode::Semantic
    }
}
