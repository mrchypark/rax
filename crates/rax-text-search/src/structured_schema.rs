use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredMemoryRecord {
    pub id: String,
    pub entity_type: String,
    pub attributes: HashMap<String, String>,
}

impl StructuredMemoryRecord {
    pub fn matches(&self, key: &str, value: &str) -> bool {
        self.attributes.get(key).map(|v| v == value).unwrap_or(false)
    }
}
