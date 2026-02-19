use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StructuredEntity {
    pub id: String,
    pub attrs: HashMap<String, String>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct StructuredMemory {
    entities: HashMap<String, StructuredEntity>,
}

impl StructuredMemory {
    pub fn new() -> Self {
        Self {
            entities: HashMap::new(),
        }
    }

    pub fn upsert(&mut self, id: impl Into<String>, attrs: HashMap<String, String>) {
        let id = canonicalize(&id.into());
        self.entities.insert(
            id.clone(),
            StructuredEntity {
                id,
                attrs,
            },
        );
    }

    pub fn get(&self, id: &str) -> Option<&StructuredEntity> {
        self.entities.get(&canonicalize(id))
    }

    pub fn delete(&mut self, id: &str) {
        self.entities.remove(&canonicalize(id));
    }

    pub fn snapshot(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    pub fn from_snapshot(snapshot: &str) -> Self {
        serde_json::from_str(snapshot).unwrap_or_default()
    }
}

fn canonicalize(id: &str) -> String {
    id.trim().to_ascii_lowercase()
}
