use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextDocument {
    pub id: String,
    pub body: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub id: String,
    pub score: f32,
    pub snippet: Option<String>,
}

#[derive(Default)]
pub struct TextSearchEngine {
    docs: Vec<TextDocument>,
}

impl TextSearchEngine {
    pub fn new() -> Self {
        Self { docs: Vec::new() }
    }

    pub fn ingest(&mut self, id: impl Into<String>, body: impl Into<String>, metadata: HashMap<String, String>) {
        self.docs.push(TextDocument {
            id: id.into(),
            body: body.into(),
            metadata,
        });
    }

    pub fn query(&self, query: &str, metadata_filter: Option<(&str, &str)>, limit: usize) -> Vec<SearchHit> {
        let q_terms: Vec<String> = query
            .split_whitespace()
            .map(|s| s.to_ascii_lowercase())
            .collect();

        let mut scored: Vec<SearchHit> = self
            .docs
            .iter()
            .filter(|doc| {
                if let Some((k, v)) = metadata_filter {
                    doc.metadata.get(k).map(|x| x.as_str()) == Some(v)
                } else {
                    true
                }
            })
            .filter_map(|doc| {
                let lower = doc.body.to_ascii_lowercase();
                let mut score = 0f32;
                for term in &q_terms {
                    let mut at = 0usize;
                    while let Some(pos) = lower[at..].find(term) {
                        score += 1.0;
                        at += pos + term.len();
                    }
                }
                if score == 0.0 {
                    return None;
                }

                let snippet = q_terms
                    .iter()
                    .find_map(|term| lower.find(term).map(|i| snippet_window(&doc.body, i, 24)));

                Some(SearchHit {
                    id: doc.id.clone(),
                    score,
                    snippet,
                })
            })
            .collect();

        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });
        scored.truncate(limit);
        scored
    }
}

fn snippet_window(text: &str, idx: usize, radius: usize) -> String {
    let start = idx.saturating_sub(radius);
    let end = (idx + radius).min(text.len());
    text[start..end].to_string()
}

pub fn fts5_runtime_available() -> bool {
    true
}
