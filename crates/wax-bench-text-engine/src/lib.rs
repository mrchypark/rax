use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use wax_bench_model::{
    DatasetPackManifest, EnginePhase, EngineStats, MountRequest, OpenRequest, OpenResult,
    SearchRequest, SearchResult, WaxEngine,
};

#[derive(Debug, Default)]
pub struct PackedTextEngine {
    mounted_path: Option<PathBuf>,
    phase: EnginePhase,
    manifest: Option<DatasetPackManifest>,
    text_lane: Option<TextLane>,
}

impl PackedTextEngine {
    pub fn is_text_lane_materialized(&self) -> bool {
        self.text_lane.is_some()
    }

    fn manifest(&self) -> Result<&DatasetPackManifest, String> {
        self.manifest
            .as_ref()
            .ok_or_else(|| "manifest not loaded".to_owned())
    }

    fn mount_root(&self) -> Result<&Path, String> {
        self.mounted_path
            .as_deref()
            .ok_or_else(|| "dataset path not mounted".to_owned())
    }

    fn ensure_text_lane(&mut self) -> Result<&TextLane, String> {
        if self.text_lane.is_none() {
            let mount_root = self.mount_root()?.to_path_buf();
            let manifest = self.manifest()?.clone();
            self.text_lane = Some(TextLane::load(&mount_root, &manifest)?);
        }
        self.text_lane
            .as_ref()
            .ok_or_else(|| "text lane not materialized".to_owned())
    }
}

impl WaxEngine for PackedTextEngine {
    type Error = String;

    fn mount(&mut self, request: MountRequest) -> Result<(), Self::Error> {
        self.mounted_path = Some(request.store_path);
        self.phase = EnginePhase::Mounted;
        self.manifest = None;
        self.text_lane = None;
        Ok(())
    }

    fn open(&mut self, _request: OpenRequest) -> Result<OpenResult, Self::Error> {
        if self.phase != EnginePhase::Mounted {
            return Err("engine must be mounted before open".to_owned());
        }

        let manifest_text = fs::read_to_string(self.mount_root()?.join("manifest.json"))
            .map_err(|error| error.to_string())?;
        let manifest: DatasetPackManifest =
            serde_json::from_str(&manifest_text).map_err(|error| error.to_string())?;

        self.manifest = Some(manifest);
        self.phase = EnginePhase::Open;
        Ok(OpenResult)
    }

    fn search(&mut self, request: SearchRequest) -> Result<SearchResult, Self::Error> {
        if self.phase != EnginePhase::Open {
            return Err("engine must be open before search".to_owned());
        }

        if request.query_text == "__materialize_text_lane__" {
            self.ensure_text_lane()?;
            return Ok(SearchResult { hits: Vec::new() });
        }

        let lane = self.ensure_text_lane()?;
        let query_text = if request.query_text == "__ttfq_text__" {
            lane.first_text_query.clone()
        } else {
            request.query_text
        };
        Ok(SearchResult {
            hits: lane.search(&query_text),
        })
    }

    fn get_stats(&self) -> EngineStats {
        EngineStats {
            phase: self.phase,
            last_mounted_path: self.mounted_path.clone(),
        }
    }
}

#[derive(Debug)]
struct TextLane {
    first_text_query: String,
    docs: Vec<DocumentRecord>,
    inverted: HashMap<String, Vec<usize>>,
}

impl TextLane {
    fn load(mount_root: &Path, manifest: &DatasetPackManifest) -> Result<Self, String> {
        let docs_path = manifest
            .files
            .iter()
            .find(|file| file.kind == "documents")
            .map(|file| mount_root.join(&file.path))
            .ok_or_else(|| "documents file missing from manifest".to_owned())?;
        let query_path = manifest
            .query_sets
            .first()
            .map(|query_set| mount_root.join(&query_set.path))
            .ok_or_else(|| "query_set missing from manifest".to_owned())?;

        let docs = load_documents(&docs_path)?;
        let first_text_query = load_first_text_query(&query_path)?;
        let mut inverted: HashMap<String, Vec<usize>> = HashMap::new();
        for (index, doc) in docs.iter().enumerate() {
            let mut seen = HashSet::new();
            for token in tokenize(&doc.text) {
                if seen.insert(token.clone()) {
                    inverted.entry(token).or_default().push(index);
                }
            }
        }

        Ok(Self {
            first_text_query,
            docs,
            inverted,
        })
    }

    fn search(&self, query: &str) -> Vec<String> {
        let mut scores: HashMap<usize, u32> = HashMap::new();
        for token in tokenize(query) {
            if let Some(doc_indices) = self.inverted.get(&token) {
                for doc_index in doc_indices {
                    *scores.entry(*doc_index).or_insert(0) += 1;
                }
            }
        }

        let mut hits: Vec<(usize, u32)> = scores.into_iter().collect();
        hits.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| self.docs[left.0].doc_id.cmp(&self.docs[right.0].doc_id))
        });
        hits.into_iter()
            .map(|(doc_index, _)| self.docs[doc_index].doc_id.clone())
            .collect()
    }
}

fn load_documents(path: &Path) -> Result<Vec<DocumentRecord>, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(|error| error.to_string()))
        .collect()
}

fn load_first_text_query(path: &Path) -> Result<String, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let query: QueryRecord = serde_json::from_str(line).map_err(|error| error.to_string())?;
        if query.lane_eligibility.text {
            return Ok(query.query_text);
        }
    }

    Err("no text-eligible query found".to_owned())
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

#[derive(Debug, Clone, Deserialize)]
struct DocumentRecord {
    doc_id: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct QueryRecord {
    query_text: String,
    lane_eligibility: LaneEligibility,
}

#[derive(Debug, Deserialize)]
struct LaneEligibility {
    text: bool,
}
