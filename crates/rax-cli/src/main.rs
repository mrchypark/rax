use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use rax_runtime::{
    Memory, MemorySearchOptions, NewDocument, NewDocumentVector, RuntimeSearchMode,
    RuntimeSearchRequest, RuntimeStore,
};
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(name = "rax")]
#[command(about = "Rax product CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Create {
        #[arg(long, help = "Direct .rax product store file")]
        store: PathBuf,
    },
    Ingest {
        #[command(subcommand)]
        command: IngestCommand,
    },
    Remember {
        #[arg(long)]
        store: PathBuf,
        text: String,
    },
    Recall {
        #[arg(long)]
        store: PathBuf,
        query: String,
        #[arg(long, default_value_t = 5)]
        top_k: usize,
        #[arg(long = "no-preview", action = ArgAction::SetFalse, default_value_t = true)]
        preview: bool,
    },
    Search {
        #[arg(long, help = "Direct .rax product store file")]
        store: PathBuf,
        #[arg(long, help = "Text query for raw runtime text search")]
        text: Option<String>,
        #[arg(long, value_enum, default_value_t = CliSearchMode::Text)]
        mode: CliSearchMode,
        #[arg(
            long,
            help = "JSON file containing a query vector array or {\"values\": [...]}"
        )]
        vector_input: Option<PathBuf>,
        #[arg(long, default_value_t = 5)]
        top_k: usize,
        #[arg(long, default_value_t = false)]
        preview: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CliSearchMode {
    Text,
    Vector,
    Hybrid,
}

#[derive(Debug, Subcommand)]
enum IngestCommand {
    Docs {
        #[arg(long, help = "Direct .rax product store file")]
        store: PathBuf,
        #[arg(long, help = "JSONL raw document input")]
        input: PathBuf,
    },
    Vectors {
        #[arg(long, help = "Direct .rax product store file")]
        store: PathBuf,
        #[arg(long, help = "JSONL explicit vector input for existing documents")]
        input: PathBuf,
    },
}

#[derive(Debug, Deserialize)]
struct CliNewDocument {
    doc_id: String,
    text: String,
    #[serde(default = "default_metadata")]
    metadata: serde_json::Value,
    #[serde(default)]
    timestamp_ms: Option<u64>,
    #[serde(flatten)]
    extra_fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct CliNewDocumentVector {
    doc_id: String,
    values: Vec<f32>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CliQueryVector {
    Values(Vec<f32>),
    Object(CliQueryVectorObject),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CliQueryVectorObject {
    values: Vec<f32>,
}

fn main() -> Result<(), String> {
    let cli = Cli::parse();

    match cli.command {
        Command::Create { store } => {
            let mut runtime =
                RuntimeStore::open_or_create_at(&store).map_err(|error| error.to_string())?;
            runtime.close().map_err(|error| error.to_string())?;
            Ok(())
        }
        Command::Ingest { command } => match command {
            IngestCommand::Docs { store, input } => {
                let documents = read_jsonl::<CliNewDocument>(&input)?
                    .into_iter()
                    .map(|document| {
                        let mut runtime_document = NewDocument::new(document.doc_id, document.text)
                            .with_metadata(document.metadata);
                        if let Some(timestamp_ms) = document.timestamp_ms {
                            runtime_document = runtime_document.with_timestamp_ms(timestamp_ms);
                        }
                        for (key, value) in document.extra_fields {
                            runtime_document = runtime_document.with_extra_field(key, value);
                        }
                        runtime_document
                    })
                    .collect::<Vec<_>>();
                let mut runtime =
                    RuntimeStore::open_or_create_at(&store).map_err(|error| error.to_string())?;
                let report = runtime
                    .writer()
                    .map_err(|error| error.to_string())?
                    .publish_raw_documents(documents)
                    .map_err(|error| error.to_string())?;
                println!("{}", render_publish_report(&report)?);
                runtime.close().map_err(|error| error.to_string())?;
                Ok(())
            }
            IngestCommand::Vectors { store, input } => {
                let vectors = read_jsonl::<CliNewDocumentVector>(&input)?
                    .into_iter()
                    .map(|vector| NewDocumentVector::new(vector.doc_id, vector.values))
                    .collect::<Vec<_>>();
                let mut runtime = open_existing_runtime_store_for_vectors(&store)?;
                let report = runtime
                    .writer()
                    .map_err(|error| error.to_string())?
                    .publish_raw_vectors(vectors)
                    .map_err(|error| error.to_string())?;
                println!("{}", render_publish_report(&report)?);
                runtime.close().map_err(|error| error.to_string())?;
                Ok(())
            }
        },
        Command::Remember { store, text } => {
            let mut memory = Memory::open(&store).map_err(|error| error.to_string())?;
            let doc_id = memory.remember(text).map_err(|error| error.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "doc_id": doc_id }))
                    .map_err(|error| error.to_string())?
            );
            memory.close().map_err(|error| error.to_string())?;
            Ok(())
        }
        Command::Recall {
            store,
            query,
            top_k,
            preview,
        } => {
            let mut memory =
                Memory::open_existing_read_only(&store).map_err(|error| error.to_string())?;
            let response = memory
                .search_with_options(
                    query,
                    MemorySearchOptions {
                        mode: RuntimeSearchMode::Hybrid,
                        top_k,
                        include_preview: preview,
                    },
                )
                .map_err(|error| error.to_string())?;
            println!("{}", render_hits(response.hits)?);
            memory.close().map_err(|error| error.to_string())?;
            Ok(())
        }
        Command::Search {
            store,
            text,
            mode,
            vector_input,
            top_k,
            preview,
        } => {
            let mut runtime = RuntimeStore::open_existing_read_only_at(&store)
                .map_err(|error| error.to_string())?;
            let request = build_search_request(mode, text, vector_input, top_k, preview)?;
            let response = runtime.search(request).map_err(|error| error.to_string())?;
            println!("{}", render_hits(response.hits)?);
            runtime.close().map_err(|error| error.to_string())?;
            Ok(())
        }
    }
}

fn build_search_request(
    mode: CliSearchMode,
    text: Option<String>,
    vector_input: Option<PathBuf>,
    top_k: usize,
    include_preview: bool,
) -> Result<RuntimeSearchRequest, String> {
    let runtime_mode = match mode {
        CliSearchMode::Text => RuntimeSearchMode::Text,
        CliSearchMode::Vector => RuntimeSearchMode::Vector,
        CliSearchMode::Hybrid => RuntimeSearchMode::Hybrid,
    };
    let text_query = match mode {
        CliSearchMode::Text | CliSearchMode::Hybrid => {
            Some(text.ok_or_else(|| format!("search --mode {} requires --text", mode_name(mode)))?)
        }
        CliSearchMode::Vector => {
            if text.is_some() {
                return Err("search --mode vector does not accept --text".to_owned());
            }
            None
        }
    };
    let vector_query = match mode {
        CliSearchMode::Text => {
            if vector_input.is_some() {
                return Err("search --mode text does not accept --vector-input".to_owned());
            }
            None
        }
        CliSearchMode::Vector | CliSearchMode::Hybrid => {
            Some(read_query_vector(&vector_input.ok_or_else(|| {
                format!("search --mode {} requires --vector-input", mode_name(mode))
            })?)?)
        }
    };
    Ok(RuntimeSearchRequest {
        mode: runtime_mode,
        text_query,
        vector_query,
        top_k,
        include_preview,
    })
}

fn mode_name(mode: CliSearchMode) -> &'static str {
    match mode {
        CliSearchMode::Text => "text",
        CliSearchMode::Vector => "vector",
        CliSearchMode::Hybrid => "hybrid",
    }
}

fn read_query_vector(path: &std::path::Path) -> Result<Vec<f32>, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let vector = serde_json::from_reader::<_, CliQueryVector>(BufReader::new(file))
        .map_err(|error| error.to_string())?;
    let values = match vector {
        CliQueryVector::Values(values) => values,
        CliQueryVector::Object(object) => object.values,
    };
    if values.is_empty() {
        return Err("query vector must contain at least one value".to_owned());
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err("query vector must contain only finite float values".to_owned());
    }
    Ok(values)
}

fn open_existing_runtime_store_for_vectors(
    store: &std::path::Path,
) -> Result<RuntimeStore, String> {
    if !store.exists() {
        return Err(format!(
            "store file {} does not exist; run ingest docs first",
            store.display()
        ));
    }
    RuntimeStore::open_existing_at(store).map_err(|error| error.to_string())
}

fn render_hits(hits: Vec<rax_runtime::RuntimeSearchHit>) -> Result<String, String> {
    let rendered_hits = hits
        .into_iter()
        .map(|hit| {
            serde_json::json!({
                "doc_id": hit.doc_id,
                "preview": hit.preview,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&rendered_hits).map_err(|error| error.to_string())
}

fn default_metadata() -> serde_json::Value {
    serde_json::json!({})
}

fn read_jsonl<T: for<'de> Deserialize<'de>>(path: &std::path::Path) -> Result<Vec<T>, String> {
    BufReader::new(File::open(path).map_err(|error| error.to_string())?)
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some(other),
        })
        .map(|line| {
            let line = line.map_err(|error| error.to_string())?;
            serde_json::from_str(&line).map_err(|error| error.to_string())
        })
        .collect()
}

fn render_publish_report(report: &rax_runtime::RuntimePublishReport) -> Result<String, String> {
    serde_json::to_string_pretty(&serde_json::json!({
        "generation": report.generation,
        "published_families": report
            .published_families
            .iter()
            .map(runtime_publish_family_name)
            .collect::<Vec<_>>(),
    }))
    .map_err(|error| error.to_string())
}

fn runtime_publish_family_name(family: &rax_runtime::RuntimePublishFamily) -> &'static str {
    match family {
        rax_runtime::RuntimePublishFamily::Doc => "doc",
        rax_runtime::RuntimePublishFamily::Text => "text",
        rax_runtime::RuntimePublishFamily::Vector => "vector",
    }
}
