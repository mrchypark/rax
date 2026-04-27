use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand};
use serde::Deserialize;
use wax_v2_runtime::{
    Memory, MemorySearchOptions, NewDocument, NewDocumentVector, RuntimeSearchMode,
    RuntimeSearchRequest, RuntimeStore,
};

#[derive(Debug, Parser)]
#[command(name = "wax")]
#[command(about = "Wax v2 product CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Create {
        #[arg(long)]
        root: PathBuf,
    },
    Ingest {
        #[command(subcommand)]
        command: IngestCommand,
    },
    ImportCompat {
        #[arg(long)]
        root: PathBuf,
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
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long)]
        store: Option<PathBuf>,
        #[arg(long)]
        text: String,
        #[arg(long, default_value_t = 5)]
        top_k: usize,
        #[arg(long, default_value_t = false)]
        preview: bool,
    },
}

#[derive(Debug, Subcommand)]
enum IngestCommand {
    Docs {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        input: PathBuf,
    },
    Vectors {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
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

fn main() -> Result<(), String> {
    let cli = Cli::parse();

    match cli.command {
        Command::Create { root } => {
            let mut runtime = RuntimeStore::create(&root).map_err(|error| error.to_string())?;
            runtime.close().map_err(|error| error.to_string())?;
            Ok(())
        }
        Command::Ingest { command } => match command {
            IngestCommand::Docs { root, input } => {
                let mut runtime = RuntimeStore::open(&root).map_err(|error| error.to_string())?;
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
                let report = runtime
                    .writer()
                    .map_err(|error| error.to_string())?
                    .publish_raw_documents(documents)
                    .map_err(|error| error.to_string())?;
                println!("{}", render_publish_report(&report)?);
                runtime.close().map_err(|error| error.to_string())?;
                Ok(())
            }
            IngestCommand::Vectors { root, input } => {
                let mut runtime = RuntimeStore::open(&root).map_err(|error| error.to_string())?;
                let vectors = read_jsonl::<CliNewDocumentVector>(&input)?
                    .into_iter()
                    .map(|vector| NewDocumentVector::new(vector.doc_id, vector.values))
                    .collect::<Vec<_>>();
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
        Command::ImportCompat { root } => {
            let mut runtime = RuntimeStore::open(&root).map_err(|error| error.to_string())?;
            let report = runtime
                .writer()
                .map_err(|error| error.to_string())?
                .import_compatibility_snapshot()
                .map_err(|error| error.to_string())?;
            println!("{}", render_publish_report(&report)?);
            runtime.close().map_err(|error| error.to_string())?;
            Ok(())
        }
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
            let mut memory = Memory::open_existing(&store).map_err(|error| error.to_string())?;
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
            root,
            store,
            text,
            top_k,
            preview,
        } => {
            if let Some(store) = store {
                let mut memory =
                    Memory::open_existing(&store).map_err(|error| error.to_string())?;
                let response = memory
                    .search_with_options(
                        text,
                        MemorySearchOptions {
                            mode: RuntimeSearchMode::Hybrid,
                            top_k,
                            include_preview: preview,
                        },
                    )
                    .map_err(|error| error.to_string())?;
                println!("{}", render_hits(response.hits)?);
                memory.close().map_err(|error| error.to_string())?;
                return Ok(());
            }
            let root = root.ok_or_else(|| "search requires --root or --store".to_owned())?;
            let mut runtime = RuntimeStore::open(&root).map_err(|error| error.to_string())?;
            let response = runtime
                .search(RuntimeSearchRequest {
                    mode: RuntimeSearchMode::Text,
                    text_query: Some(text),
                    vector_query: None,
                    top_k,
                    include_preview: preview,
                })
                .map_err(|error| error.to_string())?;
            println!("{}", render_hits(response.hits)?);
            runtime.close().map_err(|error| error.to_string())?;
            Ok(())
        }
    }
}

fn render_hits(hits: Vec<wax_v2_runtime::RuntimeSearchHit>) -> Result<String, String> {
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

fn render_publish_report(report: &wax_v2_runtime::RuntimePublishReport) -> Result<String, String> {
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

fn runtime_publish_family_name(family: &wax_v2_runtime::RuntimePublishFamily) -> &'static str {
    match family {
        wax_v2_runtime::RuntimePublishFamily::Doc => "doc",
        wax_v2_runtime::RuntimePublishFamily::Text => "text",
        wax_v2_runtime::RuntimePublishFamily::Vector => "vector",
    }
}
