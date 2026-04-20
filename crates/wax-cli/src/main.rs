use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use serde::Deserialize;
use wax_v2_runtime::{
    NewDocument, NewDocumentVector, RuntimeSearchMode, RuntimeSearchRequest, RuntimeStore,
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
    Search {
        #[arg(long)]
        root: PathBuf,
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
                        let mut runtime_document =
                            NewDocument::new(document.doc_id, document.text)
                                .with_metadata(document.metadata);
                        if let Some(timestamp_ms) = document.timestamp_ms {
                            runtime_document = runtime_document.with_timestamp_ms(timestamp_ms);
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
        Command::Search {
            root,
            text,
            top_k,
            preview,
        } => {
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
            let rendered_hits = response
                .hits
                .into_iter()
                .map(|hit| {
                    serde_json::json!({
                        "doc_id": hit.doc_id,
                        "preview": hit.preview,
                    })
                })
                .collect::<Vec<_>>();
            println!(
                "{}",
                serde_json::to_string_pretty(&rendered_hits).map_err(|error| error.to_string())?
            );
            runtime.close().map_err(|error| error.to_string())?;
            Ok(())
        }
    }
}

fn default_metadata() -> serde_json::Value {
    serde_json::json!({})
}

fn read_jsonl<T: for<'de> Deserialize<'de>>(path: &std::path::Path) -> Result<Vec<T>, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(|error| error.to_string()))
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
