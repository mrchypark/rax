use std::path::PathBuf;

use clap::{Parser, Subcommand};
use wax_bench_packer::PackRequest;
use wax_bench_runner::{BenchmarkRunner, NoopWaxEngine, RunRequest, Workload};
use wax_bench_model::MaterializationMode;

#[derive(Debug, Parser)]
#[command(name = "wax-bench-cli")]
#[command(about = "Wax v2 benchmark harness CLI")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Pack {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        tier: String,
        #[arg(long)]
        variant: String,
    },
    Run {
        #[arg(long)]
        dataset: PathBuf,
        #[arg(long)]
        workload: String,
        #[arg(long)]
        sample_count: u32,
    },
}

fn main() -> Result<(), String> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Pack {
            source,
            out,
            tier,
            variant,
        }) => {
            wax_bench_packer::pack_dataset(&PackRequest::new(source, out, tier, variant))
                .map_err(|error| error.message)?;
            Ok(())
        }
        Some(Command::Run {
            dataset,
            workload,
            sample_count,
        }) => {
            let workload = match workload.as_str() {
                "container_open" => Workload::ContainerOpen,
                "ttfq_text" => Workload::TtfqText,
                _ => return Err("unsupported workload".to_owned()),
            };
            let mut runner = BenchmarkRunner::new(NoopWaxEngine);
            for _ in 0..sample_count {
                let _ = runner
                    .run(&RunRequest {
                        dataset_path: dataset.clone(),
                        workload: workload.clone(),
                        materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
                    })
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        }
        None => Ok(()),
    }
}
