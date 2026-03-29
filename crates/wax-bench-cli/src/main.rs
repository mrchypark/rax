use std::path::PathBuf;

use clap::{Parser, Subcommand};
use wax_bench_packer::PackRequest;

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
        None => Ok(()),
    }
}
