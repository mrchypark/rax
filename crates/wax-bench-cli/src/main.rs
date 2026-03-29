use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "wax-bench-cli")]
#[command(about = "Wax v2 benchmark harness CLI")]
struct Cli;

fn main() {
    let _ = Cli::parse();
}
