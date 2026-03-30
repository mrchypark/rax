use std::time::Instant;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use wax_bench_artifacts::{
    read_run_bundle, render_replay_command, write_run_bundle_with_replay_config,
    ReplayConfigArtifact,
};
use wax_bench_metrics::{MemoryReading, MemorySampler, MetricCollector, MonotonicClock};
use wax_bench_model::{BenchmarkId, DatasetPackManifest, MaterializationMode};
use wax_bench_packer::PackRequest;
use wax_bench_reducer::reduce_run_dir;
use wax_bench_runner::{BenchmarkRunner, NoopWaxEngine, RunRequest, Workload};

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
        #[arg(long)]
        artifact_dir: Option<PathBuf>,
    },
    Reduce {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        baseline: Option<PathBuf>,
    },
    Replay {
        #[arg(long)]
        input: PathBuf,
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
            artifact_dir,
        }) => {
            let workload = match workload.as_str() {
                "container_open" => Workload::ContainerOpen,
                "ttfq_text" => Workload::TtfqText,
                _ => return Err("unsupported workload".to_owned()),
            };
            let manifest_text = std::fs::read_to_string(dataset.join("manifest.json"))
                .map_err(|error| error.to_string())?;
            let manifest: DatasetPackManifest =
                serde_json::from_str(&manifest_text).map_err(|error| error.to_string())?;
            let benchmark_id = BenchmarkId {
                dataset_id: manifest.identity.dataset_id,
                workload_id: workload_label(&workload).to_owned(),
                sample_index: 0,
            };
            let mut runner = BenchmarkRunner::new(NoopWaxEngine);
            let use_test_mode = std::env::var("WAX_BENCH_TEST_MODE").ok().as_deref() == Some("1");
            let request = RunRequest {
                dataset_path: dataset,
                workload,
                materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
            };
            let measured = if use_test_mode {
                wax_bench_runner::run_benchmark_samples(&mut runner, &request, sample_count, || {
                    MetricCollector::new(DeterministicClock::new(), TestMemorySampler)
                })
            } else {
                wax_bench_runner::run_benchmark_samples(&mut runner, &request, sample_count, || {
                    MetricCollector::new(SystemClock::new(), UnavailableMemorySampler)
                })
            }
            .map_err(|error| error.to_string())?;
            let artifact_dir = artifact_dir
                .or_else(|| {
                    std::env::var("WAX_BENCH_ARTIFACT_DIR")
                        .ok()
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from("artifacts/latest"));
            let replay = ReplayConfigArtifact {
                dataset_path: Some(request.dataset_path.display().to_string()),
                workload_id: workload_label(&request.workload).to_owned(),
                sample_count,
                materialization_mode: request.materialization_mode,
                artifact_dir: artifact_dir.display().to_string(),
            };
            write_run_bundle_with_replay_config(
                artifact_dir.as_path(),
                "run-local",
                &benchmark_id,
                &manifest.checksums.fairness_fingerprint,
                &measured,
                &replay,
            )
            .map_err(|error| error.to_string())?;
            Ok(())
        }
        Some(Command::Reduce { input, baseline }) => {
            let report = reduce_run_dir(input.as_path(), baseline.as_deref())
                .map_err(|error| error.message)?;
            std::fs::write(
                input.join("reduced-summary.json"),
                serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
            println!("{}", report.markdown);
            Ok(())
        }
        Some(Command::Replay { input }) => {
            let bundle = read_run_bundle(input.as_path()).map_err(|error| error.message)?;
            println!(
                "{}",
                render_replay_command(&bundle.manifest.replay).map_err(|error| error.to_string())?
            );
            Ok(())
        }
        None => Ok(()),
    }
}

fn workload_label(workload: &Workload) -> &'static str {
    match workload {
        Workload::ContainerOpen => "container_open",
        Workload::TtfqText => "ttfq_text",
    }
}

struct SystemClock {
    start: Instant,
}

impl SystemClock {
    fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl MonotonicClock for SystemClock {
    fn now_ms(&mut self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

struct UnavailableMemorySampler;

impl MemorySampler for UnavailableMemorySampler {
    fn sample_resident_bytes(&self) -> MemoryReading {
        MemoryReading::Unavailable {
            reason: "platform_not_supported".to_owned(),
        }
    }
}

struct DeterministicClock {
    ticks: [u64; 4],
    index: usize,
}

impl DeterministicClock {
    fn new() -> Self {
        Self {
            ticks: [0, 4, 8, 12],
            index: 0,
        }
    }
}

impl MonotonicClock for DeterministicClock {
    fn now_ms(&mut self) -> u64 {
        let value = self.ticks[self.index.min(self.ticks.len() - 1)];
        self.index += 1;
        value
    }
}

struct TestMemorySampler;

impl MemorySampler for TestMemorySampler {
    fn sample_resident_bytes(&self) -> MemoryReading {
        MemoryReading::Unavailable {
            reason: "test_mode".to_owned(),
        }
    }
}
