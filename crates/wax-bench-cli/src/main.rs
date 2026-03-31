use std::time::Instant;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use wax_bench_artifacts::{
    read_run_bundle, render_replay_command, write_run_bundle_with_replay_config,
    ReplayConfigArtifact,
};
use wax_bench_metrics::{MemoryReading, MemorySampler, MetricCollector, MonotonicClock};
use wax_bench_model::{BenchmarkId, DatasetPackManifest, MaterializationMode, VectorQueryMode};
use wax_bench_packer::{AdhocPackRequest, PackRequest};
use wax_bench_reducer::{
    build_vector_lane_matrix_report, compute_search_quality_summary_from_paths, reduce_run_dir,
    render_vector_mode_compare_report,
};
use wax_bench_runner::{BenchmarkRunner, RunRequest, Workload};
use wax_bench_text_engine::{query_text_preview, PackedTextEngine};

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
    PackAdhoc {
        #[arg(long)]
        docs: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        tier: String,
    },
    Run {
        #[arg(long)]
        dataset: PathBuf,
        #[arg(long)]
        workload: String,
        #[arg(long)]
        sample_count: u32,
        #[arg(long, default_value = "auto")]
        vector_mode: String,
        #[arg(long)]
        artifact_dir: Option<PathBuf>,
    },
    Query {
        #[arg(long)]
        dataset: PathBuf,
        #[arg(long)]
        text: String,
        #[arg(long, default_value_t = 5)]
        top_k: usize,
    },
    QualityReport {
        #[arg(long)]
        query_set: PathBuf,
        #[arg(long)]
        qrels: PathBuf,
        #[arg(long)]
        results: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    Reduce {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        baseline: Option<PathBuf>,
    },
    MatrixReport {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    ModeCompareReport {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
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
        Some(Command::PackAdhoc { docs, out, tier }) => {
            wax_bench_packer::pack_adhoc_dataset(&AdhocPackRequest::new(docs, out, tier))
                .map_err(|error| error.message)?;
            Ok(())
        }
        Some(Command::Run {
            dataset,
            workload,
            sample_count,
            vector_mode,
            artifact_dir,
        }) => {
            let workload = match workload.as_str() {
                "container_open" => Workload::ContainerOpen,
                "materialize_vector" => Workload::MaterializeVector,
                "ttfq_text" => Workload::TtfqText,
                "ttfq_vector" => Workload::TtfqVector,
                "warm_text" => Workload::WarmText,
                "warm_vector" => Workload::WarmVector,
                "warm_hybrid" => Workload::WarmHybrid,
                _ => return Err("unsupported workload".to_owned()),
            };
            let vector_mode = parse_vector_mode(&vector_mode)?;
            let manifest_text = std::fs::read_to_string(dataset.join("manifest.json"))
                .map_err(|error| error.to_string())?;
            let manifest: DatasetPackManifest =
                serde_json::from_str(&manifest_text).map_err(|error| error.to_string())?;
            let benchmark_id = BenchmarkId {
                dataset_id: manifest.identity.dataset_id,
                workload_id: workload_label(&workload).to_owned(),
                sample_index: 0,
            };
            let use_test_mode = std::env::var("WAX_BENCH_TEST_MODE").ok().as_deref() == Some("1");
            let request = RunRequest {
                dataset_path: dataset,
                workload,
                materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
            };
            let measured = if use_test_mode {
                wax_bench_runner::run_benchmark_samples_with_runner_factory(
                    || BenchmarkRunner::new(PackedTextEngine::with_vector_mode(vector_mode)),
                    &request,
                    sample_count,
                    || MetricCollector::new(DeterministicClock::new(), TestMemorySampler),
                )
            } else {
                wax_bench_runner::run_benchmark_samples_with_runner_factory(
                    || BenchmarkRunner::new(PackedTextEngine::with_vector_mode(vector_mode)),
                    &request,
                    sample_count,
                    || MetricCollector::new(SystemClock::new(), UnavailableMemorySampler),
                )
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
                vector_mode,
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
        Some(Command::Query {
            dataset,
            text,
            top_k,
        }) => {
            let hits = query_text_preview(&dataset, &text, top_k)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&hits).map_err(|error| error.to_string())?
            );
            Ok(())
        }
        Some(Command::QualityReport {
            query_set,
            qrels,
            results,
            output,
        }) => {
            let summary = compute_search_quality_summary_from_paths(&query_set, &qrels, &results)
                .map_err(|error| error.message)?;
            let rendered =
                serde_json::to_string_pretty(&summary).map_err(|error| error.to_string())?;
            if let Some(output) = output {
                std::fs::write(output, &rendered).map_err(|error| error.to_string())?;
            }
            println!("{rendered}");
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
        Some(Command::MatrixReport { input, output }) => {
            let report =
                build_vector_lane_matrix_report(input.as_path()).map_err(|error| error.message)?;
            if let Some(output) = output {
                std::fs::write(&output, &report.markdown).map_err(|error| error.to_string())?;
            }
            println!("{}", report.markdown);
            Ok(())
        }
        Some(Command::ModeCompareReport { input, output }) => {
            let markdown = render_vector_mode_compare_report(input.as_path())
                .map_err(|error| error.message)?;
            if let Some(output) = output {
                std::fs::write(&output, &markdown).map_err(|error| error.to_string())?;
            }
            println!("{}", markdown);
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

fn parse_vector_mode(value: &str) -> Result<VectorQueryMode, String> {
    match value {
        "auto" => Ok(VectorQueryMode::Auto),
        "exact_flat" => Ok(VectorQueryMode::ExactFlat),
        "hnsw" => Ok(VectorQueryMode::Hnsw),
        "preview_q8" => Ok(VectorQueryMode::PreviewQ8),
        _ => Err("unsupported vector_mode".to_owned()),
    }
}

fn workload_label(workload: &Workload) -> &'static str {
    match workload {
        Workload::ContainerOpen => "container_open",
        Workload::MaterializeVector => "materialize_vector",
        Workload::TtfqText => "ttfq_text",
        Workload::TtfqVector => "ttfq_vector",
        Workload::WarmText => "warm_text",
        Workload::WarmVector => "warm_vector",
        Workload::WarmHybrid => "warm_hybrid",
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
    fn now_us(&mut self) -> u64 {
        self.start.elapsed().as_micros() as u64
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
            ticks: [0, 4_000, 8_000, 12_000],
            index: 0,
        }
    }
}

impl MonotonicClock for DeterministicClock {
    fn now_us(&mut self) -> u64 {
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
