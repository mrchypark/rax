use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::Command as ProcessCommand;
use std::time::Instant;

use clap::{Parser, Subcommand};
use rax_bench_artifacts::{
    read_run_bundle, render_replay_command, write_run_bundle_with_replay_config,
    ReplayConfigArtifact,
};
use rax_bench_metrics::{MemoryReading, MemorySampler, MetricCollector, MonotonicClock};
use rax_bench_model::{
    embed_text, parse_workload, BenchmarkId, DatasetPackManifest, MaterializationMode,
    RankedDocumentHit, RankedQueryResult, VectorQueryMode,
};
use rax_bench_packer::{AdhocPackRequest, PackRequest};
use rax_bench_reducer::{
    build_vector_lane_matrix_report, compute_search_quality_summary_from_paths, reduce_run_dir,
    render_vector_mode_compare_report,
};
use rax_bench_runner::BenchmarkRunner;
use rax_bench_runner::RunRequest;
use rax_bench_text_engine::{
    profile_first_vector_query, query_batch_ranked_results, query_text_preview, PackedTextEngine,
};
use rax_runtime::{RuntimeSearchMode, RuntimeSearchRequest};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Parser)]
#[command(name = "rax-bench-cli")]
#[command(about = "Rax benchmark harness CLI")]
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
    QueryBatch {
        #[arg(long)]
        dataset: PathBuf,
        #[arg(long)]
        query_set: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long, default_value = "auto")]
        vector_mode: String,
    },
    ProfileVectorQuery {
        #[arg(long)]
        dataset: PathBuf,
        #[arg(long, default_value = "auto")]
        vector_mode: String,
        #[arg(long, default_value_t = 1)]
        sample_count: u32,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    SearchBench {
        #[arg(long)]
        dataset: PathBuf,
        #[arg(long)]
        query_set: PathBuf,
        #[arg(long)]
        sample_count: u32,
        #[arg(long, default_value = "auto")]
        vector_mode: String,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long)]
        artifact_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 1)]
        concurrency: u32,
        #[arg(long)]
        scale_label: Option<String>,
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
            rax_bench_packer::pack_dataset(&PackRequest::new(source, out, tier, variant))
                .map_err(|error| error.message)?;
            Ok(())
        }
        Some(Command::PackAdhoc { docs, out, tier }) => {
            rax_bench_packer::pack_adhoc_dataset(&AdhocPackRequest::new(docs, out, tier))
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
            let workload = parse_workload(&workload)
                .ok_or_else(|| format!("unsupported workload: {workload}"))?;
            let vector_mode = parse_vector_mode(&vector_mode)?;
            let manifest_text = std::fs::read_to_string(dataset.join("manifest.json"))
                .map_err(|error| error.to_string())?;
            let manifest: DatasetPackManifest =
                serde_json::from_str(&manifest_text).map_err(|error| error.to_string())?;
            let benchmark_id = BenchmarkId {
                dataset_id: manifest.identity.dataset_id,
                workload_id: workload.label().to_owned(),
                sample_index: 0,
            };
            let use_test_mode = std::env::var("RAX_BENCH_TEST_MODE").ok().as_deref() == Some("1");
            let request = RunRequest {
                dataset_path: dataset,
                workload,
                materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
            };
            let measured = if use_test_mode {
                rax_bench_runner::run_benchmark_samples_with_runner_factory(
                    || BenchmarkRunner::new(PackedTextEngine::with_vector_mode(vector_mode)),
                    &request,
                    sample_count,
                    || MetricCollector::new(DeterministicClock::new(), TestMemorySampler),
                )
            } else {
                rax_bench_runner::run_benchmark_samples_with_runner_factory(
                    || BenchmarkRunner::new(PackedTextEngine::with_vector_mode(vector_mode)),
                    &request,
                    sample_count,
                    || MetricCollector::new(SystemClock::new(), UnavailableMemorySampler),
                )
            }
            .map_err(|error| error.to_string())?;
            let artifact_dir = artifact_dir
                .or_else(|| {
                    std::env::var("RAX_BENCH_ARTIFACT_DIR")
                        .ok()
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from("artifacts/latest"));
            let replay = ReplayConfigArtifact {
                dataset_path: Some(request.dataset_path.display().to_string()),
                workload_id: request.workload.label().to_owned(),
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
        Some(Command::QueryBatch {
            dataset,
            query_set,
            output,
            vector_mode,
        }) => {
            let results =
                query_batch_ranked_results(&dataset, &query_set, parse_vector_mode(&vector_mode)?)?;
            let rendered =
                serde_json::to_string_pretty(&results).map_err(|error| error.to_string())?;
            if let Some(output) = output {
                std::fs::write(output, &rendered).map_err(|error| error.to_string())?;
            }
            println!("{rendered}");
            Ok(())
        }
        Some(Command::ProfileVectorQuery {
            dataset,
            vector_mode,
            sample_count,
            output,
        }) => {
            let vector_mode = parse_vector_mode(&vector_mode)?;
            let mut profiles = Vec::with_capacity(sample_count as usize);
            for _ in 0..sample_count {
                profiles.push(profile_first_vector_query(&dataset, vector_mode)?);
            }
            let rendered =
                serde_json::to_string_pretty(&profiles).map_err(|error| error.to_string())?;
            if let Some(output) = output {
                std::fs::write(output, &rendered).map_err(|error| error.to_string())?;
            }
            println!("{rendered}");
            Ok(())
        }
        Some(Command::SearchBench {
            dataset,
            query_set,
            sample_count,
            vector_mode,
            output,
            artifact_dir,
            concurrency,
            scale_label,
        }) => {
            let vector_mode = parse_vector_mode(&vector_mode)?;
            let run = run_runtime_search_bench(
                &dataset,
                &query_set,
                sample_count,
                vector_mode,
                concurrency,
                scale_label,
            )?;
            let rendered =
                serde_json::to_string_pretty(&run.summary).map_err(|error| error.to_string())?;
            if let Some(output) = output {
                std::fs::write(output, &rendered).map_err(|error| error.to_string())?;
            }
            if let Some(artifact_dir) = artifact_dir {
                write_runtime_search_bench_artifacts(
                    artifact_dir.as_path(),
                    &run.summary,
                    &run.ranked_results,
                    &query_set,
                    run.qrels_path.as_deref(),
                )?;
            }
            println!("{rendered}");
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

#[derive(Debug, Clone, Serialize)]
struct RuntimeSearchBenchSummary {
    dataset_id: String,
    scale_label: String,
    query_set: String,
    sample_count: u32,
    concurrency: u32,
    query_count: usize,
    filter_query_count: usize,
    supports_metadata_filters: bool,
    total_searches: usize,
    store_build_ms: f64,
    request_build_ms: f64,
    total_elapsed_ms: f64,
    total_search_only_ms: f64,
    warm_concurrent_elapsed_ms: f64,
    rss_kb_before: u64,
    rss_kb_after: u64,
    rss_kb_delta: i64,
    qps: f64,
    qps_end_to_end: f64,
    qps_search_only: f64,
    qps_warm_concurrent: f64,
    p50_query_latency_ms: f64,
    p95_query_latency_ms: f64,
    p99_query_latency_ms: f64,
    p50_cold_query_latency_ms: f64,
    p95_cold_query_latency_ms: f64,
    p99_cold_query_latency_ms: f64,
    p50_warm_query_latency_ms: f64,
    p95_warm_query_latency_ms: f64,
    p99_warm_query_latency_ms: f64,
    p50_warm_concurrent_query_latency_ms: f64,
    p95_warm_concurrent_query_latency_ms: f64,
    p99_warm_concurrent_query_latency_ms: f64,
    min_query_latency_ms: f64,
    max_query_latency_ms: f64,
    total_hit_count: usize,
    vector_mode: &'static str,
    per_query: Vec<RuntimeSearchBenchQuerySample>,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeSearchBenchQuerySample {
    sample_index: u32,
    phase: &'static str,
    worker_index: Option<u32>,
    query_id: String,
    query_class: String,
    mode: &'static str,
    include_preview: bool,
    latency_ms: f64,
    hit_count: usize,
}

#[derive(Debug, Clone)]
struct RuntimeSearchBenchQuery {
    query_id: String,
    query_class: String,
    query_text: String,
    top_k: usize,
    mode: RuntimeSearchMode,
    include_preview: bool,
    filter_spec: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone)]
struct RuntimeSearchBenchPreparedQuery {
    query: RuntimeSearchBenchQuery,
    request: RuntimeSearchRequest,
}

#[derive(Debug, Clone)]
struct RuntimeSearchBenchRun {
    summary: RuntimeSearchBenchSummary,
    ranked_results: Vec<RankedQueryResult>,
    qrels_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
struct RuntimeSearchBenchQueryRecord {
    query_id: String,
    query_class: String,
    query_text: String,
    top_k: u32,
    #[serde(default)]
    filter_spec: serde_json::Map<String, Value>,
    #[serde(default)]
    preview_expected: bool,
    lane_eligibility: RuntimeSearchBenchLaneEligibility,
}

#[derive(Debug, Clone, Deserialize)]
struct RuntimeSearchBenchLaneEligibility {
    text: bool,
    vector: bool,
    hybrid: bool,
}

fn run_runtime_search_bench(
    dataset: &std::path::Path,
    query_set: &std::path::Path,
    sample_count: u32,
    vector_mode: VectorQueryMode,
    concurrency: u32,
    scale_label: Option<String>,
) -> Result<RuntimeSearchBenchRun, String> {
    if sample_count == 0 {
        return Err("sample_count must be greater than zero".to_owned());
    }
    if concurrency == 0 {
        return Err("concurrency must be greater than zero".to_owned());
    }
    if vector_mode != VectorQueryMode::Auto {
        return Err("runtime search-bench currently supports only --vector-mode auto".to_owned());
    }

    let manifest = read_manifest(dataset)?;
    let qrels_path = discover_qrels_path(dataset, &manifest, query_set);
    let queries = load_runtime_search_bench_queries(query_set)?;
    if queries.is_empty() {
        return Err("query_set must contain at least one query".to_owned());
    }
    let filter_query_count = queries
        .iter()
        .filter(|query| !query.filter_spec.is_empty())
        .count();
    if filter_query_count > 0 {
        return Err(
            "runtime search-bench does not support metadata filter query sets yet".to_owned(),
        );
    }
    let dataset_id = manifest.identity.dataset_id.clone();
    let scale_label = scale_label.unwrap_or_else(|| dataset_id.clone());

    let rss_kb_before = rss_kb();
    let store_build_start = Instant::now();
    let runtime_root = tempfile::tempdir().map_err(|error| error.to_string())?;
    copy_dataset_pack(dataset, runtime_root.path())?;
    let store_path = runtime_store_path(runtime_root.path(), &manifest);
    {
        let mut runtime = rax_runtime::RuntimeStore::create(runtime_root.path())
            .map_err(|error| error.to_string())?;
        runtime
            .writer()
            .map_err(|error| error.to_string())?
            .publish_staged_compatibility_snapshot()
            .map_err(|error| error.to_string())?;
        runtime.close().map_err(|error| error.to_string())?;
    }
    let store_build_ms = elapsed_ms(store_build_start.elapsed());

    let total_start = Instant::now();
    let request_build_start = Instant::now();
    let prepared_queries = queries
        .into_iter()
        .map(|query| RuntimeSearchBenchPreparedQuery {
            request: runtime_search_request(&query, manifest.vector_profile.embedding_dimensions),
            query,
        })
        .collect::<Vec<_>>();
    let request_build_ms = elapsed_ms(request_build_start.elapsed());

    let measured_phase_count = 2 + if concurrency > 1 {
        concurrency as usize
    } else {
        0
    };
    let mut per_query =
        Vec::with_capacity(prepared_queries.len() * sample_count as usize * measured_phase_count);
    let mut ranked_results = Vec::with_capacity(prepared_queries.len());
    let mut total_search_only_ms = 0.0;
    for sample_index in 0..sample_count {
        for prepared in &prepared_queries {
            let mut runtime = rax_runtime::RuntimeStore::open_existing_read_only_at(&store_path)
                .map_err(|error| error.to_string())?;
            let request = prepared.request.clone();
            let query_start = Instant::now();
            let response = runtime.search(request).map_err(|error| error.to_string())?;
            let latency_ms = elapsed_ms(query_start.elapsed());
            runtime.close().map_err(|error| error.to_string())?;
            total_search_only_ms += latency_ms;
            per_query.push(runtime_search_bench_sample(
                prepared,
                sample_index,
                "cold_query",
                None,
                latency_ms,
                response.hits.len(),
            ));
        }

        let mut runtime = rax_runtime::RuntimeStore::open_existing_read_only_at(&store_path)
            .map_err(|error| error.to_string())?;
        for prepared in &prepared_queries {
            runtime
                .search(prepared.request.clone())
                .map_err(|error| error.to_string())?;
        }
        for prepared in &prepared_queries {
            let request = prepared.request.clone();
            let query_start = Instant::now();
            let response = runtime.search(request).map_err(|error| error.to_string())?;
            let latency_ms = elapsed_ms(query_start.elapsed());
            total_search_only_ms += latency_ms;
            if sample_index == 0 {
                ranked_results.push(RankedQueryResult {
                    query_id: prepared.query.query_id.clone(),
                    hits: response
                        .hits
                        .iter()
                        .map(|hit| RankedDocumentHit {
                            doc_id: hit.doc_id.clone(),
                        })
                        .collect(),
                });
            }
            per_query.push(runtime_search_bench_sample(
                prepared,
                sample_index,
                "warm_steady",
                None,
                latency_ms,
                response.hits.len(),
            ));
        }

        runtime.close().map_err(|error| error.to_string())?;
    }

    if concurrency > 1 {
        let concurrent_start = Instant::now();
        let mut workers = Vec::with_capacity(concurrency as usize);
        for worker_index in 0..concurrency {
            let worker_store_path = store_path.clone();
            let worker_prepared_queries = prepared_queries.clone();
            workers.push(std::thread::spawn(
                move || -> Result<(Vec<RuntimeSearchBenchQuerySample>, f64), String> {
                    let mut runtime =
                        rax_runtime::RuntimeStore::open_existing_read_only_at(&worker_store_path)
                            .map_err(|error| error.to_string())?;
                    for prepared in &worker_prepared_queries {
                        runtime
                            .search(prepared.request.clone())
                            .map_err(|error| error.to_string())?;
                    }

                    let mut worker_samples =
                        Vec::with_capacity(worker_prepared_queries.len() * sample_count as usize);
                    let mut worker_search_only_ms = 0.0;
                    for sample_index in 0..sample_count {
                        for prepared in &worker_prepared_queries {
                            let request = prepared.request.clone();
                            let query_start = Instant::now();
                            let response =
                                runtime.search(request).map_err(|error| error.to_string())?;
                            let latency_ms = elapsed_ms(query_start.elapsed());
                            worker_search_only_ms += latency_ms;
                            worker_samples.push(runtime_search_bench_sample(
                                prepared,
                                sample_index,
                                "warm_concurrent",
                                Some(worker_index),
                                latency_ms,
                                response.hits.len(),
                            ));
                        }
                    }
                    runtime.close().map_err(|error| error.to_string())?;
                    Ok((worker_samples, worker_search_only_ms))
                },
            ));
        }

        for worker in workers {
            let (mut worker_samples, worker_search_only_ms) = worker
                .join()
                .map_err(|_| "warm_concurrent worker panicked".to_owned())??;
            total_search_only_ms += worker_search_only_ms;
            per_query.append(&mut worker_samples);
        }
        let warm_concurrent_elapsed_ms = elapsed_ms(concurrent_start.elapsed());
        let total_elapsed_ms = elapsed_ms(total_start.elapsed());
        return runtime_search_bench_run_from_samples(RuntimeSearchBenchRunInput {
            dataset_id,
            scale_label,
            query_set,
            sample_count,
            concurrency,
            query_count: prepared_queries.len(),
            filter_query_count,
            store_build_ms,
            request_build_ms,
            total_elapsed_ms,
            total_search_only_ms,
            warm_concurrent_elapsed_ms,
            rss_kb_before,
            rss_kb_after: rss_kb(),
            vector_mode,
            per_query,
            ranked_results,
            qrels_path,
        });
    }
    let total_elapsed_ms = elapsed_ms(total_start.elapsed());
    runtime_search_bench_run_from_samples(RuntimeSearchBenchRunInput {
        dataset_id,
        scale_label,
        query_set,
        sample_count,
        concurrency,
        query_count: prepared_queries.len(),
        filter_query_count,
        store_build_ms,
        request_build_ms,
        total_elapsed_ms,
        total_search_only_ms,
        warm_concurrent_elapsed_ms: 0.0,
        rss_kb_before,
        rss_kb_after: rss_kb(),
        vector_mode,
        per_query,
        ranked_results,
        qrels_path,
    })
}

struct RuntimeSearchBenchRunInput<'a> {
    dataset_id: String,
    scale_label: String,
    query_set: &'a std::path::Path,
    sample_count: u32,
    concurrency: u32,
    query_count: usize,
    filter_query_count: usize,
    store_build_ms: f64,
    request_build_ms: f64,
    total_elapsed_ms: f64,
    total_search_only_ms: f64,
    warm_concurrent_elapsed_ms: f64,
    rss_kb_before: u64,
    rss_kb_after: u64,
    vector_mode: VectorQueryMode,
    per_query: Vec<RuntimeSearchBenchQuerySample>,
    ranked_results: Vec<RankedQueryResult>,
    qrels_path: Option<PathBuf>,
}

fn runtime_search_bench_run_from_samples(
    input: RuntimeSearchBenchRunInput<'_>,
) -> Result<RuntimeSearchBenchRun, String> {
    let mut latencies = input
        .per_query
        .iter()
        .map(|sample| sample.latency_ms)
        .collect::<Vec<_>>();
    latencies.sort_by(f64::total_cmp);
    let cold_latencies = sorted_phase_latencies(&input.per_query, "cold_query");
    let warm_latencies = sorted_phase_latencies(&input.per_query, "warm_steady");
    let warm_concurrent_latencies = sorted_phase_latencies(&input.per_query, "warm_concurrent");
    let total_searches = input.per_query.len();
    let total_hit_count = input.per_query.iter().map(|sample| sample.hit_count).sum();
    let qps_end_to_end = qps(total_searches, input.total_elapsed_ms);
    let warm_concurrent_searches = warm_concurrent_latencies.len();
    let rss_kb_delta = input.rss_kb_after as i64 - input.rss_kb_before as i64;

    Ok(RuntimeSearchBenchRun {
        summary: RuntimeSearchBenchSummary {
            dataset_id: input.dataset_id,
            scale_label: input.scale_label,
            query_set: input.query_set.display().to_string(),
            sample_count: input.sample_count,
            concurrency: input.concurrency,
            query_count: input.query_count,
            filter_query_count: input.filter_query_count,
            supports_metadata_filters: false,
            total_searches,
            store_build_ms: input.store_build_ms,
            request_build_ms: input.request_build_ms,
            total_elapsed_ms: input.total_elapsed_ms,
            total_search_only_ms: input.total_search_only_ms,
            warm_concurrent_elapsed_ms: input.warm_concurrent_elapsed_ms,
            rss_kb_before: input.rss_kb_before,
            rss_kb_after: input.rss_kb_after,
            rss_kb_delta,
            qps: qps_end_to_end,
            qps_end_to_end,
            qps_search_only: qps(total_searches, input.total_search_only_ms),
            qps_warm_concurrent: qps(warm_concurrent_searches, input.warm_concurrent_elapsed_ms),
            p50_query_latency_ms: percentile(&latencies, 0.50),
            p95_query_latency_ms: percentile(&latencies, 0.95),
            p99_query_latency_ms: percentile(&latencies, 0.99),
            p50_cold_query_latency_ms: percentile(&cold_latencies, 0.50),
            p95_cold_query_latency_ms: percentile(&cold_latencies, 0.95),
            p99_cold_query_latency_ms: percentile(&cold_latencies, 0.99),
            p50_warm_query_latency_ms: percentile(&warm_latencies, 0.50),
            p95_warm_query_latency_ms: percentile(&warm_latencies, 0.95),
            p99_warm_query_latency_ms: percentile(&warm_latencies, 0.99),
            p50_warm_concurrent_query_latency_ms: percentile(&warm_concurrent_latencies, 0.50),
            p95_warm_concurrent_query_latency_ms: percentile(&warm_concurrent_latencies, 0.95),
            p99_warm_concurrent_query_latency_ms: percentile(&warm_concurrent_latencies, 0.99),
            min_query_latency_ms: latencies.first().copied().unwrap_or(0.0),
            max_query_latency_ms: latencies.last().copied().unwrap_or(0.0),
            total_hit_count,
            vector_mode: vector_mode_label(input.vector_mode),
            per_query: input.per_query,
        },
        ranked_results: input.ranked_results,
        qrels_path: input.qrels_path,
    })
}

fn runtime_search_bench_sample(
    prepared: &RuntimeSearchBenchPreparedQuery,
    sample_index: u32,
    phase: &'static str,
    worker_index: Option<u32>,
    latency_ms: f64,
    hit_count: usize,
) -> RuntimeSearchBenchQuerySample {
    RuntimeSearchBenchQuerySample {
        sample_index,
        phase,
        worker_index,
        query_id: prepared.query.query_id.clone(),
        query_class: prepared.query.query_class.clone(),
        mode: runtime_search_mode_label(prepared.query.mode),
        include_preview: prepared.query.include_preview,
        latency_ms,
        hit_count,
    }
}

fn write_runtime_search_bench_artifacts(
    artifact_dir: &std::path::Path,
    summary: &RuntimeSearchBenchSummary,
    ranked_results: &[RankedQueryResult],
    query_set: &std::path::Path,
    qrels_path: Option<&std::path::Path>,
) -> Result<(), String> {
    std::fs::create_dir_all(artifact_dir).map_err(|error| error.to_string())?;
    std::fs::write(
        artifact_dir.join("summary.json"),
        serde_json::to_string_pretty(summary).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;

    let mut samples = String::new();
    for sample in &summary.per_query {
        samples.push_str(&serde_json::to_string(sample).map_err(|error| error.to_string())?);
        samples.push('\n');
    }
    std::fs::write(artifact_dir.join("samples.ndjson"), samples)
        .map_err(|error| error.to_string())?;

    let ranked_results_path = artifact_dir.join("ranked-results.json");
    std::fs::write(
        &ranked_results_path,
        serde_json::to_string_pretty(ranked_results).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;

    if let Some(qrels_path) = qrels_path {
        let quality =
            compute_search_quality_summary_from_paths(query_set, qrels_path, &ranked_results_path)
                .map_err(|error| error.message)?;
        std::fs::write(
            artifact_dir.join("quality.json"),
            serde_json::to_string_pretty(&quality).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    }

    Ok(())
}

fn discover_qrels_path(
    dataset: &std::path::Path,
    manifest: &DatasetPackManifest,
    query_set: &std::path::Path,
) -> Option<PathBuf> {
    manifest.query_sets.iter().find_map(|entry| {
        let manifest_query_set = dataset.join(&entry.path);
        let matches_query_set = same_file_path(query_set, &manifest_query_set)
            || query_set
                .strip_prefix(dataset)
                .ok()
                .is_some_and(|relative| relative == std::path::Path::new(entry.path.as_str()));
        if matches_query_set {
            entry.qrels_path.as_ref().map(|path| dataset.join(path))
        } else {
            None
        }
    })
}

fn same_file_path(left: &std::path::Path, right: &std::path::Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

fn read_manifest(dataset: &std::path::Path) -> Result<DatasetPackManifest, String> {
    let manifest_text = std::fs::read_to_string(dataset.join("manifest.json"))
        .map_err(|error| error.to_string())?;
    serde_json::from_str(&manifest_text).map_err(|error| error.to_string())
}

fn load_runtime_search_bench_queries(
    query_set: &std::path::Path,
) -> Result<Vec<RuntimeSearchBenchQuery>, String> {
    BufReader::new(File::open(query_set).map_err(|error| error.to_string())?)
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some(other),
        })
        .map(|line| {
            let line = line.map_err(|error| error.to_string())?;
            let record: RuntimeSearchBenchQueryRecord =
                serde_json::from_str(&line).map_err(|error| error.to_string())?;
            let mode = runtime_search_mode_for_query(&record)?;
            Ok(RuntimeSearchBenchQuery {
                query_id: record.query_id,
                query_class: record.query_class,
                query_text: record.query_text,
                top_k: record.top_k as usize,
                mode,
                include_preview: record.preview_expected,
                filter_spec: record.filter_spec,
            })
        })
        .collect()
}

fn runtime_search_mode_for_query(
    record: &RuntimeSearchBenchQueryRecord,
) -> Result<RuntimeSearchMode, String> {
    match record.query_class.as_str() {
        "keyword" if record.lane_eligibility.text => Ok(RuntimeSearchMode::Text),
        "keyword" => Err(format!(
            "query_id {} has query_class keyword but text lane is not eligible",
            record.query_id
        )),
        "vector" if record.lane_eligibility.vector => Ok(RuntimeSearchMode::Vector),
        "vector" => Err(format!(
            "query_id {} has query_class vector but vector lane is not eligible",
            record.query_id
        )),
        "hybrid" if record.lane_eligibility.hybrid => Ok(RuntimeSearchMode::Hybrid),
        "hybrid" => Err(format!(
            "query_id {} has query_class hybrid but hybrid lane is not eligible",
            record.query_id
        )),
        other => Err(format!(
            "query_id {} has unsupported query_class: {other}",
            record.query_id
        )),
    }
}

fn copy_dataset_pack(
    source: &std::path::Path,
    destination: &std::path::Path,
) -> Result<(), String> {
    for entry in std::fs::read_dir(source).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type().map_err(|error| error.to_string())?;
        if file_type.is_dir() {
            std::fs::create_dir_all(&destination_path).map_err(|error| error.to_string())?;
            copy_dataset_pack(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            std::fs::copy(&source_path, &destination_path).map_err(|error| error.to_string())?;
        } else {
            return Err(format!(
                "dataset pack contains unsupported file type: {}",
                source_path.display()
            ));
        }
    }
    Ok(())
}

fn runtime_store_path(root: &std::path::Path, manifest: &DatasetPackManifest) -> PathBuf {
    manifest
        .files
        .iter()
        .find(|file| file.kind == "store")
        .map(|file| root.join(&file.path))
        .unwrap_or_else(|| root.join("store.rax"))
}

fn runtime_search_request(
    query: &RuntimeSearchBenchQuery,
    dimensions: u32,
) -> RuntimeSearchRequest {
    let vector_query = matches!(
        query.mode,
        RuntimeSearchMode::Vector | RuntimeSearchMode::Hybrid
    )
    .then(|| embed_text(&query.query_text, dimensions));
    let text_query = matches!(
        query.mode,
        RuntimeSearchMode::Text | RuntimeSearchMode::Hybrid
    )
    .then(|| query.query_text.clone());
    RuntimeSearchRequest {
        mode: query.mode,
        text_query,
        vector_query,
        top_k: query.top_k,
        include_preview: query.include_preview,
    }
}

fn percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }

    let index = ((sorted_values.len() as f64 * percentile).ceil() as usize).saturating_sub(1);
    sorted_values[index.min(sorted_values.len() - 1)]
}

fn sorted_phase_latencies(
    samples: &[RuntimeSearchBenchQuerySample],
    phase: &'static str,
) -> Vec<f64> {
    let mut latencies = samples
        .iter()
        .filter(|sample| sample.phase == phase)
        .map(|sample| sample.latency_ms)
        .collect::<Vec<_>>();
    latencies.sort_by(f64::total_cmp);
    latencies
}

fn qps(total_searches: usize, elapsed_ms: f64) -> f64 {
    if elapsed_ms > 0.0 {
        total_searches as f64 / (elapsed_ms / 1_000.0)
    } else {
        0.0
    }
}

fn elapsed_ms(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(target_os = "linux")]
fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")
                    .and_then(|rest| rest.split_whitespace().next())
                    .and_then(|value| value.parse::<u64>().ok())
            })
        })
        .unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn rss_kb() -> u64 {
    let pid = std::process::id().to_string();
    ProcessCommand::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|stdout| stdout.trim().parse::<u64>().ok())
        .unwrap_or(0)
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

fn vector_mode_label(mode: VectorQueryMode) -> &'static str {
    match mode {
        VectorQueryMode::Auto => "auto",
        VectorQueryMode::ExactFlat => "exact_flat",
        VectorQueryMode::Hnsw => "hnsw",
        VectorQueryMode::PreviewQ8 => "preview_q8",
    }
}

fn runtime_search_mode_label(mode: RuntimeSearchMode) -> &'static str {
    match mode {
        RuntimeSearchMode::Text => "text",
        RuntimeSearchMode::Vector => "vector",
        RuntimeSearchMode::Hybrid => "hybrid",
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
