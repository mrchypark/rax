use std::path::PathBuf;

use wax_bench_model::{
    EnginePhase, EngineStats, MaterializationMode, MountRequest, OpenRequest, OpenResult,
    SearchRequest, SearchResult, WaxEngine,
};
use wax_bench_metrics::{MemoryReading, MetricCollector, MonotonicClock, MemorySampler};
use wax_bench_runner::{BenchmarkRunner, LifecycleEvent, RunRequest, Workload};

#[test]
fn runner_lifecycle_emits_phases_in_order() {
    let engine = RecordingEngine::default();
    let mut runner = BenchmarkRunner::new(engine);

    let trace = runner
        .run(&RunRequest {
            dataset_path: PathBuf::from("/tmp/wax-pack"),
            workload: Workload::TtfqText,
            materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
        })
        .unwrap();

    assert_eq!(
        trace.events,
        vec![
            LifecycleEvent::Mounted,
            LifecycleEvent::Opened,
            LifecycleEvent::SearchExecuted,
        ]
    );
}

#[test]
fn container_open_excludes_lane_materialization() {
    let engine = RecordingEngine::default();
    let mut runner = BenchmarkRunner::new(engine);

    let trace = runner
        .run(&RunRequest {
            dataset_path: PathBuf::from("/tmp/wax-pack"),
            workload: Workload::ContainerOpen,
            materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
        })
        .unwrap();

    assert_eq!(
        trace.events,
        vec![LifecycleEvent::Mounted, LifecycleEvent::Opened]
    );
    assert!(trace.search_queries.is_empty());
}

#[test]
fn audit_mode_can_force_lane_materialization_before_first_query() {
    let engine = RecordingEngine::default();
    let mut runner = BenchmarkRunner::new(engine);

    let trace = runner
        .run(&RunRequest {
            dataset_path: PathBuf::from("/tmp/wax-pack"),
            workload: Workload::TtfqText,
            materialization_mode: MaterializationMode::ForceTextLane,
        })
        .unwrap();

    assert_eq!(
        trace.events,
        vec![
            LifecycleEvent::Mounted,
            LifecycleEvent::Opened,
            LifecycleEvent::TextLaneMaterialized,
            LifecycleEvent::SearchExecuted,
        ]
    );
    assert_eq!(
        trace.search_queries,
        vec![
            "__materialize_text_lane__".to_owned(),
            "__ttfq_text__".to_owned(),
        ]
    );
}

#[test]
fn force_all_lanes_materializes_text_and_vector_before_search() {
    let engine = RecordingEngine::default();
    let mut runner = BenchmarkRunner::new(engine);

    let trace = runner
        .run(&RunRequest {
            dataset_path: PathBuf::from("/tmp/wax-pack"),
            workload: Workload::TtfqText,
            materialization_mode: MaterializationMode::ForceAllLanes,
        })
        .unwrap();

    assert_eq!(
        trace.events,
        vec![
            LifecycleEvent::Mounted,
            LifecycleEvent::Opened,
            LifecycleEvent::TextLaneMaterialized,
            LifecycleEvent::VectorLaneMaterialized,
            LifecycleEvent::SearchExecuted,
        ]
    );
    assert_eq!(
        trace.search_queries,
        vec![
            "__materialize_text_lane__".to_owned(),
            "__materialize_vector_lane__".to_owned(),
            "__ttfq_text__".to_owned(),
        ]
    );
}

#[test]
fn vector_workload_executes_vector_first_query() {
    let engine = RecordingEngine::default();
    let mut runner = BenchmarkRunner::new(engine);

    let trace = runner
        .run(&RunRequest {
            dataset_path: PathBuf::from("/tmp/wax-pack"),
            workload: Workload::TtfqVector,
            materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
        })
        .unwrap();

    assert_eq!(
        trace.events,
        vec![
            LifecycleEvent::Mounted,
            LifecycleEvent::Opened,
            LifecycleEvent::SearchExecuted,
        ]
    );
    assert_eq!(trace.search_queries, vec!["__ttfq_vector__".to_owned()]);
}

#[test]
fn benchmark_samples_use_fresh_engine_instances() {
    let request = RunRequest {
        dataset_path: PathBuf::from("/tmp/wax-pack"),
        workload: Workload::TtfqVector,
        materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
    };

    let samples = wax_bench_runner::run_benchmark_samples_with_runner_factory(
        || BenchmarkRunner::new(FreshOnlyEngine::default()),
        &request,
        2,
        || MetricCollector::new(FixedClock, FixedMemorySampler),
    )
    .unwrap();

    assert_eq!(samples.len(), 2);
}

#[test]
fn warm_text_workload_warms_then_measures_text_search() {
    let engine = RecordingEngine::default();
    let mut runner = BenchmarkRunner::new(engine);

    let trace = runner
        .run(&RunRequest {
            dataset_path: PathBuf::from("/tmp/wax-pack"),
            workload: Workload::WarmText,
            materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
        })
        .unwrap();

    assert_eq!(
        trace.search_queries,
        vec!["__ttfq_text__".to_owned(), "__warm_text__".to_owned()]
    );
}

#[test]
fn warm_hybrid_workload_warms_then_measures_hybrid_search() {
    let engine = RecordingEngine::default();
    let mut runner = BenchmarkRunner::new(engine);

    let trace = runner
        .run(&RunRequest {
            dataset_path: PathBuf::from("/tmp/wax-pack"),
            workload: Workload::WarmHybrid,
            materialization_mode: MaterializationMode::NoForcedLaneMaterialization,
        })
        .unwrap();

    assert_eq!(
        trace.search_queries,
        vec!["__ttfq_hybrid__".to_owned(), "__warm_hybrid__".to_owned()]
    );
}

#[derive(Default)]
struct RecordingEngine {
    phase: EnginePhase,
    mounted_path: Option<PathBuf>,
    search_queries: Vec<String>,
}

#[derive(Default)]
struct FreshOnlyEngine {
    was_used: bool,
}

impl WaxEngine for RecordingEngine {
    type Error = &'static str;

    fn mount(&mut self, request: MountRequest) -> Result<(), Self::Error> {
        self.phase = EnginePhase::Mounted;
        self.mounted_path = Some(request.store_path);
        Ok(())
    }

    fn open(&mut self, _request: OpenRequest) -> Result<OpenResult, Self::Error> {
        self.phase = EnginePhase::Open;
        Ok(OpenResult)
    }

    fn search(&mut self, request: SearchRequest) -> Result<SearchResult, Self::Error> {
        self.search_queries.push(request.query_text);
        Ok(SearchResult { hits: Vec::new() })
    }

    fn get_stats(&self) -> EngineStats {
        EngineStats {
            phase: self.phase,
            last_mounted_path: self.mounted_path.clone(),
        }
    }
}

impl WaxEngine for FreshOnlyEngine {
    type Error = &'static str;

    fn mount(&mut self, _request: MountRequest) -> Result<(), Self::Error> {
        if self.was_used {
            return Err("reused engine");
        }
        Ok(())
    }

    fn open(&mut self, _request: OpenRequest) -> Result<OpenResult, Self::Error> {
        Ok(OpenResult)
    }

    fn search(&mut self, _request: SearchRequest) -> Result<SearchResult, Self::Error> {
        self.was_used = true;
        Ok(SearchResult { hits: Vec::new() })
    }

    fn get_stats(&self) -> EngineStats {
        EngineStats {
            phase: EnginePhase::Open,
            last_mounted_path: None,
        }
    }
}

struct FixedClock;

impl MonotonicClock for FixedClock {
    fn now_us(&mut self) -> u64 {
        0
    }
}

struct FixedMemorySampler;

impl MemorySampler for FixedMemorySampler {
    fn sample_resident_bytes(&self) -> MemoryReading {
        MemoryReading::Unavailable {
            reason: "test".to_owned(),
        }
    }
}
