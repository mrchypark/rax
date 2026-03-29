use std::path::PathBuf;

use wax_bench_model::{
    EnginePhase, EngineStats, MaterializationMode, MountRequest, OpenRequest, OpenResult,
    SearchRequest, SearchResult, WaxEngine,
};
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

    assert_eq!(trace.events, vec![LifecycleEvent::Mounted, LifecycleEvent::Opened]);
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

#[derive(Default)]
struct RecordingEngine {
    phase: EnginePhase,
    mounted_path: Option<PathBuf>,
    search_queries: Vec<String>,
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
