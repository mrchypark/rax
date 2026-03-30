use std::path::PathBuf;

use wax_bench_metrics::{
    CompilerOptimization, MemorySampler, MetricCollector, MonotonicClock, SampleMetrics,
    ThermalState,
};
use wax_bench_model::{MaterializationMode, MountRequest, OpenRequest, SearchRequest, WaxEngine};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Workload {
    ContainerOpen,
    TtfqText,
    TtfqVector,
    WarmText,
    WarmVector,
    WarmHybrid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRequest {
    pub dataset_path: PathBuf,
    pub workload: Workload,
    pub materialization_mode: MaterializationMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEvent {
    Mounted,
    Opened,
    TextLaneMaterialized,
    VectorLaneMaterialized,
    SearchExecuted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTrace {
    pub events: Vec<LifecycleEvent>,
    pub search_queries: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MeasuredRun {
    pub trace: RunTrace,
    pub metrics: SampleMetrics,
}

pub struct BenchmarkRunner<E> {
    engine: E,
}

impl<E> BenchmarkRunner<E> {
    pub fn new(engine: E) -> Self {
        Self { engine }
    }
}

pub fn run_benchmark_samples_with_runner_factory<E, R, F, C, M>(
    mut runner_factory: R,
    request: &RunRequest,
    sample_count: u32,
    mut collector_factory: F,
) -> Result<Vec<SampleMetrics>, E::Error>
where
    E: WaxEngine,
    R: FnMut() -> BenchmarkRunner<E>,
    F: FnMut() -> MetricCollector<C, M>,
    C: MonotonicClock,
    M: MemorySampler,
{
    let mut samples = Vec::new();
    for _ in 0..sample_count {
        let mut runner = runner_factory();
        let mut collector = collector_factory();
        let measured = runner.run_with_metrics(
            request,
            &mut collector,
            Some(active_compiler_optimization()),
            Some(ThermalState::Nominal),
        )?;
        samples.push(measured.metrics);
    }

    Ok(samples)
}

fn active_compiler_optimization() -> CompilerOptimization {
    if cfg!(debug_assertions) {
        CompilerOptimization::Debug
    } else {
        CompilerOptimization::Release
    }
}

impl<E> BenchmarkRunner<E>
where
    E: WaxEngine,
{
    pub fn run(&mut self, request: &RunRequest) -> Result<RunTrace, E::Error> {
        let mut trace = RunTrace {
            events: Vec::new(),
            search_queries: Vec::new(),
        };

        self.engine.mount(MountRequest {
            store_path: request.dataset_path.clone(),
        })?;
        trace.events.push(LifecycleEvent::Mounted);

        self.engine.open(OpenRequest)?;
        trace.events.push(LifecycleEvent::Opened);

        if matches!(
            request.materialization_mode,
            MaterializationMode::ForceTextLane | MaterializationMode::ForceAllLanes
        ) {
            materialize_lane(
                &mut self.engine,
                "__materialize_text_lane__",
                LifecycleEvent::TextLaneMaterialized,
                &mut trace,
            )?;
        }

        if matches!(
            request.materialization_mode,
            MaterializationMode::ForceVectorLane | MaterializationMode::ForceAllLanes
        ) {
            materialize_lane(
                &mut self.engine,
                "__materialize_vector_lane__",
                LifecycleEvent::VectorLaneMaterialized,
                &mut trace,
            )?;
        }

        if let Some(query_text) = first_query_for_workload(&request.workload) {
            self.engine.search(SearchRequest {
                query_text: query_text.clone(),
            })?;
            trace.events.push(LifecycleEvent::SearchExecuted);
            trace.search_queries.push(query_text);
        }

        if let Some(query_text) = measured_query_for_workload(&request.workload) {
            self.engine.search(SearchRequest {
                query_text: query_text.clone(),
            })?;
            trace.events.push(LifecycleEvent::SearchExecuted);
            trace.search_queries.push(query_text);
        }

        Ok(trace)
    }

    pub fn run_with_metrics<C, M>(
        &mut self,
        request: &RunRequest,
        collector: &mut MetricCollector<C, M>,
        compiler_optimization: Option<CompilerOptimization>,
        thermal_state: Option<ThermalState>,
    ) -> Result<MeasuredRun, E::Error>
    where
        C: MonotonicClock,
        M: MemorySampler,
    {
        collector.start_run();
        let mut trace = RunTrace {
            events: Vec::new(),
            search_queries: Vec::new(),
        };

        self.engine.mount(MountRequest {
            store_path: request.dataset_path.clone(),
        })?;
        trace.events.push(LifecycleEvent::Mounted);

        self.engine.open(OpenRequest)?;
        trace.events.push(LifecycleEvent::Opened);
        collector.mark_container_open_done();

        if matches!(
            request.materialization_mode,
            MaterializationMode::ForceTextLane | MaterializationMode::ForceAllLanes
        ) {
            materialize_lane(
                &mut self.engine,
                "__materialize_text_lane__",
                LifecycleEvent::TextLaneMaterialized,
                &mut trace,
            )?;
        }

        if matches!(
            request.materialization_mode,
            MaterializationMode::ForceVectorLane | MaterializationMode::ForceAllLanes
        ) {
            materialize_lane(
                &mut self.engine,
                "__materialize_vector_lane__",
                LifecycleEvent::VectorLaneMaterialized,
                &mut trace,
            )?;
        }

        collector.mark_metadata_ready();

        if let Some(query_text) = first_query_for_workload(&request.workload) {
            self.engine.search(SearchRequest {
                query_text: query_text.clone(),
            })?;
            trace.events.push(LifecycleEvent::SearchExecuted);
            trace.search_queries.push(query_text);
            if measured_query_for_workload(&request.workload).is_none() {
                collector.mark_query_done();
            }
        }

        if let Some(query_text) = measured_query_for_workload(&request.workload) {
            collector.start_search_measurement();
            self.engine.search(SearchRequest {
                query_text: query_text.clone(),
            })?;
            trace.events.push(LifecycleEvent::SearchExecuted);
            trace.search_queries.push(query_text);
            collector.mark_search_done();
        }

        collector.snapshot_memory();

        Ok(MeasuredRun {
            trace,
            metrics: collector.finish(compiler_optimization, thermal_state),
        })
    }
}

fn materialize_lane<E>(
    engine: &mut E,
    query_text: &str,
    event: LifecycleEvent,
    trace: &mut RunTrace,
) -> Result<(), E::Error>
where
    E: WaxEngine,
{
    engine.search(SearchRequest {
        query_text: query_text.to_owned(),
    })?;
    trace.events.push(event);
    trace.search_queries.push(query_text.to_owned());
    Ok(())
}

#[derive(Default)]
pub struct NoopWaxEngine;

impl WaxEngine for NoopWaxEngine {
    type Error = String;

    fn mount(&mut self, _request: MountRequest) -> Result<(), Self::Error> {
        Ok(())
    }

    fn open(&mut self, _request: OpenRequest) -> Result<wax_bench_model::OpenResult, Self::Error> {
        Ok(wax_bench_model::OpenResult)
    }

    fn search(
        &mut self,
        _request: SearchRequest,
    ) -> Result<wax_bench_model::SearchResult, Self::Error> {
        Ok(wax_bench_model::SearchResult { hits: Vec::new() })
    }

    fn get_stats(&self) -> wax_bench_model::EngineStats {
        wax_bench_model::EngineStats {
            phase: wax_bench_model::EnginePhase::Open,
            last_mounted_path: None,
        }
    }
}

fn first_query_for_workload(workload: &Workload) -> Option<String> {
    match workload {
        Workload::ContainerOpen => None,
        Workload::TtfqText => Some("__ttfq_text__".to_owned()),
        Workload::TtfqVector => Some("__ttfq_vector__".to_owned()),
        Workload::WarmText => Some("__ttfq_text__".to_owned()),
        Workload::WarmVector => Some("__ttfq_vector__".to_owned()),
        Workload::WarmHybrid => Some("__ttfq_hybrid__".to_owned()),
    }
}

fn measured_query_for_workload(workload: &Workload) -> Option<String> {
    match workload {
        Workload::WarmText => Some("__warm_text__".to_owned()),
        Workload::WarmVector => Some("__warm_vector__".to_owned()),
        Workload::WarmHybrid => Some("__warm_hybrid__".to_owned()),
        _ => None,
    }
}
