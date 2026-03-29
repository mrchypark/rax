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

#[derive(Debug, Clone, PartialEq, Eq)]
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

        if matches!(request.workload, Workload::TtfqText) {
            let query_text = "__ttfq_text__".to_owned();
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

        if matches!(request.workload, Workload::TtfqText) {
            let query_text = "__ttfq_text__".to_owned();
            self.engine.search(SearchRequest {
                query_text: query_text.clone(),
            })?;
            trace.events.push(LifecycleEvent::SearchExecuted);
            trace.search_queries.push(query_text);
            collector.mark_query_done();
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
