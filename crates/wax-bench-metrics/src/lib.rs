#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryReading {
    Available { value: u64 },
    Unavailable { reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompilerOptimization {
    Debug,
    Release,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThermalState {
    Nominal,
    Fair,
    Serious,
    Critical,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SampleMetrics {
    pub container_open_ms: f64,
    pub metadata_readiness_ms: f64,
    pub total_ttfq_ms: f64,
    pub resident_memory_bytes: MemoryReading,
    pub compiler_optimization: Option<CompilerOptimization>,
    pub thermal_state: Option<ThermalState>,
}

pub trait MonotonicClock {
    fn now_us(&mut self) -> u64;
}

pub trait MemorySampler {
    fn sample_resident_bytes(&self) -> MemoryReading;
}

pub struct MetricCollector<C, M> {
    clock: C,
    memory_sampler: M,
    start_us: Option<u64>,
    container_open_us: Option<u64>,
    metadata_readiness_us: Option<u64>,
    query_done_us: Option<u64>,
    resident_memory_bytes: Option<MemoryReading>,
}

impl<C, M> MetricCollector<C, M>
where
    C: MonotonicClock,
    M: MemorySampler,
{
    pub fn new(clock: C, memory_sampler: M) -> Self {
        Self {
            clock,
            memory_sampler,
            start_us: None,
            container_open_us: None,
            metadata_readiness_us: None,
            query_done_us: None,
            resident_memory_bytes: None,
        }
    }

    pub fn start_run(&mut self) {
        self.start_us = Some(self.clock.now_us());
    }

    pub fn mark_container_open_done(&mut self) {
        let start_us = self.start_us.expect("start_run must be called first");
        self.container_open_us = Some(self.clock.now_us() - start_us);
    }

    pub fn mark_metadata_ready(&mut self) {
        let start_us = self.start_us.expect("start_run must be called first");
        self.metadata_readiness_us = Some(self.clock.now_us() - start_us);
    }

    pub fn mark_query_done(&mut self) {
        let start_us = self.start_us.expect("start_run must be called first");
        self.query_done_us = Some(self.clock.now_us() - start_us);
    }

    pub fn snapshot_memory(&mut self) {
        self.resident_memory_bytes = Some(self.memory_sampler.sample_resident_bytes());
    }

    pub fn finish(
        &self,
        compiler_optimization: Option<CompilerOptimization>,
        thermal_state: Option<ThermalState>,
    ) -> SampleMetrics {
        SampleMetrics {
            container_open_ms: duration_ms(self.container_open_us.unwrap_or(0)),
            metadata_readiness_ms: duration_ms(self.metadata_readiness_us.unwrap_or(0)),
            total_ttfq_ms: duration_ms(self.query_done_us.unwrap_or(0)),
            resident_memory_bytes: self
                .resident_memory_bytes
                .clone()
                .unwrap_or_else(|| self.memory_sampler.sample_resident_bytes()),
            compiler_optimization,
            thermal_state,
        }
    }
}

fn duration_ms(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}
