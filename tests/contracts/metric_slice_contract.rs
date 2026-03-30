use std::collections::VecDeque;

use wax_bench_metrics::{
    CompilerOptimization, MemoryReading, MemorySampler, MetricCollector, MonotonicClock,
    SampleMetrics, ThermalState,
};

#[test]
fn metric_collector_records_core_ttfq_slices() {
    let clock = SequenceClock::new([0, 4_000, 9_000, 15_000]);
    let sampler = FixedMemorySampler::available(4096);
    let mut collector = MetricCollector::new(clock, sampler);

    collector.start_run();
    collector.mark_container_open_done();
    collector.mark_metadata_ready();
    collector.mark_query_done();

    let snapshot = collector.finish(
        Some(CompilerOptimization::Debug),
        Some(ThermalState::Nominal),
    );

    assert_eq!(
        snapshot,
        SampleMetrics {
            container_open_ms: 4.0,
            metadata_readiness_ms: 9.0,
            total_ttfq_ms: 15.0,
            resident_memory_bytes: MemoryReading::Available { value: 4096 },
            compiler_optimization: Some(CompilerOptimization::Debug),
            thermal_state: Some(ThermalState::Nominal),
        }
    );
}

#[test]
fn metric_collector_preserves_explicit_unavailable_memory() {
    let clock = SequenceClock::new([0, 1_000, 2_000, 3_000]);
    let sampler = FixedMemorySampler::unavailable("platform_not_supported");
    let mut collector = MetricCollector::new(clock, sampler);

    collector.start_run();
    collector.mark_container_open_done();
    collector.mark_metadata_ready();
    collector.mark_query_done();

    let snapshot = collector.finish(None, None);
    assert_eq!(
        snapshot.resident_memory_bytes,
        MemoryReading::Unavailable {
            reason: "platform_not_supported".to_owned(),
        }
    );
    assert_eq!(snapshot.compiler_optimization, None);
    assert_eq!(snapshot.thermal_state, None);
}

#[test]
fn metric_collector_preserves_submillisecond_precision() {
    let clock = SequenceClock::new([0, 250, 750, 1_250]);
    let sampler = FixedMemorySampler::available(2048);
    let mut collector = MetricCollector::new(clock, sampler);

    collector.start_run();
    collector.mark_container_open_done();
    collector.mark_metadata_ready();
    collector.mark_query_done();

    let snapshot = collector.finish(None, None);

    assert_eq!(snapshot.container_open_ms, 0.25);
    assert_eq!(snapshot.metadata_readiness_ms, 0.75);
    assert_eq!(snapshot.total_ttfq_ms, 1.25);
}

struct SequenceClock {
    ticks: VecDeque<u64>,
}

impl SequenceClock {
    fn new(ticks: impl IntoIterator<Item = u64>) -> Self {
        Self {
            ticks: ticks.into_iter().collect(),
        }
    }
}

impl MonotonicClock for SequenceClock {
    fn now_us(&mut self) -> u64 {
        self.ticks.pop_front().unwrap()
    }
}

struct FixedMemorySampler {
    reading: MemoryReading,
}

impl FixedMemorySampler {
    fn available(value: u64) -> Self {
        Self {
            reading: MemoryReading::Available { value },
        }
    }

    fn unavailable(reason: &str) -> Self {
        Self {
            reading: MemoryReading::Unavailable {
                reason: reason.to_owned(),
            },
        }
    }
}

impl MemorySampler for FixedMemorySampler {
    fn sample_resident_bytes(&self) -> MemoryReading {
        self.reading.clone()
    }
}
