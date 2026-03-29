#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkId {
    pub dataset_id: String,
    pub workload_id: String,
    pub sample_index: u32,
}
