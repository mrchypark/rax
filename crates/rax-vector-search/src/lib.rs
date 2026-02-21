pub mod cpu_backend;
pub mod embedvec_backend;
pub mod engine;
pub mod factory;
pub mod high_perf_backend;
pub mod hnsw_rs_backend;
pub mod hybrid_backend;
pub mod usearch_backend;

pub fn bootstrap_marker() -> &'static str {
    "rax-vector-search"
}
