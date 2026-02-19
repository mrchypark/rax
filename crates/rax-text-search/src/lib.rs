pub mod fts5_engine;
pub mod structured_schema;

pub fn bootstrap_marker() -> &'static str {
    "rax-text-search"
}
