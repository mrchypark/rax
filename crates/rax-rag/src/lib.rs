pub mod fast_rag;
pub mod query_classifier;
pub mod search_request;
pub mod surrogate_tiers;
pub mod token_counter;
pub mod unified_search;

pub fn bootstrap_marker() -> &'static str {
    "rax-rag"
}
