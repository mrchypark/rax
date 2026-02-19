pub mod live_set_rewrite;
pub mod maintenance;
pub mod memory_orchestrator;
pub mod session;
pub mod structured_memory;

pub fn bootstrap_marker() -> &'static str {
    "rax-orchestrator"
}
