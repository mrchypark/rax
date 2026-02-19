pub mod memory_orchestrator;
pub mod session;

pub fn bootstrap_marker() -> &'static str {
    "rax-orchestrator"
}
