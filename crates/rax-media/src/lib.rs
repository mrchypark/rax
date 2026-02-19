pub mod photo_orchestrator;
pub mod providers;
pub mod video_orchestrator;

pub fn bootstrap_marker() -> &'static str {
    "rax-media"
}
