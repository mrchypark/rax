pub mod backup;
pub mod codec;
pub mod format;
pub mod frame;
pub mod io;
pub mod store;
pub mod wal;

pub fn bootstrap_marker() -> &'static str {
    "rax-core"
}
