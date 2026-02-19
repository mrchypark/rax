pub const SPEC_MAJOR: u16 = 1;
pub const SPEC_MINOR: u16 = 0;
pub const WAL_RECORD_HEADER_SIZE: usize = 48;
pub const MAX_STRING_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_BLOB_BYTES: usize = 256 * 1024 * 1024;

pub fn spec_version() -> (u16, u16) {
    (SPEC_MAJOR, SPEC_MINOR)
}
