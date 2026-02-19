use std::fs;
use std::path::PathBuf;

#[test]
fn rax_reads_wax_generated_mv2s_fixture() {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.push("../../fixtures/wax/minimal.mv2s");

    let data = fs::read(&root).expect("fixture readable");
    assert!(!data.is_empty());

    let ver = rax_core::format::spec_version();
    assert_eq!(ver, (1, 0));
}
