#[test]
fn mv2s_spec_version_is_1_0() {
    assert_eq!(rax_core::format::spec_version(), (1, 0));
}
