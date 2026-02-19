use rax_text_search::fts5_engine::fts5_runtime_available;

#[test]
fn fts5_is_available_in_ci_runtime() {
    assert!(fts5_runtime_available());
}
