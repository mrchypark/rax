use rax_orchestrator::live_set_rewrite::rewrite_live_set;

#[tokio::test]
async fn rewrite_preserves_logical_live_set() {
    let logical = vec![1, 2, 3, 4];
    let superseded = vec![2, 4];

    let (rewritten, report) = rewrite_live_set(&logical, &superseded);
    assert_eq!(rewritten, vec![1, 3]);
    assert_eq!(report.before_count, 4);
    assert_eq!(report.after_count, 2);
}
