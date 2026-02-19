use rax_core::store::LifecycleStore;

#[test]
fn timeline_orders_and_excludes_superseded_by_default() {
    let mut store = LifecycleStore::new();
    let older = store.put(vec![1], 10);
    let newer = store.put(vec![2], 20);
    store.supersede(older, newer);

    let visible = store.timeline(false);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, newer);

    let all = store.timeline(true);
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].timestamp, 10);
    assert_eq!(all[1].timestamp, 20);
}
