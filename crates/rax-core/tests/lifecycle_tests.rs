use rax_core::store::LifecycleStore;

#[test]
fn superseded_frame_hidden_from_default_reads() {
    let mut store = LifecycleStore::new();
    let old_id = store.put(vec![1], 10);
    let new_id = store.put(vec![2], 11);
    store.supersede(old_id, new_id);

    assert!(store.get_visible(old_id).is_none());
    assert!(store.get_visible(new_id).is_some());
}
