use rax_core::store::LifecycleStore;

#[test]
fn delete_and_supersede_metadata_links_are_recorded() {
    let mut store = LifecycleStore::new();
    let a = store.put(vec![1], 1);
    let b = store.put(vec![2], 2);
    store.supersede(a, b);
    store.delete(a);

    let a_meta = store.meta(a).unwrap();
    let b_meta = store.meta(b).unwrap();
    assert_eq!(a_meta.superseded_by, Some(b));
    assert_eq!(b_meta.supersedes, Some(a));
}
