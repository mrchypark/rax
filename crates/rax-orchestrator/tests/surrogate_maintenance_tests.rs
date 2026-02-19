use rax_orchestrator::maintenance::SurrogateMaintenance;

#[test]
fn surrogate_maintenance_rebuild_clears_stale_entries() {
    let mut m = SurrogateMaintenance::default();
    m.mark_stale(10);
    m.mark_stale(11);
    assert_eq!(m.rebuild(), 2);
    assert_eq!(m.rebuild(), 0);
}
