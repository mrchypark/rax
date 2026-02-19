use rax_orchestrator::session::WaxSession;

#[test]
fn wax_session_tracks_and_flushes_memories() {
    let mut s = WaxSession::new();
    s.remember("alpha memory");
    s.remember("beta");
    assert_eq!(s.recall("memory"), vec!["alpha memory".to_string()]);
    assert_eq!(s.flush(), 2);
    assert!(s.recall("alpha").is_empty());
}
