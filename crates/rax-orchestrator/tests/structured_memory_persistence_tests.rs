use std::collections::HashMap;

use rax_orchestrator::structured_memory::StructuredMemory;

#[tokio::test]
async fn structured_entity_persists_after_reopen() {
    let mut mem = StructuredMemory::new();
    let mut attrs = HashMap::new();
    attrs.insert("team".to_string(), "core".to_string());
    mem.upsert("Entity-1", attrs);

    let snapshot = mem.snapshot();
    let reopened = StructuredMemory::from_snapshot(&snapshot);

    assert_eq!(
        reopened.get("entity-1").unwrap().attrs.get("team"),
        Some(&"core".to_string())
    );
}
