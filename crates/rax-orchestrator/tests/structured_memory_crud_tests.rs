use std::collections::HashMap;

use rax_orchestrator::structured_memory::StructuredMemory;

#[test]
fn structured_memory_crud_round_trip() {
    let mut mem = StructuredMemory::new();
    let mut attrs = HashMap::new();
    attrs.insert("city".to_string(), "seoul".to_string());

    mem.upsert("User-1", attrs);
    assert_eq!(mem.get("user-1").unwrap().attrs.get("city"), Some(&"seoul".to_string()));

    mem.delete("USER-1");
    assert!(mem.get("user-1").is_none());
}
