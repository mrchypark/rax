use std::collections::HashMap;

use rax_text_search::structured_schema::StructuredMemoryRecord;

#[test]
fn structured_memory_record_matches_attribute_filter() {
    let mut attrs = HashMap::new();
    attrs.insert("city".to_string(), "seoul".to_string());
    let rec = StructuredMemoryRecord {
        id: "1".to_string(),
        entity_type: "profile".to_string(),
        attributes: attrs,
    };

    assert!(rec.matches("city", "seoul"));
    assert!(!rec.matches("city", "busan"));
}
