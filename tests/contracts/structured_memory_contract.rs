use tempfile::tempdir;

use wax_v2_structured_memory::{
    NewStructuredEntity, NewStructuredFact, NewStructuredMemoryRecord, StructuredEntityQuery,
    StructuredFactQuery, StructuredMemoryQuery, StructuredMemorySession,
};

#[test]
fn structured_memory_session_records_bootstrap_fact_items() {
    let dataset_dir = tempdir().unwrap();

    let mut session = StructuredMemorySession::open(dataset_dir.path()).unwrap();
    let record = session
        .record(NewStructuredMemoryRecord::fact(
            "person:alice",
            "name",
            serde_json::json!("Alice"),
            "bootstrap-test",
            1_717_171_717_000,
        ))
        .unwrap();

    assert_eq!(record.record_id, 0);
    assert_eq!(record.subject, "person:alice");
    assert_eq!(record.predicate, "name");
    assert_eq!(record.value, serde_json::json!("Alice"));
    assert_eq!(record.provenance.source, "bootstrap-test");
    assert_eq!(record.provenance.asserted_at_ms, 1_717_171_717_000);
    session.close().unwrap();
}

#[test]
fn structured_memory_session_queries_records_after_reopen() {
    let dataset_dir = tempdir().unwrap();

    let mut initial = StructuredMemorySession::open(dataset_dir.path()).unwrap();
    initial
        .record(NewStructuredMemoryRecord::fact(
            "person:alice",
            "name",
            serde_json::json!("Alice"),
            "bootstrap-test",
            1_717_171_717_000,
        ))
        .unwrap();
    initial
        .record(NewStructuredMemoryRecord::fact(
            "person:alice",
            "works_at",
            serde_json::json!("company:acme"),
            "bootstrap-test",
            1_717_171_718_000,
        ))
        .unwrap();
    initial.close().unwrap();

    let mut reopened = StructuredMemorySession::open(dataset_dir.path()).unwrap();
    let by_subject = reopened
        .query(StructuredMemoryQuery::subject("person:alice"))
        .unwrap();
    assert_eq!(by_subject.len(), 2);

    let by_subject_and_predicate = reopened
        .query(StructuredMemoryQuery::subject("person:alice").with_predicate("works_at"))
        .unwrap();
    assert_eq!(by_subject_and_predicate.len(), 1);
    assert_eq!(
        by_subject_and_predicate[0].value,
        serde_json::json!("company:acme")
    );
}

#[test]
fn structured_memory_session_upserts_entities_with_kind_and_aliases() {
    let dataset_dir = tempdir().unwrap();

    let mut initial = StructuredMemorySession::open(dataset_dir.path()).unwrap();
    initial
        .upsert_entity(
            NewStructuredEntity::new("person:alice", "person", "bootstrap-test", 1_717_171_719_000)
                .with_alias("Alice")
                .with_alias("Alice Kim"),
        )
        .unwrap();
    initial.close().unwrap();

    let mut reopened = StructuredMemorySession::open(dataset_dir.path()).unwrap();
    let entity = reopened
        .entity(StructuredEntityQuery::entity_id("person:alice"))
        .unwrap()
        .unwrap();
    assert_eq!(entity.entity_id, "person:alice");
    assert_eq!(entity.kind, "person");
    assert_eq!(entity.aliases, vec!["Alice", "Alice Kim"]);
}

#[test]
fn structured_memory_entity_upsert_deduplicates_aliases_within_single_request() {
    let dataset_dir = tempdir().unwrap();

    let mut session = StructuredMemorySession::open(dataset_dir.path()).unwrap();
    session
        .upsert_entity(
            NewStructuredEntity::new("person:alice", "person", "bootstrap-test", 1_717_171_719_000)
                .with_alias("Alice")
                .with_alias("Alice")
                .with_alias("Alice Kim")
                .with_alias("Alice"),
        )
        .unwrap();
    session.close().unwrap();

    let mut reopened = StructuredMemorySession::open(dataset_dir.path()).unwrap();
    let entity = reopened
        .entity(StructuredEntityQuery::entity_id("person:alice"))
        .unwrap()
        .unwrap();
    assert_eq!(entity.aliases, vec!["Alice", "Alice Kim"]);
}

#[test]
fn structured_memory_session_asserts_and_reads_facts_via_fact_api() {
    let dataset_dir = tempdir().unwrap();

    let mut initial = StructuredMemorySession::open(dataset_dir.path()).unwrap();
    initial
        .upsert_entity(NewStructuredEntity::new(
            "person:alice",
            "person",
            "bootstrap-test",
            1_717_171_719_000,
        ))
        .unwrap();
    initial
        .assert_fact(NewStructuredFact::new(
            "person:alice",
            "works_at",
            serde_json::json!("company:acme"),
            "bootstrap-test",
            1_717_171_720_000,
        ))
        .unwrap();
    initial.close().unwrap();

    let mut reopened = StructuredMemorySession::open(dataset_dir.path()).unwrap();
    let facts = reopened
        .facts(StructuredFactQuery::subject("person:alice"))
        .unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].subject, "person:alice");
    assert_eq!(facts[0].predicate, "works_at");
    assert_eq!(facts[0].value, serde_json::json!("company:acme"));
}
