use rax_rag::fast_rag::{build_context, ContextChunk};

#[test]
fn same_inputs_produce_identical_context_order() {
    let chunks = vec![
        ContextChunk {
            id: 2,
            text: "b b b".to_string(),
            importance: 7,
        },
        ContextChunk {
            id: 1,
            text: "a a".to_string(),
            importance: 7,
        },
        ContextChunk {
            id: 3,
            text: "c".to_string(),
            importance: 5,
        },
    ];

    let left = build_context(chunks.clone(), 100);
    let right = build_context(chunks, 100);

    assert_eq!(left, right);
    assert_eq!(left.iter().map(|c| c.id).collect::<Vec<_>>(), vec![1, 2, 3]);
}
