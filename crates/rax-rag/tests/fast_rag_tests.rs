use rax_rag::fast_rag::{build_context, ContextChunk};

#[test]
fn build_context_respects_token_budget() {
    let chunks = vec![
        ContextChunk {
            id: 1,
            text: "alpha beta".to_string(),
            importance: 10,
        },
        ContextChunk {
            id: 2,
            text: "gamma delta epsilon".to_string(),
            importance: 9,
        },
    ];

    let ctx = build_context(chunks, 2);
    assert_eq!(ctx.len(), 1);
    assert_eq!(ctx[0].id, 1);
}
