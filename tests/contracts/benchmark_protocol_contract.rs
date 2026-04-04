use wax_bench_model::{
    embed_text, parse_benchmark_query, parse_workload, tokenize, BenchmarkQuery, Workload,
};

#[test]
fn workload_labels_and_protocol_queries_are_stable() {
    let cases = [
        (Workload::ContainerOpen, "container_open", None, None),
        (
            Workload::MaterializeVector,
            "materialize_vector",
            Some(BenchmarkQuery::MaterializeVectorLane),
            None,
        ),
        (
            Workload::TtfqText,
            "ttfq_text",
            Some(BenchmarkQuery::TtfqText),
            None,
        ),
        (
            Workload::TtfqVector,
            "ttfq_vector",
            Some(BenchmarkQuery::TtfqVector),
            None,
        ),
        (
            Workload::WarmText,
            "warm_text",
            Some(BenchmarkQuery::TtfqText),
            Some(BenchmarkQuery::WarmText),
        ),
        (
            Workload::WarmVector,
            "warm_vector",
            Some(BenchmarkQuery::WarmupVector),
            Some(BenchmarkQuery::WarmVector),
        ),
        (
            Workload::WarmHybrid,
            "warm_hybrid",
            Some(BenchmarkQuery::WarmupHybrid),
            Some(BenchmarkQuery::WarmHybrid),
        ),
        (
            Workload::WarmHybridWithPreviews,
            "warm_hybrid_with_previews",
            Some(BenchmarkQuery::WarmupHybridWithPreviews),
            Some(BenchmarkQuery::WarmHybridWithPreviews),
        ),
    ];

    for (workload, label, first_query, measured_query) in cases {
        assert_eq!(workload.label(), label);
        assert_eq!(parse_workload(label), Some(workload));
        assert_eq!(workload.first_query(), first_query);
        assert_eq!(workload.measured_query(), measured_query);
    }
}

#[test]
fn benchmark_query_labels_round_trip() {
    let cases = [
        BenchmarkQuery::MaterializeTextLane,
        BenchmarkQuery::MaterializeVectorLane,
        BenchmarkQuery::TtfqText,
        BenchmarkQuery::TtfqVector,
        BenchmarkQuery::WarmupVector,
        BenchmarkQuery::WarmVector,
        BenchmarkQuery::TtfqHybrid,
        BenchmarkQuery::WarmupHybrid,
        BenchmarkQuery::WarmHybrid,
        BenchmarkQuery::WarmupHybridWithPreviews,
        BenchmarkQuery::WarmHybridWithPreviews,
    ];

    for query in cases {
        assert_eq!(parse_benchmark_query(query.as_str()), Some(query));
    }

    assert_eq!(parse_benchmark_query("plain user query"), None);
}

#[test]
fn embedding_helpers_are_stable_for_ascii_text() {
    assert_eq!(
        tokenize("Vector, HYBRID! vector"),
        vec!["vector", "hybrid", "vector"]
    );

    let vector = embed_text("Vector, HYBRID! vector", 8);
    assert_eq!(vector.len(), 8);

    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 1e-6);
    assert_eq!(embed_text("", 8), vec![0.0; 8]);
}
