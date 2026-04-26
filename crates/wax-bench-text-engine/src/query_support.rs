use wax_bench_model::embed_text;
use wax_v2_text::TextBatchQuery;

pub(crate) fn load_query_vector_records(
    queries: &[TextBatchQuery],
    dimensions: usize,
) -> Result<Vec<QueryVectorRecord>, String> {
    queries
        .iter()
        .map(|query| {
            Ok(QueryVectorRecord {
                vector: embed_text(&query.query_text, dimensions as u32),
            })
        })
        .collect()
}

#[derive(Debug)]
pub(crate) struct QueryVectorRecord {
    pub(crate) vector: Vec<f32>,
}
