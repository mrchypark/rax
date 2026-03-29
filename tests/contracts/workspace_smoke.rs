#[test]
fn workspace_smoke_loads_model_crate() {
    let id = wax_bench_model::BenchmarkId {
        dataset_id: "dataset".to_owned(),
        workload_id: "workload".to_owned(),
        sample_index: 0,
    };
    assert_eq!(id.dataset_id, "dataset");
}
