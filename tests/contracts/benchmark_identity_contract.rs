use serde_json::{from_str, to_string};
use strum::IntoEnumIterator;
use wax_bench_model::{
    CacheState, ColdState, MaterializationMode, PreviewMode, QueryEmbeddingMode,
};

#[test]
fn benchmark_identity_enums_round_trip_with_stable_labels() {
    let expected_cache_labels = [
        "warm_process",
        "cold_process",
        "cold_process_warm_fs_cache",
        "cold_process_cold_fs_cache",
    ];
    let expected_cold_labels = ["restart_cold", "pressure_cold", "reboot_cold"];
    let expected_materialization_labels = [
        "no_forced_lane_materialization",
        "force_text_lane",
        "force_vector_lane",
        "force_all_lanes",
    ];
    let expected_preview_labels = ["no_preview", "with_preview"];
    let expected_embedding_labels = [
        "none",
        "precomputed",
        "runtime_generic",
        "runtime_ane_cold",
        "runtime_ane_warm",
    ];

    assert_wire_labels(CacheState::iter(), &expected_cache_labels);
    assert_wire_labels(ColdState::iter(), &expected_cold_labels);
    assert_wire_labels(
        MaterializationMode::iter(),
        &expected_materialization_labels,
    );
    assert_wire_labels(PreviewMode::iter(), &expected_preview_labels);
    assert_wire_labels(QueryEmbeddingMode::iter(), &expected_embedding_labels);
}

#[test]
fn benchmark_identity_enums_reject_unknown_variants() {
    assert!(from_str::<CacheState>("\"lukewarm_process\"").is_err());
    assert!(from_str::<ColdState>("\"frozen\"").is_err());
    assert!(from_str::<MaterializationMode>("\"force_everything_now\"").is_err());
    assert!(from_str::<PreviewMode>("\"previewish\"").is_err());
    assert!(from_str::<QueryEmbeddingMode>("\"runtime_quantum\"").is_err());
}

fn assert_wire_labels<T, I>(values: I, expected_labels: &[&str])
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + std::fmt::Debug,
    I: IntoIterator<Item = T>,
{
    let actual: Vec<T> = values.into_iter().collect();

    assert_eq!(actual.len(), expected_labels.len());

    for (value, expected_label) in actual.into_iter().zip(expected_labels.iter()) {
        let encoded = to_string(&value).unwrap();

        assert_eq!(encoded, format!("\"{expected_label}\""));
        assert_eq!(from_str::<T>(&encoded).unwrap(), value);
    }
}
