use wax_v2_runtime::{
    RuntimeAccelerationAvailability, RuntimeAccelerationPreference,
    RuntimeExecutionBackend, RuntimePlatformAccelerationFamily, RuntimeStore,
};

#[test]
fn runtime_reports_apple_acceleration_capability_explicitly() {
    let capabilities = RuntimeStore::capabilities();
    let apple = capabilities
        .platform_acceleration
        .iter()
        .find(|capability| capability.family == RuntimePlatformAccelerationFamily::Apple)
        .unwrap();

    if cfg!(target_os = "macos") {
        assert_eq!(
            apple.availability,
            RuntimeAccelerationAvailability::BackendNotCompiled
        );
    } else {
        assert_eq!(
            apple.availability,
            RuntimeAccelerationAvailability::UnsupportedPlatform
        );
    }
    assert!(apple.detail.as_deref().unwrap_or("").len() > 0);
}

#[test]
fn runtime_resolves_platform_preference_without_changing_default_backend() {
    let selection = RuntimeStore::resolve_acceleration(RuntimeAccelerationPreference::PreferPlatform);

    assert_eq!(selection.preference, RuntimeAccelerationPreference::PreferPlatform);
    assert_eq!(selection.chosen_backend, RuntimeExecutionBackend::RustDefault);
    assert_eq!(
        selection.requested_family,
        Some(RuntimePlatformAccelerationFamily::Apple)
    );
    assert!(selection.fallback_reason.as_deref().unwrap_or("").len() > 0);
}
