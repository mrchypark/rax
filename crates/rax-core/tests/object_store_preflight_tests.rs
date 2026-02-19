use object_store::path::Path;

fn parse_backend_scheme(url: &str) -> Option<&'static str> {
    if url.starts_with("file://") {
        Some("file")
    } else if url.starts_with("s3://") {
        Some("s3")
    } else if url.starts_with("gs://") {
        Some("gcs")
    } else if url.starts_with("az://") {
        Some("azure")
    } else {
        None
    }
}

#[test]
fn object_store_backend_schemes_are_recognized() {
    assert_eq!(parse_backend_scheme("file:///tmp/backup"), Some("file"));
    assert_eq!(parse_backend_scheme("s3://bucket/prefix"), Some("s3"));
    assert_eq!(parse_backend_scheme("gs://bucket/prefix"), Some("gcs"));
    assert_eq!(parse_backend_scheme("az://container/prefix"), Some("azure"));

    let p = Path::from("backup/manifest.json");
    assert_eq!(p.as_ref(), "backup/manifest.json");
}
