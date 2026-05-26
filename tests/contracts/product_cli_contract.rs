use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use tempfile::tempdir;
use wax_v2_core::open_store;

#[test]
fn product_cli_remembers_and_recalls_from_single_wax_file() {
    let store_dir = tempdir().unwrap();
    let store_path = store_dir.path().join("agent.wax");

    let remember = wax_output(&[
        "remember",
        "--store",
        store_path.to_str().unwrap(),
        "The user is building a habit tracker in Rust",
    ]);
    assert_success(&remember);
    assert!(store_path.exists());
    assert_eq!(
        store_dir_entries(&store_dir),
        vec!["agent.wax".to_owned()],
        "product memory must keep the user-visible store as a single .wax file"
    );

    let recall = wax_output(&[
        "recall",
        "--store",
        store_path.to_str().unwrap(),
        "What is the user building?",
        "--top-k",
        "3",
    ]);
    assert_success(&recall);
    let stdout = String::from_utf8(recall.stdout).unwrap();
    assert!(stdout.contains("\"doc_id\": \"mem-0000000000000001\""));
    assert!(stdout.contains("\"preview\": \"The user is building a habit tracker in Rust\""));

    let recall_without_preview = wax_output(&[
        "recall",
        "--store",
        store_path.to_str().unwrap(),
        "What is the user building?",
        "--top-k",
        "3",
        "--no-preview",
    ]);
    assert_success(&recall_without_preview);
    let stdout = String::from_utf8(recall_without_preview.stdout).unwrap();
    assert!(stdout.contains("\"doc_id\": \"mem-0000000000000001\""));
    assert!(stdout.contains("\"preview\": null"));
    assert_eq!(
        store_dir_entries(&store_dir),
        vec!["agent.wax".to_owned()],
        "recall must not create lock or sidecar files next to the product store"
    );
}

#[cfg(unix)]
#[test]
fn product_cli_recalls_from_read_only_single_wax_file() {
    let store_dir = tempdir().unwrap();
    let store_path = store_dir.path().join("agent.wax");

    let remember = wax_output(&[
        "remember",
        "--store",
        store_path.to_str().unwrap(),
        "The user stores read-only memories",
    ]);
    assert_success(&remember);

    let mut permissions = fs::metadata(&store_path).unwrap().permissions();
    permissions.set_mode(0o400);
    fs::set_permissions(&store_path, permissions).unwrap();

    let recall = wax_output(&[
        "recall",
        "--store",
        store_path.to_str().unwrap(),
        "What does the user store?",
        "--top-k",
        "1",
        "--no-preview",
    ]);

    let mut permissions = fs::metadata(&store_path).unwrap().permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(&store_path, permissions).unwrap();

    assert_success(&recall);
    let stdout = String::from_utf8(recall.stdout).unwrap();
    assert!(stdout.contains("\"doc_id\": \"mem-0000000000000001\""));
    assert!(stdout.contains("\"preview\": null"));
}

#[test]
fn product_cli_create_targets_direct_store_file() {
    let store_dir = tempdir().unwrap();
    let store_path = store_dir.path().join("agent.wax");

    let output = wax_output(&["create", "--store", store_path.to_str().unwrap()]);
    assert_success(&output);

    assert!(store_path.exists());
    let opened = open_store(&store_path).unwrap();
    assert_eq!(opened.manifest.generation, 0);
    assert_eq!(store_dir_entries(&store_dir), vec!["agent.wax".to_owned()]);
}

#[test]
fn product_cli_rejects_removed_root_flag() {
    let store_dir = tempdir().unwrap();
    let output = wax_output(&["create", "--root", store_dir.path().to_str().unwrap()]);

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument '--root'"));
}

fn store_dir_entries(store_dir: &tempfile::TempDir) -> Vec<String> {
    let mut entries = fs::read_dir(store_dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

fn wax_output(args: &[&str]) -> std::process::Output {
    Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(["run", "-p", "wax-cli", "--"])
        .args(args)
        .output()
        .unwrap()
}

fn assert_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
