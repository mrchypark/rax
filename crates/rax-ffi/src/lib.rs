#![allow(clippy::missing_safety_doc)]

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::raw::{c_char, c_int};
use std::path::{Path, PathBuf};

use rax_runtime::{
    Memory, MemorySearchOptions, NewDocument, NewDocumentVector, RuntimePublishFamily,
    RuntimeSearchMode, RuntimeSearchRequest, RuntimeStore,
};
use serde::Deserialize;

pub const RAX_STATUS_OK: c_int = 0;
pub const RAX_STATUS_ERROR: c_int = 1;
pub const RAX_STATUS_INVALID_ARGUMENT: c_int = 2;
pub const RAX_STATUS_PANIC: c_int = 3;

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::new("").expect("empty c string"));
}

#[derive(Debug)]
struct FfiError {
    status: c_int,
    message: String,
}

impl FfiError {
    fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            status: RAX_STATUS_INVALID_ARGUMENT,
            message: message.into(),
        }
    }

    fn runtime(message: impl Into<String>) -> Self {
        Self {
            status: RAX_STATUS_ERROR,
            message: message.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct FfiNewDocument {
    doc_id: String,
    text: String,
    #[serde(default = "default_metadata")]
    metadata: serde_json::Value,
    #[serde(default)]
    timestamp_ms: Option<u64>,
    #[serde(flatten)]
    extra_fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct FfiNewDocumentVector {
    doc_id: String,
    values: Vec<f32>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FfiQueryVector {
    Values(Vec<f32>),
    Object(FfiQueryVectorObject),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FfiQueryVectorObject {
    values: Vec<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FfiSearchMode {
    Text,
    Vector,
    Hybrid,
}

#[no_mangle]
pub extern "C" fn rax_version() -> *const c_char {
    concat!("rax ", env!("CARGO_PKG_VERSION"), "\0")
        .as_ptr()
        .cast()
}

#[no_mangle]
pub unsafe extern "C" fn rax_create(store: *const c_char) -> c_int {
    ffi_status(|| {
        let store = required_path(store, "store")?;
        let mut runtime = RuntimeStore::open_or_create_at(&store).map_err(runtime_error)?;
        runtime.close().map_err(runtime_error)?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn rax_ingest_docs(
    store: *const c_char,
    input: *const c_char,
    out_json: *mut *mut c_char,
) -> c_int {
    ffi_status(|| {
        ensure_output(out_json)?;
        let store = required_path(store, "store")?;
        let input = required_path(input, "input")?;
        let documents = read_jsonl::<FfiNewDocument>(&input)?
            .into_iter()
            .map(|document| {
                let mut runtime_document = NewDocument::new(document.doc_id, document.text)
                    .with_metadata(document.metadata);
                if let Some(timestamp_ms) = document.timestamp_ms {
                    runtime_document = runtime_document.with_timestamp_ms(timestamp_ms);
                }
                for (key, value) in document.extra_fields {
                    runtime_document = runtime_document.with_extra_field(key, value);
                }
                runtime_document
            })
            .collect::<Vec<_>>();
        let mut runtime = RuntimeStore::open_or_create_at(&store).map_err(runtime_error)?;
        let report = runtime
            .writer()
            .map_err(runtime_error)?
            .publish_raw_documents(documents)
            .map_err(runtime_error)?;
        let json = render_publish_report(&report)?;
        runtime.close().map_err(runtime_error)?;
        write_output(out_json, json)
    })
}

#[no_mangle]
pub unsafe extern "C" fn rax_ingest_vectors(
    store: *const c_char,
    input: *const c_char,
    out_json: *mut *mut c_char,
) -> c_int {
    ffi_status(|| {
        ensure_output(out_json)?;
        let store = required_path(store, "store")?;
        let input = required_path(input, "input")?;
        let vectors = read_jsonl::<FfiNewDocumentVector>(&input)?
            .into_iter()
            .map(|vector| NewDocumentVector::new(vector.doc_id, vector.values))
            .collect::<Vec<_>>();
        let mut runtime = open_existing_runtime_store_for_vectors(&store)?;
        let report = runtime
            .writer()
            .map_err(runtime_error)?
            .publish_raw_vectors(vectors)
            .map_err(runtime_error)?;
        let json = render_publish_report(&report)?;
        runtime.close().map_err(runtime_error)?;
        write_output(out_json, json)
    })
}

#[no_mangle]
pub unsafe extern "C" fn rax_remember(
    store: *const c_char,
    text: *const c_char,
    out_json: *mut *mut c_char,
) -> c_int {
    ffi_status(|| {
        ensure_output(out_json)?;
        let store = required_path(store, "store")?;
        let text = required_string(text, "text")?;
        let mut memory = Memory::open(&store).map_err(runtime_error)?;
        let doc_id = memory.remember(text).map_err(runtime_error)?;
        let json = serde_json::to_string_pretty(&serde_json::json!({ "doc_id": doc_id }))
            .map_err(|error| FfiError::runtime(error.to_string()))?;
        memory.close().map_err(runtime_error)?;
        write_output(out_json, json)
    })
}

#[no_mangle]
pub unsafe extern "C" fn rax_recall(
    store: *const c_char,
    query: *const c_char,
    top_k: c_int,
    preview: bool,
    out_json: *mut *mut c_char,
) -> c_int {
    ffi_status(|| {
        ensure_output(out_json)?;
        let store = required_path(store, "store")?;
        let query = required_string(query, "query")?;
        let top_k = ffi_top_k(top_k)?;
        let mut memory = Memory::open_existing_read_only(&store).map_err(runtime_error)?;
        let response = memory
            .search_with_options(
                query,
                MemorySearchOptions {
                    mode: RuntimeSearchMode::Hybrid,
                    top_k,
                    include_preview: preview,
                },
            )
            .map_err(runtime_error)?;
        let json = render_hits(response.hits)?;
        memory.close().map_err(runtime_error)?;
        write_output(out_json, json)
    })
}

#[no_mangle]
pub unsafe extern "C" fn rax_search(
    store: *const c_char,
    mode: *const c_char,
    text: *const c_char,
    vector_input: *const c_char,
    top_k: c_int,
    preview: bool,
    out_json: *mut *mut c_char,
) -> c_int {
    ffi_status(|| {
        ensure_output(out_json)?;
        let store = required_path(store, "store")?;
        let mode = parse_search_mode(required_string(mode, "mode")?)?;
        let text = optional_string(text, "text")?;
        let vector_input = optional_path(vector_input, "vector_input")?;
        let top_k = ffi_top_k(top_k)?;
        let mut runtime =
            RuntimeStore::open_existing_read_only_at(&store).map_err(runtime_error)?;
        let request = build_search_request(mode, text, vector_input, top_k, preview)?;
        let response = runtime.search(request).map_err(runtime_error)?;
        let json = render_hits(response.hits)?;
        runtime.close().map_err(runtime_error)?;
        write_output(out_json, json)
    })
}

#[no_mangle]
pub unsafe extern "C" fn rax_string_free(value: *mut c_char) {
    if !value.is_null() {
        drop(CString::from_raw(value));
    }
}

#[no_mangle]
pub extern "C" fn rax_last_error() -> *const c_char {
    LAST_ERROR.with(|error| error.borrow().as_ptr())
}

fn ffi_status(operation: impl FnOnce() -> Result<(), FfiError>) -> c_int {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)) {
        Ok(Ok(())) => {
            clear_last_error();
            RAX_STATUS_OK
        }
        Ok(Err(error)) => {
            set_last_error(&error.message);
            error.status
        }
        Err(_) => {
            set_last_error("panic across rax FFI boundary");
            RAX_STATUS_PANIC
        }
    }
}

fn clear_last_error() {
    set_last_error("");
}

fn set_last_error(message: &str) {
    let sanitized = message.replace('\0', "\\0");
    LAST_ERROR.with(|error| {
        *error.borrow_mut() = CString::new(sanitized).expect("interior nul replaced");
    });
}

fn required_string(value: *const c_char, name: &str) -> Result<String, FfiError> {
    if value.is_null() {
        return Err(FfiError::invalid_argument(format!("{name} is required")));
    }
    unsafe { CStr::from_ptr(value) }
        .to_str()
        .map(|value| value.to_owned())
        .map_err(|error| FfiError::invalid_argument(format!("{name} must be valid UTF-8: {error}")))
}

fn optional_string(value: *const c_char, name: &str) -> Result<Option<String>, FfiError> {
    if value.is_null() {
        Ok(None)
    } else {
        required_string(value, name).map(Some)
    }
}

fn required_path(value: *const c_char, name: &str) -> Result<PathBuf, FfiError> {
    required_string(value, name).map(PathBuf::from)
}

fn optional_path(value: *const c_char, name: &str) -> Result<Option<PathBuf>, FfiError> {
    if value.is_null() {
        Ok(None)
    } else {
        required_string(value, name).map(PathBuf::from).map(Some)
    }
}

fn ffi_top_k(top_k: c_int) -> Result<usize, FfiError> {
    usize::try_from(top_k).map_err(|_| FfiError::invalid_argument("top_k must be non-negative"))
}

fn write_output(out_json: *mut *mut c_char, json: String) -> Result<(), FfiError> {
    ensure_output(out_json)?;
    let json = CString::new(json)
        .map_err(|_| FfiError::runtime("output JSON contained an interior NUL"))?;
    unsafe {
        *out_json = json.into_raw();
    }
    Ok(())
}

fn ensure_output(out_json: *mut *mut c_char) -> Result<(), FfiError> {
    if out_json.is_null() {
        return Err(FfiError::invalid_argument("out_json is required"));
    }
    unsafe {
        *out_json = std::ptr::null_mut();
    }
    Ok(())
}

fn parse_search_mode(mode: String) -> Result<FfiSearchMode, FfiError> {
    match mode.as_str() {
        "text" => Ok(FfiSearchMode::Text),
        "vector" => Ok(FfiSearchMode::Vector),
        "hybrid" => Ok(FfiSearchMode::Hybrid),
        _ => Err(FfiError::invalid_argument(format!(
            "unsupported search mode {mode}; expected text, vector, or hybrid"
        ))),
    }
}

fn build_search_request(
    mode: FfiSearchMode,
    text: Option<String>,
    vector_input: Option<PathBuf>,
    top_k: usize,
    include_preview: bool,
) -> Result<RuntimeSearchRequest, FfiError> {
    let runtime_mode = match mode {
        FfiSearchMode::Text => RuntimeSearchMode::Text,
        FfiSearchMode::Vector => RuntimeSearchMode::Vector,
        FfiSearchMode::Hybrid => RuntimeSearchMode::Hybrid,
    };
    let text_query = match mode {
        FfiSearchMode::Text | FfiSearchMode::Hybrid => Some(text.ok_or_else(|| {
            FfiError::invalid_argument(format!("search --mode {} requires --text", mode_name(mode)))
        })?),
        FfiSearchMode::Vector => {
            if text.is_some() {
                return Err(FfiError::invalid_argument(
                    "search --mode vector does not accept --text",
                ));
            }
            None
        }
    };
    let vector_query = match mode {
        FfiSearchMode::Text => {
            if vector_input.is_some() {
                return Err(FfiError::invalid_argument(
                    "search --mode text does not accept --vector-input",
                ));
            }
            None
        }
        FfiSearchMode::Vector | FfiSearchMode::Hybrid => {
            let path = vector_input.ok_or_else(|| {
                FfiError::invalid_argument(format!(
                    "search --mode {} requires --vector-input",
                    mode_name(mode)
                ))
            })?;
            Some(read_query_vector(&path)?)
        }
    };
    Ok(RuntimeSearchRequest {
        mode: runtime_mode,
        text_query,
        vector_query,
        top_k,
        include_preview,
    })
}

fn mode_name(mode: FfiSearchMode) -> &'static str {
    match mode {
        FfiSearchMode::Text => "text",
        FfiSearchMode::Vector => "vector",
        FfiSearchMode::Hybrid => "hybrid",
    }
}

fn read_query_vector(path: &Path) -> Result<Vec<f32>, FfiError> {
    let file = File::open(path).map_err(runtime_error)?;
    let vector = serde_json::from_reader::<_, FfiQueryVector>(BufReader::new(file))
        .map_err(runtime_error)?;
    let values = match vector {
        FfiQueryVector::Values(values) => values,
        FfiQueryVector::Object(object) => object.values,
    };
    if values.is_empty() {
        return Err(FfiError::runtime(
            "query vector must contain at least one value",
        ));
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(FfiError::runtime(
            "query vector must contain only finite float values",
        ));
    }
    Ok(values)
}

fn open_existing_runtime_store_for_vectors(store: &Path) -> Result<RuntimeStore, FfiError> {
    if !store.exists() {
        return Err(FfiError::runtime(format!(
            "store file {} does not exist; run ingest docs first",
            store.display()
        )));
    }
    RuntimeStore::open_existing_at(store).map_err(runtime_error)
}

fn render_hits(hits: Vec<rax_runtime::RuntimeSearchHit>) -> Result<String, FfiError> {
    let rendered_hits = hits
        .into_iter()
        .map(|hit| {
            serde_json::json!({
                "doc_id": hit.doc_id,
                "preview": hit.preview,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&rendered_hits)
        .map_err(|error| FfiError::runtime(error.to_string()))
}

fn render_publish_report(report: &rax_runtime::RuntimePublishReport) -> Result<String, FfiError> {
    serde_json::to_string_pretty(&serde_json::json!({
        "generation": report.generation,
        "published_families": report
            .published_families
            .iter()
            .map(runtime_publish_family_name)
            .collect::<Vec<_>>(),
    }))
    .map_err(|error| FfiError::runtime(error.to_string()))
}

fn runtime_publish_family_name(family: &RuntimePublishFamily) -> &'static str {
    match family {
        RuntimePublishFamily::Doc => "doc",
        RuntimePublishFamily::Text => "text",
        RuntimePublishFamily::Vector => "vector",
    }
}

fn default_metadata() -> serde_json::Value {
    serde_json::json!({})
}

fn read_jsonl<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>, FfiError> {
    BufReader::new(File::open(path).map_err(runtime_error)?)
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some(other),
        })
        .map(|line| {
            let line = line.map_err(runtime_error)?;
            serde_json::from_str(&line).map_err(runtime_error)
        })
        .collect()
}

fn runtime_error(error: impl ToString) -> FfiError {
    FfiError::runtime(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::fs;
    use std::ptr;

    #[test]
    fn cli_contract_endpoints_round_trip_through_c_abi() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let store_path = tempdir.path().join("agent.rax");
        let store_path = CString::new(store_path.to_string_lossy().as_bytes()).expect("path");
        let text = CString::new("The user is building a Go package for rax.").expect("text");
        let query = CString::new("What package is being built?").expect("query");

        let version = unsafe { CStr::from_ptr(super::rax_version()) }
            .to_string_lossy()
            .into_owned();
        assert_eq!(version, format!("rax {}", env!("CARGO_PKG_VERSION")));

        assert_eq!(0, unsafe { super::rax_create(store_path.as_ptr()) });

        let mut remember_json = ptr::null_mut();
        assert_eq!(0, unsafe {
            super::rax_remember(store_path.as_ptr(), text.as_ptr(), &mut remember_json)
        });
        let remembered: serde_json::Value =
            serde_json::from_str(unsafe { CStr::from_ptr(remember_json) }.to_str().unwrap())
                .unwrap();
        assert_eq!(remembered["doc_id"], "mem-0000000000000001");
        unsafe { super::rax_string_free(remember_json) };

        let mut recall_json = ptr::null_mut();
        assert_eq!(0, unsafe {
            super::rax_recall(
                store_path.as_ptr(),
                query.as_ptr(),
                5,
                true,
                &mut recall_json,
            )
        });
        let recalled: serde_json::Value =
            serde_json::from_str(unsafe { CStr::from_ptr(recall_json) }.to_str().unwrap()).unwrap();
        assert_eq!(recalled[0]["doc_id"], "mem-0000000000000001");
        assert_eq!(
            recalled[0]["preview"],
            "The user is building a Go package for rax."
        );
        unsafe { super::rax_string_free(recall_json) };
    }

    #[test]
    fn raw_ingest_and_search_match_cli_json_shapes() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let store_path = tempdir.path().join("raw.rax");
        let docs_path = tempdir.path().join("docs.jsonl");
        let vectors_path = tempdir.path().join("vectors.jsonl");
        let query_vector_path = tempdir.path().join("query-vector.json");
        fs::write(
            &docs_path,
            concat!(
                "{\"doc_id\":\"doc-1\",\"text\":\"rust ffi search target\"}\n",
                "{\"doc_id\":\"doc-2\",\"text\":\"unrelated memory\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            &vectors_path,
            format!(
                "{}\n{}\n",
                vector_record("doc-1", 0),
                vector_record("doc-2", 1)
            ),
        )
        .unwrap();
        fs::write(
            &query_vector_path,
            serde_json::json!({ "values": fixture_vector(0) }).to_string(),
        )
        .unwrap();

        let store_path = CString::new(store_path.to_string_lossy().as_bytes()).unwrap();
        let docs_path = CString::new(docs_path.to_string_lossy().as_bytes()).unwrap();
        let vectors_path = CString::new(vectors_path.to_string_lossy().as_bytes()).unwrap();
        let query_vector_path =
            CString::new(query_vector_path.to_string_lossy().as_bytes()).unwrap();
        let mode_text = CString::new("text").unwrap();
        let mode_vector = CString::new("vector").unwrap();
        let text_query = CString::new("rust ffi").unwrap();

        let mut docs_json = ptr::null_mut();
        assert_eq!(0, unsafe {
            super::rax_ingest_docs(store_path.as_ptr(), docs_path.as_ptr(), &mut docs_json)
        });
        let docs_report: serde_json::Value =
            serde_json::from_str(unsafe { CStr::from_ptr(docs_json) }.to_str().unwrap()).unwrap();
        assert_eq!(docs_report["generation"], 1);
        assert_eq!(
            docs_report["published_families"],
            serde_json::json!(["doc", "text"])
        );
        unsafe { super::rax_string_free(docs_json) };

        let mut vectors_json = ptr::null_mut();
        assert_eq!(0, unsafe {
            super::rax_ingest_vectors(
                store_path.as_ptr(),
                vectors_path.as_ptr(),
                &mut vectors_json,
            )
        });
        let vectors_report: serde_json::Value =
            serde_json::from_str(unsafe { CStr::from_ptr(vectors_json) }.to_str().unwrap())
                .unwrap();
        assert_eq!(
            vectors_report["published_families"],
            serde_json::json!(["vector"])
        );
        unsafe { super::rax_string_free(vectors_json) };

        let mut search_json = ptr::null_mut();
        assert_eq!(0, unsafe {
            super::rax_search(
                store_path.as_ptr(),
                mode_text.as_ptr(),
                text_query.as_ptr(),
                ptr::null(),
                1,
                true,
                &mut search_json,
            )
        });
        let search_hits: serde_json::Value =
            serde_json::from_str(unsafe { CStr::from_ptr(search_json) }.to_str().unwrap()).unwrap();
        assert_eq!(search_hits[0]["doc_id"], "doc-1");
        assert_eq!(search_hits[0]["preview"], "rust ffi search target");
        unsafe { super::rax_string_free(search_json) };

        let mut vector_search_json = ptr::null_mut();
        assert_eq!(0, unsafe {
            super::rax_search(
                store_path.as_ptr(),
                mode_vector.as_ptr(),
                ptr::null(),
                query_vector_path.as_ptr(),
                1,
                true,
                &mut vector_search_json,
            )
        });
        let vector_hits: serde_json::Value = serde_json::from_str(
            unsafe { CStr::from_ptr(vector_search_json) }
                .to_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(vector_hits[0]["doc_id"], "doc-1");
        assert_eq!(vector_hits[0]["preview"], "rust ffi search target");
        unsafe { super::rax_string_free(vector_search_json) };
    }

    #[test]
    fn invalid_arguments_return_invalid_argument_status_and_last_error() {
        assert_eq!(super::RAX_STATUS_INVALID_ARGUMENT, unsafe {
            super::rax_create(ptr::null())
        });
        let error = unsafe { CStr::from_ptr(super::rax_last_error()) }.to_string_lossy();
        assert!(error.contains("store is required"));
    }

    fn fixture_vector(active_index: usize) -> Vec<f32> {
        let mut values = vec![0.0; 384];
        values[active_index] = 1.0;
        values
    }

    fn vector_record(doc_id: &str, active_index: usize) -> String {
        serde_json::json!({
            "doc_id": doc_id,
            "values": fixture_vector(active_index),
        })
        .to_string()
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod c_header_smoke {
    use std::env;
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    #[test]
    fn shipped_header_links_from_c() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let source = tempdir.path().join("smoke.c");
        let output = tempdir.path().join("smoke");
        let store = tempdir.path().join("smoke.rax");
        let smoke_source = r#"
#include "rax.h"

#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    char *json = NULL;
    const char *store = argv[1];

    if (strcmp(rax_version(), "rax @RAX_VERSION@") != 0) {
        fprintf(stderr, "unexpected version: %s\n", rax_version());
        return 2;
    }
    if (rax_create(store) != RAX_STATUS_OK) {
        fprintf(stderr, "create failed: %s\n", rax_last_error());
        return 3;
    }
    if (rax_remember(store, "c header smoke memory", &json) != RAX_STATUS_OK) {
        fprintf(stderr, "remember failed: %s\n", rax_last_error());
        return 4;
    }
    if (json == NULL || strstr(json, "mem-0000000000000001") == NULL) {
        fprintf(stderr, "bad remember json\n");
        return 5;
    }
    rax_string_free(json);
    json = NULL;

    if (rax_recall(store, "smoke memory", 1, true, &json) != RAX_STATUS_OK) {
        fprintf(stderr, "recall failed: %s\n", rax_last_error());
        return 6;
    }
    if (json == NULL || strstr(json, "c header smoke memory") == NULL) {
        fprintf(stderr, "bad recall json\n");
        return 7;
    }
    rax_string_free(json);
    return 0;
}
"#
        .replace("@RAX_VERSION@", env!("CARGO_PKG_VERSION"));
        fs::write(&source, smoke_source).expect("write smoke source");

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let include_dir = manifest_dir.join("include");
        let library_dir = target_debug_dir();
        let status = Command::new("cc")
            .arg("-I")
            .arg(&include_dir)
            .arg(&source)
            .arg("-L")
            .arg(&library_dir)
            .arg("-lrax_ffi")
            .arg(format!("-Wl,-rpath,{}", library_dir.display()))
            .arg("-o")
            .arg(&output)
            .status()
            .expect("run cc");
        assert!(status.success(), "C header smoke compile failed");

        let mut command = Command::new(&output);
        command.arg(&store);
        add_library_path(&mut command, &library_dir);
        let run = command.output().expect("run C header smoke");
        assert!(
            run.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&run.stdout),
            String::from_utf8_lossy(&run.stderr)
        );
    }

    fn target_debug_dir() -> PathBuf {
        let current_exe = env::current_exe().expect("current test executable");
        current_exe
            .parent()
            .and_then(Path::parent)
            .expect("target debug directory")
            .to_path_buf()
    }

    fn add_library_path(command: &mut Command, library_dir: &Path) {
        let key = if cfg!(target_os = "macos") {
            "DYLD_LIBRARY_PATH"
        } else {
            "LD_LIBRARY_PATH"
        };
        let mut value = OsString::new();
        value.push(library_dir);
        if let Some(existing) = env::var_os(key) {
            value.push(":");
            value.push(existing);
        }
        command.env(key, value);
    }
}
