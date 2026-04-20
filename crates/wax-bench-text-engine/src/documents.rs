use std::collections::HashMap;
use std::path::Path;

use serde_json::Value;
use wax_bench_model::DatasetPackManifest;
use wax_v2_docstore::{validate_store_segment_against_dataset_pack, Docstore};
use wax_v2_text::validate_store_segment_against_dataset_pack as validate_text_segment_against_dataset_pack;
use wax_v2_vector::validate_store_segment_against_dataset_pack as validate_vector_segment_against_dataset_pack;

pub(crate) fn open_docstore(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<Docstore, String> {
    Docstore::open(mount_root, manifest).map_err(docstore_error)
}

pub(crate) fn validate_store_segments_against_dataset_pack(
    mount_root: &Path,
    manifest: &DatasetPackManifest,
) -> Result<(), String> {
    validate_store_segment_against_dataset_pack(mount_root, manifest).map_err(docstore_error)?;
    validate_text_segment_against_dataset_pack(mount_root, manifest)?;
    validate_vector_segment_against_dataset_pack(mount_root, manifest)?;
    Ok(())
}

pub(crate) fn load_documents_by_id(
    docstore: &Docstore,
    target_doc_ids: &[String],
) -> Result<HashMap<String, Value>, String> {
    docstore
        .load_documents_by_id(target_doc_ids)
        .map_err(docstore_error)
}

fn docstore_error(error: wax_v2_docstore::DocstoreError) -> String {
    match error {
        wax_v2_docstore::DocstoreError::Io(message)
        | wax_v2_docstore::DocstoreError::Json(message)
        | wax_v2_docstore::DocstoreError::InvalidDocument(message) => message,
        wax_v2_docstore::DocstoreError::MissingDocumentsFile => {
            "dataset pack missing documents file".to_owned()
        }
    }
}
