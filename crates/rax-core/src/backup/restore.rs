use std::error::Error;
use std::ops::Range;
use std::sync::Arc;

use object_store::path::Path;
use object_store::ObjectStore;

use crate::backup::chain::verify_chain;
use crate::backup::manifest::BackupManifest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreState {
    pub applied_snapshots: Vec<String>,
}

pub fn restore_full(base: &BackupManifest) -> RestoreState {
    RestoreState {
        applied_snapshots: vec![base.snapshot_id.clone()],
    }
}

pub fn restore_incremental(chain: &[BackupManifest]) -> Option<RestoreState> {
    if !verify_chain(chain) {
        return None;
    }
    Some(RestoreState {
        applied_snapshots: chain.iter().map(|m| m.snapshot_id.clone()).collect(),
    })
}

pub async fn restore_range_read(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    key: &str,
    range: Range<usize>,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let p = Path::from(prefix).child(key);
    let bytes = store.get_range(&p, range).await?;
    Ok(bytes.to_vec())
}
