use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;

use crate::backup::manifest::BackupManifest;

pub struct BackupExporter {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl BackupExporter {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: Path::from(prefix),
        }
    }

    pub async fn export_manifest(
        &self,
        key: &str,
        manifest: &BackupManifest,
    ) -> Result<(), Box<dyn Error>> {
        let path = self.prefix.child(key);
        let body = serde_json::to_vec(manifest)?;
        self.store.put(&path, Bytes::from(body)).await?;
        Ok(())
    }

    pub async fn read_manifest_json(&self, key: &str) -> Result<String, Box<dyn Error>> {
        let path = self.prefix.child(key);
        let res = self.store.get(&path).await?;
        let bytes = res.bytes().await?;
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}
