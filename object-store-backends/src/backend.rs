use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;

use crate::error::BackendResult;

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PublicUrlPurpose {
    #[default]
    Retrieve,
    Upload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub etag: String,
    pub last_modified: DateTime<Utc>,
    pub custom_metadata: HashMap<String, String>,
}

pub struct ObjectData {
    pub metadata: ObjectMetadata,
    pub stream: ByteStream,
}

#[derive(Debug, Clone, Default)]
pub struct ObjectPage {
    pub objects: Vec<ObjectMetadata>,
    pub next_continuation_token: Option<String>,
}

#[async_trait]
pub trait Backend: Send + Sync {
    async fn init(&self) -> BackendResult<()>;

    async fn put_object(
        &self,
        key: &str,
        stream: ByteStream,
        content_type: Option<String>,
        metadata: HashMap<String, String>,
    ) -> BackendResult<ObjectMetadata>;

    async fn get_object(&self, key: &str) -> BackendResult<ObjectData>;

    async fn head_object(&self, key: &str) -> BackendResult<ObjectMetadata>;

    async fn delete_object(&self, key: &str) -> BackendResult<()>;

    async fn delete_objects(&self, keys: &[String]) -> BackendResult<Vec<BackendResult<()>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.delete_object(key).await);
        }
        Ok(results)
    }

    async fn copy_object(&self, source_key: &str, dest_key: &str) -> BackendResult<ObjectMetadata> {
        let source = self.get_object(source_key).await?;

        self.put_object(
            dest_key,
            source.stream,
            source.metadata.content_type,
            source.metadata.custom_metadata,
        )
        .await
    }

    async fn list_objects(
        &self,
        prefix: Option<&str>,
        max_keys: Option<usize>,
        continuation_token: Option<&str>,
    ) -> BackendResult<ObjectPage>;

    async fn list_all_objects(&self, prefix: Option<&str>) -> BackendResult<Vec<ObjectMetadata>> {
        let mut all = Vec::new();
        let mut token: Option<String> = None;

        loop {
            let page = self.list_objects(prefix, None, token.as_deref()).await?;
            all.extend(page.objects);

            match page.next_continuation_token {
                Some(next) => token = Some(next),
                None => return Ok(all),
            }
        }
    }

    async fn object_exists(&self, key: &str) -> BackendResult<bool> {
        match self.head_object(key).await {
            Ok(_) => Ok(true),
            Err(crate::error::BackendError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn get_public_url(
        &self,
        key: &str,
        expiration_secs: u64,
        purpose: PublicUrlPurpose,
    ) -> BackendResult<String>;
}

pub fn compute_etag(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}
