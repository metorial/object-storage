use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info};

use crate::backend::{compute_etag, Backend, ObjectData, ObjectMetadata};
use crate::error::{BackendError, BackendResult};

pub struct LocalBackend {
    root_path: PathBuf,
    bucket_name: String,
}

impl LocalBackend {
    pub fn new(root_path: PathBuf, bucket_name: String) -> Self {
        Self {
            root_path,
            bucket_name,
        }
    }

    fn get_full_path(&self, key: &str) -> BackendResult<PathBuf> {
        if key.contains("..") || key.starts_with('/') {
            return Err(BackendError::InvalidPath(format!("Invalid key: {}", key)));
        }

        let path = self.root_path.join(&self.bucket_name).join(key);
        Ok(path)
    }

    fn get_metadata_path(&self, key: &str) -> BackendResult<PathBuf> {
        let object_path = self.get_full_path(key)?;
        Ok(object_path.with_extension("meta.json"))
    }

    async fn read_metadata(&self, key: &str) -> BackendResult<ObjectMetadata> {
        let meta_path = self.get_metadata_path(key)?;

        if !meta_path.exists() {
            return Err(BackendError::NotFound(key.to_string()));
        }

        let content = fs::read_to_string(&meta_path).await?;
        let metadata: ObjectMetadata = serde_json::from_str(&content)?;
        Ok(metadata)
    }

    async fn write_metadata(&self, metadata: &ObjectMetadata) -> BackendResult<()> {
        let meta_path = self.get_metadata_path(&metadata.key)?;

        if let Some(parent) = meta_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let content = serde_json::to_string_pretty(metadata)?;
        fs::write(&meta_path, content).await?;
        Ok(())
    }
}

#[async_trait]
impl Backend for LocalBackend {
    async fn init(&self) -> BackendResult<()> {
        let bucket_path = self.root_path.join(&self.bucket_name);
        fs::create_dir_all(&bucket_path).await?;
        info!("Initialized local backend at {:?}", bucket_path);
        Ok(())
    }

    async fn put_object(
        &self,
        key: &str,
        data: Vec<u8>,
        content_type: Option<String>,
        custom_metadata: HashMap<String, String>,
    ) -> BackendResult<ObjectMetadata> {
        debug!("Putting object: {} ({} bytes)", key, data.len());

        let object_path = self.get_full_path(key)?;

        if let Some(parent) = object_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut file = fs::File::create(&object_path).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;

        let metadata = ObjectMetadata {
            key: key.to_string(),
            size: data.len() as u64,
            content_type,
            etag: compute_etag(&data),
            last_modified: Utc::now(),
            custom_metadata,
        };

        self.write_metadata(&metadata).await?;

        info!("Object stored: {} (etag: {})", key, metadata.etag);
        Ok(metadata)
    }

    async fn get_object(&self, key: &str) -> BackendResult<ObjectData> {
        debug!("Getting object: {}", key);

        let object_path = self.get_full_path(key)?;

        if !object_path.exists() {
            return Err(BackendError::NotFound(key.to_string()));
        }

        let data = fs::read(&object_path).await?;
        let metadata = self.read_metadata(key).await?;

        Ok(ObjectData { metadata, data })
    }

    async fn head_object(&self, key: &str) -> BackendResult<ObjectMetadata> {
        debug!("Getting object metadata: {}", key);
        self.read_metadata(key).await
    }

    async fn delete_object(&self, key: &str) -> BackendResult<()> {
        debug!("Deleting object: {}", key);

        let object_path = self.get_full_path(key)?;
        let meta_path = self.get_metadata_path(key)?;

        if !object_path.exists() {
            return Err(BackendError::NotFound(key.to_string()));
        }

        fs::remove_file(&object_path).await?;

        if meta_path.exists() {
            fs::remove_file(&meta_path).await?;
        }

        info!("Object deleted: {}", key);
        Ok(())
    }

    async fn list_objects(
        &self,
        prefix: Option<&str>,
        max_keys: Option<usize>,
    ) -> BackendResult<Vec<ObjectMetadata>> {
        debug!("Listing objects with prefix: {:?}", prefix);

        let bucket_path = self.root_path.join(&self.bucket_name);
        let mut results = Vec::new();

        let prefix_str = prefix.unwrap_or("");
        let search_path = if prefix_str.is_empty() {
            bucket_path.clone()
        } else {
            bucket_path.join(prefix_str)
        };

        self.list_recursive(
            &bucket_path,
            &search_path,
            prefix_str,
            &mut results,
            max_keys,
        )
        .await?;

        Ok(results)
    }
}

impl LocalBackend {
    fn list_recursive<'a>(
        &'a self,
        bucket_path: &'a Path,
        current_path: &'a Path,
        prefix: &'a str,
        results: &'a mut Vec<ObjectMetadata>,
        max_keys: Option<usize>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult<()>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(max) = max_keys {
                if results.len() >= max {
                    return Ok(());
                }
            }

            if !current_path.exists() {
                return Ok(());
            }

            if current_path.is_file() {
                if current_path.extension().and_then(|s| s.to_str()) == Some("json") {
                    return Ok(());
                }

                if let Ok(relative) = current_path.strip_prefix(bucket_path) {
                    let key = relative.to_string_lossy().to_string();

                    if key.starts_with(prefix) {
                        if let Ok(metadata) = self.read_metadata(&key).await {
                            results.push(metadata);
                        }
                    }
                }
                return Ok(());
            }

            let mut entries = fs::read_dir(current_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();

                if path.is_dir() {
                    self.list_recursive(bucket_path, &path, prefix, results, max_keys)
                        .await?;
                } else if !path.to_string_lossy().ends_with(".meta.json") {
                    if let Some(max) = max_keys {
                        if results.len() >= max {
                            break;
                        }
                    }

                    if let Ok(relative) = path.strip_prefix(bucket_path) {
                        let key = relative.to_string_lossy().to_string();

                        if key.starts_with(prefix) {
                            if let Ok(metadata) = self.read_metadata(&key).await {
                                results.push(metadata);
                            }
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_backend_put_get() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalBackend::new(temp_dir.path().to_path_buf(), "test-bucket".to_string());

        backend.init().await.unwrap();

        let data = b"Hello, World!".to_vec();
        let metadata = backend
            .put_object(
                "test.txt",
                data.clone(),
                Some("text/plain".to_string()),
                HashMap::new(),
            )
            .await
            .unwrap();

        assert_eq!(metadata.key, "test.txt");
        assert_eq!(metadata.size, 13);

        let obj = backend.get_object("test.txt").await.unwrap();
        assert_eq!(obj.data, data);
    }

    #[tokio::test]
    async fn test_local_backend_delete() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalBackend::new(temp_dir.path().to_path_buf(), "test-bucket".to_string());

        backend.init().await.unwrap();

        let data = b"Hello, World!".to_vec();
        backend
            .put_object(
                "test.txt",
                data,
                Some("text/plain".to_string()),
                HashMap::new(),
            )
            .await
            .unwrap();

        backend.delete_object("test.txt").await.unwrap();

        let result = backend.get_object("test.txt").await;
        assert!(matches!(result, Err(BackendError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_path_traversal_prevention() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalBackend::new(temp_dir.path().to_path_buf(), "test-bucket".to_string());

        backend.init().await.unwrap();

        let result = backend
            .put_object("../etc/passwd", vec![1, 2, 3], None, HashMap::new())
            .await;
        assert!(matches!(result, Err(BackendError::InvalidPath(_))));
    }
}
