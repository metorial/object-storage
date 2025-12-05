use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::ServiceResult;
use crate::metadata::Bucket;
use crate::service::ObjectStoreService;

pub type SharedService = Arc<ObjectStoreService>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateBucketRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BucketResponse {
    pub name: String,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListBucketsResponse {
    pub buckets: Vec<BucketResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectMetadataResponse {
    pub key: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub etag: String,
    pub last_modified: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListObjectsResponse {
    pub objects: Vec<ObjectMetadataResponse>,
}

#[derive(Debug, Deserialize)]
pub struct ListObjectsQuery {
    pub prefix: Option<String>,
    pub max_keys: Option<usize>,
}

impl From<Bucket> for BucketResponse {
    fn from(bucket: Bucket) -> Self {
        Self {
            name: bucket.name,
            created_at: bucket.created_at,
        }
    }
}

impl From<object_store_backends::ObjectMetadata> for ObjectMetadataResponse {
    fn from(metadata: object_store_backends::ObjectMetadata) -> Self {
        Self {
            key: metadata.key,
            size: metadata.size,
            content_type: metadata.content_type,
            etag: metadata.etag,
            last_modified: metadata.last_modified.to_rfc3339(),
            metadata: metadata.custom_metadata,
        }
    }
}

pub async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "object-store"
    }))
}

pub async fn create_bucket(
    State(service): State<SharedService>,
    Json(payload): Json<CreateBucketRequest>,
) -> ServiceResult<Json<BucketResponse>> {
    let bucket = service.create_bucket(&payload.name).await?;
    Ok(Json(bucket.into()))
}

pub async fn list_buckets(
    State(service): State<SharedService>,
) -> ServiceResult<Json<ListBucketsResponse>> {
    let buckets = service.list_buckets().await?;
    let response = ListBucketsResponse {
        buckets: buckets.into_iter().map(|b| b.into()).collect(),
    };
    Ok(Json(response))
}

pub async fn delete_bucket(
    State(service): State<SharedService>,
    Path(bucket): Path<String>,
) -> ServiceResult<StatusCode> {
    service.delete_bucket(&bucket).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn put_object(
    State(service): State<SharedService>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> ServiceResult<Json<ObjectMetadataResponse>> {
    // Extract content type from headers
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| {
            // Try to guess content type from file extension
            mime_guess::from_path(&key).first().map(|m| m.to_string())
        });

    // Extract custom metadata from headers (x-amz-meta-* pattern)
    let mut metadata = HashMap::new();
    for (key, value) in headers.iter() {
        if let Some(meta_key) = key.as_str().strip_prefix("x-object-meta-") {
            if let Ok(meta_value) = value.to_str() {
                metadata.insert(meta_key.to_string(), meta_value.to_string());
            }
        }
    }

    let obj_metadata = service
        .put_object(&bucket, &key, body.to_vec(), content_type, metadata)
        .await?;

    Ok(Json(obj_metadata.into()))
}

pub async fn get_object(
    State(service): State<SharedService>,
    Path((bucket, key)): Path<(String, String)>,
) -> ServiceResult<Response> {
    let obj_data = service.get_object(&bucket, &key).await?;

    let mut headers = HeaderMap::new();

    if let Some(ct) = obj_data.metadata.content_type {
        if let Ok(header_value) = ct.parse() {
            headers.insert("content-type", header_value);
        }
    }

    headers.insert(
        "etag",
        obj_data
            .metadata
            .etag
            .parse()
            .unwrap_or_else(|_| "unknown".parse().unwrap()),
    );

    headers.insert(
        "last-modified",
        obj_data
            .metadata
            .last_modified
            .to_rfc2822()
            .parse()
            .unwrap_or_else(|_| "unknown".parse().unwrap()),
    );

    headers.insert(
        "content-length",
        obj_data
            .data
            .len()
            .to_string()
            .parse()
            .unwrap_or_else(|_| "0".parse().unwrap()),
    );

    Ok((headers, obj_data.data).into_response())
}

pub async fn head_object(
    State(service): State<SharedService>,
    Path((bucket, key)): Path<(String, String)>,
) -> ServiceResult<Response> {
    let metadata = service.head_object(&bucket, &key).await?;

    let mut headers = HeaderMap::new();

    if let Some(ct) = metadata.content_type {
        if let Ok(header_value) = ct.parse() {
            headers.insert("content-type", header_value);
        }
    }

    headers.insert(
        "etag",
        metadata
            .etag
            .parse()
            .unwrap_or_else(|_| "unknown".parse().unwrap()),
    );

    headers.insert(
        "last-modified",
        metadata
            .last_modified
            .to_rfc2822()
            .parse()
            .unwrap_or_else(|_| "unknown".parse().unwrap()),
    );

    headers.insert(
        "content-length",
        metadata
            .size
            .to_string()
            .parse()
            .unwrap_or_else(|_| "0".parse().unwrap()),
    );

    Ok((StatusCode::OK, headers).into_response())
}

pub async fn delete_object(
    State(service): State<SharedService>,
    Path((bucket, key)): Path<(String, String)>,
) -> ServiceResult<StatusCode> {
    service.delete_object(&bucket, &key).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_objects(
    State(service): State<SharedService>,
    Path(bucket): Path<String>,
    Query(params): Query<ListObjectsQuery>,
) -> ServiceResult<Json<ListObjectsResponse>> {
    let objects = service
        .list_objects(&bucket, params.prefix.as_deref(), params.max_keys)
        .await?;

    let response = ListObjectsResponse {
        objects: objects.into_iter().map(|o| o.into()).collect(),
    };

    Ok(Json(response))
}
