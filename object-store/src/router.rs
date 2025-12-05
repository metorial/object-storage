use axum::routing::{delete, get, head, post, put};
use axum::Router;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::api::*;
use crate::service::ObjectStoreService;

pub fn create_router(service: Arc<ObjectStoreService>) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/buckets", post(create_bucket))
        .route("/buckets", get(list_buckets))
        .route("/buckets/:bucket", delete(delete_bucket))
        .route("/buckets/:bucket/objects/*key", put(put_object))
        .route("/buckets/:bucket/objects/*key", get(get_object))
        .route("/buckets/:bucket/objects/*key", head(head_object))
        .route("/buckets/:bucket/objects/*key", delete(delete_object))
        .route("/buckets/:bucket/objects", get(list_objects))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive())
                .layer(TimeoutLayer::new(Duration::from_secs(60))),
        )
        .with_state(service)
}
