use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use axum::response::IntoResponse;
use axum::Router;
use axum::routing::get;
use serde_json::json;

use crate::common_libs::utils::{
    request_parser::v1::RequestContext,
    security_headers::v1::add_headers
};

pub fn routes() -> Router {
    Router::new()
        .route("/", get(general_handler).post(general_handler))
}

pub async fn general_handler(
    _headers: HeaderMap,
    _request: RequestContext,
) -> impl IntoResponse {
    tracing::info!("Welcome to the Rust gcp app!");
    let security_headers = add_headers();

    return (
        StatusCode::OK,
        security_headers,
        Json(json!({
            "success": true,
            "description": "Welcome to the Rust gcp app!",
        }))
    ).into_response();
}