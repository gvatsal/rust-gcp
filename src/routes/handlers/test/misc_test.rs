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
        .route("/", get(dev_handler).post(dev_handler))
}

pub async fn dev_handler(
    _headers: HeaderMap,
    _request: RequestContext,
) -> impl IntoResponse {
    let security_headers = add_headers();

    return (
        StatusCode::OK,
        security_headers,
        Json(json!({
            "success": true,
            "description": "This handler is limited to DEV environment!",
        }))
    ).into_response();
}