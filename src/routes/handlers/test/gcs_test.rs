use axum::extract::Form;
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use axum::Router;
use axum::routing::get;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;

use crate::common_libs::utils::security_headers::v1::add_headers;
use crate::state::APP_STATE;

pub fn routes() -> Router {
    Router::new()
        .route("/get_bucket", get(handle_gcs_get_bucket))
        .route("/read_json", get(handle_gcs_read_json))
}

pub async fn handle_gcs_get_bucket(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.gcs_client.get_bucket(
        payload.get("bucket_name").unwrap()
    ).await {
        Ok(value) => {
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "value": value,
                }))
            )
        },
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "success": false,
                "error": e.to_string()
            }))
        ),
    };

    (response.0, security_headers, response.1).into_response()
}

pub async fn handle_gcs_read_json(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.gcs_client.read_json_from_gcs::<JsonValue>(
        payload.get("bucket_name").unwrap(),
        payload.get("source_path").unwrap()
    ).await {
        Ok(Some(value)) => {
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "value": value
                }))
            )
        },
        Ok(None) => {
            (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "success": false,
                    "error": "Key not found"
                }))
            )
        },
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "success": false,
                "error": e.to_string()
            }))
        ),
    };

    (response.0, security_headers, response.1).into_response()
}