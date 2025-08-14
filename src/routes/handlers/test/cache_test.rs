use axum::extract::Form;
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use axum::Router;
use axum::routing::{get, post};
use serde_json::{json, Map, Value as JsonValue};
use std::collections::HashMap;

use crate::common_libs::utils::security_headers::v1::add_headers;
use crate::state::APP_STATE;

pub fn routes() -> Router {
    Router::new()
        .route("/get", get(handle_cache_get))
        .route("/get_partitioned", get(handle_cache_get_partitioned))
        .route("/get_partitioned_json", get(handle_cache_get_partitioned_json))
        .route("/set", post(handle_cache_set))
        .route("/set_partitioned", post(handle_cache_set_partitioned))
        .route("/set_partitioned_json", post(handle_cache_set_partitioned_json))
        .route("/delete", post(handle_cache_delete))
        .route("/delete_partitioned", post(handle_cache_delete_partitioned))
}

pub async fn handle_cache_get(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.redis_client.get::<String>(
        payload.get("key_name").unwrap()
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

pub async fn handle_cache_get_partitioned(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.redis_client.get_partitioned::<String>(
        payload.get("key_name").unwrap()
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

pub async fn handle_cache_get_partitioned_json(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.redis_client.get_partitioned::<JsonValue>(
        payload.get("key_name").unwrap()
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

pub async fn handle_cache_set(
    Form(mut payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.redis_client.set(
        payload.remove("key_name").unwrap(),
        payload.remove("data").unwrap(),
        None
    ).await {
        Ok(_) => {
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "value": "Data set successfully"
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

pub async fn handle_cache_set_partitioned(
    Form(mut payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.redis_client.set_partitioned(
        payload.remove("key_name").unwrap(),
        payload.remove("data").unwrap(),
        None
    ).await {
        Ok(_) => {
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "value": "Data set successfully"
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

pub async fn handle_cache_set_partitioned_json(
    Json(mut payload): Json<Map<String, JsonValue>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.redis_client.set_partitioned(
        payload.remove("key_name").unwrap().as_str().unwrap().to_string(),
        payload.remove("data").unwrap(),
        None
    ).await {
        Ok(_) => {
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "value": "Data set successfully"
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

pub async fn handle_cache_delete(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.redis_client.delete(
        payload.get("key_name").unwrap()
    ).await {
        Ok(value) => {
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "value": format!("{} keys deleted", value)
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

pub async fn handle_cache_delete_partitioned(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.redis_client.delete_partitioned(
        payload.get("key_name").unwrap()
    ).await {
        Ok(value) => {
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "value": format!("{} partitioned keys deleted", value)
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