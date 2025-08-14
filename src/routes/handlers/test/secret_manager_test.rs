use axum::extract::Form;
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use axum::Router;
use axum::routing::get;
use serde_json::json;
use std::collections::HashMap;

use crate::common_libs::utils::security_headers::v1::add_headers;
use crate::state::APP_STATE;

pub fn routes() -> Router{
    Router::new()
    .route("/get_secret", get(handle_secret_manager_get_secret))
}

pub async fn handle_secret_manager_get_secret(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_state = APP_STATE.get().unwrap();
    let security_headers = add_headers();

    let response = match app_state.secret_manager_client.get_secret_manager_data(
        payload.get("secret_key").unwrap(),
        payload.get("secret_version").unwrap_or(&"latest".to_string()),
        payload.get("ttl").and_then(|ttl| ttl.parse::<u64>().ok()).unwrap_or(300),
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