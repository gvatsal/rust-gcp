use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use axum::response::IntoResponse;
use axum::Router;
use axum::routing::post;
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;

use crate::common_libs::{
    pubsub::v1::bigquery_constants::{bigquery_dataset, bigquery_table},
    pubsub::v1::models::test_stats::TestStats,
    utils::{
        request_parser::v1::RequestContext,
        security_headers::v1::add_headers
    },
};
use crate::make_stats;
use crate::state::AppState;

pub fn routes(app_state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", post(handle_test_stats))
        .with_state(app_state)
}

pub async fn handle_test_stats(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    mut request: RequestContext,
) -> impl IntoResponse {
    let security_headers = add_headers();

    if request.payload.get("event_type").is_none() {
        tracing::error!("Missing event_type in test_stats request - payload: {:?}", request.payload);
        return (
            StatusCode::BAD_REQUEST,
            security_headers,
            Json(json!({
                "success": false,
                "error": "Missing event_type"
            }))
        ).into_response();
    }

    let bigquery_data = make_stats!(TestStats {
        event_type: request.payload.remove("event_type").unwrap(),
        created_at: Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        app_pkg: request.payload.remove("app_pkg"),
        guid: request.payload.remove("guid"),
        country: request.payload.remove("country"),
        player_version: request.payload.remove("player_version"),
        oem: request.payload.remove("oem"),
        machine_id: request.payload.remove("machine_id"),
        version_machine_id: request.payload.remove("version_machine_id"),
        instance: request.payload.remove("instance"),
        image_name: request.payload.remove("android_image"),
        arg1: request.payload.remove("arg1"),
        arg2: request.payload.remove("arg2"),
        arg3: request.payload.remove("arg3"),
        arg4: request.payload.remove("arg4"),
        source: request.payload.remove("source"),
        count: request.payload.remove("count").and_then(|s| s.parse::<i32>().ok()),
        ad_refresh_rate: request.payload.remove("ad_refresh_rate").and_then(|s| s.parse::<i32>().ok()),
    });

    match state.pubsub_publisher.publish(
        bigquery_dataset::STATS.to_string(),
        bigquery_table::TEST_STATS.to_string(),
        bigquery_data,
    ).await {
        Ok(_) => {
            return (
                StatusCode::OK,
                security_headers,
                Json(json!({
                    "success": true,
                }))
            ).into_response();
        },
        Err(error) => {
            tracing::error!("Error publishing test_stats request - err: {:?}", &error);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                security_headers,
                Json(json!({
                    "success": false,
                    "error": format!("Could not publish the request, err: {:?}", &error)
                }))
            ).into_response();
        }
    }
}