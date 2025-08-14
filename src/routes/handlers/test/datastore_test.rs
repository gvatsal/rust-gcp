use axum::extract::Form;
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use axum::Router;
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value as JsonValue};
use std::collections::HashMap;

use crate::common_libs::{
    datastore::v1::{datastore_wrapper::DatastoreModel, models::test_data::TestData},
    utils::security_headers::v1::add_headers,
};

pub fn routes() -> Router {
    Router::new()
        .route("/get", get(handle_datastore_get))
        .route("/get_by_id", get(handle_datastore_get_by_id))
        .route("/multi_get", post(handle_datastore_multi_get))
        .route("/put", post(handle_datastore_put))
        .route("/multi_put", post(handle_datastore_multi_put))
        .route("/delete", post(handle_datastore_delete))
        .route("/multi_delete", post(handle_datastore_multi_delete))
}

pub async fn handle_datastore_get(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let security_headers = add_headers();

    let response = match TestData::get(
        payload.get("gift_code").unwrap()
    ).await {
        Ok(Some(gift_card)) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "gift_card": gift_card
            }))
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "success": false,
                "error": "Gift card not found"
            }))
        ),
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

pub async fn handle_datastore_get_by_id(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let security_headers = add_headers();

    let response = match TestData::get_by_id(
        payload.get("key_id").and_then(|v| v.parse::<i64>().ok()).unwrap()
    ).await {
        Ok(Some(gift_card)) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "gift_card": gift_card
            }))
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "success": false,
                "error": "Gift card not found"
            }))
        ),
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

pub async fn handle_datastore_multi_get(
    Json(payload): Json<Map<String, JsonValue>>,
) -> impl IntoResponse {
    let security_headers = add_headers();

    let gift_codes = payload.get("gift_codes")
        .and_then(|v| v.as_array())
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect::<Vec<_>>();
    let response = match TestData::multi_get(&gift_codes).await {
        Ok(Some(gift_cards)) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "gift_cards": gift_cards
            }))
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "success": false,
                "error": "No gift cards found"
            }))
        ),
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

pub async fn handle_datastore_put(
    Form(mut payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let security_headers = add_headers();

    let mut gift_card = TestData {
        key_name: payload.get("gift_code").cloned(),
        gc: payload.get("gift_code").cloned(),
        amt: payload.get("gift_code_amount").and_then(|v| v.parse::<f64>().ok()),
        coups_allw: payload.get("coupons_allowed").and_then(|v| v.parse::<i64>().ok()),
        coups_clmd: Some(0),
        rule_id: payload.remove("ruleset_id"),
        valid_from: payload.get("valid_from").and_then(|v| v.parse::<DateTime<Utc>>().ok()),
        valid_upto: payload.get("valid_upto").and_then(|v| v.parse::<DateTime<Utc>>().ok()),
        created_by: payload.remove("created_by"),
        desc: payload.remove("desc"),
        created_at: Some(Utc::now()),
        modified_at: Some(Utc::now()),
    };

    match gift_card.put().await {
        Ok(_) => {
            tracing::info!("Card saved successfully");
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                security_headers,
                Json(json!({
                    "success": false,
                    "error": e.to_string()
                })),
            ).into_response();
        },
    }

    let response = match TestData::get(
        &payload.get("gift_code").unwrap()
    ).await {
        Ok(Some(gift_card)) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "gift_card": gift_card
            }))
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "success": false,
                "error": "Gift card not found"
            }))
        ),
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

pub async fn handle_datastore_multi_put(
    Json(payload): Json<Map<String, JsonValue>>,
) -> impl IntoResponse {
    let security_headers = add_headers();

    let mut gift_cards = Vec::new();
    let gift_codes = payload.get("gift_codes")
        .and_then(|v| v.as_array())
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect::<Vec<_>>();
    for gift_code in &gift_codes {
        let gift_card = TestData {
            key_name: Some(gift_code.to_string()),
            gc: Some(gift_code.to_string()),
            amt: payload.get("gift_code_amount").and_then(|v| v.as_f64()),
            coups_allw: payload.get("coupons_allowed").and_then(|v| v.as_i64()),
            coups_clmd: Some(0),
            rule_id: payload.get("ruleset_id").and_then(|v| v.as_str()).map(|v| v.to_string()),
            valid_from: payload.get("valid_from").and_then(|v| v.as_str()).and_then(|v| v.parse::<DateTime<Utc>>().ok()),
            valid_upto: payload.get("valid_upto").and_then(|v| v.as_str()).and_then(|v| v.parse::<DateTime<Utc>>().ok()),
            created_by: payload.get("created_by").and_then(|v| v.as_str()).map(|v| v.to_string()),
            desc: payload.get("desc").and_then(|v| v.as_str()).map(|v| v.to_string()),
            created_at: Some(Utc::now()),
            modified_at: Some(Utc::now()),
        };
        gift_cards.push(gift_card);
    }

    let mut gift_cards = gift_cards.iter_mut().map(|card| card).collect::<Vec<_>>();
    match TestData::multi_put(&mut gift_cards).await {
        Ok(_) => {
            tracing::info!("Cards saved successfully");
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                security_headers,
                Json(json!({
                    "success": false,
                    "error": e.to_string()
                })),
            ).into_response();
        },
    }

    let response = match TestData::multi_get(&gift_codes).await {
        Ok(Some(gift_cards)) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "gift_cards": gift_cards
            }))
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "success": false,
                "error": "No gift cards found"
            }))
        ),
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

pub async fn handle_datastore_delete(
    Form(payload): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let security_headers = add_headers();

    let response = match TestData::get(
        payload.get("gift_code").unwrap()
    ).await {
        Ok(Some(gift_card)) => {
            tracing::info!("Deleting gift card: {:?}", gift_card);
            match gift_card.delete().await {
                Ok(_) => (
                    StatusCode::OK,
                    Json(json!({
                        "success": true,
                        "message": "Gift card deleted successfully"
                    }))
                ),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "success": false,
                        "error": e.to_string()
                    }))
                ),
            }
        },
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "success": false,
                "error": "Gift card not found"
            }))
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "success": false,
                "error": e.to_string()
            })),
        ),
    };

    (response.0, security_headers, response.1).into_response()
}

pub async fn handle_datastore_multi_delete(
    Json(payload): Json<Map<String, JsonValue>>,
) -> impl IntoResponse {
    let security_headers = add_headers();

    let gift_codes = payload.get("gift_codes")
        .and_then(|v| v.as_array())
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect::<Vec<_>>();
    let response = match TestData::multi_get(&gift_codes).await {
        Ok(Some(gift_cards)) => {
            tracing::info!("Deleting gift cards: {:?}", gift_cards);
            let gift_cards = gift_cards.iter().map(|card| card).collect::<Vec<_>>();
            match TestData::multi_delete(&gift_cards).await {
                Ok(_) => (
                    StatusCode::OK,
                    Json(json!({
                        "success": true,
                        "message": "Gift cards deleted successfully"
                    }))
                ),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "success": false,
                        "error": e.to_string()
                    }))
                ),
            }
        },
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "success": false,
                "error": "No gift cards found"
            }))
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "success": false,
                "error": e.to_string()
            })),
        ),
    };

    (response.0, security_headers, response.1).into_response()
}