pub mod handlers;

use axum::body::Body;
use axum::http::{header::{HeaderValue, self}, Method, StatusCode};
use axum::response::IntoResponse;
use axum::Router;
use axum::routing::{any, get};
use std::sync::Arc;
use tokio_util::io::ReaderStream;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::Level;

use crate::common_libs::utils::{
    security_headers::v1::add_headers,
    structured_logging::v1::CustomMakeSpan,
};
use crate::state::AppState;

pub fn create_router(app_state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_methods([Method::POST, Method::GET, Method::OPTIONS])
        .allow_origin(Any)
        .allow_headers(Any);

    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(
            CustomMakeSpan::new()
                .level(Level::INFO)
                .include_headers(false),
        );

    Router::new()
        .nest("/home", handlers::general::routes())
        .nest("/app/rust_test", handlers::test::routes(app_state.clone()))
        .route("/robots.txt", get(|| async {
            let file = match tokio::fs::File::open("robots.txt").await {
                Ok(file) => file,
                Err(err) => return Err((StatusCode::NOT_FOUND, format!("File not found: {}", err))),
            };
            let stream = ReaderStream::new(file);
            let body = Body::from_stream(stream);
            let mut headers = add_headers();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain"),
            );
            Ok((headers, body))
        }))
        .layer(cors)
        .layer(trace_layer)
        .fallback_service(any(|| async {
            let security_headers = add_headers();
            (StatusCode::NOT_FOUND, security_headers).into_response()
        }))
}