mod common_libs;
mod config;
mod routes;
mod state;

use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing_stackdriver::{layer, CloudTraceConfiguration};
use tracing_subscriber::{EnvFilter, prelude::*};

use common_libs::pubsub::v1::pubsub_publisher::PubSubPublisher;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = config::AppConfig::new().unwrap_or_default();

    // Initialize tracing with Stackdriver and environment filter
    let filter_layer = EnvFilter::new(config.log_level.clone());
    let stackdriver_layer = layer().with_cloud_trace(CloudTraceConfiguration {
        project_id: config.google_cloud_project.clone(),
    });
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(stackdriver_layer)
        .init();

    tracing::info!("Starting Application");

    let listener = TcpListener::bind(format!("0.0.0.0:{}", config.port)).await.unwrap();

    let app_state = Arc::new(state::AppState::new(config).await);

    // Initialize global state
    let _ = state::APP_STATE.set(app_state.clone());

    // Start PubSubPublisher tasks
    PubSubPublisher::start_stats_processing_tasks();

    let app = routes::create_router(app_state);

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}