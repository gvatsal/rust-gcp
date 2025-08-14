use once_cell::sync::OnceCell;
use std::sync::Arc;

use crate::common_libs::{
    cache_service::v1::{instance_cache::InstanceCache, redis_client::RedisClient},
    datastore::v1::datastore_client::DatastoreClient,
    gcs_storage::v1::gcs_client::GCSClient,
    pubsub::v1::{pubsub_client::PubSubClient, pubsub_publisher::PubSubPublisher},
    secret_manager::v1::secret_manager_client::SecretManagerClient,
};
use crate::config::AppConfig;

pub static APP_STATE: OnceCell<Arc<AppState>> = OnceCell::new();

pub struct AppState {
    pub config: AppConfig,
    pub pubsub_client: PubSubClient,
    pub pubsub_publisher: PubSubPublisher,
    pub redis_client: RedisClient,
    pub instance_cache: InstanceCache,
    pub datastore_client: DatastoreClient,
    pub gcs_client: GCSClient,
    pub secret_manager_client: SecretManagerClient,
}

impl AppState {
    pub async fn new(config: AppConfig) -> Self {
        // Initialize PubSub
        let pubsub_client = PubSubClient::new().await;

        // Initialize PubSubPublisher
        let pubsub_publisher = PubSubPublisher::new();

        // Initialize RedisClient
        let redis_client = RedisClient::new(
            &config.redishost,
            config.redisport
        );

        // Initialize InstanceCache
        let instance_cache = InstanceCache::new();

        // Initialize DatastoreClient
        let datastore_client = DatastoreClient::new(
            config.google_cloud_project.clone()
        ).await;

        // Initialize GCSClient
        let gcs_client = GCSClient::new().await;

        // Initialize SecretManagerClient
        let secret_manager_client = SecretManagerClient::new(
            config.google_cloud_project.clone()
        ).await;

        Self {
            config,
            pubsub_client,
            pubsub_publisher,
            redis_client,
            instance_cache,
            datastore_client,
            gcs_client,
            secret_manager_client,
        }
    }
}