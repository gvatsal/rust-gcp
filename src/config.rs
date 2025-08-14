use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PubSubConfig {
    pub max_messages: usize,
    pub max_bytes: usize,
    pub max_latency: u64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    pub env: String,
    pub log_level: String,
    pub google_cloud_project: String,
    pub gae_service: String,
    pub port: u16,
    pub redishost: String,
    pub redisport: u16,
    pub pubsub: PubSubConfig,
}

impl AppConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let env_name = env::var("ENV").unwrap_or_else(|_| "dev".into());

        // Build configuration from multiple sources with precedence
        let config = Config::builder()
            // Start with default settings
            .add_source(File::with_name("config/default").required(false))
            // Add environment-specific settings
            .add_source(File::with_name(&format!("config/{}", env_name)).required(false))
            // Add local overrides (not in version control)
            .add_source(File::with_name("config/local").required(false))
            // Add environment variables
            .add_source(Environment::default())
            .build()?;

        config.try_deserialize::<AppConfig>()
    }

    #[allow(dead_code)]
    pub fn is_live_env(&self) -> bool {
        self.env == "prod"
    }
}

// Default implementation
impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            env: "dev".into(),
            log_level: "rust_gcp=debug,tower_http::trace=debug".into(),
            google_cloud_project: "getcloudy-469014".into(),
            gae_service: "rust-gcp".into(),
            port: 3000,
            redishost: "10.207.177.140".into(),
            redisport: 6379,
            pubsub: PubSubConfig {
                max_messages: 10,
                max_bytes: 1024,
                max_latency: 5,
            },
        }
    }
}