use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use serde::de::DeserializeOwned;

use crate::state::APP_STATE;

pub struct SecretManagerClient {
    client: SecretManagerService,
    project_id: String,
}

impl SecretManagerClient {
    pub async fn new(project_id: String) -> Self {
        let client = SecretManagerService::builder().build().await.unwrap();

        Self { client, project_id }
    }

    pub async fn get_secret_manager_data(
        &self,
        key: &str,
        version: &str,
        ttl: u64,
    ) -> Result<String, String> {
        let app_state = APP_STATE.get().unwrap();

        // Create cache key
        let cache_key = format!("secret_manager:{}:{}", version, key);

        // Try instance cache
        if let Some(secret_data) = app_state.instance_cache.get::<String>(&cache_key) {
            return Ok(secret_data.to_string());
        }

        // Try Redis cache
        if let Ok(Some(secret_data)) = app_state.redis_client.get::<String>(&cache_key).await {
            // Cache in instance cache
            app_state.instance_cache.set(&cache_key, secret_data.clone(), ttl);
            return Ok(secret_data);
        }

        let name = format!("projects/{}/secrets/{}/versions/{}", self.project_id, key, version);
        let payload = match self.client
            .access_secret_version()
            .set_name(name)
            .send()
            .await {
                Ok(resp) => {
                    match resp.payload {
                        Some(payload) => payload,
                        None => return Err("No payload found in response".to_string()),
                    }
                },
                Err(e) => return Err(format!("Failed to access secret version: {}", e)),
            };

        let data = match STANDARD.decode(payload.data) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(string) => string,
                Err(e) => return Err(format!("Failed to decode secret data: {}", e)),
            },
            Err(e) => return Err(format!("Failed to decode secret data: {}", e)),
        };

        // Cache in Redis
        match app_state.redis_client.set(cache_key.clone(), data.clone(), Some(86400)).await {
            Ok(_) => {},
            Err(e) => tracing::error!("Failed to set secret in Redis - err: {}", e),
        }

        // Cache in instance cache
        app_state.instance_cache.set(&cache_key, data.clone(), ttl);

        Ok(data)
    }

    #[allow(dead_code)]
    pub async fn get_secret_manager_data_json<T>(
        &self,
        key: &str,
        version: &str,
        ttl: u64,
    ) -> Result<T, String>
    where
        T: DeserializeOwned,
    {
        let raw_data = self.get_secret_manager_data(key, version, ttl).await?;
        match serde_json::from_str(&raw_data) {
            Ok(data) => Ok(data),
            Err(e) => Err(format!("Failed to deserialize secret data: {}", e)),
        }
    }
}