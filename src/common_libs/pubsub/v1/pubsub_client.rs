use apache_avro::Schema;
use google_cloud_gax::conn::Channel;
use google_cloud_googleapis::pubsub::v1::{
    GetSchemaRequest,
    PubsubMessage,
    SchemaView,
    schema_service_client::SchemaServiceClient,
};
use google_cloud_pubsub::apiv1::conn_pool::ConnectionManager;
use google_cloud_pubsub::client::{Client, ClientConfig};
use tokio::sync::Mutex;
use tonic::Request;

use crate::state::APP_STATE;
use super::avro_parser;
use super::pubsub_constants::pubsub_topic;
use super::pubsub_publisher::StatWithMetadata;

pub struct PubSubClient {
    client: Client,
    schema_client: Mutex<SchemaServiceClient<Channel>>,
    avro_parser: avro_parser::AvroParser,
}

impl PubSubClient {
    pub async fn new() -> Self {
        let config = ClientConfig::default().with_auth().await.unwrap();

        let cm = ConnectionManager::new(
            config.pool_size.unwrap_or_default(),
            config.endpoint.as_str(),
            &config.environment,
            &config.connection_option,
        )
        .await.unwrap();

        let schema_client = Mutex::new(SchemaServiceClient::new(cm.conn()));

        let client = Client::new(config).await.unwrap();
        let avro_parser = avro_parser::AvroParser;

        Self { client, schema_client, avro_parser }
    }

    pub async fn publish_batch(
        &self,
        topic: &str,
        batch: Vec<StatWithMetadata>,
    ) {
        if batch.is_empty() {
            return;
        }

        let publisher = self.client.topic(topic).new_publisher(None);
        let mut avro_data: Vec<PubsubMessage> = Vec::new();
        let schema = match self.get_schema(topic).await {
            Ok(schema) => schema,
            Err(e) => {
                tracing::error!("Could not get schema - err: {:?}", &e);
                return;
            }
        };

        for stat_with_metadata in &batch {
            let stat = &stat_with_metadata.stat;

            match self.avro_parser.parse_and_encode(stat, &schema) {
                Ok(data) => {
                    let pubsub_message = PubsubMessage {
                        data: data.into(),
                        ..Default::default()
                    };
                    avro_data.push(pubsub_message)
                },
                Err(e) => {
                    tracing::error!("Could not parse to avro - err: {:?}", &e);
                }
            }
        }

        let awaiter = publisher.publish_bulk(avro_data).await;
        for result in awaiter {
            match result.get().await {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("Could not publish message - err: {:?}", e);
                    let first_item = batch.first().unwrap();
                    let _ = self.publish_err_message(
                        &first_item.dataset_id,
                        &first_item.table_id,
                        &e.to_string(),
                    ).await;
                }
            }
        }
    }

    async fn publish_err_message(
        &self,
        dataset_id: &str,
        table_id: &str,
        error_message: &str,
    ) -> Result<(), String> {
        let data = serde_json::json!({
            "dataset_id": dataset_id.to_string(),
            "bq_table_id": table_id.to_string(),
            "error_message": error_message.to_string(),
        }).to_string();

        let topic_id = pubsub_topic::GENERAL_ERROR_TOPIC;
        let publisher = self.client.topic(topic_id).new_publisher(None);
        let msg = PubsubMessage {
            data: data.into_bytes(),
            ..Default::default()
        };
        let awaiter = publisher.publish(msg).await;
        match awaiter.get().await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Failed to publish error message: {}", e))
        }
    }

    async fn fetch_schema_definition(
        &self,
        project_id: &str,
        schema_id: &str
    ) -> Result<String, String> {
        let schema_request = Request::new(GetSchemaRequest {
            name: format!("projects/{}/schemas/{}", project_id, schema_id),
            view: SchemaView::Full as i32,
        });

        let mut client = self.schema_client.lock().await;
        match client.get_schema(schema_request).await {
            Ok(resp) => return Ok(resp.into_inner().definition),
            Err(e) => return Err(format!("Failed to fetch schema for {}: {}", schema_id, e)),
        }
    }

    async fn get_schema(&self, schema_id: &str) -> Result<Schema, String> {
        let app_state = APP_STATE.get().unwrap();

        // Create cache key
        let cache_key = format!("{}_pubsub_schema", schema_id);

        // Try instance cache
        if let Some(schema_str) = app_state.instance_cache.get::<String>(&cache_key) {
            match Schema::parse_str(&schema_str) {
                Ok(schema) => return Ok(schema),
                Err(e) => tracing::error!("Failed to parse schema from instance cache - err: {}", e),
            }
        }

        // Try Redis cache
        if let Ok(Some(schema_str)) = app_state.redis_client.get::<String>(&cache_key).await {
            // Cache in instance cache
            app_state.instance_cache.set(&cache_key, schema_str.clone(), 86400);

            match Schema::parse_str(&schema_str) {
                Ok(schema) => return Ok(schema),
                Err(e) => tracing::error!("Failed to parse schema from Redis - err: {}", e),
            }
        }

        // Fetch directly from PubSub
        match self.fetch_schema_definition(
            &app_state.config.google_cloud_project,
            schema_id
        ).await {
            Ok(schema_definition) => {
                // Cache in Redis
                match app_state.redis_client.set(cache_key.clone(), schema_definition.clone(), Some(86400)).await {
                    Ok(_) => {},
                    Err(e) => tracing::error!("Failed to set schema in Redis - err: {}", e),
                }

                // Cache in instance cache
                app_state.instance_cache.set(&cache_key, schema_definition.clone(), 86400);

                match Schema::parse_str(&schema_definition) {
                    Ok(schema) => return Ok(schema),
                    Err(e) => return Err(format!("Failed to parse schema: {}", e)),
                }
            },
            Err(e) => return Err(format!("Failed to fetch schema: {}", e)),
        }
    }
}