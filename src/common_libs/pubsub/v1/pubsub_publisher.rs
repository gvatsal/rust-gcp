use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

use crate::state::APP_STATE;
use super::models::StatRecord;

pub struct StatWithMetadata {
    pub dataset_id: String,
    pub table_id: String,
    pub stat: Box<dyn StatRecord>,
}

pub struct PubSubPublisher {
    tx: Mutex<mpsc::Sender<StatWithMetadata>>,
    rx: Mutex<mpsc::Receiver<StatWithMetadata>>,
    stat_buffers: Mutex<HashMap<String, Vec<StatWithMetadata>>>,
}

impl PubSubPublisher {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<StatWithMetadata>(1000);
        Self {
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
            stat_buffers: Mutex::new(HashMap::new()),
        }
    }

    pub async fn publish<T>(
        &self,
        dataset_id: String,
        table_id: String,
        stat: T,
    ) -> Result<(), String>
    where
        T: StatRecord + 'static,
    {
        let metadata = StatWithMetadata {
            dataset_id,
            table_id,
            stat: Box::new(stat),
        };
        let tx = self.tx.lock().await;
        match tx.send(metadata).await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Failed to send stat across message queue: {}", e)),
        }
    }

    pub fn start_stats_processing_tasks() {
        // Task for processing messages in batches based on count or size
        tokio::spawn(async move {
            Self::process_stats_in_sized_batches().await;
        });

        // Task for processing messages based on time
        tokio::spawn(async move {
            Self::process_stats_in_timed_batches().await;
        });
    }

    async fn process_stats_in_sized_batches() {
        let app_state = APP_STATE.get().unwrap();
        let mut rx = app_state.pubsub_publisher.rx.lock().await;
        while let Some(stat_with_metadata) = rx.recv().await {
            let topic_id = format!("{}_{}", &stat_with_metadata.dataset_id, &stat_with_metadata.table_id);

            let mut buffers = app_state.pubsub_publisher.stat_buffers.lock().await;
            let batch = buffers.entry(topic_id.clone()).or_insert_with(Vec::new);
            batch.push(stat_with_metadata);

            // Calculate total data bytes
            let total_data_bytes = batch.iter()
                .map(|item| item.stat.data_len())
                .sum::<usize>();

            if batch.len() >= app_state.config.pubsub.max_messages || total_data_bytes >= app_state.config.pubsub.max_bytes {
                let to_publish = std::mem::take(batch);
                let topic_clone = topic_id.clone();
                tokio::spawn(async move {
                    app_state.pubsub_client.publish_batch(&topic_clone, to_publish).await;
                });
            }
        }
    }

    async fn process_stats_in_timed_batches() {
        loop {
            let app_state: &std::sync::Arc<crate::state::AppState> = APP_STATE.get().unwrap();
            tokio::time::sleep(Duration::from_secs(app_state.config.pubsub.max_latency)).await;
            let mut buffers = app_state.pubsub_publisher.stat_buffers.lock().await;

            for (topic, batch) in buffers.iter_mut() {
                if batch.len() > 0 {
                    let to_publish = std::mem::take(batch);
                    let topic_clone = topic.clone();
                    tokio::spawn(async move {
                        app_state.pubsub_client.publish_batch(&topic_clone, to_publish).await;
                    });
                }
            }
        }
    }
}