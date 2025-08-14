use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::{
    buckets::{Bucket, get::GetBucketRequest},
    objects::{
        download::Range,
        get::GetObjectRequest,
        Object,
        upload::{UploadObjectRequest, UploadType},
    },
};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct GCSClient {
    client: Client,
}

impl GCSClient {
    pub async fn new() -> Self {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let client = Client::new(config);

        GCSClient { client }
    }

    #[allow(dead_code)]
    pub async fn get_bucket(
        &self,
        bucket_name: &str
    ) -> Result<Bucket, String> {
        let req = GetBucketRequest {
            bucket: bucket_name.to_string(),
            ..Default::default()
        };

        match self.client.get_bucket(&req).await {
            Ok(bucket) => Ok(bucket),
            Err(e) => return Err(format!("Failed to get bucket: {}", e)),
        }
    }

    pub async fn read_json_from_gcs<T>(
        &self,
        bucket_name: &str,
        source_path: &str
    ) -> Result<Option<T>, String>
    where
        T: DeserializeOwned,
    {
        let request = GetObjectRequest {
            bucket: bucket_name.to_string(),
            object: source_path.to_string(),
            ..Default::default()
        };

        match self.client.download_object(&request, &Range::default()).await {
            Ok(data) => {
                match serde_json::from_slice::<T>(&data) {
                    Ok(value) => Ok(Some(value)),
                    Err(e) => Err(format!("Failed to deserialize bucket data: {}", e)),
                }
            }
            Err(e) => Err(format!("Failed to download object {}/{}: {}", bucket_name, source_path, e)),
        }
    }

    #[allow(dead_code)]
    pub async fn upload_json_to_gcs<T>(
        &self,
        bucket_name: String,
        destination_path: String,
        data: T,
    ) -> Result<(), String>
    where
        T: Serialize,
    {
        let request = UploadObjectRequest {
            bucket: bucket_name.to_string(),
            ..Default::default()
        };

        let upload_type = UploadType::Multipart(Box::new(Object {
            name: destination_path.clone(),
            content_type: Some("application/json".to_string()),
            cache_control: Some("no-store".to_string()),
            ..Default::default()
        }));

        let bytes = match serde_json::to_vec(&data) {
            Ok(bytes) => bytes,
            Err(e) => return Err(format!("Failed to serialize data: {}", e)),
        };

        match self.client.upload_object(&request, bytes, &upload_type).await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Failed to upload object to {}/{}: {}", bucket_name, destination_path, e)),
        }
    }
}