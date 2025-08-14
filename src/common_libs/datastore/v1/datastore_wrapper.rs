use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

use crate::common_libs::datastore::v1::datastore_client::DatastoreClient;
use crate::state::APP_STATE;

#[async_trait]
pub trait DatastoreModel: Serialize + DeserializeOwned + Sized + Send + Sync + Debug {
    /// Return this entity’s key name/ID.
    fn primary_key(&self) -> Option<String>;

    /// Return the database ID.
    /// None is used for the default database.
    fn database_id() -> Option<String> {
        None
    }

    /// Get a static reference to your DatastoreClient.
    fn datastore_client() -> &'static DatastoreClient {
        let app_state = APP_STATE.get().unwrap();
        &app_state.datastore_client
    }

    /// Get the entity by key name.
    async fn get(key_name: &str) -> Result<Option<Self>, String> {
        Self::datastore_client().get::<Self>(key_name).await
    }

    /// Get the entity by key id.
    #[allow(dead_code)]
    async fn get_by_id(key_id: i64) -> Result<Option<Self>, String> {
        Self::datastore_client().get_by_id::<Self>(key_id).await
    }

    /// Get multiple entities by key names.
    async fn multi_get(key_names: &[&str]) -> Result<Option<Vec<Self>>, String> {
        Self::datastore_client().multi_get::<Self>(key_names).await
    }

    /// Put the entity into Datastore.
    async fn put(&mut self) -> Result<(), String> {
        self.auto_update_fields();
        self.validate()?;
        Self::datastore_client().put(self).await
    }

    /// Put multiple entities into Datastore.
    #[allow(dead_code)]
    async fn multi_put(data_list: &mut [&mut Self]) -> Result<(), String> {
        for data in data_list.iter_mut() {
            data.auto_update_fields();
            data.validate()?;
        }
        Self::datastore_client().multi_put(data_list).await
    }

    /// Delete the entity from Datastore.
    #[allow(dead_code)]
    async fn delete(&self) -> Result<(), String> {
        Self::datastore_client().delete(self).await
    }

    /// Delete multiple entities from Datastore.
    #[allow(dead_code)]
    async fn multi_delete(data_list: &[&Self]) -> Result<(), String> {
        Self::datastore_client().multi_delete(data_list).await
    }

    /// List of property names to exclude from Datastore indexes.
    fn excluded_from_indexes() -> &'static [&'static str] {
        &[]
    }

    /// Update fields automatically before put
    fn auto_update_fields(&mut self) {
    }

    /// Hook for any validation prior to save.
    fn validate(&self) -> Result<(), String> {
        Ok(())
    }
}