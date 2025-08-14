use google_datastore1::api::{BeginTransactionRequest, CommitRequest, Entity, Key, LookupRequest, Mutation, PartitionId, PathElement};
use google_datastore1::Datastore;
use google_datastore1::hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use google_datastore1::hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use google_datastore1::yup_oauth2::{
    ApplicationDefaultCredentialsAuthenticator,
    ApplicationDefaultCredentialsFlowOpts,
    authenticator::ApplicationDefaultCredentialsTypes,
};

use super::datastore_wrapper::DatastoreModel;
use super::utils;

pub struct DatastoreClient {
    hub: Datastore<HttpsConnector<HttpConnector>>,
    project_id: String,
}

impl DatastoreClient {
    pub async fn new(project_id: String) -> Self {
        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .unwrap()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(https);

        let adc_opts = ApplicationDefaultCredentialsFlowOpts::default();
        let adc_type = ApplicationDefaultCredentialsAuthenticator::builder(adc_opts).await;
        let auth = match adc_type {
            ApplicationDefaultCredentialsTypes::InstanceMetadata(builder) => {
                builder.build().await.unwrap()
            }
            ApplicationDefaultCredentialsTypes::ServiceAccount(builder) => {
                tracing::warn!("This code is unreachable, as cloud run should use the metadata server, not a json key");
                builder.build().await.unwrap()
            }
        };

        let hub = Datastore::new(client, auth);

        Self { hub, project_id }
    }

    fn create_key(&self, kind: &str, database_id: Option<String>, key_name: Option<String>, key_id: Option<i64>) -> Key {
        let path_element = PathElement {
            kind: Some(kind.to_string()),
            id: key_id,
            name: key_name,
        };

        Key {
            partition_id: Some(PartitionId {
                project_id: Some(self.project_id.clone()),
                namespace_id: None, // Default namespace
                database_id: database_id,
            }),
            path: Some(vec![path_element]),
        }
    }

    async fn lookup_entities(&self, database_id: Option<String>, keys: Vec<Key>) -> Result<Vec<Entity>, String> {
        let req = LookupRequest {
            database_id: database_id,
            keys: Some(keys),
            property_mask: None,
            read_options: None,
        };

        let (_, response) = match self.hub.projects()
           .lookup(req, &self.project_id)
           .doit()
           .await
        {
            Ok(response) => response,
            Err(e) => {
                tracing::error!("Datastore lookup failed - err: {:?}", e);
                return Err(format!("Datastore lookup failed: {:?}", e));
            }
        };

        let entities = response.found.unwrap_or_default().into_iter()
            .filter_map(|result| result.entity)
            .collect::<Vec<_>>();

        Ok(entities)
    }

    async fn begin_db_transaction(&self, database_id: Option<String>) -> Result<Vec<u8>, String> {
        // Begin a transaction
        let (_, begin_resp) = match self.hub.projects()
            .begin_transaction(
                BeginTransactionRequest {
                    transaction_options: None,
                    database_id: database_id
                },
                &self.project_id
            )
            .doit()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                tracing::error!("Datastore begin transaction failed - err: {:?}", e);
                return Err(format!("Datastore begin transaction failed: {:?}", e));
            }
        };

        // Extract the transaction ID
        match begin_resp.transaction {
            Some(tx_id) => Ok(tx_id),
            None => Err(format!("Begin transaction response did not contain a transaction ID")),
        }
    }

    async fn commit_entity(&self, database_id: Option<String>, mutations: Vec<Mutation>) -> Result<(), String> {
        let tx_id = self.begin_db_transaction(database_id.clone()).await?;
        let mutations_len = mutations.len();
        let req = CommitRequest {
            mode: Some("TRANSACTIONAL".to_string()),
            mutations: Some(mutations),
            transaction: Some(tx_id),
            single_use_transaction: None,
            database_id: database_id,
        };

        let (_, response) = match self.hub.projects()
           .commit(req, &self.project_id)
           .doit()
           .await
        {
            Ok(response) => response,
            Err(e) => {
                tracing::error!("Datastore commit failed - err: {:?}", e);
                return Err(format!("Datastore commit failed: {:?}", e));
            }
        };

        let results = response.mutation_results.as_ref().map(|v| v.len()).unwrap_or(0);
        if results != mutations_len {
            tracing::error!("Commit entity failed - result: {}, expected: {}", results, mutations_len);
            return Err(format!("Commit entity returned {} results but expected {}", results, mutations_len));
        }

        Ok(())
    }

    async fn commit_delete(&self, database_id: Option<String>, mutations: Vec<Mutation>) -> Result<(), String> {
        let tx_id = self.begin_db_transaction(database_id.clone()).await?;
        let mutations_len = mutations.len();
        let req = CommitRequest {
            mode: Some("TRANSACTIONAL".to_string()),
            mutations: Some(mutations),
            transaction: Some(tx_id),
            single_use_transaction: None,
            database_id: database_id,
        };

        let (_, response) = match self.hub.projects()
           .commit(req, &self.project_id)
           .doit()
           .await
        {
            Ok(response) => response,
            Err(e) => {
                tracing::error!("Datastore delete commit failed - err: {:?}", e);
                return Err(format!("Datastore delete commit failed: {:?}", e));
            }
        };

        let results = response.mutation_results.as_ref().map(|v| v.len()).unwrap_or(0);
        if results != mutations_len {
            tracing::error!("Commit delete failed - result: {}, expected: {}", results, mutations_len);
            return Err(format!("Commit delete returned {} results but expected {}", results, mutations_len));
        }

        Ok(())
    }

    pub async fn get<T>(&self, key_name: &str) -> Result<Option<T>, String>
    where
        T: DatastoreModel,
    {
        let kind = utils::infer_kind::<T>();
        let entity_key = self.create_key(&kind, T::database_id(), Some(key_name.to_string()), None);
        let entities = self.lookup_entities(T::database_id(), vec![entity_key]).await?;

        // Process the first result (if any)
        if let Some(entity) = entities.into_iter().next() {
            let data: T = utils::entity_to_struct::<T>(entity)?;
            Ok(Some(data))
        }
        else {
            Ok(None) // No entity found
        }
    }

    pub async fn get_by_id<T>(&self, key_id: i64) -> Result<Option<T>, String>
    where
        T: DatastoreModel,
    {
        let kind = utils::infer_kind::<T>();
        let entity_key = self.create_key(&kind, T::database_id(), None, Some(key_id));
        let entities = self.lookup_entities(T::database_id(), vec![entity_key]).await?;

        // Process the first result (if any)
        if let Some(entity) = entities.into_iter().next() {
            let data: T = utils::entity_to_struct::<T>(entity)?;
            Ok(Some(data))
        }
        else {
            Ok(None) // No entity found
        }
    }

    pub async fn multi_get<T>(&self, key_names: &[&str]) -> Result<Option<Vec<T>>, String>
    where
        T: DatastoreModel,
    {
        let kind = utils::infer_kind::<T>();
        let entity_keys = key_names.iter()
            .map(|&key_name| self.create_key(&kind, T::database_id(), Some(key_name.to_string()), None))
            .collect::<Vec<_>>();
        let entities = self.lookup_entities(T::database_id(), entity_keys).await?;

        let mut results = Vec::new();
        for entity in entities {
            let data: T = utils::entity_to_struct::<T>(entity)?;
            results.push(data);
        }

        if results.is_empty() {
            Ok(None) // No entity found
        }
        else {
            Ok(Some(results)) // Return all found entities
        }
    }

    pub async fn put<T>(&self, data: &T) -> Result<(), String>
    where
        T: DatastoreModel,
    {
        let kind = utils::infer_kind::<T>();
        let entity_key = self.create_key(&kind, T::database_id(), data.primary_key(), None);
        let entity = utils::struct_to_entity(entity_key, data)?;
        let mutation = Mutation { upsert: Some(entity), ..Default::default() };
        self.commit_entity(T::database_id(), vec![mutation]).await
    }

    pub async fn multi_put<T>(&self, data_list: &[&mut T]) -> Result<(), String>
    where
        T: DatastoreModel,
    {
        let kind = utils::infer_kind::<T>();
        let mut entities = Vec::new();
        for data in data_list {
            let entity_key = self.create_key(&kind, T::database_id(), data.primary_key(), None);
            let entity = utils::struct_to_entity(entity_key, *data)?;
            entities.push(entity);
        }

        let mutations = entities.into_iter().map(|entity| {
            Mutation { upsert: Some(entity), ..Default::default() }
        }).collect::<Vec<_>>();

        self.commit_entity(T::database_id(), mutations).await
    }

    pub async fn delete<T>(&self, data: &T) -> Result<(), String>
    where
        T: DatastoreModel,
    {
        let kind = utils::infer_kind::<T>();
        let key = self.create_key(&kind, T::database_id(), data.primary_key(), None);
        let mutation = Mutation { delete: Some(key), ..Default::default() };
        self.commit_delete(T::database_id(), vec![mutation]).await
    }

    pub async fn multi_delete<T>(&self, data_list: &[&T]) -> Result<(), String>
    where
        T: DatastoreModel,
    {
        let kind = utils::infer_kind::<T>();
        let entities = data_list.iter()
            .map(|data| self.create_key(&kind, T::database_id(), data.primary_key(), None))
            .collect::<Vec<_>>();

        let mutations = entities.into_iter().map(|key| {
            Mutation { delete: Some(key), ..Default::default() }
        }).collect::<Vec<_>>();

        self.commit_delete(T::database_id(), mutations).await
    }
}