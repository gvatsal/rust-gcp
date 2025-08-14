use deadpool_redis::{Config, Connection, Pool, redis::{AsyncCommands, self}, Runtime};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value as JsonValue;
use serde_pickle::{DeOptions, SerOptions};
use std::any::TypeId;
use std::collections::HashMap;
use std::mem::{forget, transmute_copy};

const MAX_REDIS_BYTE_SIZE: usize = 2000000; // 2 MB

pub struct RedisClient {
    pool: Pool,
}

impl RedisClient {
    pub fn new(host: &str, port: u16) -> Self {
        let cfg = Config::from_url(format!("redis://{}:{}/", host, port));
        let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

        Self { pool }
    }

    async fn get_connection(&self) -> Result<Connection, String> {
        match self.pool.get().await {
            Ok(conn) => Ok(conn),
            Err(err) => Err(format!("Failed to get redis connection: {}", err)),
        }
    }

    pub async fn get<RV>(
        &self,
        key: &str
    ) -> Result<Option<RV>, String>
    where
        RV: redis::FromRedisValue,
    {
        let mut conn = self.get_connection().await?;
        match conn.get(key).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to get redis value for key {}: {}", key, err)),
        }
    }

    pub async fn get_multi<RV>(
        &self,
        keys: &[&str]
    ) -> Result<Vec<Option<RV>>, String>
    where
        RV: redis::FromRedisValue,
    {
        let mut conn = self.get_connection().await?;
        match conn.mget(keys).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to get redis values for keys {:?}: {}", keys, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn get_replica<RV>(
        &self,
        key: &str,
        replica_count: u32
    ) -> Result<Option<RV>, String>
    where
        RV: redis::FromRedisValue,
    {
        let replica_key = format!("{}:{}", key, rand::random::<u32>() % replica_count);
        self.get(&replica_key).await
    }

    pub async fn get_partitioned<RV>(
        &self,
        key: &str,
    ) -> Result<Option<RV>, String>
    where
        RV: DeserializeOwned + 'static,
    {
        let meta_key = format!("{}:partitioned", key);

        let meta_info_raw = match self.get::<Vec<u8>>(&meta_key).await? {
            Some(data) => data,
            None => return Err(format!("Failed to get partition metadata")),
        };
        let meta_info = match serde_json::from_slice::<JsonValue>(&meta_info_raw) {
            Ok(value) => value,
            Err(e) => return Err(format!("Failed to parse partition metadata: {}", e)),
        };

        let partition_count = meta_info.get("partition_count")
            .and_then(|v| v.as_u64())
            .ok_or("Missing partition_count")?;
        let is_pickled = meta_info.get("is_pickled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let partition_keys = (0..partition_count)
            .map(|i| format!("{}:{}", &meta_key, i))
            .collect::<Vec<_>>();
        let partition_keys_str = partition_keys.iter().map(|s| s.as_str()).collect::<Vec<_>>();

        let partition_values = self.get_multi::<Vec<u8>>(partition_keys_str.as_slice()).await?;

        if partition_values.len() != partition_count as usize {
            return Err(format!("Mismatch in partition count: expected {}, got {}", partition_count, partition_values.len()));
        }

        let mut merged_partition_data = Vec::new();
        for part in partition_values.into_iter() {
            if part.is_none() {
                return Err(format!("Failed to get complete partitioned data"));
            }
            merged_partition_data.extend(part.unwrap());
        }

        if is_pickled {
            match serde_pickle::from_slice::<RV>(&merged_partition_data, DeOptions::default()) {
                Ok(value) => Ok(Some(value)),
                Err(e) => Err(format!("Failed to deserialize pickled partitioned data: {}", e)),
            }
        }
        else {
            // If RV = String, just UTF‑8 decode; otherwise try JSON
            // unpickled strings are set from python code
            if TypeId::of::<RV>() == TypeId::of::<String>() {
                match String::from_utf8(merged_partition_data) {
                    Ok(s) => {
                        let rv = unsafe { transmute_copy::<String, RV>(&s) };
                        forget(s);
                        Ok(Some(rv))
                    }
                    Err(e) => Err(format!("Failed to get UTF‑8 partitioned data: {}", e)),
                }
            }
            else {
                match serde_json::from_slice::<RV>(&merged_partition_data) {
                    Ok(value) => Ok(Some(value)),
                    Err(e) => Err(format!("Failed to deserialize json partitioned data: {}", e)),
                }
            }
        }
    }

    pub async fn set<V>(
        &self,
        key: String,
        value: V,
        expiry_seconds: Option<u64>
    ) -> Result<(), String>
    where
        V: redis::ToRedisArgs + Send + Sync + 'static,
    {
        let mut conn = self.get_connection().await?;
        match expiry_seconds {
            Some(expiry) => {
                match conn.set_ex::<_, _, String>(key.clone(), value, expiry).await {
                    Ok(_) => Ok(()),
                    Err(err) => Err(format!("Failed to set redis value for key {}: {}", key, err)),
                }
            },
            None => {
                match conn.set::<_, _, String>(key.clone(), value).await {
                    Ok(_) => Ok(()),
                    Err(err) => Err(format!("Failed to set redis value for key {}: {}", key, err)),
                }
            }
        }
    }

    pub async fn set_multi<V>(
        &self,
        key_values: Vec<(String, V)>,
        expiry_seconds: Option<u64>
    ) -> Result<(), String>
    where
        V: redis::ToRedisArgs + Send + Sync + 'static,
    {
        let mut conn = self.get_connection().await?;
        match conn.mset::<_, _, String>(&key_values).await {
            Ok(_) => {
                if let Some(expiry) = expiry_seconds {
                    for (key, _) in &key_values {
                        self.expire(key, expiry as i64).await?;
                    }
                }
                Ok(())
            },
            Err(err) => Err(format!("Failed to set redis value for keys {:?}: {}",
                key_values.iter().map(|(k, _)| k).collect::<Vec<_>>(),
                err
            )),
        }
    }

    #[allow(dead_code)]
    pub async fn set_replica<V>(
        &self,
        key: String,
        data: V,
        replica_count: u32,
        expiry_seconds: Option<u64>
    ) -> Result<(), String>
    where
        V: redis::ToRedisArgs + Send + Sync + Clone + 'static,
    {
        let key_values = (0..replica_count).map(|i| (format!("{}:{}", key, i), data.clone())).collect::<Vec<_>>();
        self.set_multi(key_values, expiry_seconds).await
    }

    pub async fn set_partitioned<V>(
        &self,
        key: String,
        data: V,
        expiry_seconds: Option<u64>,
    ) -> Result<(), String>
    where
        V: Serialize,
    {
        let meta_key = format!("{}:partitioned", key);

        let bytes = match serde_pickle::to_vec(&data, SerOptions::default()) {
            Ok(bytes) => bytes,
            Err(e) => return Err(format!("Failed to serialize data: {}", e)),
        };

        let partition_count = (bytes.len() + MAX_REDIS_BYTE_SIZE - 1) / MAX_REDIS_BYTE_SIZE;
        let mut partition_values: Vec<(String, Vec<u8>)> = Vec::with_capacity(partition_count + 1);

        for i in 0..partition_count {
            let start = i * MAX_REDIS_BYTE_SIZE;
            let end = ((i + 1) * MAX_REDIS_BYTE_SIZE).min(bytes.len());
            partition_values.push((format!("{}:{}", meta_key, i), bytes[start..end].to_vec()));
        }

        let meta_info = serde_json::json!({ "partition_count": partition_count, "is_pickled": true });
        partition_values.push((meta_key, serde_json::to_vec(&meta_info).unwrap()));

        self.set_multi(partition_values, expiry_seconds).await
    }

    pub async fn delete(
        &self,
        key: &str
    ) -> Result<u64, String> {
        let mut conn = self.get_connection().await?;
        match conn.del(key).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to delete redis key {:?}: {}", key, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn delete_multi(
        &self,
        keys: &[&str]
    ) -> Result<u64, String> {
        let mut conn = self.get_connection().await?;
        match conn.del(keys).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to delete redis keys {:?}: {}", keys, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn delete_replica(
        &self,
        key: &str,
    ) -> Result<u64, String> {
        let mut i = 0;
        loop {
            let replica_key = format!("{}:{}", key, i);
            match self.delete(&replica_key).await {
                Ok(1) => i += 1,
                Ok(_) => break,
                Err(e) => return Err(e)
            }
        }
        Ok(i)
    }

    #[allow(dead_code)]
    pub async fn delete_partitioned(
        &self,
        key: &str,
    ) -> Result<u64, String> {
        let key = format!("{}:partitioned", key);
        self.delete(&key).await?;
        let mut i = 0;
        loop {
            let keys_pattern = format!("{}:{}", key, i);
            match self.delete(&keys_pattern).await {
                Ok(1) => i += 1,
                Ok(_) => break,
                Err(e) => return Err(e)
            }
        }
        Ok(i + 1) // +1 for the metadata key
    }

    #[allow(dead_code)]
    pub async fn ttl(
        &self,
        key: &str
    ) -> Result<i64, String> {
        let mut conn = self.get_connection().await?;
        match conn.ttl(key).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to get ttl for key {}: {}", key, err)),
        }
    }

    pub async fn expire(
        &self,
        key: &str,
        secs: i64
    ) -> Result<bool, String> {
        let mut conn = self.get_connection().await?;
        match conn.expire(key, secs).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to set expire for key {}: {}", key, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn incr_by(
        &self,
        key: &str,
        by: isize
    ) -> Result<i64, String> {
        let mut conn = self.get_connection().await?;
        match conn.incr(key, by).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to incr_by for key {}: {}", key, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn decr_by(
        &self,
        key: &str,
        by: isize
    ) -> Result<i64, String> {
        let mut conn = self.get_connection().await?;
        match conn.decr(key, by).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to decr_by for key {}: {}", key, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn hmset<K, V>(
        &self,
        key: &str,
        items: &[(K, V)]
    ) -> Result<(), String>
    where
        K: redis::ToRedisArgs + Send + Sync + 'static,
        V: redis::ToRedisArgs + Send + Sync + 'static,
    {
        let mut conn = self.get_connection().await?;
        match conn.hset_multiple::<_, _, _, String>(key, items).await {
            Ok(_) => Ok(()),
            Err(err) => Err(format!("Failed to hmset for key {}: {}", key, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn hincr_by(
        &self,
        key: &str,
        field: &str,
        by: isize
    ) -> Result<i64, String> {
        let mut conn = self.get_connection().await?;
        match conn.hincr(key, field, by).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to hincr_by for key {}.{}: {}", key, field, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn hgetall<H>(
        &self,
        key: &str
    ) -> Result<HashMap<String, H>, String>
    where
        H: redis::FromRedisValue,
    {
        let mut conn = self.get_connection().await?;
        match conn.hgetall(key).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to hgetall for key {}: {}", key, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn sadd<V>(
        &self,
        set: &str,
        vals: &[V]
    ) -> Result<i64, String>
    where
        V: redis::ToRedisArgs + Send + Sync + 'static,
    {
        let mut conn = self.get_connection().await?;
        match conn.sadd(set, vals).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to sadd for set {}: {}", set, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn sismember<V>(
        &self,
        set: &str,
        val: V
    ) -> Result<bool, String>
    where
        V: redis::ToRedisArgs + Send + Sync + 'static,
    {
        let mut conn = self.get_connection().await?;
        match conn.sismember(set, val).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to determine sismember for set {}: {}", set, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn smembers<T>(
        &self,
        set: &str
    ) -> Result<Vec<T>, String>
    where
        T: redis::FromRedisValue,
    {
        let mut conn = self.get_connection().await?;
        match conn.smembers(set).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to smembers for set {}: {}", set, err)),
        }
    }

    #[allow(dead_code)]
    pub async fn srem<V>(
        &self,
        set: &str,
        vals: &[V]
    ) -> Result<i64, String>
    where
        V: redis::ToRedisArgs + Send + Sync + 'static,
    {
        let mut conn = self.get_connection().await?;
        match conn.srem(set, vals).await {
            Ok(value) => Ok(value),
            Err(err) => Err(format!("Failed to srem from set {}: {}", set, err)),
        }
    }

}