use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

#[derive(Clone)]
struct CachedItem {
    data: Arc<dyn Any + Send + Sync>,
    expires_at: SystemTime,
}

pub struct InstanceCache {
    cache: Mutex<HashMap<String, CachedItem>>,
}

impl InstanceCache {
    pub fn new() -> Self {
        InstanceCache {
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn get_raw(&self, key: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        let mut cache = self.cache.lock().unwrap();
        match cache.get(key) {
            Some(cached_item) => {
                if SystemTime::now() >= cached_item.expires_at {
                    cache.remove(key);
                    return None;
                }
                return Some(cached_item.data.clone());
            }
            None => return None,
        }
    }

    pub fn get<T>(&self, key: &str) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static
    {
        match self.get_raw(key) {
            Some(data) => {
                // Downcast to the specified type
                let typed_data = match data.downcast::<T>() {
                    Ok(typed_data) => Some(typed_data),
                    Err(_) => None, // Type mismatch
                };
                typed_data
            }
            None => None,
        }
    }

    fn multi_get_raw(&self, keys: &[&str]) -> Vec<Option<Arc<dyn Any + Send + Sync>>> {
        let mut results = Vec::new();
        for key in keys {
            results.push(self.get_raw(key));
        }
        results
    }

    #[allow(dead_code)]
    pub fn multi_get<T>(&self, keys: &[&str]) -> Vec<Option<Arc<T>>>
    where
        T: Any + Send + Sync + 'static
    {
        let raw_results = self.multi_get_raw(keys);
        raw_results
            .into_iter()
            .map(|data| {
                data.and_then(|data| match data.downcast::<T>() {
                    Ok(typed_data) => Some(typed_data),
                    Err(_) => None,
                })
            })
            .collect()
    }

    pub fn set<T>(&self, key: &str, data: T, ttl_secs: u64)
    where
        T: Any + Send + Sync + 'static
    {
        let item = CachedItem {
            data: Arc::new(data),
            expires_at: SystemTime::now() + Duration::from_secs(ttl_secs),
        };
        let mut cache = self.cache.lock().unwrap();
        cache.insert(key.to_string(), item);
    }

    #[allow(dead_code)]
    pub fn delete(&self, key: &str) {
        let mut cache = self.cache.lock().unwrap();
        cache.remove(key);
    }

    pub fn clear_old_cache(&self) {
        let mut cache = self.cache.lock().unwrap();
        let now = SystemTime::now();
        cache.retain(|_, item| item.expires_at > now);
    }

    #[allow(dead_code)]
    pub fn get_keys(&self) -> Vec<String> {
        self.clear_old_cache();
        let cache = self.cache.lock().unwrap();
        cache.keys().cloned().collect::<Vec<_>>()
    }

    #[allow(dead_code)]
    pub fn get_len(&self) -> usize {
        self.clear_old_cache();
        let cache = self.cache.lock().unwrap();
        cache.len()
    }
}