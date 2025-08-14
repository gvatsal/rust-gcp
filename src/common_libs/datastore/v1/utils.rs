use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Utc};
use google_datastore1::api::{ArrayValue, Entity, Key, LatLng, Value as DatastoreValue};
use serde_json::{Map, Value as JsonValue};
use std::any::type_name;
use std::collections::HashMap;

use super::datastore_wrapper::DatastoreModel;

pub fn infer_kind<T>() -> String {
    type_name::<T>().rsplit("::").next().unwrap_or("Unknown").to_string()
}

pub fn entity_to_struct<T>(entity: Entity) -> Result<T, String>
where
    T: DatastoreModel,
{
    tracing::debug!("Entity: {:?}", entity);
    let mut json_map = Map::new();

    // Add the key_name to the JSON map
    let key = entity.key.unwrap_or_default();
    let key_name = key.path
        .unwrap_or_default()
        .into_iter()
        .next()
        .and_then(|path| path.name);
    json_map.insert("key_name".to_string(), serde_json::to_value(key_name).unwrap_or(JsonValue::Null));

    // Add the properties to the JSON map
    let properties = entity.properties.unwrap_or_default();
    for (k, v) in properties {
        json_map.insert(k, datastore_value_to_json_value(&v));
    }

    let json_value = JsonValue::Object(json_map);
    let data: T = match serde_json::from_value(json_value) {
        Ok(data) => data,
        Err(e) => return Err(format!("Failed to deserialize entity to struct: {}", e)),
    };

    tracing::debug!("Struct: {:?}", data);
    Ok(data)
}

fn datastore_value_to_json_value(val: &DatastoreValue) -> JsonValue {
    match val {
        DatastoreValue { array_value: Some(arr), .. } => {
            JsonValue::Array(
                arr.values.as_ref().unwrap_or(&Vec::new())
                    .iter()
                    .map(|value| datastore_value_to_json_value(value))
                    .collect::<Vec<_>>()
            )
        },
        DatastoreValue { blob_value: Some(bytes), .. } => JsonValue::String(STANDARD.encode(bytes)),
        DatastoreValue { boolean_value: Some(b), .. } => JsonValue::Bool(*b),
        DatastoreValue { double_value: Some(d), .. } => {
            match serde_json::Number::from_f64(*d) {
                Some(n) => JsonValue::Number(n),
                None => JsonValue::Null,
            }
        },
        DatastoreValue { entity_value: Some(ent), .. } => {
            let mut map = Map::new();
            if let Some(props) = &ent.properties {
                for (k, val) in props {
                    map.insert(k.clone(), datastore_value_to_json_value(val));
                }
            }
            JsonValue::Object(map)
        },
        DatastoreValue { geo_point_value: Some(latlng), .. } => {
            let mut pt = Map::new();
            pt.insert("latitude".to_string(), latlng.latitude.into());
            pt.insert("longitude".to_string(), latlng.longitude.into());
            JsonValue::Object(pt)
        },
        DatastoreValue { integer_value: Some(i), .. } => JsonValue::Number((*i).into()),
        DatastoreValue { key_value: Some(key), .. } => serde_json::to_value(key).unwrap_or(JsonValue::Null),
        DatastoreValue { null_value: Some(_), .. } => JsonValue::Null,
        DatastoreValue { string_value: Some(s), .. } => JsonValue::String(s.clone()),
        DatastoreValue { timestamp_value: Some(ts), .. } => JsonValue::String(ts.to_rfc3339()),
        _ => JsonValue::Null
    }
}

pub fn struct_to_entity<T>(entity_key: Key, data: &T) -> Result<Entity, String>
where
    T: DatastoreModel,
{
    tracing::debug!("Struct: {:?}", data);
    let json_value = match serde_json::to_value(data) {
        Ok(value) => value,
        Err(e) => return Err(format!("Failed to serialize struct to JSON: {}", e)),
    };

    if let JsonValue::Object(json_map) = json_value {
        let mut properties = HashMap::new();
        for (k, v) in json_map {
            // Exclude key_name field
            if k == "key_name" {
                continue;
            }

            let mut val = json_value_to_datastore_value(&v);
            if T::excluded_from_indexes().contains(&k.as_str()) {
                val.exclude_from_indexes = Some(true);
            }
            properties.insert(k, val);
        }

        let entity = Entity {
            key: Some(entity_key),
            properties: Some(properties),
        };
        tracing::debug!("Entity: {:?}", entity);
        Ok(entity)
    }
    else {
        Err(format!("Failed to serialize struct to JSON"))
    }
}

fn json_value_to_datastore_value(val: &JsonValue) -> DatastoreValue {
    let mut base = DatastoreValue { ..Default::default() };
    match val {
        JsonValue::Null => { base.null_value = Some("NULL_VALUE".to_string()); },
        JsonValue::Bool(b) => { base.boolean_value = Some(*b); },
        JsonValue::Number(n) => {
            if n.is_i64() {
                base.integer_value = n.as_i64();
            }
            else if n.is_f64() {
                base.double_value = n.as_f64();
            }
        },
        JsonValue::String(s) => {
            if DateTime::parse_from_rfc3339(s).is_ok() {
                let dt = DateTime::parse_from_rfc3339(s)
                    .unwrap()
                    .with_timezone(&Utc);
                base.timestamp_value = Some(dt);
            }
            else if !s.is_empty() && STANDARD.decode(s).is_ok() {
                let bytes = STANDARD.decode(s).unwrap();
                base.blob_value = Some(bytes);
            }
            else {
                base.string_value = Some(s.clone());
            }
        },
        JsonValue::Array(arr) => {
            let arr_values = arr.iter()
                           .map(|value| json_value_to_datastore_value(value))
                           .collect::<Vec<_>>();
            base.array_value = Some(ArrayValue { values: Some(arr_values) });
        },
        JsonValue::Object(map) => {
            if map.len() == 2 && map.contains_key("latitude") && map.contains_key("longitude") {
                let lat = map["latitude"].as_f64();
                let lng = map["longitude"].as_f64();
                base.geo_point_value = Some(LatLng { latitude: lat, longitude: lng });
            }
            else if map.contains_key("partition_id") && map.contains_key("path") {
                if let Ok(key) = serde_json::from_value::<Key>(JsonValue::Object(map.clone())) {
                    base.key_value = Some(key);
                }
            }
            else {
                let mut props = HashMap::new();
                for (k, v) in map {
                    props.insert(k.clone(), json_value_to_datastore_value(v));
                }
                base.entity_value = Some(Entity { key: None, properties: Some(props) });
            }
        }
    }
    base
}