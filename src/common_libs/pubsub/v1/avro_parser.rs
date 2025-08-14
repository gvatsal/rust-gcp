use apache_avro::Schema;
use apache_avro::to_avro_datum;
use apache_avro::types::Value as AvroValue;
use serde::Serialize;
use std::collections::HashMap;

pub struct AvroParser;

impl AvroParser {
    fn validate(
        &self,
        avro_value: AvroValue,
        schema: &Schema,
    ) -> Result<AvroValue, String> {
        let mut avro_map = match avro_value {
            AvroValue::Record(kv_pairs) => kv_pairs.into_iter().collect::<HashMap<_, _>>(),
            other => return Err(format!("Record value was expected from to_value(), got: {:?}", other)),
        };

        let record_schema = match schema {
            Schema::Record(record_schema) => record_schema,
            other => return Err(format!("Record schema was expected, got: {:?}", other)),
        };

        let mut updated_avro_value: Vec<(String, AvroValue)> = Vec::with_capacity(record_schema.fields.len());

        for field in record_schema.fields.iter() {
            let name = &field.name;
            if let Some(v) = avro_map.remove(name) {
                // If the struct had this field, keep it
                updated_avro_value.push((name.clone(), v));
            }
            else {
                // If field is missing, check if it’s a nullable
                if let Schema::Union(union_schema) = &field.schema {
                    if union_schema
                        .variants()
                        .iter()
                        .any(|s| matches!(s, Schema::Null))
                    {
                        updated_avro_value.push((name.clone(), AvroValue::Null));
                        continue;
                    }
                }
                // If it’s not nullable, we have an error
                return Err(format!("Field `{}` missing in struct and is not nullable!", name));
            }
        }

        Ok(AvroValue::Record(updated_avro_value))
    }

    pub fn parse_and_encode<T>(
        &self,
        message: &T,
        schema: &Schema
    ) -> Result<Vec<u8>, String>
    where
        T: Serialize
    {
        // Convert message to Avro
        let avro_value = match apache_avro::to_value(message) {
            Ok(value) => value,
            Err(e) => return Err(format!("Failed to serialize to JSON: {}", e)),
        };

        //  Validate the Avro value with the schema
        let updated_avro_value = self.validate(avro_value, schema)?;

        // Encode to binary Avro
        match to_avro_datum(schema, updated_avro_value) {
            Ok(encoded_data) => Ok(encoded_data),
            Err(e) => Err(format!("Failed to encode to Avro: {}", e)),
        }
    }
}