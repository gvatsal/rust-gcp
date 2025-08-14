pub mod test_stats;

use erased_serde::{serialize, serialize_trait_object};
use serde_json::Serializer;

#[macro_export]
macro_rules! make_stats {
    (
        $struct_name:ident {
            $($field:ident : $value:expr),* $(,)?
        }
    ) => {{
        // This helper function enforces the trait bound at compile time
        fn assert_stat_record<T: $crate::common_libs::pubsub::v1::models::StatRecord>(_: &T) {}
        let instance = $struct_name {
            $($field: $value,)*
            ..Default::default()
        };
        // Will cause a compile error if $struct_name doesn't implement StatRecord
        assert_stat_record(&instance);
        instance
    }};
}

pub trait StatRecord: erased_serde::Serialize + Send + Sync {
    /// Get the data size in bytes
    fn data_len(&self) -> usize {
        let mut buffer = Vec::new();
        let mut serializer = Serializer::new(&mut buffer);

        match serialize(self, &mut serializer) {
            Ok(_) => buffer.len(),
            Err(_) => 0,
        }
    }
}

serialize_trait_object!(StatRecord);