use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::common_libs::datastore::v1::datastore_wrapper::DatastoreModel;

#[derive(Debug, Deserialize, Serialize, SmartDefault)]
pub struct TestData {
    pub key_name: Option<String>,
    pub gc: Option<String>,
    pub amt: Option<f64>,
    pub coups_allw: Option<i64>,
    #[default(Some(0))]
    pub coups_clmd: Option<i64>,
    pub rule_id: Option<String>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_upto: Option<DateTime<Utc>>,
    pub created_by: Option<String>,
    pub desc: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub modified_at: Option<DateTime<Utc>>,
}

impl DatastoreModel for TestData {
    fn primary_key(&self) -> Option<String> {
        self.key_name.clone()
    }

    fn excluded_from_indexes() -> &'static [&'static str] {
        &["amt", "rule_id", "created_by", "desc"]
    }

    fn auto_update_fields(&mut self) {
        let now = Utc::now();
        if self.created_at.is_none() {
            self.created_at = Some(now);
        }
        self.modified_at = Some(now);
    }
}