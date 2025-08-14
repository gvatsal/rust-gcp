use serde::{Deserialize, Serialize};

use super::StatRecord;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TestStats {
    pub event_type: String,
    pub created_at: String,
    pub app_pkg: Option<String>,
    pub guid: Option<String>,
    pub country: Option<String>,
    pub player_version: Option<String>,
    pub oem: Option<String>,
    pub machine_id: Option<String>,
    pub version_machine_id: Option<String>,
    pub instance: Option<String>,
    pub image_name: Option<String>,
    pub arg1: Option<String>,
    pub arg2: Option<String>,
    pub arg3: Option<String>,
    pub arg4: Option<String>,
    pub source: Option<String>,
    pub count: Option<i32>,
    pub ad_refresh_rate: Option<i32>,
}

impl StatRecord for TestStats { }