pub mod cache_test;
pub mod datastore_test;
pub mod gcs_test;
#[cfg(feature = "dev")]
pub mod misc_test;
pub mod secret_manager_test;
pub mod stats_test;

use axum::Router;
use std::sync::Arc;

use crate::state::AppState;

pub fn routes(app_state: Arc<AppState>) -> Router {
    let mut router = Router::new();

    // Test routes for ALL environments
    router = router
        .nest("/stats", stats_test::routes(app_state.clone()))
        .nest("/cache", cache_test::routes())
        .nest("/datastore", datastore_test::routes())
        .nest("/gcs", gcs_test::routes())
        .nest("/secret_manager", secret_manager_test::routes());

    // Test routes for DEV environment ONLY
    #[cfg(feature = "dev")]
    {
        router = router
            .nest("/misc", misc_test::routes())
    }

    router
}