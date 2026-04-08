use std::path::PathBuf;

use gestalt_datastore_sqlite::Provider;

#[path = "../../internal/testkit.rs"]
mod testkit;

#[tokio::test]
async fn sqlite_provider_smoke_test() {
    let provider = Provider::default();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("testdata")
        .join(format!("{}.sqlite", testkit::unique_suffix("sqlite-smoke")));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create sqlite test directory");
    }

    let mut config = serde_json::Map::new();
    config.insert(
        "path".to_string(),
        serde_json::Value::String(path.to_string_lossy().into_owned()),
    );

    testkit::run_datastore_smoke_suite(&provider, "sqlite", config).await;
}
