use gestalt_datastore_mysql::Provider;

#[path = "../../internal/testkit.rs"]
mod testkit;

#[tokio::test]
async fn mysql_provider_smoke_test() {
    let Some(dsn) = std::env::var("GESTALT_TEST_MYSQL_DSN").ok() else {
        eprintln!("skipping mysql smoke test: GESTALT_TEST_MYSQL_DSN is not set");
        return;
    };

    let provider = Provider::default();
    let mut config = serde_json::Map::new();
    config.insert("dsn".to_string(), serde_json::Value::String(dsn));
    if let Ok(version) = std::env::var("GESTALT_TEST_MYSQL_VERSION") {
        config.insert("version".to_string(), serde_json::Value::String(version));
    }

    testkit::run_datastore_smoke_suite(&provider, "mysql", config).await;
}
