use gestalt_datastore_postgres::Provider;

#[path = "../../internal/testkit.rs"]
mod testkit;

#[tokio::test]
async fn postgres_provider_smoke_test() {
    let Some(dsn) = std::env::var("GESTALT_TEST_POSTGRES_DSN").ok() else {
        eprintln!("skipping postgres smoke test: GESTALT_TEST_POSTGRES_DSN is not set");
        return;
    };

    let provider = Provider::default();
    let mut config = serde_json::Map::new();
    config.insert("dsn".to_string(), serde_json::Value::String(dsn));

    testkit::run_datastore_smoke_suite(&provider, "postgres", config).await;
}
