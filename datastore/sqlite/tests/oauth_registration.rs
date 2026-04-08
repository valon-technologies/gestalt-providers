use std::path::PathBuf;

use gestalt_datastore_sqlite::Provider;
use gestalt_plugin_sdk::{DatastoreProvider, OAuthRegistration};
use prost_types::Timestamp;
use serde_json::{Map, Value};

#[path = "../../internal/testkit.rs"]
mod testkit;

#[tokio::test]
async fn migrate_creates_oauth_registrations_table() {
    let provider = Provider::default();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("testdata")
        .join(format!(
            "{}.sqlite",
            testkit::unique_suffix("oauth-registration")
        ));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create sqlite test directory");
    }
    if path.exists() {
        std::fs::remove_file(&path).expect("remove old sqlite test database");
    }

    let mut config = Map::new();
    config.insert(
        "path".to_string(),
        Value::String(path.to_string_lossy().into_owned()),
    );

    provider
        .configure("sqlite", config)
        .await
        .expect("configure sqlite provider");
    provider.migrate().await.expect("migrate sqlite provider");

    let registration = OAuthRegistration {
        auth_server_url: "https://issuer.example".to_string(),
        redirect_uri: "http://localhost/callback".to_string(),
        client_id: "client-id".to_string(),
        client_secret_sealed: vec![0x01, 0x02, 0x03, 0x04],
        expires_at: Some(Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        authorization_endpoint: "https://issuer.example/oauth/authorize".to_string(),
        token_endpoint: "https://issuer.example/oauth/token".to_string(),
        scopes_supported: "openid email profile".to_string(),
        discovered_at: Some(Timestamp {
            seconds: 1_699_999_000,
            nanos: 0,
        }),
    };

    provider
        .put_oauth_registration(registration.clone())
        .await
        .expect("put oauth registration");

    let fetched = provider
        .get_oauth_registration(&registration.auth_server_url, &registration.redirect_uri)
        .await
        .expect("get oauth registration")
        .expect("oauth registration exists");

    assert_eq!(fetched.client_id, registration.client_id);
    assert_eq!(
        fetched.client_secret_sealed,
        registration.client_secret_sealed
    );

    provider.close().await.expect("close sqlite provider");
}
