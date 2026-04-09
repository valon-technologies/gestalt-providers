#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use gestalt_plugin_sdk::{
    DatastoreProvider, OAuthRegistration, StoredApiToken, StoredIntegrationToken,
};
use prost_types::Timestamp;
use serde_json::{Map, Value};

pub fn unique_suffix(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos();
    format!("{prefix}-{nanos}-{}", std::process::id())
}

pub fn now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_secs() as i64
}

pub fn timestamp(seconds: i64) -> Option<Timestamp> {
    Some(Timestamp { seconds, nanos: 0 })
}

fn short_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos();
    format!("{prefix}-{:x}", nanos ^ u128::from(std::process::id()))
}

pub async fn run_datastore_smoke_suite<P: DatastoreProvider>(
    provider: &P,
    provider_name: &str,
    config: Map<String, Value>,
) {
    provider
        .configure(provider_name, config)
        .await
        .expect("configure datastore provider");
    provider
        .migrate()
        .await
        .expect("migrate datastore provider");
    provider
        .health_check()
        .await
        .expect("health check datastore provider");

    let unique = unique_suffix(provider_name);
    let now = now_seconds();
    let email = format!("{unique}@example.com");

    let user = provider
        .find_or_create_user(&email)
        .await
        .expect("find or create user");
    assert_eq!(user.email, email);

    let same_user = provider
        .find_or_create_user(&email)
        .await
        .expect("find same user");
    assert_eq!(same_user.id, user.id);

    let fetched_user = provider
        .get_user(&user.id)
        .await
        .expect("get user")
        .expect("user exists");
    assert_eq!(fetched_user.id, user.id);
    assert_eq!(fetched_user.email, email);

    let tenant = format!("tenant-{unique}");
    let integration_token = StoredIntegrationToken {
        id: short_id("tok"),
        user_id: user.id.clone(),
        integration: "github".to_string(),
        connection: "default".to_string(),
        instance: "primary".to_string(),
        access_token_sealed: format!("access-{unique}").into_bytes(),
        refresh_token_sealed: format!("refresh-{unique}").into_bytes(),
        scopes: "repo".to_string(),
        expires_at: timestamp(now + 3600),
        last_refreshed_at: timestamp(now - 60),
        refresh_error_count: 0,
        connection_params: BTreeMap::from([("tenant".to_string(), tenant.clone())]),
        created_at: timestamp(now - 300),
        updated_at: timestamp(now - 120),
    };
    provider
        .put_integration_token(integration_token.clone())
        .await
        .expect("put integration token");
    let fetched_integration_token = provider
        .get_integration_token(
            &user.id,
            &integration_token.integration,
            &integration_token.connection,
            &integration_token.instance,
        )
        .await
        .expect("get integration token")
        .expect("integration token exists");
    assert_eq!(fetched_integration_token.id, integration_token.id);
    assert_eq!(
        fetched_integration_token
            .connection_params
            .get("tenant")
            .map(String::as_str),
        Some(tenant.as_str())
    );

    let listed_integration_tokens = provider
        .list_integration_tokens(
            &user.id,
            &integration_token.integration,
            &integration_token.connection,
        )
        .await
        .expect("list integration tokens");
    assert_eq!(listed_integration_tokens.len(), 1);
    assert_eq!(listed_integration_tokens[0].id, integration_token.id);

    provider
        .delete_integration_token(&integration_token.id)
        .await
        .expect("delete integration token");
    assert!(
        provider
            .get_integration_token(
                &user.id,
                &integration_token.integration,
                &integration_token.connection,
                &integration_token.instance,
            )
            .await
            .expect("get deleted integration token")
            .is_none()
    );

    let api_token = StoredApiToken {
        id: short_id("api"),
        user_id: user.id.clone(),
        name: "CLI".to_string(),
        hashed_token: format!("hash-{unique}"),
        scopes: "read write".to_string(),
        expires_at: timestamp(now + 3600),
        created_at: timestamp(now - 300),
        updated_at: timestamp(now - 120),
    };
    provider
        .put_api_token(api_token.clone())
        .await
        .expect("put api token");
    let fetched_api_token = provider
        .get_api_token_by_hash(&api_token.hashed_token)
        .await
        .expect("get api token by hash")
        .expect("api token exists");
    assert_eq!(fetched_api_token.id, api_token.id);

    let listed_api_tokens = provider
        .list_api_tokens(&user.id)
        .await
        .expect("list api tokens");
    assert_eq!(listed_api_tokens.len(), 1);
    assert_eq!(listed_api_tokens[0].id, api_token.id);

    provider
        .revoke_api_token(&user.id, &api_token.id)
        .await
        .expect("revoke api token");
    assert!(
        provider
            .get_api_token_by_hash(&api_token.hashed_token)
            .await
            .expect("get revoked api token")
            .is_none()
    );

    let api_token_two = StoredApiToken {
        id: short_id("api-two"),
        user_id: user.id.clone(),
        name: "CLI two".to_string(),
        hashed_token: format!("hash-two-{unique}"),
        scopes: "read".to_string(),
        expires_at: timestamp(now + 7200),
        created_at: timestamp(now - 180),
        updated_at: timestamp(now - 60),
    };
    provider
        .put_api_token(api_token_two.clone())
        .await
        .expect("put api token again");
    let revoked = provider
        .revoke_all_api_tokens(&user.id)
        .await
        .expect("revoke all api tokens");
    assert_eq!(revoked, 1);
    assert!(
        provider
            .list_api_tokens(&user.id)
            .await
            .expect("list api tokens after revoke all")
            .is_empty()
    );

    let auth_server_url = format!("https://issuer-{unique}.example");
    let redirect_uri = format!("https://host-{unique}.example/callback");
    let registration = OAuthRegistration {
        auth_server_url: auth_server_url.clone(),
        redirect_uri: redirect_uri.clone(),
        client_id: format!("client-{unique}"),
        client_secret_sealed: format!("secret-{unique}").into_bytes(),
        expires_at: timestamp(now + 3600),
        authorization_endpoint: format!("{auth_server_url}/authorize"),
        token_endpoint: format!("{auth_server_url}/token"),
        scopes_supported: "openid profile".to_string(),
        discovered_at: timestamp(now - 30),
    };
    provider
        .put_oauth_registration(registration.clone())
        .await
        .expect("put oauth registration");
    let fetched_registration = provider
        .get_oauth_registration(&registration.auth_server_url, &registration.redirect_uri)
        .await
        .expect("get oauth registration")
        .expect("oauth registration exists");
    assert_eq!(fetched_registration.client_id, registration.client_id);
    assert_eq!(
        fetched_registration.client_secret_sealed,
        registration.client_secret_sealed
    );

    provider
        .delete_oauth_registration(&registration.auth_server_url, &registration.redirect_uri)
        .await
        .expect("delete oauth registration");
    assert!(
        provider
            .get_oauth_registration(&registration.auth_server_url, &registration.redirect_uri)
            .await
            .expect("get deleted oauth registration")
            .is_none()
    );

    provider.close().await.expect("close datastore provider");
}
