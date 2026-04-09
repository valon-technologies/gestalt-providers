use std::sync::{Arc, RwLock};

use gestalt_datastore_sqlstore::{
    PROVIDER_VERSION, PostgresStore, connect_postgres, decode_config, migrate_postgres,
    provider_error,
};
use gestalt_plugin_sdk as gestalt;
use serde::Deserialize;

#[derive(Default)]
pub struct Provider {
    store: RwLock<Option<Arc<PostgresStore>>>,
}

#[derive(Deserialize)]
struct Config {
    dsn: String,
}

impl Provider {
    fn new() -> Self {
        Self::default()
    }

    fn configured_store(&self) -> gestalt::Result<Arc<PostgresStore>> {
        self.store
            .read()
            .expect("lock postgres store")
            .clone()
            .ok_or_else(|| {
                gestalt::Error::internal("postgres datastore: provider is not configured")
            })
    }
}

#[gestalt::async_trait]
impl gestalt::DatastoreProvider for Provider {
    async fn configure(
        &self,
        _name: &str,
        config: serde_json::Map<String, serde_json::Value>,
    ) -> gestalt::Result<()> {
        let config: Config =
            decode_config(config).map_err(|error| provider_error("postgres datastore", error))?;
        let store = Arc::new(
            connect_postgres(&config.dsn)
                .await
                .map_err(|error| provider_error("postgres datastore", error))?,
        );
        let previous = self
            .store
            .write()
            .expect("lock postgres store")
            .replace(Arc::clone(&store));
        if let Some(previous) = previous {
            previous.close().await?;
        }
        Ok(())
    }

    fn metadata(&self) -> Option<gestalt::RuntimeMetadata> {
        Some(gestalt::RuntimeMetadata {
            name: "postgres".to_string(),
            display_name: "PostgreSQL".to_string(),
            description: "PostgreSQL datastore provider for production deployments.".to_string(),
            version: PROVIDER_VERSION.to_string(),
        })
    }

    async fn health_check(&self) -> gestalt::Result<()> {
        self.configured_store()?.health_check().await
    }

    async fn close(&self) -> gestalt::Result<()> {
        let previous = self.store.write().expect("lock postgres store").take();
        if let Some(previous) = previous {
            previous.close().await?;
        }
        Ok(())
    }

    async fn migrate(&self) -> gestalt::Result<()> {
        let store = self.configured_store()?;
        migrate_postgres(store.as_ref()).await
    }

    async fn get_user(&self, id: &str) -> gestalt::Result<Option<gestalt::StoredUser>> {
        self.configured_store()?.get_user(id).await
    }

    async fn find_or_create_user(&self, email: &str) -> gestalt::Result<gestalt::StoredUser> {
        self.configured_store()?.find_or_create_user(email).await
    }

    async fn put_integration_token(
        &self,
        token: gestalt::StoredIntegrationToken,
    ) -> gestalt::Result<()> {
        self.configured_store()?.put_integration_token(token).await
    }

    async fn get_integration_token(
        &self,
        user_id: &str,
        integration: &str,
        connection: &str,
        instance: &str,
    ) -> gestalt::Result<Option<gestalt::StoredIntegrationToken>> {
        self.configured_store()?
            .get_integration_token(user_id, integration, connection, instance)
            .await
    }

    async fn list_integration_tokens(
        &self,
        user_id: &str,
        integration: &str,
        connection: &str,
    ) -> gestalt::Result<Vec<gestalt::StoredIntegrationToken>> {
        self.configured_store()?
            .list_integration_tokens(user_id, integration, connection)
            .await
    }

    async fn delete_integration_token(&self, id: &str) -> gestalt::Result<()> {
        self.configured_store()?.delete_integration_token(id).await
    }

    async fn put_api_token(&self, token: gestalt::StoredApiToken) -> gestalt::Result<()> {
        self.configured_store()?.put_api_token(token).await
    }

    async fn get_api_token_by_hash(
        &self,
        hashed_token: &str,
    ) -> gestalt::Result<Option<gestalt::StoredApiToken>> {
        self.configured_store()?
            .get_api_token_by_hash(hashed_token)
            .await
    }

    async fn list_api_tokens(
        &self,
        user_id: &str,
    ) -> gestalt::Result<Vec<gestalt::StoredApiToken>> {
        self.configured_store()?.list_api_tokens(user_id).await
    }

    async fn revoke_api_token(&self, user_id: &str, id: &str) -> gestalt::Result<()> {
        self.configured_store()?.revoke_api_token(user_id, id).await
    }

    async fn revoke_all_api_tokens(&self, user_id: &str) -> gestalt::Result<i64> {
        self.configured_store()?
            .revoke_all_api_tokens(user_id)
            .await
    }

    async fn get_oauth_registration(
        &self,
        auth_server_url: &str,
        redirect_uri: &str,
    ) -> gestalt::Result<Option<gestalt::OAuthRegistration>> {
        self.configured_store()?
            .get_oauth_registration(auth_server_url, redirect_uri)
            .await
    }

    async fn put_oauth_registration(
        &self,
        registration: gestalt::OAuthRegistration,
    ) -> gestalt::Result<()> {
        self.configured_store()?
            .put_oauth_registration(registration)
            .await
    }

    async fn delete_oauth_registration(
        &self,
        auth_server_url: &str,
        redirect_uri: &str,
    ) -> gestalt::Result<()> {
        self.configured_store()?
            .delete_oauth_registration(auth_server_url, redirect_uri)
            .await
    }
}

gestalt::export_datastore_provider!(constructor = Provider::new);
