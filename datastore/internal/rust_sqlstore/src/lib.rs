use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use base64::Engine as _;
use chrono::{DateTime, Timelike, Utc};
use gestalt_plugin_sdk as gestalt;
use prost_types::Timestamp;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{MySqlPool, PgPool, Row, SqlitePool};
use uuid::Uuid;

pub const PROVIDER_VERSION: &str = "0.0.1-alpha.7";

const SQLITE_WARNING: &str = "using SQLite for the datastore; this is not recommended for production. See https://docs.valon.tools/deploy for alternatives.";

const SQLITE_MIGRATIONS: &[&str] = &[
    r#"
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        display_name TEXT NOT NULL DEFAULT '',
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS integration_tokens (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id),
        integration TEXT NOT NULL,
        connection TEXT NOT NULL DEFAULT '',
        instance TEXT NOT NULL,
        access_token_encrypted TEXT NOT NULL,
        refresh_token_encrypted TEXT NOT NULL DEFAULT '',
        scopes TEXT NOT NULL DEFAULT '',
        expires_at DATETIME,
        last_refreshed_at DATETIME,
        refresh_error_count INTEGER NOT NULL DEFAULT 0,
        metadata_json TEXT NOT NULL DEFAULT '',
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        UNIQUE(user_id, integration, connection, instance)
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS api_tokens (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id),
        name TEXT NOT NULL,
        hashed_token TEXT UNIQUE NOT NULL,
        scopes TEXT NOT NULL DEFAULT '',
        expires_at DATETIME,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL
    )
    "#,
];

const POSTGRES_MIGRATIONS: &[&str] = &[
    r#"
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        display_name TEXT NOT NULL DEFAULT '',
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS integration_tokens (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id),
        integration TEXT NOT NULL,
        connection TEXT NOT NULL DEFAULT '',
        instance TEXT NOT NULL,
        access_token_encrypted TEXT NOT NULL,
        refresh_token_encrypted TEXT NOT NULL DEFAULT '',
        scopes TEXT NOT NULL DEFAULT '',
        expires_at TIMESTAMPTZ,
        last_refreshed_at TIMESTAMPTZ,
        refresh_error_count INTEGER NOT NULL DEFAULT 0,
        metadata_json TEXT NOT NULL DEFAULT '',
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        UNIQUE(user_id, integration, connection, instance)
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS api_tokens (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id),
        name TEXT NOT NULL,
        hashed_token TEXT UNIQUE NOT NULL,
        scopes TEXT NOT NULL DEFAULT '',
        expires_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    "#,
];

const MYSQL_MIGRATIONS: &[&str] = &[
    r#"
    CREATE TABLE IF NOT EXISTS users (
        id VARCHAR(36) NOT NULL PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        display_name VARCHAR(255) NOT NULL DEFAULT '',
        created_at DATETIME(6) NOT NULL,
        updated_at DATETIME(6) NOT NULL,
        UNIQUE KEY idx_users_email (email)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS integration_tokens (
        id VARCHAR(36) NOT NULL PRIMARY KEY,
        user_id VARCHAR(36) NOT NULL,
        integration VARCHAR(128) NOT NULL,
        connection VARCHAR(128) NOT NULL DEFAULT '',
        instance VARCHAR(128) NOT NULL,
        access_token_encrypted TEXT NOT NULL,
        refresh_token_encrypted TEXT NOT NULL,
        scopes TEXT NOT NULL,
        expires_at DATETIME(6) NULL,
        last_refreshed_at DATETIME(6) NULL,
        refresh_error_count INT NOT NULL DEFAULT 0,
        metadata_json TEXT NOT NULL,
        created_at DATETIME(6) NOT NULL,
        updated_at DATETIME(6) NOT NULL,
        UNIQUE KEY idx_integration_tokens_user_integ_conn_inst (user_id, integration, connection, instance),
        CONSTRAINT fk_integration_tokens_user FOREIGN KEY (user_id) REFERENCES users(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS api_tokens (
        id VARCHAR(36) NOT NULL PRIMARY KEY,
        user_id VARCHAR(36) NOT NULL,
        name VARCHAR(255) NOT NULL,
        hashed_token VARCHAR(255) NOT NULL,
        scopes TEXT NOT NULL,
        expires_at DATETIME(6) NULL,
        created_at DATETIME(6) NOT NULL,
        updated_at DATETIME(6) NOT NULL,
        UNIQUE KEY idx_api_tokens_hashed (hashed_token),
        CONSTRAINT fk_api_tokens_user FOREIGN KEY (user_id) REFERENCES users(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    "#,
];

pub fn decode_config<T>(config: Map<String, Value>) -> gestalt::Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_value(Value::Object(config))
        .map_err(|error| gestalt::Error::internal(format!("decode config: {error}")))
}

pub fn sqlite_warnings() -> Vec<String> {
    vec![SQLITE_WARNING.to_string()]
}

pub fn provider_error(provider: &str, error: gestalt::Error) -> gestalt::Error {
    gestalt::Error::internal(format!("{provider}: {}", error.message()))
}

pub fn now_timestamp() -> Timestamp {
    let now = Utc::now().with_nanosecond(0).expect("truncate nanos");
    datetime_to_timestamp(now)
}

fn datetime_to_timestamp(value: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn timestamp_to_datetime(
    field: &str,
    value: &Option<Timestamp>,
) -> gestalt::Result<Option<DateTime<Utc>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let nanos = u32::try_from(value.nanos)
        .map_err(|_| gestalt::Error::internal(format!("{field} nanos out of range")))?;
    DateTime::from_timestamp(value.seconds, nanos)
        .map(Some)
        .ok_or_else(|| gestalt::Error::internal(format!("{field} is out of range")))
}

fn required_timestamp(field: &str, value: &Option<Timestamp>) -> gestalt::Result<DateTime<Utc>> {
    timestamp_to_datetime(field, value)?
        .ok_or_else(|| gestalt::Error::internal(format!("{field} is required")))
}

fn default_created_updated(
    created_at: Option<Timestamp>,
    updated_at: Option<Timestamp>,
) -> (Timestamp, Timestamp) {
    let now = now_timestamp();
    (
        created_at.unwrap_or_else(|| now.clone()),
        updated_at.unwrap_or(now),
    )
}

fn encode_sealed(sealed: &[u8]) -> String {
    if sealed.is_empty() {
        return String::new();
    }
    format!(
        "b64:{}",
        base64::engine::general_purpose::STANDARD.encode(sealed)
    )
}

fn decode_sealed(encoded: &str) -> gestalt::Result<Vec<u8>> {
    if encoded.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(value) = encoded.strip_prefix("b64:") {
        return base64::engine::general_purpose::STANDARD
            .decode(value)
            .map_err(|error| gestalt::Error::internal(format!("decode sealed bytes: {error}")));
    }
    Ok(encoded.as_bytes().to_vec())
}

fn connection_params_to_json(values: &BTreeMap<String, String>) -> gestalt::Result<String> {
    if values.is_empty() {
        return Ok(String::new());
    }
    serde_json::to_string(values)
        .map_err(|error| gestalt::Error::internal(format!("encode connection params: {error}")))
}

fn connection_params_from_json(encoded: &str) -> gestalt::Result<BTreeMap<String, String>> {
    if encoded.is_empty() {
        return Ok(BTreeMap::new());
    }
    serde_json::from_str(encoded)
        .map_err(|error| gestalt::Error::internal(format!("decode connection params: {error}")))
}

fn normalize_requested_version(requested: &str) -> String {
    requested.trim().to_ascii_lowercase()
}

fn validate_detected_version(
    provider: &str,
    requested: &str,
    supported: &[&str],
    resolved: String,
    raw: String,
) -> gestalt::Result<String> {
    let requested = normalize_requested_version(requested);
    if !requested.is_empty() && requested != "auto" && !supported.contains(&requested.as_str()) {
        return Err(gestalt::Error::internal(format!(
            "{provider}: unsupported version {requested:?} (supported: {})",
            supported.join(", ")
        )));
    }

    if !supported.contains(&resolved.as_str()) {
        return Err(gestalt::Error::internal(format!(
            "{provider}: detected unsupported version {resolved:?} from {raw:?} (supported: {})",
            supported.join(", ")
        )));
    }
    if !requested.is_empty() && requested != "auto" && requested != resolved {
        return Err(gestalt::Error::internal(format!(
            "{provider}: configured version {requested:?} does not match detected version {resolved:?} ({raw})"
        )));
    }
    Ok(resolved)
}

pub trait SqlDialect: Clone + Send + Sync + 'static {
    fn placeholder(&self, n: usize) -> String;
    fn upsert_token_sql(&self) -> String;
    fn registration_ddl(&self) -> &'static str;
    fn is_duplicate_key_error(&self, error: &sqlx::Error) -> bool;

    fn normalize_connection(&self, connection: &str) -> String {
        connection.to_string()
    }

    fn denormalize_connection(&self, connection: &str) -> String {
        connection.to_string()
    }
}

#[derive(Clone, Copy, Default)]
pub struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn placeholder(&self, _n: usize) -> String {
        "?".to_string()
    }

    fn upsert_token_sql(&self) -> String {
        r#"
        INSERT INTO integration_tokens
            (id, user_id, integration, connection, instance, access_token_encrypted, refresh_token_encrypted,
             scopes, expires_at, last_refreshed_at, refresh_error_count, metadata_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, integration, connection, instance) DO UPDATE SET
            access_token_encrypted = excluded.access_token_encrypted,
            refresh_token_encrypted = excluded.refresh_token_encrypted,
            scopes = excluded.scopes,
            expires_at = excluded.expires_at,
            last_refreshed_at = excluded.last_refreshed_at,
            refresh_error_count = excluded.refresh_error_count,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at
        "#
        .to_string()
    }

    fn registration_ddl(&self) -> &'static str {
        r#"
        CREATE TABLE IF NOT EXISTS oauth_registrations (
            id TEXT PRIMARY KEY,
            auth_server_url TEXT NOT NULL,
            redirect_uri TEXT NOT NULL,
            client_id TEXT NOT NULL,
            client_secret_encrypted TEXT,
            expires_at DATETIME,
            authorization_endpoint TEXT NOT NULL,
            token_endpoint TEXT NOT NULL,
            scopes_supported TEXT,
            discovered_at DATETIME NOT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            UNIQUE (auth_server_url, redirect_uri)
        )
        "#
    }

    fn is_duplicate_key_error(&self, error: &sqlx::Error) -> bool {
        match error {
            sqlx::Error::Database(db_error) => {
                db_error.message().contains("UNIQUE constraint failed")
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn placeholder(&self, n: usize) -> String {
        format!("${n}")
    }

    fn upsert_token_sql(&self) -> String {
        r#"
        INSERT INTO integration_tokens
            (id, user_id, integration, connection, instance, access_token_encrypted, refresh_token_encrypted,
             scopes, expires_at, last_refreshed_at, refresh_error_count, metadata_json, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT(user_id, integration, connection, instance) DO UPDATE SET
            access_token_encrypted = EXCLUDED.access_token_encrypted,
            refresh_token_encrypted = EXCLUDED.refresh_token_encrypted,
            scopes = EXCLUDED.scopes,
            expires_at = EXCLUDED.expires_at,
            last_refreshed_at = EXCLUDED.last_refreshed_at,
            refresh_error_count = EXCLUDED.refresh_error_count,
            metadata_json = EXCLUDED.metadata_json,
            updated_at = EXCLUDED.updated_at
        "#
        .to_string()
    }

    fn registration_ddl(&self) -> &'static str {
        r#"
        CREATE TABLE IF NOT EXISTS oauth_registrations (
            id TEXT PRIMARY KEY,
            auth_server_url TEXT NOT NULL,
            redirect_uri TEXT NOT NULL,
            client_id TEXT NOT NULL,
            client_secret_encrypted TEXT,
            expires_at TIMESTAMPTZ,
            authorization_endpoint TEXT NOT NULL,
            token_endpoint TEXT NOT NULL,
            scopes_supported TEXT,
            discovered_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            UNIQUE (auth_server_url, redirect_uri)
        )
        "#
    }

    fn is_duplicate_key_error(&self, error: &sqlx::Error) -> bool {
        match error {
            sqlx::Error::Database(db_error) => db_error.code().as_deref() == Some("23505"),
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct MySqlDialect {
    version: String,
}

impl MySqlDialect {
    fn new(version: String) -> Self {
        Self { version }
    }
}

impl SqlDialect for MySqlDialect {
    fn placeholder(&self, _n: usize) -> String {
        "?".to_string()
    }

    fn upsert_token_sql(&self) -> String {
        if self.version == "8.0" {
            return r#"
            INSERT INTO integration_tokens
                (id, user_id, integration, connection, instance, access_token_encrypted, refresh_token_encrypted,
                 scopes, expires_at, last_refreshed_at, refresh_error_count, metadata_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                access_token_encrypted = VALUES(access_token_encrypted),
                refresh_token_encrypted = VALUES(refresh_token_encrypted),
                scopes = VALUES(scopes),
                expires_at = VALUES(expires_at),
                last_refreshed_at = VALUES(last_refreshed_at),
                refresh_error_count = VALUES(refresh_error_count),
                metadata_json = VALUES(metadata_json),
                updated_at = VALUES(updated_at)
            "#
            .to_string();
        }

        r#"
        INSERT INTO integration_tokens
            (id, user_id, integration, connection, instance, access_token_encrypted, refresh_token_encrypted,
             scopes, expires_at, last_refreshed_at, refresh_error_count, metadata_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS new
        ON DUPLICATE KEY UPDATE
            access_token_encrypted = new.access_token_encrypted,
            refresh_token_encrypted = new.refresh_token_encrypted,
            scopes = new.scopes,
            expires_at = new.expires_at,
            last_refreshed_at = new.last_refreshed_at,
            refresh_error_count = new.refresh_error_count,
            metadata_json = new.metadata_json,
            updated_at = new.updated_at
        "#
        .to_string()
    }

    fn registration_ddl(&self) -> &'static str {
        r#"
        CREATE TABLE IF NOT EXISTS oauth_registrations (
            id VARCHAR(36) PRIMARY KEY,
            auth_server_url VARCHAR(255) NOT NULL,
            redirect_uri VARCHAR(255) NOT NULL,
            client_id VARCHAR(255) NOT NULL,
            client_secret_encrypted TEXT,
            expires_at DATETIME(6) NULL,
            authorization_endpoint VARCHAR(500) NOT NULL,
            token_endpoint VARCHAR(500) NOT NULL,
            scopes_supported TEXT,
            discovered_at DATETIME(6) NOT NULL,
            created_at DATETIME(6) NOT NULL,
            updated_at DATETIME(6) NOT NULL,
            UNIQUE KEY idx_oauth_registrations_auth_redirect (auth_server_url, redirect_uri)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        "#
    }

    fn is_duplicate_key_error(&self, error: &sqlx::Error) -> bool {
        match error {
            sqlx::Error::Database(db_error) => db_error.code().as_deref() == Some("1062"),
            _ => false,
        }
    }
}

pub struct SqliteStore {
    pool: SqlitePool,
    dialect: SqliteDialect,
}

pub struct PostgresStore {
    pool: PgPool,
    dialect: PostgresDialect,
}

pub struct MySqlStore {
    pool: MySqlPool,
    dialect: MySqlDialect,
}

macro_rules! impl_sql_store {
    ($store:ident, $pool_ty:ty, $row_ty:ty, $dialect_ty:ty) => {
        impl $store {
            fn new(pool: $pool_ty, dialect: $dialect_ty) -> Self {
                Self { pool, dialect }
            }

            pub async fn health_check(&self) -> gestalt::Result<()> {
                sqlx::query("SELECT 1")
                    .execute(&self.pool)
                    .await
                    .map_err(|error| gestalt::Error::internal(format!("health check: {error}")))?;
                Ok(())
            }

            pub async fn close(&self) -> gestalt::Result<()> {
                self.pool.close().await;
                Ok(())
            }

            pub async fn migrate(&self, migrations: &[&str]) -> gestalt::Result<()> {
                for statement in migrations {
                    sqlx::query(statement)
                        .execute(&self.pool)
                        .await
                        .map_err(|error| gestalt::Error::internal(format!("migration failed: {error}")))?;
                }
                sqlx::query(self.dialect.registration_ddl())
                    .execute(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!(
                            "creating oauth_registrations table: {error}"
                        ))
                    })?;
                Ok(())
            }

            fn scan_user_row(&self, row: $row_ty) -> gestalt::Result<gestalt::StoredUser> {
                let created_at: DateTime<Utc> = row
                    .try_get("created_at")
                    .map_err(|error| gestalt::Error::internal(format!("read created_at: {error}")))?;
                let updated_at: DateTime<Utc> = row
                    .try_get("updated_at")
                    .map_err(|error| gestalt::Error::internal(format!("read updated_at: {error}")))?;
                Ok(gestalt::StoredUser {
                    id: row
                        .try_get("id")
                        .map_err(|error| gestalt::Error::internal(format!("read id: {error}")))?,
                    email: row
                        .try_get("email")
                        .map_err(|error| gestalt::Error::internal(format!("read email: {error}")))?,
                    display_name: row.try_get("display_name").map_err(|error| {
                        gestalt::Error::internal(format!("read display_name: {error}"))
                    })?,
                    created_at: Some(datetime_to_timestamp(created_at)),
                    updated_at: Some(datetime_to_timestamp(updated_at)),
                })
            }

            fn scan_integration_token_row(
                &self,
                row: $row_ty,
            ) -> gestalt::Result<gestalt::StoredIntegrationToken> {
                let connection: String = row
                    .try_get("connection")
                    .map_err(|error| gestalt::Error::internal(format!("read connection: {error}")))?;
                let access_token_encrypted: String = row.try_get("access_token_encrypted").map_err(
                    |error| gestalt::Error::internal(format!("read access token: {error}")),
                )?;
                let refresh_token_encrypted: String = row
                    .try_get("refresh_token_encrypted")
                    .map_err(|error| gestalt::Error::internal(format!("read refresh token: {error}")))?;
                let metadata_json: String = row.try_get("metadata_json").map_err(|error| {
                    gestalt::Error::internal(format!("read connection params: {error}"))
                })?;
                let expires_at: Option<DateTime<Utc>> = row.try_get("expires_at").map_err(|error| {
                    gestalt::Error::internal(format!("read expires_at: {error}"))
                })?;
                let last_refreshed_at: Option<DateTime<Utc>> = row
                    .try_get("last_refreshed_at")
                    .map_err(|error| {
                        gestalt::Error::internal(format!("read last_refreshed_at: {error}"))
                    })?;
                let created_at: DateTime<Utc> = row
                    .try_get("created_at")
                    .map_err(|error| gestalt::Error::internal(format!("read created_at: {error}")))?;
                let updated_at: DateTime<Utc> = row
                    .try_get("updated_at")
                    .map_err(|error| gestalt::Error::internal(format!("read updated_at: {error}")))?;
                Ok(gestalt::StoredIntegrationToken {
                    id: row
                        .try_get("id")
                        .map_err(|error| gestalt::Error::internal(format!("read id: {error}")))?,
                    user_id: row
                        .try_get("user_id")
                        .map_err(|error| gestalt::Error::internal(format!("read user_id: {error}")))?,
                    integration: row.try_get("integration").map_err(|error| {
                        gestalt::Error::internal(format!("read integration: {error}"))
                    })?,
                    connection: self.dialect.denormalize_connection(&connection),
                    instance: row
                        .try_get("instance")
                        .map_err(|error| gestalt::Error::internal(format!("read instance: {error}")))?,
                    access_token_sealed: decode_sealed(&access_token_encrypted)?,
                    refresh_token_sealed: decode_sealed(&refresh_token_encrypted)?,
                    scopes: row
                        .try_get("scopes")
                        .map_err(|error| gestalt::Error::internal(format!("read scopes: {error}")))?,
                    expires_at: expires_at.map(datetime_to_timestamp),
                    last_refreshed_at: last_refreshed_at.map(datetime_to_timestamp),
                    refresh_error_count: row.try_get("refresh_error_count").map_err(|error| {
                        gestalt::Error::internal(format!("read refresh_error_count: {error}"))
                    })?,
                    connection_params: connection_params_from_json(&metadata_json)?,
                    created_at: Some(datetime_to_timestamp(created_at)),
                    updated_at: Some(datetime_to_timestamp(updated_at)),
                })
            }

            fn scan_api_token_row(
                &self,
                row: $row_ty,
            ) -> gestalt::Result<gestalt::StoredApiToken> {
                let expires_at: Option<DateTime<Utc>> = row.try_get("expires_at").map_err(|error| {
                    gestalt::Error::internal(format!("read expires_at: {error}"))
                })?;
                let created_at: DateTime<Utc> = row
                    .try_get("created_at")
                    .map_err(|error| gestalt::Error::internal(format!("read created_at: {error}")))?;
                let updated_at: DateTime<Utc> = row
                    .try_get("updated_at")
                    .map_err(|error| gestalt::Error::internal(format!("read updated_at: {error}")))?;
                Ok(gestalt::StoredApiToken {
                    id: row
                        .try_get("id")
                        .map_err(|error| gestalt::Error::internal(format!("read id: {error}")))?,
                    user_id: row
                        .try_get("user_id")
                        .map_err(|error| gestalt::Error::internal(format!("read user_id: {error}")))?,
                    name: row
                        .try_get("name")
                        .map_err(|error| gestalt::Error::internal(format!("read name: {error}")))?,
                    hashed_token: row.try_get("hashed_token").map_err(|error| {
                        gestalt::Error::internal(format!("read hashed_token: {error}"))
                    })?,
                    scopes: row
                        .try_get("scopes")
                        .map_err(|error| gestalt::Error::internal(format!("read scopes: {error}")))?,
                    expires_at: expires_at.map(datetime_to_timestamp),
                    created_at: Some(datetime_to_timestamp(created_at)),
                    updated_at: Some(datetime_to_timestamp(updated_at)),
                })
            }

            pub async fn get_user(&self, id: &str) -> gestalt::Result<Option<gestalt::StoredUser>> {
                let sql = format!(
                    "SELECT id, email, display_name, created_at, updated_at FROM users WHERE id = {}",
                    self.dialect.placeholder(1)
                );
                let row = sqlx::query(&sql)
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|error| gestalt::Error::internal(format!("querying user by id: {error}")))?;
                row.map(|row| self.scan_user_row(row)).transpose()
            }

            pub async fn find_or_create_user(
                &self,
                email: &str,
            ) -> gestalt::Result<gestalt::StoredUser> {
                let select_sql = format!(
                    "SELECT id, email, display_name, created_at, updated_at FROM users WHERE email = {}",
                    self.dialect.placeholder(1)
                );
                if let Some(row) = sqlx::query(&select_sql)
                    .bind(email)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|error| gestalt::Error::internal(format!("querying user: {error}")))?
                {
                    return self.scan_user_row(row);
                }

                let now = now_timestamp();
                let now_datetime = required_timestamp("now", &Some(now.clone()))?;
                let user = gestalt::StoredUser {
                    id: Uuid::new_v4().to_string(),
                    email: email.to_string(),
                    display_name: String::new(),
                    created_at: Some(now.clone()),
                    updated_at: Some(now),
                };

                let insert_sql = format!(
                    "INSERT INTO users (id, email, display_name, created_at, updated_at) VALUES ({}, {}, {}, {}, {})",
                    self.dialect.placeholder(1),
                    self.dialect.placeholder(2),
                    self.dialect.placeholder(3),
                    self.dialect.placeholder(4),
                    self.dialect.placeholder(5)
                );

                let insert = sqlx::query(&insert_sql)
                    .bind(&user.id)
                    .bind(&user.email)
                    .bind(&user.display_name)
                    .bind(now_datetime)
                    .bind(now_datetime)
                    .execute(&self.pool)
                    .await;
                match insert {
                    Ok(_) => Ok(user),
                    Err(error) if self.dialect.is_duplicate_key_error(&error) => {
                        let row = sqlx::query(&select_sql)
                            .bind(email)
                            .fetch_one(&self.pool)
                            .await
                            .map_err(|requery_error| {
                                gestalt::Error::internal(format!(
                                    "re-querying user after duplicate key: {requery_error}"
                                ))
                            })?;
                        self.scan_user_row(row)
                    }
                    Err(error) => Err(gestalt::Error::internal(format!(
                        "inserting user: {error}"
                    ))),
                }
            }

            pub async fn put_integration_token(
                &self,
                token: gestalt::StoredIntegrationToken,
            ) -> gestalt::Result<()> {
                let (created_at, updated_at) =
                    default_created_updated(token.created_at.clone(), token.updated_at.clone());
                let created_at = required_timestamp("created_at", &Some(created_at))?;
                let updated_at = required_timestamp("updated_at", &Some(updated_at))?;
                let expires_at = timestamp_to_datetime("expires_at", &token.expires_at)?;
                let last_refreshed_at =
                    timestamp_to_datetime("last_refreshed_at", &token.last_refreshed_at)?;
                let metadata_json = connection_params_to_json(&token.connection_params)?;
                let connection = self.dialect.normalize_connection(&token.connection);

                sqlx::query(&self.dialect.upsert_token_sql())
                    .bind(&token.id)
                    .bind(&token.user_id)
                    .bind(&token.integration)
                    .bind(connection)
                    .bind(&token.instance)
                    .bind(encode_sealed(&token.access_token_sealed))
                    .bind(encode_sealed(&token.refresh_token_sealed))
                    .bind(&token.scopes)
                    .bind(expires_at)
                    .bind(last_refreshed_at)
                    .bind(token.refresh_error_count)
                    .bind(metadata_json)
                    .bind(created_at)
                    .bind(updated_at)
                    .execute(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!("upserting integration token: {error}"))
                    })?;
                Ok(())
            }

            pub async fn get_integration_token(
                &self,
                user_id: &str,
                integration: &str,
                connection: &str,
                instance: &str,
            ) -> gestalt::Result<Option<gestalt::StoredIntegrationToken>> {
                let connection = self.dialect.normalize_connection(connection);
                let sql = format!(
                    "SELECT id, user_id, integration, connection, instance, access_token_encrypted, refresh_token_encrypted, scopes, expires_at, last_refreshed_at, refresh_error_count, metadata_json, created_at, updated_at FROM integration_tokens WHERE user_id = {} AND integration = {} AND connection = {} AND instance = {}",
                    self.dialect.placeholder(1),
                    self.dialect.placeholder(2),
                    self.dialect.placeholder(3),
                    self.dialect.placeholder(4)
                );
                let row = sqlx::query(&sql)
                    .bind(user_id)
                    .bind(integration)
                    .bind(connection)
                    .bind(instance)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|error| gestalt::Error::internal(format!("querying token: {error}")))?;
                row.map(|row| self.scan_integration_token_row(row)).transpose()
            }

            pub async fn list_integration_tokens(
                &self,
                user_id: &str,
                integration: &str,
                connection: &str,
            ) -> gestalt::Result<Vec<gestalt::StoredIntegrationToken>> {
                let mut sql = format!(
                    "SELECT id, user_id, integration, connection, instance, access_token_encrypted, refresh_token_encrypted, scopes, expires_at, last_refreshed_at, refresh_error_count, metadata_json, created_at, updated_at FROM integration_tokens WHERE user_id = {}",
                    self.dialect.placeholder(1)
                );
                let mut next = 2;

                if !integration.is_empty() {
                    sql.push_str(&format!(
                        " AND integration = {}",
                        self.dialect.placeholder(next)
                    ));
                    next += 1;
                }
                if !connection.is_empty() {
                    sql.push_str(&format!(
                        " AND connection = {}",
                        self.dialect.placeholder(next)
                    ));
                }

                let mut query = sqlx::query(&sql).bind(user_id.to_string());
                if !integration.is_empty() {
                    query = query.bind(integration.to_string());
                }
                if !connection.is_empty() {
                    query = query.bind(self.dialect.normalize_connection(connection));
                }

                let rows = query
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!("listing integration tokens: {error}"))
                    })?;
                rows.into_iter()
                    .map(|row| self.scan_integration_token_row(row))
                    .collect()
            }

            pub async fn delete_integration_token(&self, id: &str) -> gestalt::Result<()> {
                let sql = format!(
                    "DELETE FROM integration_tokens WHERE id = {}",
                    self.dialect.placeholder(1)
                );
                sqlx::query(&sql)
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!("deleting integration token: {error}"))
                    })?;
                Ok(())
            }

            pub async fn put_api_token(
                &self,
                token: gestalt::StoredApiToken,
            ) -> gestalt::Result<()> {
                let (created_at, updated_at) =
                    default_created_updated(token.created_at.clone(), token.updated_at.clone());
                let created_at = required_timestamp("created_at", &Some(created_at))?;
                let updated_at = required_timestamp("updated_at", &Some(updated_at))?;
                let expires_at = timestamp_to_datetime("expires_at", &token.expires_at)?;
                let sql = format!(
                    "INSERT INTO api_tokens (id, user_id, name, hashed_token, scopes, expires_at, created_at, updated_at) VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
                    self.dialect.placeholder(1),
                    self.dialect.placeholder(2),
                    self.dialect.placeholder(3),
                    self.dialect.placeholder(4),
                    self.dialect.placeholder(5),
                    self.dialect.placeholder(6),
                    self.dialect.placeholder(7),
                    self.dialect.placeholder(8)
                );
                sqlx::query(&sql)
                    .bind(&token.id)
                    .bind(&token.user_id)
                    .bind(&token.name)
                    .bind(&token.hashed_token)
                    .bind(&token.scopes)
                    .bind(expires_at)
                    .bind(created_at)
                    .bind(updated_at)
                    .execute(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!("inserting api token: {error}"))
                    })?;
                Ok(())
            }

            pub async fn get_api_token_by_hash(
                &self,
                hashed_token: &str,
            ) -> gestalt::Result<Option<gestalt::StoredApiToken>> {
                let sql = format!(
                    "SELECT id, user_id, name, hashed_token, scopes, expires_at, created_at, updated_at FROM api_tokens WHERE hashed_token = {} AND (expires_at IS NULL OR expires_at > {})",
                    self.dialect.placeholder(1),
                    self.dialect.placeholder(2)
                );
                let row = sqlx::query(&sql)
                    .bind(hashed_token)
                    .bind(Utc::now())
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!(
                            "getting api token by hash: {error}"
                        ))
                    })?;
                row.map(|row| self.scan_api_token_row(row)).transpose()
            }

            pub async fn list_api_tokens(
                &self,
                user_id: &str,
            ) -> gestalt::Result<Vec<gestalt::StoredApiToken>> {
                let sql = format!(
                    "SELECT id, user_id, name, hashed_token, scopes, expires_at, created_at, updated_at FROM api_tokens WHERE user_id = {}",
                    self.dialect.placeholder(1)
                );
                let rows = sqlx::query(&sql)
                    .bind(user_id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!("listing api tokens: {error}"))
                    })?;
                rows.into_iter().map(|row| self.scan_api_token_row(row)).collect()
            }

            pub async fn revoke_api_token(
                &self,
                user_id: &str,
                id: &str,
            ) -> gestalt::Result<()> {
                let sql = format!(
                    "DELETE FROM api_tokens WHERE id = {} AND user_id = {}",
                    self.dialect.placeholder(1),
                    self.dialect.placeholder(2)
                );
                let result = sqlx::query(&sql)
                    .bind(id)
                    .bind(user_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!("revoking api token: {error}"))
                    })?;
                if result.rows_affected() == 0 {
                    return Err(gestalt::Error::not_found(format!(
                        "api token {id} for user {user_id} not found"
                    )));
                }
                Ok(())
            }

            pub async fn revoke_all_api_tokens(&self, user_id: &str) -> gestalt::Result<i64> {
                let sql = format!(
                    "DELETE FROM api_tokens WHERE user_id = {}",
                    self.dialect.placeholder(1)
                );
                let result = sqlx::query(&sql)
                    .bind(user_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!(
                            "revoking all api tokens: {error}"
                        ))
                    })?;
                Ok(result.rows_affected() as i64)
            }

            pub async fn get_oauth_registration(
                &self,
                auth_server_url: &str,
                redirect_uri: &str,
            ) -> gestalt::Result<Option<gestalt::OAuthRegistration>> {
                let sql = format!(
                    "SELECT auth_server_url, redirect_uri, client_id, client_secret_encrypted, expires_at, authorization_endpoint, token_endpoint, scopes_supported, discovered_at FROM oauth_registrations WHERE auth_server_url = {} AND redirect_uri = {}",
                    self.dialect.placeholder(1),
                    self.dialect.placeholder(2)
                );
                let row = sqlx::query(&sql)
                    .bind(auth_server_url)
                    .bind(redirect_uri)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!(
                            "querying oauth registration: {error}"
                        ))
                    })?;
                let Some(row) = row else {
                    return Ok(None);
                };

                let client_secret: String = row.try_get("client_secret_encrypted").map_err(|error| {
                    gestalt::Error::internal(format!("read client_secret_encrypted: {error}"))
                })?;
                let expires_at: Option<DateTime<Utc>> = row.try_get("expires_at").map_err(|error| {
                    gestalt::Error::internal(format!("read expires_at: {error}"))
                })?;
                let discovered_at: DateTime<Utc> = row.try_get("discovered_at").map_err(|error| {
                    gestalt::Error::internal(format!("read discovered_at: {error}"))
                })?;
                Ok(Some(gestalt::OAuthRegistration {
                    auth_server_url: row.try_get("auth_server_url").map_err(|error| {
                        gestalt::Error::internal(format!("read auth_server_url: {error}"))
                    })?,
                    redirect_uri: row.try_get("redirect_uri").map_err(|error| {
                        gestalt::Error::internal(format!("read redirect_uri: {error}"))
                    })?,
                    client_id: row
                        .try_get("client_id")
                        .map_err(|error| gestalt::Error::internal(format!("read client_id: {error}")))?,
                    client_secret_sealed: decode_sealed(&client_secret)?,
                    expires_at: expires_at.map(datetime_to_timestamp),
                    authorization_endpoint: row.try_get("authorization_endpoint").map_err(
                        |error| {
                            gestalt::Error::internal(format!(
                                "read authorization_endpoint: {error}"
                            ))
                        },
                    )?,
                    token_endpoint: row.try_get("token_endpoint").map_err(|error| {
                        gestalt::Error::internal(format!("read token_endpoint: {error}"))
                    })?,
                    scopes_supported: row.try_get("scopes_supported").map_err(|error| {
                        gestalt::Error::internal(format!("read scopes_supported: {error}"))
                    })?,
                    discovered_at: Some(datetime_to_timestamp(discovered_at)),
                }))
            }

            pub async fn put_oauth_registration(
                &self,
                registration: gestalt::OAuthRegistration,
            ) -> gestalt::Result<()> {
                let now = Utc::now().with_nanosecond(0).expect("truncate nanos");
                let expires_at = timestamp_to_datetime("expires_at", &registration.expires_at)?;
                let discovered_at =
                    required_timestamp("discovered_at", &registration.discovered_at)?;

                let update_sql = format!(
                    "UPDATE oauth_registrations SET client_id = {}, client_secret_encrypted = {}, expires_at = {}, authorization_endpoint = {}, token_endpoint = {}, scopes_supported = {}, discovered_at = {}, updated_at = {} WHERE auth_server_url = {} AND redirect_uri = {}",
                    self.dialect.placeholder(1),
                    self.dialect.placeholder(2),
                    self.dialect.placeholder(3),
                    self.dialect.placeholder(4),
                    self.dialect.placeholder(5),
                    self.dialect.placeholder(6),
                    self.dialect.placeholder(7),
                    self.dialect.placeholder(8),
                    self.dialect.placeholder(9),
                    self.dialect.placeholder(10)
                );

                let result = sqlx::query(&update_sql)
                    .bind(&registration.client_id)
                    .bind(encode_sealed(&registration.client_secret_sealed))
                    .bind(expires_at)
                    .bind(&registration.authorization_endpoint)
                    .bind(&registration.token_endpoint)
                    .bind(&registration.scopes_supported)
                    .bind(discovered_at)
                    .bind(now)
                    .bind(&registration.auth_server_url)
                    .bind(&registration.redirect_uri)
                    .execute(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!(
                            "updating oauth registration: {error}"
                        ))
                    })?;

                if result.rows_affected() == 0 {
                    let insert_sql = format!(
                        "INSERT INTO oauth_registrations (id, auth_server_url, redirect_uri, client_id, client_secret_encrypted, expires_at, authorization_endpoint, token_endpoint, scopes_supported, discovered_at, created_at, updated_at) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                        self.dialect.placeholder(1),
                        self.dialect.placeholder(2),
                        self.dialect.placeholder(3),
                        self.dialect.placeholder(4),
                        self.dialect.placeholder(5),
                        self.dialect.placeholder(6),
                        self.dialect.placeholder(7),
                        self.dialect.placeholder(8),
                        self.dialect.placeholder(9),
                        self.dialect.placeholder(10),
                        self.dialect.placeholder(11),
                        self.dialect.placeholder(12)
                    );
                    sqlx::query(&insert_sql)
                        .bind(Uuid::new_v4().to_string())
                        .bind(&registration.auth_server_url)
                        .bind(&registration.redirect_uri)
                        .bind(&registration.client_id)
                        .bind(encode_sealed(&registration.client_secret_sealed))
                        .bind(timestamp_to_datetime("expires_at", &registration.expires_at)?)
                        .bind(&registration.authorization_endpoint)
                        .bind(&registration.token_endpoint)
                        .bind(&registration.scopes_supported)
                        .bind(required_timestamp("discovered_at", &registration.discovered_at)?)
                        .bind(now)
                        .bind(now)
                        .execute(&self.pool)
                        .await
                        .map_err(|error| {
                            gestalt::Error::internal(format!(
                                "inserting oauth registration: {error}"
                            ))
                        })?;
                }

                Ok(())
            }

            pub async fn delete_oauth_registration(
                &self,
                auth_server_url: &str,
                redirect_uri: &str,
            ) -> gestalt::Result<()> {
                let sql = format!(
                    "DELETE FROM oauth_registrations WHERE auth_server_url = {} AND redirect_uri = {}",
                    self.dialect.placeholder(1),
                    self.dialect.placeholder(2)
                );
                sqlx::query(&sql)
                    .bind(auth_server_url)
                    .bind(redirect_uri)
                    .execute(&self.pool)
                    .await
                    .map_err(|error| {
                        gestalt::Error::internal(format!(
                            "deleting oauth registration: {error}"
                        ))
                    })?;
                Ok(())
            }
        }
    };
}

impl_sql_store!(
    SqliteStore,
    sqlx::SqlitePool,
    sqlx::sqlite::SqliteRow,
    SqliteDialect
);
impl_sql_store!(
    PostgresStore,
    sqlx::PgPool,
    sqlx::postgres::PgRow,
    PostgresDialect
);
impl_sql_store!(
    MySqlStore,
    sqlx::MySqlPool,
    sqlx::mysql::MySqlRow,
    MySqlDialect
);

pub async fn connect_sqlite(path: &str) -> gestalt::Result<SqliteStore> {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")
        .map_err(|error| gestalt::Error::internal(format!("opening sqlite options: {error}")))?
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(5000))
        .synchronous(SqliteSynchronous::Normal);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| gestalt::Error::internal(format!("opening sqlite: {error}")))?;
    Ok(SqliteStore::new(pool, SqliteDialect))
}

pub async fn connect_postgres(dsn: &str) -> gestalt::Result<PostgresStore> {
    let pool = PgPoolOptions::new()
        .max_connections(25)
        .acquire_timeout(Duration::from_secs(30))
        .max_lifetime(Some(Duration::from_secs(300)))
        .connect(dsn)
        .await
        .map_err(|error| gestalt::Error::internal(format!("opening postgres: {error}")))?;
    resolve_postgres_version(&pool, "").await?;
    Ok(PostgresStore::new(pool, PostgresDialect))
}

pub async fn connect_mysql(dsn: &str, requested_version: &str) -> gestalt::Result<MySqlStore> {
    let options = mysql_connect_options_from_dsn(dsn)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(25)
        .acquire_timeout(Duration::from_secs(30))
        .max_lifetime(Some(Duration::from_secs(300)))
        .connect_with(options)
        .await
        .map_err(|error| gestalt::Error::internal(format!("opening mysql: {error}")))?;
    let version = resolve_mysql_version(&pool, requested_version).await?;
    Ok(MySqlStore::new(pool, MySqlDialect::new(version)))
}

pub async fn migrate_sqlite(store: &SqliteStore) -> gestalt::Result<()> {
    store.migrate(SQLITE_MIGRATIONS).await
}

pub async fn migrate_postgres(store: &PostgresStore) -> gestalt::Result<()> {
    store.migrate(POSTGRES_MIGRATIONS).await
}

pub async fn migrate_mysql(store: &MySqlStore) -> gestalt::Result<()> {
    store.migrate(MYSQL_MIGRATIONS).await
}

async fn resolve_mysql_version(
    pool: &MySqlPool,
    requested_version: &str,
) -> gestalt::Result<String> {
    let raw = sqlx::query_scalar::<_, String>("SELECT VERSION()")
        .fetch_one(pool)
        .await
        .map_err(|error| gestalt::Error::internal(format!("mysql: detecting version: {error}")))?;
    if raw.to_ascii_lowercase().contains("mariadb") {
        return Err(gestalt::Error::internal(format!(
            "mysql: MariaDB is not supported ({raw})"
        )));
    }

    let mut parts = raw.split('.');
    let major = parts.next().ok_or_else(|| {
        gestalt::Error::internal(format!("mysql: parsing server version {raw:?}"))
    })?;
    let minor = parts.next().ok_or_else(|| {
        gestalt::Error::internal(format!("mysql: parsing server version {raw:?}"))
    })?;
    validate_detected_version(
        "mysql",
        requested_version,
        &["8.0", "8.4", "9.6"],
        format!("{major}.{minor}"),
        raw,
    )
}

async fn resolve_postgres_version(
    pool: &PgPool,
    requested_version: &str,
) -> gestalt::Result<String> {
    let raw = sqlx::query_scalar::<_, String>("SHOW server_version_num")
        .fetch_one(pool)
        .await
        .map_err(|error| {
            gestalt::Error::internal(format!("postgres: detecting version: {error}"))
        })?;
    let version_num: i32 = raw.parse().map_err(|error| {
        gestalt::Error::internal(format!(
            "postgres: parsing server_version_num {raw:?}: {error}"
        ))
    })?;
    validate_detected_version(
        "postgres",
        requested_version,
        &["15", "16", "17", "18"],
        (version_num / 10000).to_string(),
        raw,
    )
}

fn mysql_connect_options_from_dsn(dsn: &str) -> gestalt::Result<MySqlConnectOptions> {
    if dsn.starts_with("mysql://") {
        return MySqlConnectOptions::from_str(dsn)
            .map_err(|error| gestalt::Error::internal(format!("parsing dsn: {error}")));
    }
    mysql_connect_options_from_legacy_dsn(dsn)
}

fn mysql_connect_options_from_legacy_dsn(dsn: &str) -> gestalt::Result<MySqlConnectOptions> {
    let (main, query) = match dsn.split_once('?') {
        Some((main, query)) => (main, Some(query)),
        None => (dsn, None),
    };
    let slash = mysql_dsn_database_separator(main)
        .ok_or_else(|| gestalt::Error::internal("parsing dsn: missing database name"))?;
    let (head, database) = (&main[..slash], &main[slash + 1..]);
    if database.is_empty() {
        return Err(gestalt::Error::internal(
            "parsing dsn: missing database name",
        ));
    }

    let (auth, network) = mysql_dsn_auth_split(head);
    let mut options = MySqlConnectOptions::new().database(database);
    if let Some(auth) = auth {
        let (username, password) = match auth.split_once(':') {
            Some((username, password)) => (username, Some(password)),
            None => (auth, None),
        };
        if !username.is_empty() {
            options = options.username(username);
        }
        if let Some(password) = password {
            options = options.password(password);
        }
    }
    options = apply_legacy_mysql_network(options, network)?;
    options = apply_legacy_mysql_query(options, query)?;
    Ok(options)
}

fn mysql_dsn_database_separator(dsn: &str) -> Option<usize> {
    let mut depth = 0usize;
    for (index, ch) in dsn.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            '/' if depth == 0 => return Some(index),
            _ => {}
        }
    }
    None
}

fn mysql_dsn_auth_split(dsn: &str) -> (Option<&str>, &str) {
    let mut depth = 0usize;
    let mut split = None;
    for (index, ch) in dsn.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            '@' if depth == 0 => split = Some(index),
            _ => {}
        }
    }
    match split {
        Some(index) => (Some(&dsn[..index]), &dsn[index + 1..]),
        None => (None, dsn),
    }
}

fn apply_legacy_mysql_network(
    mut options: MySqlConnectOptions,
    network: &str,
) -> gestalt::Result<MySqlConnectOptions> {
    if network.is_empty() {
        return Ok(options);
    }

    let (kind, address) = match network.split_once('(') {
        Some((kind, address)) => {
            let address = address.strip_suffix(')').ok_or_else(|| {
                gestalt::Error::internal("parsing dsn: malformed network address")
            })?;
            (kind, Some(address))
        }
        None => (network, None),
    };

    match kind {
        "tcp" => {
            if let Some(address) = address {
                let (host, port) = split_mysql_tcp_address(address)?;
                options = options.host(host);
                if let Some(port) = port {
                    options = options.port(port);
                }
            }
            Ok(options)
        }
        "unix" => {
            let address = address.ok_or_else(|| {
                gestalt::Error::internal("parsing dsn: unix network requires a socket path")
            })?;
            Ok(options.socket(address))
        }
        other => Err(gestalt::Error::internal(format!(
            "parsing dsn: unsupported network {other:?}"
        ))),
    }
}

fn split_mysql_tcp_address(address: &str) -> gestalt::Result<(&str, Option<u16>)> {
    if let Some(rest) = address.strip_prefix('[') {
        let (host, remainder) = rest
            .split_once(']')
            .ok_or_else(|| gestalt::Error::internal("parsing dsn: malformed tcp address"))?;
        if remainder.is_empty() {
            return Ok((host, None));
        }
        let port = remainder
            .strip_prefix(':')
            .ok_or_else(|| gestalt::Error::internal("parsing dsn: malformed tcp address"))?;
        return Ok((host, Some(parse_mysql_port(port)?)));
    }
    if let Some((host, port)) = address.rsplit_once(':') {
        if port.chars().all(|ch| ch.is_ascii_digit()) {
            return Ok((host, Some(parse_mysql_port(port)?)));
        }
    }
    Ok((address, None))
}

fn parse_mysql_port(port: &str) -> gestalt::Result<u16> {
    port.parse().map_err(|error| {
        gestalt::Error::internal(format!("parsing dsn: invalid port {port:?}: {error}"))
    })
}

fn apply_legacy_mysql_query(
    mut options: MySqlConnectOptions,
    query: Option<&str>,
) -> gestalt::Result<MySqlConnectOptions> {
    let Some(query) = query else {
        return Ok(options);
    };
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((key, value)) => (key, value),
            None => (pair, ""),
        };
        match key {
            "charset" => options = options.charset(value),
            "collation" => options = options.collation(value),
            "socket" => options = options.socket(value),
            "tls" => {
                options = options.ssl_mode(mysql_ssl_mode_from_legacy_value(value));
            }
            _ => {}
        }
    }
    Ok(options)
}

fn mysql_ssl_mode_from_legacy_value(value: &str) -> MySqlSslMode {
    match value {
        "false" | "disabled" => MySqlSslMode::Disabled,
        "true" | "required" | "skip-verify" => MySqlSslMode::Required,
        "verify-ca" => MySqlSslMode::VerifyCa,
        "verify-identity" => MySqlSslMode::VerifyIdentity,
        _ => MySqlSslMode::Preferred,
    }
}

#[cfg(test)]
mod tests {
    use super::mysql_connect_options_from_dsn;
    use sqlx::mysql::MySqlSslMode;

    #[test]
    fn parses_mysql_url_dsn() {
        let options =
            mysql_connect_options_from_dsn("mysql://alice:secret@db.internal:3307/gestalt")
                .expect("parse mysql url dsn");

        assert_eq!(options.get_host(), "db.internal");
        assert_eq!(options.get_port(), 3307);
        assert_eq!(options.get_username(), "alice");
        assert_eq!(options.get_database(), Some("gestalt"));
        assert!(options.get_socket().is_none());
    }

    #[test]
    fn parses_legacy_mysql_unix_socket_dsn() {
        let options = mysql_connect_options_from_dsn(
            "alice:secret@unix(/cloudsql/project:region:instance)/gestalt",
        )
        .expect("parse legacy unix mysql dsn");

        assert_eq!(options.get_username(), "alice");
        assert_eq!(options.get_database(), Some("gestalt"));
        assert_eq!(
            options.get_socket().and_then(|path| path.to_str()),
            Some("/cloudsql/project:region:instance")
        );
    }

    #[test]
    fn parses_legacy_mysql_tcp_dsn_with_query_options() {
        let options = mysql_connect_options_from_dsn(
            "alice:secret@tcp(db.internal:3307)/gestalt?charset=utf8mb4&tls=required",
        )
        .expect("parse legacy tcp mysql dsn");

        assert_eq!(options.get_host(), "db.internal");
        assert_eq!(options.get_port(), 3307);
        assert_eq!(options.get_database(), Some("gestalt"));
        assert_eq!(options.get_charset(), "utf8mb4");
        assert!(matches!(options.get_ssl_mode(), MySqlSslMode::Required));
    }
}
