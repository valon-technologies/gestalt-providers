use std::collections::HashMap;
use std::sync::Mutex;

use gestalt::proto::v1::{ColumnDef, IndexSchema, ObjectStoreSchema};
use sqlx::AnyPool;
use sqlx::Row;
use tokio::sync::RwLock;
use tonic::Status;

use crate::schema;

/// Execute a simple query on the pool (resolves AnyPool type inference).
async fn exec(pool: &AnyPool, sql: &str) -> Result<sqlx::any::AnyQueryResult, sqlx::Error> {
    sqlx::query(sql).execute(pool).await
}

async fn exec_bind1(pool: &AnyPool, sql: &str, val: &str) -> Result<sqlx::any::AnyQueryResult, sqlx::Error> {
    sqlx::query(sql).bind(val).execute(pool).await
}

async fn exec_bind2(pool: &AnyPool, sql: &str, v1: &str, v2: &str) -> Result<sqlx::any::AnyQueryResult, sqlx::Error> {
    sqlx::query(sql).bind(v1).bind(v2).execute(pool).await
}

async fn fetch_all(pool: &AnyPool, sql: &str) -> Result<Vec<sqlx::any::AnyRow>, sqlx::Error> {
    sqlx::query(sql).fetch_all(pool).await
}

async fn fetch_one(pool: &AnyPool, sql: &str) -> Result<sqlx::any::AnyRow, sqlx::Error> {
    sqlx::query(sql).fetch_one(pool).await
}

/// Cached metadata for a single object store (SQL table).
#[derive(Debug, Clone)]
pub struct StoreMeta {
    pub table_name: String,
    pub pk_column: String,
    pub columns: Vec<ColumnDef>,
    pub indexes: Vec<IndexSchema>,
}

impl StoreMeta {
    fn from_schema(name: &str, schema: &ObjectStoreSchema) -> Self {
        let pk_column = schema
            .columns
            .iter()
            .find(|c| c.primary_key)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| "id".to_string());

        Self {
            table_name: name.to_string(),
            pk_column,
            columns: schema.columns.clone(),
            indexes: schema.indexes.clone(),
        }
    }
}

/// Core state shared across all gRPC handlers.
pub struct SqlStore {
    pool: Mutex<Option<AnyPool>>,
    schemas: RwLock<HashMap<String, StoreMeta>>,
}

impl SqlStore {
    pub fn new() -> Self {
        Self {
            pool: Mutex::new(None),
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Connect to the database using the given DSN, create the metadata table,
    /// and load any existing object store schemas into the in-memory cache.
    pub async fn connect(&self, dsn: &str) -> Result<(), Status> {
        sqlx::any::install_default_drivers();

        let pool: AnyPool = AnyPool::connect(dsn)
            .await
            .map_err(|e| Status::internal(format!("connect: {e}")))?;

        // Create the metadata table.
        exec(
            &pool,
            "CREATE TABLE IF NOT EXISTS \"_gestalt_object_stores\" (\
                \"name\" TEXT NOT NULL PRIMARY KEY, \
                \"schema_json\" TEXT NOT NULL\
            )",
        )
        .await
        .map_err(|e| Status::internal(format!("create metadata table: {e}")))?;

        // Load existing schemas into cache.
        let rows = fetch_all(
            &pool,
            "SELECT \"name\", \"schema_json\" FROM \"_gestalt_object_stores\"",
        )
        .await
        .map_err(|e| Status::internal(format!("load metadata: {e}")))?;

        let mut cache = self.schemas.write().await;
        for row in &rows {
            let name: String = row
                .try_get("name")
                .map_err(|e| Status::internal(format!("read name: {e}")))?;
            let json: String = row
                .try_get("schema_json")
                .map_err(|e| Status::internal(format!("read schema_json: {e}")))?;
            if let Ok(schema) = serde_json::from_str::<SchemaJson>(&json) {
                let os = schema.to_object_store_schema();
                cache.insert(name.clone(), StoreMeta::from_schema(&name, &os));
            }
        }

        *self.pool.lock().unwrap() = Some(pool);
        Ok(())
    }

    /// Get a clone of the connection pool.
    pub fn pool(&self) -> Result<AnyPool, Status> {
        self.pool
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| Status::failed_precondition("not configured"))
    }

    /// Get cached metadata for an object store.
    pub async fn get_meta(&self, name: &str) -> Result<StoreMeta, Status> {
        self.schemas
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("object store not found: {name}")))
    }

    /// Create a new object store (SQL table + indexes).
    pub async fn create_object_store(
        &self,
        name: &str,
        schema: &ObjectStoreSchema,
    ) -> Result<(), Status> {
        let pool = self.pool()?;

        // Check if already exists with same schema — idempotent.
        if self.schemas.read().await.contains_key(name) {
            return Ok(());
        }

        // Create the table.
        let ddl = schema::create_table_sql(name, schema);
        exec(&pool, &ddl)
            .await
            .map_err(|e| Status::internal(format!("create table: {e}")))?;

        // Create indexes.
        for index in &schema.indexes {
            let idx_sql = schema::create_index_sql(name, index);
            exec(&pool, &idx_sql)
                .await
                .map_err(|e| Status::internal(format!("create index: {e}")))?;
        }

        // Store metadata.
        let schema_json =
            serde_json::to_string(&SchemaJson::from_object_store_schema(schema))
                .map_err(|e| Status::internal(format!("serialize schema: {e}")))?;

        exec_bind2(
            &pool,
            "INSERT INTO \"_gestalt_object_stores\" (\"name\", \"schema_json\") VALUES ($1, $2)",
            name,
            &schema_json,
        )
        .await
        .or_else(|e| {
            // Ignore duplicate — idempotent.
            let msg = e.to_string().to_lowercase();
            if msg.contains("unique") || msg.contains("duplicate") || msg.contains("constraint") {
                Ok(Default::default())
            } else {
                Err(Status::internal(format!("insert metadata: {e}")))
            }
        })?;

        // Update cache.
        self.schemas
            .write()
            .await
            .insert(name.to_string(), StoreMeta::from_schema(name, schema));

        Ok(())
    }

    /// Delete an object store (DROP TABLE + metadata cleanup).
    pub async fn delete_object_store(&self, name: &str) -> Result<(), Status> {
        let pool = self.pool()?;

        let ddl = schema::drop_table_sql(name);
        exec(&pool, &ddl)
            .await
            .map_err(|e| Status::internal(format!("drop table: {e}")))?;

        exec_bind1(
            &pool,
            "DELETE FROM \"_gestalt_object_stores\" WHERE \"name\" = $1",
            name,
        )
        .await
        .map_err(|e| Status::internal(format!("delete metadata: {e}")))?;

        self.schemas.write().await.remove(name);
        Ok(())
    }

    /// Health check — run a trivial query.
    pub async fn health_check(&self) -> Result<(), Status> {
        let pool = self.pool()?;
        fetch_one(&pool, "SELECT 1")
            .await
            .map_err(|e| Status::internal(format!("health check: {e}")))?;
        Ok(())
    }

    pub fn close(&self) {
        // Take the pool out of the mutex and drop it to release connections.
        let _ = self.pool.lock().unwrap().take();
    }
}

// ---- JSON schema serialization for metadata table ----

#[derive(serde::Serialize, serde::Deserialize)]
struct SchemaJson {
    columns: Vec<ColumnJson>,
    indexes: Vec<IndexJson>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ColumnJson {
    name: String,
    r#type: i32,
    primary_key: bool,
    not_null: bool,
    unique: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct IndexJson {
    name: String,
    key_path: Vec<String>,
    unique: bool,
}

impl SchemaJson {
    fn from_object_store_schema(schema: &ObjectStoreSchema) -> Self {
        Self {
            columns: schema
                .columns
                .iter()
                .map(|c| ColumnJson {
                    name: c.name.clone(),
                    r#type: c.r#type,
                    primary_key: c.primary_key,
                    not_null: c.not_null,
                    unique: c.unique,
                })
                .collect(),
            indexes: schema
                .indexes
                .iter()
                .map(|i| IndexJson {
                    name: i.name.clone(),
                    key_path: i.key_path.clone(),
                    unique: i.unique,
                })
                .collect(),
        }
    }

    fn to_object_store_schema(&self) -> ObjectStoreSchema {
        ObjectStoreSchema {
            columns: self
                .columns
                .iter()
                .map(|c| ColumnDef {
                    name: c.name.clone(),
                    r#type: c.r#type,
                    primary_key: c.primary_key,
                    not_null: c.not_null,
                    unique: c.unique,
                })
                .collect(),
            indexes: self
                .indexes
                .iter()
                .map(|i| IndexSchema {
                    name: i.name.clone(),
                    key_path: i.key_path.clone(),
                    unique: i.unique,
                })
                .collect(),
        }
    }
}
