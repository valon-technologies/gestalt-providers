mod convert;
mod lifecycle;
mod query;
mod schema;
mod server;
pub mod store;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use gestalt::proto::v1::indexed_db_server::IndexedDbServer;
use gestalt::proto::v1::provider_lifecycle_server::ProviderLifecycleServer;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use crate::lifecycle::LifecycleServer;
use crate::server::IndexedDbGrpcServer;
use crate::store::SqlStore;

const ENV_PROVIDER_SOCKET: &str = "GESTALT_PLUGIN_SOCKET";
const ENV_PROVIDER_PARENT_PID: &str = "GESTALT_PLUGIN_PARENT_PID";

pub fn __gestalt_serve_indexeddb(_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(serve())?;
    Ok(())
}

async fn serve() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let socket = std::env::var(ENV_PROVIDER_SOCKET)
        .map_err(|_| format!("{ENV_PROVIDER_SOCKET} is required"))?;
    let socket = PathBuf::from(socket);

    if socket.exists() {
        std::fs::remove_file(&socket)?;
    }
    if let Some(parent) = socket.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let store = Arc::new(SqlStore::new());
    let lifecycle = LifecycleServer::new(Arc::clone(&store));
    let indexeddb = IndexedDbGrpcServer::new(Arc::clone(&store));

    let listener = UnixListener::bind(&socket)?;
    let incoming = UnixListenerStream::new(listener);

    Server::builder()
        .add_service(ProviderLifecycleServer::new(lifecycle))
        .add_service(IndexedDbServer::new(indexeddb))
        .serve_with_incoming_shutdown(incoming, shutdown_signal())
        .await?;

    store.close();
    let _ = std::fs::remove_file(&socket);

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    tokio::pin!(ctrl_c);

    if let Some(parent_pid) = parent_pid() {
        tokio::select! {
            _ = &mut ctrl_c => {}
            _ = watch_parent(parent_pid) => {}
        }
        return;
    }

    ctrl_c.await;
}

fn parent_pid() -> Option<u32> {
    std::env::var(ENV_PROVIDER_PARENT_PID)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|pid| *pid > 0)
}

async fn watch_parent(parent_pid: u32) {
    loop {
        if current_parent_pid() != parent_pid {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[cfg(unix)]
fn current_parent_pid() -> u32 {
    unsafe { libc::getppid() as u32 }
}

#[cfg(not(unix))]
fn current_parent_pid() -> u32 {
    0
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use gestalt::proto::v1::{ColumnDef, IndexSchema, ObjectStoreSchema};
    use prost_types::value::Kind;
    use prost_types::{Struct, Value};
    use sqlx::AnyPool;
    use sqlx::any::AnyRow;
    use sqlx::Row;

    use crate::convert::SqlValue;
    use crate::store::SqlStore;

    fn string_val(s: &str) -> Value {
        Value {
            kind: Some(Kind::StringValue(s.to_string())),
        }
    }

    fn users_schema() -> ObjectStoreSchema {
        ObjectStoreSchema {
            indexes: vec![IndexSchema {
                name: "by_email".into(),
                key_path: vec!["email".into()],
                unique: true,
            }],
            columns: vec![
                ColumnDef { name: "id".into(), r#type: 0, primary_key: true, not_null: false, unique: false },
                ColumnDef { name: "email".into(), r#type: 0, primary_key: false, not_null: true, unique: true },
                ColumnDef { name: "display_name".into(), r#type: 0, primary_key: false, not_null: false, unique: false },
                ColumnDef { name: "created_at".into(), r#type: 4, primary_key: false, not_null: false, unique: false },
                ColumnDef { name: "updated_at".into(), r#type: 4, primary_key: false, not_null: false, unique: false },
            ],
        }
    }

    fn make_user(id: &str, email: &str, name: &str) -> Struct {
        let mut fields = BTreeMap::new();
        fields.insert("id".into(), string_val(id));
        fields.insert("email".into(), string_val(email));
        fields.insert("display_name".into(), string_val(name));
        fields.insert("created_at".into(), string_val("2024-01-01T00:00:00Z"));
        fields.insert("updated_at".into(), string_val("2024-01-01T00:00:00Z"));
        Struct { fields }
    }

    async fn exec_fetch_one(pool: &AnyPool, sql: &str, values: &[SqlValue]) -> AnyRow {
        crate::server::exec_fetch_one(pool, sql, values).await.unwrap()
    }

    async fn exec_execute(pool: &AnyPool, sql: &str, values: &[SqlValue]) {
        crate::server::exec_execute(pool, sql, values).await.unwrap();
    }

    #[tokio::test]
    async fn test_full_lifecycle_sqlite() {
        let store = SqlStore::new();
        // Use shared-cache in-memory SQLite so all pool connections see the same database.
        store.connect("sqlite:file:test?mode=memory&cache=shared").await.unwrap();

        // Create object store.
        store.create_object_store("users", &users_schema()).await.unwrap();

        // Idempotent create.
        store.create_object_store("users", &users_schema()).await.unwrap();

        let pool = store.pool().unwrap();
        let meta = store.get_meta("users").await.unwrap();

        // Add a record.
        let record = make_user("u1", "alice@example.com", "Alice");
        let ins_sql = crate::query::insert(&meta);
        let values = crate::convert::struct_to_sql_values(&record, &meta.columns);
        exec_execute(&pool, &ins_sql, &values).await;

        // Get by primary key.
        let sel_sql = crate::query::select_by_pk(&meta);
        let row = exec_fetch_one(&pool, &sel_sql, &[SqlValue::Text("u1".into())]).await;
        let got = crate::convert::row_to_struct(&row, &meta.columns);
        assert_eq!(
            got.fields.get("email").and_then(|v| match &v.kind {
                Some(Kind::StringValue(s)) => Some(s.as_str()),
                _ => None,
            }),
            Some("alice@example.com")
        );

        // Count.
        let (count_sql, _count_params) = crate::query::count_with_range(&meta, None);
        let row = exec_fetch_one(&pool, &count_sql, &[]).await;
        let count: i64 = row.try_get(0).unwrap();
        assert_eq!(count, 1);

        // Index query.
        let values = vec![string_val("alice@example.com")];
        let cols = meta.columns.iter().map(|c| format!("\"{}\"", c.name)).collect::<Vec<_>>().join(", ");
        let (idx_sql, idx_params) = crate::query::index_select(
            &meta, "by_email", &values, None, true, &cols,
        ).unwrap();
        let row = exec_fetch_one(&pool, &idx_sql, &idx_params).await;
        let got = crate::convert::row_to_struct(&row, &meta.columns);
        assert_eq!(
            got.fields.get("id").and_then(|v| match &v.kind {
                Some(Kind::StringValue(s)) => Some(s.as_str()),
                _ => None,
            }),
            Some("u1")
        );

        // Delete object store.
        store.delete_object_store("users").await.unwrap();
        assert!(store.get_meta("users").await.is_err());

        store.close();
    }
}
