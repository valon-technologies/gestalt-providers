use std::sync::Arc;

use prost_types::Struct;
use sqlx::{AnyPool, any::AnyQueryResult, any::AnyRow};
use sqlx::Row;
use tonic::{Request, Response, Status};

use gestalt::proto::v1::indexed_db_server::IndexedDb;
use gestalt::proto::v1::{
    CountResponse, CreateObjectStoreRequest, DeleteObjectStoreRequest, DeleteResponse,
    IndexQueryRequest, KeyResponse, KeysResponse, ObjectStoreNameRequest, ObjectStoreRangeRequest,
    ObjectStoreRequest, RecordRequest, RecordResponse, RecordsResponse,
};

use crate::convert::{SqlValue, row_to_struct, struct_to_sql_values};
use crate::query;
use crate::store::SqlStore;

#[derive(Clone)]
pub struct IndexedDbGrpcServer {
    store: Arc<SqlStore>,
}

impl IndexedDbGrpcServer {
    pub fn new(store: Arc<SqlStore>) -> Self {
        Self { store }
    }
}

fn map_sqlx_err(op: &str, err: sqlx::Error) -> Status {
    match &err {
        sqlx::Error::RowNotFound => Status::not_found("not found"),
        sqlx::Error::Database(db_err) => {
            let msg = db_err.message().to_lowercase();
            if msg.contains("unique") || msg.contains("duplicate") || msg.contains("constraint") {
                Status::already_exists("already exists")
            } else {
                Status::internal(format!("{op}: {err}"))
            }
        }
        _ => Status::internal(format!("{op}: {err}")),
    }
}

fn extract_id(record: &Struct, pk_column: &str) -> Result<String, Status> {
    record
        .fields
        .get(pk_column)
        .and_then(|v| match &v.kind {
            Some(prost_types::value::Kind::StringValue(s)) => Some(s.clone()),
            Some(prost_types::value::Kind::NumberValue(n)) => Some(n.to_string()),
            _ => None,
        })
        .ok_or_else(|| Status::invalid_argument(format!("record missing '{pk_column}' field")))
}

/// Execute a query with bind values and return all rows.
pub(crate) async fn exec_fetch_all(pool: &AnyPool, sql: &str, values: &[SqlValue]) -> Result<Vec<AnyRow>, sqlx::Error> {
    let mut q = sqlx::query(sql);
    for v in values {
        q = bind_one(q, v);
    }
    q.fetch_all(pool).await
}

/// Execute a query with bind values and return one row.
pub(crate) async fn exec_fetch_one(pool: &AnyPool, sql: &str, values: &[SqlValue]) -> Result<AnyRow, sqlx::Error> {
    let mut q = sqlx::query(sql);
    for v in values {
        q = bind_one(q, v);
    }
    q.fetch_one(pool).await
}

/// Execute a query with bind values and return the result.
pub(crate) async fn exec_execute(pool: &AnyPool, sql: &str, values: &[SqlValue]) -> Result<AnyQueryResult, sqlx::Error> {
    let mut q = sqlx::query(sql);
    for v in values {
        q = bind_one(q, v);
    }
    q.execute(pool).await
}

fn bind_one<'q>(
    q: sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>>,
    v: &'q SqlValue,
) -> sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>> {
    match v {
        SqlValue::Text(s) => q.bind(s.as_str()),
        SqlValue::Int(n) => q.bind(*n),
        SqlValue::Float(f) => q.bind(*f),
        SqlValue::SmallInt(i) => q.bind(*i),
        SqlValue::Null => q.bind(Option::<String>::None),
    }
}

#[tonic::async_trait]
impl IndexedDb for IndexedDbGrpcServer {
    // ---- Lifecycle ----

    async fn create_object_store(
        &self,
        request: Request<CreateObjectStoreRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let schema = req.schema.unwrap_or_default();
        self.store.create_object_store(&req.name, &schema).await?;
        Ok(Response::new(()))
    }

    async fn delete_object_store(
        &self,
        request: Request<DeleteObjectStoreRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.store.delete_object_store(&req.name).await?;
        Ok(Response::new(()))
    }

    // ---- Primary key CRUD ----

    async fn get(
        &self,
        request: Request<ObjectStoreRequest>,
    ) -> Result<Response<RecordResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let sql = query::select_by_pk(&meta);
        let row = exec_fetch_one(&pool, &sql, &[SqlValue::Text(req.id)])
            .await
            .map_err(|e| map_sqlx_err("get", e))?;

        let record = row_to_struct(&row, &meta.columns);
        Ok(Response::new(RecordResponse {
            record: Some(record),
        }))
    }

    async fn get_key(
        &self,
        request: Request<ObjectStoreRequest>,
    ) -> Result<Response<KeyResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let sql = query::select_key_by_pk(&meta);
        let row = exec_fetch_one(&pool, &sql, &[SqlValue::Text(req.id)])
            .await
            .map_err(|e| map_sqlx_err("get_key", e))?;

        let key: String = row
            .try_get(meta.pk_column.as_str())
            .map_err(|e| Status::internal(format!("read key: {e}")))?;
        Ok(Response::new(KeyResponse { key }))
    }

    async fn add(
        &self,
        request: Request<RecordRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let record = req
            .record
            .ok_or_else(|| Status::invalid_argument("record is required"))?;
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let sql = query::insert(&meta);
        let values = struct_to_sql_values(&record, &meta.columns);
        exec_execute(&pool, &sql, &values)
            .await
            .map_err(|e| map_sqlx_err("add", e))?;

        Ok(Response::new(()))
    }

    async fn put(
        &self,
        request: Request<RecordRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let record = req
            .record
            .ok_or_else(|| Status::invalid_argument("record is required"))?;
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let id = extract_id(&record, &meta.pk_column)?;

        // Upsert via DELETE then INSERT.
        let del_sql = query::delete_by_pk(&meta);
        exec_execute(&pool, &del_sql, &[SqlValue::Text(id)])
            .await
            .map_err(|e| Status::internal(format!("put delete: {e}")))?;

        let ins_sql = query::insert(&meta);
        let values = struct_to_sql_values(&record, &meta.columns);
        exec_execute(&pool, &ins_sql, &values)
            .await
            .map_err(|e| map_sqlx_err("put insert", e))?;

        Ok(Response::new(()))
    }

    async fn delete(
        &self,
        request: Request<ObjectStoreRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let sql = query::delete_by_pk(&meta);
        exec_execute(&pool, &sql, &[SqlValue::Text(req.id)])
            .await
            .map_err(|e| map_sqlx_err("delete", e))?;

        Ok(Response::new(()))
    }

    // ---- Bulk operations ----

    async fn clear(
        &self,
        request: Request<ObjectStoreNameRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let sql = query::delete_all(&meta);
        exec_execute(&pool, &sql, &[])
            .await
            .map_err(|e| Status::internal(format!("clear: {e}")))?;

        Ok(Response::new(()))
    }

    async fn get_all(
        &self,
        request: Request<ObjectStoreRangeRequest>,
    ) -> Result<Response<RecordsResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let (sql, params) = query::select_all_with_range(&meta, req.range.as_ref());
        let rows = exec_fetch_all(&pool, &sql, &params)
            .await
            .map_err(|e| Status::internal(format!("get_all: {e}")))?;

        let records: Vec<Struct> = rows.iter().map(|r| row_to_struct(r, &meta.columns)).collect();
        Ok(Response::new(RecordsResponse { records }))
    }

    async fn get_all_keys(
        &self,
        request: Request<ObjectStoreRangeRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let (sql, params) = query::select_keys_with_range(&meta, req.range.as_ref());
        let rows = exec_fetch_all(&pool, &sql, &params)
            .await
            .map_err(|e| Status::internal(format!("get_all_keys: {e}")))?;

        let keys: Vec<String> = rows
            .iter()
            .filter_map(|r: &AnyRow| r.try_get::<String, _>(meta.pk_column.as_str()).ok())
            .collect();
        Ok(Response::new(KeysResponse { keys }))
    }

    async fn count(
        &self,
        request: Request<ObjectStoreRangeRequest>,
    ) -> Result<Response<CountResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let (sql, params) = query::count_with_range(&meta, req.range.as_ref());
        let row = exec_fetch_one(&pool, &sql, &params)
            .await
            .map_err(|e| Status::internal(format!("count: {e}")))?;

        let count: i64 = row
            .try_get(0)
            .map_err(|e| Status::internal(format!("read count: {e}")))?;
        Ok(Response::new(CountResponse { count }))
    }

    async fn delete_range(
        &self,
        request: Request<ObjectStoreRangeRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let (sql, params) = query::delete_with_range(&meta, req.range.as_ref());
        let result = exec_execute(&pool, &sql, &params)
            .await
            .map_err(|e| Status::internal(format!("delete_range: {e}")))?;

        Ok(Response::new(DeleteResponse {
            deleted: result.rows_affected() as i64,
        }))
    }

    // ---- Index queries ----

    async fn index_get(
        &self,
        request: Request<IndexQueryRequest>,
    ) -> Result<Response<RecordResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let cols = query::col_list(&meta.columns);
        let (sql, params) = query::index_select(
            &meta, &req.index, &req.values, req.range.as_ref(), true, &cols,
        )
        .ok_or_else(|| Status::not_found(format!("index not found: {}", req.index)))?;

        let row = exec_fetch_one(&pool, &sql, &params)
            .await
            .map_err(|e| map_sqlx_err("index_get", e))?;

        let record = row_to_struct(&row, &meta.columns);
        Ok(Response::new(RecordResponse {
            record: Some(record),
        }))
    }

    async fn index_get_key(
        &self,
        request: Request<IndexQueryRequest>,
    ) -> Result<Response<KeyResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let pk_expr = query::quote_ident(&meta.pk_column);
        let (sql, params) = query::index_select(
            &meta, &req.index, &req.values, req.range.as_ref(), true, &pk_expr,
        )
        .ok_or_else(|| Status::not_found(format!("index not found: {}", req.index)))?;

        let row = exec_fetch_one(&pool, &sql, &params)
            .await
            .map_err(|e| map_sqlx_err("index_get_key", e))?;

        let key: String = row
            .try_get(meta.pk_column.as_str())
            .map_err(|e| Status::internal(format!("read key: {e}")))?;
        Ok(Response::new(KeyResponse { key }))
    }

    async fn index_get_all(
        &self,
        request: Request<IndexQueryRequest>,
    ) -> Result<Response<RecordsResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let cols = query::col_list(&meta.columns);
        let (sql, params) = query::index_select(
            &meta, &req.index, &req.values, req.range.as_ref(), false, &cols,
        )
        .ok_or_else(|| Status::not_found(format!("index not found: {}", req.index)))?;

        let rows = exec_fetch_all(&pool, &sql, &params)
            .await
            .map_err(|e| Status::internal(format!("index_get_all: {e}")))?;

        let records: Vec<Struct> = rows.iter().map(|r| row_to_struct(r, &meta.columns)).collect();
        Ok(Response::new(RecordsResponse { records }))
    }

    async fn index_get_all_keys(
        &self,
        request: Request<IndexQueryRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let pk_expr = query::quote_ident(&meta.pk_column);
        let (sql, params) = query::index_select(
            &meta, &req.index, &req.values, req.range.as_ref(), false, &pk_expr,
        )
        .ok_or_else(|| Status::not_found(format!("index not found: {}", req.index)))?;

        let rows = exec_fetch_all(&pool, &sql, &params)
            .await
            .map_err(|e| Status::internal(format!("index_get_all_keys: {e}")))?;

        let keys: Vec<String> = rows
            .iter()
            .filter_map(|r: &AnyRow| r.try_get::<String, _>(meta.pk_column.as_str()).ok())
            .collect();
        Ok(Response::new(KeysResponse { keys }))
    }

    async fn index_count(
        &self,
        request: Request<IndexQueryRequest>,
    ) -> Result<Response<CountResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let (sql, params) = query::index_count(
            &meta, &req.index, &req.values, req.range.as_ref(),
        )
        .ok_or_else(|| Status::not_found(format!("index not found: {}", req.index)))?;

        let row = exec_fetch_one(&pool, &sql, &params)
            .await
            .map_err(|e| Status::internal(format!("index_count: {e}")))?;

        let count: i64 = row
            .try_get(0)
            .map_err(|e| Status::internal(format!("read count: {e}")))?;
        Ok(Response::new(CountResponse { count }))
    }

    async fn index_delete(
        &self,
        request: Request<IndexQueryRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let meta = self.store.get_meta(&req.store).await?;
        let pool = self.store.pool()?;

        let (sql, params) = query::index_delete(&meta, &req.index, &req.values)
            .ok_or_else(|| Status::not_found(format!("index not found: {}", req.index)))?;

        let result = exec_execute(&pool, &sql, &params)
            .await
            .map_err(|e| Status::internal(format!("index_delete: {e}")))?;

        Ok(Response::new(DeleteResponse {
            deleted: result.rows_affected() as i64,
        }))
    }
}
