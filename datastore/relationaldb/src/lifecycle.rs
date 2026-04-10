use std::sync::Arc;

use tonic::{Request, Response, Status};

use gestalt::proto::v1::provider_lifecycle_server::ProviderLifecycle;
use gestalt::proto::v1::{
    ConfigureProviderRequest, ConfigureProviderResponse, HealthCheckResponse, ProviderIdentity,
    ProviderKind,
};

use crate::store::SqlStore;

const PROTOCOL_VERSION: i32 = 2;

#[derive(Clone)]
pub struct LifecycleServer {
    store: Arc<SqlStore>,
}

impl LifecycleServer {
    pub fn new(store: Arc<SqlStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl ProviderLifecycle for LifecycleServer {
    async fn get_provider_identity(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ProviderIdentity>, Status> {
        Ok(Response::new(ProviderIdentity {
            kind: ProviderKind::Datastore as i32,
            name: "relationaldb".to_string(),
            display_name: "RelationalDB".to_string(),
            description: "RelationalDB datastore provider supporting PostgreSQL, MySQL, SQLite, and SQL Server.".to_string(),
            version: "0.0.1-alpha.1".to_string(),
            warnings: Vec::new(),
            min_protocol_version: PROTOCOL_VERSION,
            max_protocol_version: PROTOCOL_VERSION,
        }))
    }

    async fn configure_provider(
        &self,
        request: Request<ConfigureProviderRequest>,
    ) -> Result<Response<ConfigureProviderResponse>, Status> {
        let req = request.into_inner();

        if req.protocol_version != PROTOCOL_VERSION {
            return Err(Status::failed_precondition(format!(
                "host requested protocol version {}, provider requires {}",
                req.protocol_version, PROTOCOL_VERSION
            )));
        }

        // Extract the DSN from config. The config comes as a prost_types::Struct
        // serialized to a map by the host.
        let dsn = req
            .config
            .as_ref()
            .and_then(|s| s.fields.get("dsn"))
            .and_then(|v| match &v.kind {
                Some(prost_types::value::Kind::StringValue(s)) => Some(s.as_str()),
                _ => None,
            })
            .ok_or_else(|| Status::invalid_argument("config.dsn is required"))?;

        self.store.connect(dsn).await?;

        Ok(Response::new(ConfigureProviderResponse {
            protocol_version: PROTOCOL_VERSION,
        }))
    }

    async fn health_check(
        &self,
        _request: Request<()>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        match self.store.health_check().await {
            Ok(()) => Ok(Response::new(HealthCheckResponse {
                ready: true,
                message: String::new(),
            })),
            Err(e) => Ok(Response::new(HealthCheckResponse {
                ready: false,
                message: e.message().to_string(),
            })),
        }
    }
}
