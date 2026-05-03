use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use gestalt::{AgentHost, proto::v1 as proto};
use prost_types::Struct;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, ErrorData, Implementation, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool, ToolAnnotations,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::store::json_to_value;

const MCP_PAGE_SIZE: i32 = 100;
const MAX_SCAN_PAGES: usize = 100;

#[derive(Clone)]
pub struct McpBridgeHandle {
    url: String,
    bearer_token: String,
    shutdown: CancellationToken,
}

impl McpBridgeHandle {
    pub fn acp_server_config(&self) -> JsonValue {
        json!({
            "type": "http",
            "name": "gestalt",
            "url": self.url,
            "headers": [
                {"name": "Authorization", "value": format!("Bearer {}", self.bearer_token)}
            ]
        })
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

#[derive(Clone)]
struct BridgeAuth {
    bearer_token: String,
}

#[derive(Clone)]
struct GestaltMcpBridge {
    session_id: String,
    turn_id: String,
    run_grant: String,
    host: Arc<Mutex<AgentHost>>,
    tools_by_name: Arc<Mutex<HashMap<String, proto::ListedAgentTool>>>,
    next_tool_call_id: Arc<AtomicU64>,
}

pub async fn start_bridge(
    session_id: String,
    turn_id: String,
    run_grant: String,
) -> Result<McpBridgeHandle, String> {
    let host = AgentHost::connect()
        .await
        .map_err(|err| format!("connect Gestalt agent host for MCP bridge: {err}"))?;
    let bridge = GestaltMcpBridge {
        session_id,
        turn_id: turn_id.clone(),
        run_grant,
        host: Arc::new(Mutex::new(host)),
        tools_by_name: Arc::new(Mutex::new(HashMap::new())),
        next_tool_call_id: Arc::new(AtomicU64::new(1)),
    };
    let shutdown = CancellationToken::new();
    let bearer_token = random_nonce("token")?;
    let path_nonce = random_nonce("mcp")?;
    let route = format!("/{path_nonce}/mcp");
    let auth = BridgeAuth {
        bearer_token: bearer_token.clone(),
    };
    let service: StreamableHttpService<GestaltMcpBridge, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(bridge.clone()),
            Default::default(),
            StreamableHttpServerConfig::default()
                .with_stateful_mode(false)
                .with_json_response(true)
                .with_sse_keep_alive(None)
                .with_cancellation_token(shutdown.child_token()),
        );
    let app = Router::new()
        .nest_service(&route, service)
        .route_layer(middleware::from_fn_with_state(
            auth.clone(),
            require_bearer_token,
        ))
        .with_state(auth);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|err| format!("bind Gestalt MCP bridge: {err}"))?;
    let addr = listener
        .local_addr()
        .map_err(|err| format!("read Gestalt MCP bridge address: {err}"))?;
    let server_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { server_shutdown.cancelled_owned().await })
            .await;
    });

    Ok(McpBridgeHandle {
        url: format!("http://{addr}{route}"),
        bearer_token,
        shutdown,
    })
}

async fn require_bearer_token(
    State(auth): State<BridgeAuth>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let expected = format!("Bearer {}", auth.bearer_token);
    let actual = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if actual != expected {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await)
}

impl ServerHandler for GestaltMcpBridge {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new("gestalt-mcp-catalog", env!("CARGO_PKG_VERSION")),
        )
    }

    fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, ErrorData>> + Send + '_ {
        async move {
            let page_token = request.and_then(|params| params.cursor).unwrap_or_default();
            let (tools, next_page_token) = self.fetch_tools_page(page_token.clone()).await?;
            if !next_page_token.is_empty() && next_page_token == page_token {
                return Err(ErrorData::internal_error(
                    "Gestalt agent host returned a repeated MCP tool page cursor",
                    None,
                ));
            }
            Ok(ListToolsResult {
                meta: None,
                next_cursor: if next_page_token.is_empty() {
                    None
                } else {
                    Some(next_page_token)
                },
                tools: tools.into_iter().map(tool_to_mcp).collect(),
            })
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResult, ErrorData>> + Send + '_ {
        async move {
            let tool = self.find_tool(request.name.as_ref()).await?;
            let seq = self.next_tool_call_id.fetch_add(1, Ordering::Relaxed);
            let tool_call_id = format!("mcp-{seq}");
            let arguments = request.arguments.map(json_object_to_struct);
            let response = self
                .host
                .lock()
                .await
                .execute_tool(proto::ExecuteAgentToolRequest {
                    session_id: self.session_id.clone(),
                    turn_id: self.turn_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                    tool_id: tool.id,
                    arguments,
                    idempotency_key: format!(
                        "agent/hermes-mcp:{}:{seq}:{}",
                        self.turn_id, request.name
                    ),
                    run_grant: self.run_grant.clone(),
                })
                .await
                .map_err(|err| {
                    ErrorData::internal_error(format!("execute Gestalt MCP tool: {err}"), None)
                })?;
            let content = vec![Content::text(response.body)];
            if response.status >= 400 {
                Ok(CallToolResult::error(content))
            } else {
                Ok(CallToolResult::success(content))
            }
        }
    }
}

impl GestaltMcpBridge {
    async fn fetch_tools_page(
        &self,
        page_token: String,
    ) -> Result<(Vec<proto::ListedAgentTool>, String), ErrorData> {
        let response = self
            .host
            .lock()
            .await
            .list_tools(proto::ListAgentToolsRequest {
                session_id: self.session_id.clone(),
                turn_id: self.turn_id.clone(),
                page_size: MCP_PAGE_SIZE,
                page_token,
                run_grant: self.run_grant.clone(),
            })
            .await
            .map_err(|err| {
                ErrorData::internal_error(format!("list Gestalt MCP tools: {err}"), None)
            })?;
        self.cache_tools(&response.tools).await?;
        Ok((response.tools, response.next_page_token))
    }

    async fn cache_tools(&self, tools: &[proto::ListedAgentTool]) -> Result<(), ErrorData> {
        let mut cache = self.tools_by_name.lock().await;
        for tool in tools {
            validate_mcp_name(&tool.mcp_name)?;
            if let Some(existing) = cache.get(&tool.mcp_name) {
                if existing.id != tool.id {
                    return Err(ErrorData::internal_error(
                        format!("duplicate Gestalt MCP tool name {:?}", tool.mcp_name),
                        None,
                    ));
                }
            } else {
                cache.insert(tool.mcp_name.clone(), tool.clone());
            }
        }
        Ok(())
    }

    async fn find_tool(&self, name: &str) -> Result<proto::ListedAgentTool, ErrorData> {
        validate_mcp_name(name)?;
        if let Some(tool) = self.tools_by_name.lock().await.get(name).cloned() {
            return Ok(tool);
        }

        let mut page_token = String::new();
        let mut seen_tokens = HashSet::new();
        for _ in 0..MAX_SCAN_PAGES {
            let (tools, next_page_token) = self.fetch_tools_page(page_token.clone()).await?;
            if let Some(tool) = tools.into_iter().find(|tool| tool.mcp_name == name) {
                return Ok(tool);
            }
            if next_page_token.is_empty() {
                break;
            }
            if !seen_tokens.insert(next_page_token.clone()) {
                return Err(ErrorData::internal_error(
                    "Gestalt agent host returned a repeated MCP tool page cursor",
                    None,
                ));
            }
            page_token = next_page_token;
        }

        Err(ErrorData::invalid_params(
            format!("tool {name:?} not found"),
            None,
        ))
    }
}

fn tool_to_mcp(tool: proto::ListedAgentTool) -> Tool {
    let schema = schema_object(&tool.input_schema);
    let mut converted = Tool::new_with_raw(
        tool.mcp_name,
        if tool.description.trim().is_empty() {
            None
        } else {
            Some(Cow::Owned(tool.description))
        },
        Arc::new(schema),
    );
    if !tool.title.trim().is_empty() {
        converted = converted.with_title(tool.title);
    }
    if !tool.output_schema.trim().is_empty() {
        converted = converted.with_raw_output_schema(Arc::new(schema_object(&tool.output_schema)));
    }
    if let Some(annotations) = tool.annotations {
        converted = converted.with_annotations(ToolAnnotations::from_raw(
            None,
            annotations.read_only_hint,
            annotations.destructive_hint,
            annotations.idempotent_hint,
            annotations.open_world_hint,
        ));
    }
    converted
}

fn schema_object(raw: &str) -> JsonMap<String, JsonValue> {
    serde_json::from_str::<JsonValue>(raw)
        .ok()
        .and_then(|value| match value {
            JsonValue::Object(object) => Some(object),
            _ => None,
        })
        .unwrap_or_else(|| {
            json!({"type": "object"})
                .as_object()
                .expect("object schema")
                .clone()
        })
}

fn json_object_to_struct(object: JsonMap<String, JsonValue>) -> Struct {
    Struct {
        fields: object
            .into_iter()
            .map(|(key, value)| (key, json_to_value(value)))
            .collect(),
    }
}

fn validate_mcp_name(name: &str) -> Result<(), ErrorData> {
    if name.is_empty() || name.len() > 128 {
        return Err(ErrorData::internal_error(
            format!("invalid Gestalt MCP tool name {:?}", name),
            None,
        ));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err(ErrorData::internal_error(
            format!("unsafe Gestalt MCP tool name {:?}", name),
            None,
        ));
    }
    Ok(())
}

fn random_nonce(prefix: &str) -> Result<String, String> {
    let mut bytes = [0_u8; 18];
    getrandom::fill(&mut bytes).map_err(|err| format!("generate MCP bridge nonce: {err}"))?;
    let mut nonce = String::with_capacity(prefix.len() + 1 + bytes.len() * 2);
    nonce.push_str(prefix);
    nonce.push('-');
    for byte in bytes {
        write!(&mut nonce, "{byte:02x}").expect("write hex nonce");
    }
    Ok(nonce)
}
