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
use gestalt::{AgentHost, AgentHostExecuteToolInput, AgentHostListToolsInput, proto::v1 as proto};
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

const MCP_PAGE_SIZE: i32 = 100;
const MAX_SCAN_PAGES: usize = 100;
const TOOL_SEARCH_DEFAULT_MAX_RESULTS: usize = 8;
const TOOL_SEARCH_DEFAULT_CANDIDATE_LIMIT: usize = 10;
const TOOL_SEARCH_MAX_RESULTS: usize = 20;
const TOOL_SEARCH_MAX_CANDIDATES: usize = 20;
const TOOL_SEARCH_MAX_QUERY_CHARS: usize = 512;
const TOOL_SEARCH_MAX_LOAD_REFS: usize = 20;

const TOOL_SEARCH_NAME: &str = "gestalt_search_tools";
const TOOL_GET_SCHEMA_NAME: &str = "gestalt_get_tool_schema";
const TOOL_CALL_NAME: &str = "gestalt_call_tool";

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

#[derive(Debug)]
struct ProxyError {
    code: &'static str,
    message: String,
}

#[derive(Clone)]
enum ToolSelector {
    MCPName(String),
    Ref(proto::AgentToolRef),
}

struct ScoredTool {
    tool: proto::ListedAgentTool,
    score: usize,
    order: usize,
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

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            meta: None,
            next_cursor: None,
            tools: proxy_tools(),
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let name = request.name.to_string();
        let arguments = request.arguments.unwrap_or_default();
        let result = match name.as_str() {
            TOOL_SEARCH_NAME => self.search_tools(arguments).await,
            TOOL_GET_SCHEMA_NAME => self.get_tool_schema(arguments).await,
            TOOL_CALL_NAME => self.call_gestalt_tool(arguments).await,
            _ => Err(ProxyError::new(
                "unknown_proxy_tool",
                format!("unknown Gestalt MCP proxy tool {name:?}"),
            )),
        };
        Ok(result.unwrap_or_else(proxy_error_result))
    }
}

impl GestaltMcpBridge {
    async fn search_tools(
        &self,
        arguments: JsonMap<String, JsonValue>,
    ) -> Result<CallToolResult, ProxyError> {
        let query = optional_string_arg(&arguments, "query")?;
        if query.chars().count() > TOOL_SEARCH_MAX_QUERY_CHARS {
            return Err(ProxyError::new(
                "invalid_arguments",
                format!("query exceeds {TOOL_SEARCH_MAX_QUERY_CHARS} characters"),
            ));
        }
        let max_results = bounded_usize_arg(
            &arguments,
            "max_results",
            TOOL_SEARCH_DEFAULT_MAX_RESULTS,
            TOOL_SEARCH_MAX_RESULTS,
        )?;
        let candidate_limit = bounded_usize_arg(
            &arguments,
            "candidate_limit",
            TOOL_SEARCH_DEFAULT_CANDIDATE_LIMIT,
            TOOL_SEARCH_MAX_CANDIDATES,
        )?;
        let load_refs = load_refs_arg(&arguments)?;
        let desired_count = max_results + candidate_limit;
        let (matched, has_more) = if desired_count == 0 {
            (Vec::new(), false)
        } else {
            self.list_matching_tools(&query, &load_refs, desired_count)
                .await?
        };

        let full_tools: Vec<JsonValue> = matched
            .iter()
            .take(max_results)
            .map(|item| listed_tool_json(&item.tool, true))
            .collect();
        let candidates: Vec<JsonValue> = matched
            .iter()
            .skip(max_results)
            .take(candidate_limit)
            .map(|item| listed_tool_json(&item.tool, false))
            .collect();
        let mut body = JsonMap::new();
        body.insert("tools".to_string(), JsonValue::Array(full_tools));
        if !candidates.is_empty() {
            body.insert("candidates".to_string(), JsonValue::Array(candidates));
        }
        body.insert("has_more".to_string(), JsonValue::Bool(has_more));
        Ok(json_result(JsonValue::Object(body), false))
    }

    async fn get_tool_schema(
        &self,
        arguments: JsonMap<String, JsonValue>,
    ) -> Result<CallToolResult, ProxyError> {
        let tool = self.resolve_selector(selector_arg(&arguments)?).await?;
        Ok(json_result(
            json!({
                "tool": listed_tool_json(&tool, true),
            }),
            false,
        ))
    }

    async fn call_gestalt_tool(
        &self,
        arguments: JsonMap<String, JsonValue>,
    ) -> Result<CallToolResult, ProxyError> {
        let selector = selector_arg(&arguments)?;
        let tool = self.resolve_selector(selector).await?;
        let call_arguments = match arguments.get("arguments") {
            None | Some(JsonValue::Null) => JsonMap::new(),
            Some(JsonValue::Object(object)) => object.clone(),
            Some(_) => {
                return Err(ProxyError::new(
                    "invalid_arguments",
                    "arguments must be an object when provided",
                ));
            }
        };
        let seq = self.next_tool_call_id.fetch_add(1, Ordering::Relaxed);
        let tool_call_id = format!("mcp-{seq}");
        let response = self
            .host
            .lock()
            .await
            .execute_tool_for_turn(AgentHostExecuteToolInput {
                session_id: self.session_id.clone(),
                turn_id: self.turn_id.clone(),
                tool_call_id,
                tool_id: tool.id.clone(),
                arguments: Some(JsonValue::Object(call_arguments)),
                idempotency_key: format!(
                    "agent/hermes-mcp:{}:{seq}:{}",
                    self.turn_id, tool.mcp_name
                ),
                run_grant: self.run_grant.clone(),
            })
            .await
            .map_err(|err| {
                ProxyError::new(
                    "execute_tool_failed",
                    format!("execute Gestalt MCP tool {:?}: {err}", tool.mcp_name),
                )
            })?;
        let body = response.body;
        if response.status >= 400 {
            Ok(json_result(
                json!({
                    "ok": false,
                    "error": {
                        "code": "target_tool_failed",
                        "message": format!("Gestalt tool {:?} returned status {}", tool.mcp_name, response.status),
                        "status": response.status,
                        "body": body,
                    }
                }),
                true,
            ))
        } else {
            Ok(CallToolResult::success(vec![Content::text(body)]))
        }
    }

    async fn list_matching_tools(
        &self,
        query: &str,
        load_refs: &[proto::AgentToolRef],
        desired_count: usize,
    ) -> Result<(Vec<ScoredTool>, bool), ProxyError> {
        let tokens = query_tokens(query);
        let mut page_token = String::new();
        let mut seen_tokens = HashSet::new();
        let mut matched: Vec<ScoredTool> = Vec::new();
        let mut order = 0_usize;
        for _ in 0..MAX_SCAN_PAGES {
            if !seen_tokens.insert(page_token.clone()) {
                return Err(ProxyError::new(
                    "repeated_cursor",
                    "Gestalt agent host returned a repeated MCP tool page cursor",
                ));
            }
            let (tools, next_page_token) = self.fetch_tools_page(&page_token).await?;
            for tool in tools {
                order += 1;
                let Some(score) = tool_match_score(&tool, &tokens, load_refs) else {
                    continue;
                };
                matched.push(ScoredTool { tool, score, order });
            }
            if next_page_token.is_empty() {
                matched.sort_by(|left, right| {
                    right
                        .score
                        .cmp(&left.score)
                        .then(left.order.cmp(&right.order))
                });
                let has_more = matched.len() > desired_count;
                matched.truncate(desired_count);
                return Ok((matched, has_more));
            }
            page_token = next_page_token;
        }
        Err(ProxyError::new(
            "page_limit_exceeded",
            format!("ListTools exceeded {MAX_SCAN_PAGES} pages"),
        ))
    }

    async fn resolve_selector(
        &self,
        selector: ToolSelector,
    ) -> Result<proto::ListedAgentTool, ProxyError> {
        match selector {
            ToolSelector::MCPName(name) => self.find_tool_by_mcp_name(&name).await,
            ToolSelector::Ref(ref_selector) => self.find_tool_by_ref(&ref_selector).await,
        }
    }

    async fn find_tool_by_mcp_name(
        &self,
        name: &str,
    ) -> Result<proto::ListedAgentTool, ProxyError> {
        validate_mcp_name_text(name)
            .map_err(|message| ProxyError::new("invalid_selector", message))?;
        if let Some(tool) = self.tools_by_name.lock().await.get(name).cloned() {
            return Ok(tool);
        }
        let mut page_token = String::new();
        let mut seen_tokens = HashSet::new();
        for _ in 0..MAX_SCAN_PAGES {
            if !seen_tokens.insert(page_token.clone()) {
                return Err(ProxyError::new(
                    "repeated_cursor",
                    "Gestalt agent host returned a repeated MCP tool page cursor",
                ));
            }
            let (tools, next_page_token) = self.fetch_tools_page(&page_token).await?;
            if let Some(tool) = tools.into_iter().find(|tool| tool.mcp_name == name) {
                return Ok(tool);
            }
            if next_page_token.is_empty() {
                break;
            }
            page_token = next_page_token;
        }
        Err(ProxyError::new(
            "tool_lookup_failed",
            format!("tool {name:?} is not available in the current grant"),
        ))
    }

    async fn find_tool_by_ref(
        &self,
        ref_selector: &proto::AgentToolRef,
    ) -> Result<proto::ListedAgentTool, ProxyError> {
        let mut page_token = String::new();
        let mut seen_tokens = HashSet::new();
        let mut matched: Vec<proto::ListedAgentTool> = Vec::new();
        for _ in 0..MAX_SCAN_PAGES {
            if !seen_tokens.insert(page_token.clone()) {
                return Err(ProxyError::new(
                    "repeated_cursor",
                    "Gestalt agent host returned a repeated MCP tool page cursor",
                ));
            }
            let (tools, next_page_token) = self.fetch_tools_page(&page_token).await?;
            for tool in tools {
                if listed_tool_ref_matches(tool.r#ref.as_ref(), ref_selector) {
                    matched.push(tool);
                    if matched.len() > 1 {
                        return Err(ProxyError::new(
                            "ambiguous_tool_ref",
                            "ref selector matched more than one Gestalt MCP tool; use mcp_name from search results",
                        ));
                    }
                }
            }
            if next_page_token.is_empty() {
                break;
            }
            page_token = next_page_token;
        }
        matched.into_iter().next().ok_or_else(|| {
            ProxyError::new(
                "tool_lookup_failed",
                "ref selector did not match any Gestalt MCP tool",
            )
        })
    }

    async fn fetch_tools_page(
        &self,
        page_token: &str,
    ) -> Result<(Vec<proto::ListedAgentTool>, String), ProxyError> {
        let response = self
            .host
            .lock()
            .await
            .list_tools_for_turn(AgentHostListToolsInput {
                session_id: self.session_id.clone(),
                turn_id: self.turn_id.clone(),
                page_size: MCP_PAGE_SIZE,
                page_token: page_token.trim().to_string(),
                run_grant: self.run_grant.clone(),
                ..Default::default()
            })
            .await
            .map_err(|err| {
                ProxyError::new(
                    "list_tools_failed",
                    format!("list Gestalt MCP tools: {err}"),
                )
            })?;
        self.remember_tools(&response.tools).await?;
        Ok((response.tools, response.next_page_token.trim().to_string()))
    }

    async fn remember_tools(&self, tools: &[proto::ListedAgentTool]) -> Result<(), ProxyError> {
        let mut cache = self.tools_by_name.lock().await;
        for tool in tools {
            validate_mcp_name_text(&tool.mcp_name)
                .map_err(|message| ProxyError::new("invalid_catalog_tool", message))?;
            if tool.id.trim().is_empty() {
                return Err(ProxyError::new(
                    "invalid_catalog_tool",
                    format!("ListTools returned tool {:?} without an id", tool.mcp_name),
                ));
            }
            let Some(existing) = cache.get(&tool.mcp_name) else {
                cache.insert(tool.mcp_name.clone(), tool.clone());
                continue;
            };
            if existing.id != tool.id {
                return Err(ProxyError::new(
                    "invalid_catalog_tool",
                    format!("duplicate Gestalt MCP tool name {:?}", tool.mcp_name),
                ));
            }
        }
        Ok(())
    }
}

impl ProxyError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

fn proxy_tools() -> Vec<Tool> {
    vec![
        proxy_tool(
            TOOL_SEARCH_NAME,
            "Search Gestalt tools",
            "Search the authorized Gestalt integration tool catalog by tool name, title, description, and ref metadata.",
            search_input_schema(),
            true,
        ),
        proxy_tool(
            TOOL_GET_SCHEMA_NAME,
            "Get Gestalt tool schema",
            "Load one authorized Gestalt tool schema by opaque mcp_name, or by a ref that resolves to exactly one tool.",
            selector_input_schema(false),
            true,
        ),
        proxy_tool(
            TOOL_CALL_NAME,
            "Call Gestalt tool",
            "Call one authorized Gestalt tool by opaque mcp_name, or by a ref that resolves to exactly one tool.",
            selector_input_schema(true),
            false,
        ),
    ]
}

fn proxy_tool(
    name: &'static str,
    title: &str,
    description: &'static str,
    schema: JsonMap<String, JsonValue>,
    read_only: bool,
) -> Tool {
    Tool::new_with_raw(name, Some(Cow::Borrowed(description)), Arc::new(schema))
        .with_title(title)
        .with_annotations(ToolAnnotations::from_raw(
            Some(title.to_string()),
            Some(read_only),
            if read_only { Some(false) } else { None },
            None,
            Some(true),
        ))
}

fn search_input_schema() -> JsonMap<String, JsonValue> {
    json_object(json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Short natural-language search query. The bridge matches only tool name, title, description, and ref metadata.",
                "maxLength": TOOL_SEARCH_MAX_QUERY_CHARS
            },
            "max_results": {
                "type": "integer",
                "minimum": 0,
                "maximum": TOOL_SEARCH_MAX_RESULTS,
                "description": "Maximum number of matching tools to return with full schemas."
            },
            "candidate_limit": {
                "type": "integer",
                "minimum": 0,
                "maximum": TOOL_SEARCH_MAX_CANDIDATES,
                "description": "Maximum number of additional compact candidates to return."
            },
            "load_refs": {
                "type": "array",
                "maxItems": TOOL_SEARCH_MAX_LOAD_REFS,
                "description": "Filter results to tools matching one or more refs from a previous search response.",
                "items": ref_schema()
            }
        },
        "additionalProperties": false
    }))
}

fn selector_input_schema(include_arguments: bool) -> JsonMap<String, JsonValue> {
    let mut properties = json_object(json!({
        "mcp_name": {
            "type": "string",
            "description": "Opaque MCP tool name returned by gestalt_search_tools."
        },
        "ref": ref_schema()
    }));
    if include_arguments {
        properties.insert(
            "arguments".to_string(),
            json!({
                "type": "object",
                "description": "Arguments to pass to the selected Gestalt tool.",
                "additionalProperties": true
            }),
        );
    }
    let mut schema = json_object(json!({
        "type": "object",
        "description": concat!(
            "Select exactly one Gestalt tool by mcp_name or ref. ",
            "The bridge validates that exactly one selector is present."
        ),
        "additionalProperties": false
    }));
    schema.insert("properties".to_string(), JsonValue::Object(properties));
    schema
}

fn ref_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "system": {"type": "string"},
            "plugin": {"type": "string"},
            "operation": {"type": "string"},
            "connection": {"type": "string"},
            "instance": {"type": "string"}
        },
        "additionalProperties": false
    })
}

fn selector_arg(arguments: &JsonMap<String, JsonValue>) -> Result<ToolSelector, ProxyError> {
    let mcp_name = optional_string_arg(arguments, "mcp_name")?;
    let ref_value = arguments.get("ref").filter(|value| !value.is_null());
    match (mcp_name.trim().is_empty(), ref_value) {
        (false, None) => Ok(ToolSelector::MCPName(mcp_name)),
        (true, Some(value)) => Ok(ToolSelector::Ref(agent_tool_ref_arg(value)?)),
        (true, None) => Err(ProxyError::new(
            "invalid_selector",
            "exactly one of mcp_name or ref is required",
        )),
        (false, Some(_)) => Err(ProxyError::new(
            "invalid_selector",
            "mcp_name and ref are mutually exclusive",
        )),
    }
}

fn optional_string_arg(
    arguments: &JsonMap<String, JsonValue>,
    key: &str,
) -> Result<String, ProxyError> {
    match arguments.get(key) {
        None | Some(JsonValue::Null) => Ok(String::new()),
        Some(JsonValue::String(value)) => Ok(value.trim().to_string()),
        Some(_) => Err(ProxyError::new(
            "invalid_arguments",
            format!("{key} must be a string when provided"),
        )),
    }
}

fn bounded_usize_arg(
    arguments: &JsonMap<String, JsonValue>,
    key: &str,
    default: usize,
    maximum: usize,
) -> Result<usize, ProxyError> {
    let Some(value) = arguments.get(key) else {
        return Ok(default);
    };
    if value.is_null() {
        return Ok(default);
    }
    let Some(number) = value.as_i64() else {
        return Err(ProxyError::new(
            "invalid_arguments",
            format!("{key} must be an integer between 0 and {maximum}"),
        ));
    };
    if number < 0 || number as usize > maximum {
        return Err(ProxyError::new(
            "invalid_arguments",
            format!("{key} must be between 0 and {maximum}"),
        ));
    }
    Ok(number as usize)
}

fn load_refs_arg(
    arguments: &JsonMap<String, JsonValue>,
) -> Result<Vec<proto::AgentToolRef>, ProxyError> {
    let Some(value) = arguments.get("load_refs") else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let JsonValue::Array(items) = value else {
        return Err(ProxyError::new(
            "invalid_arguments",
            "load_refs must be an array when provided",
        ));
    };
    if items.len() > TOOL_SEARCH_MAX_LOAD_REFS {
        return Err(ProxyError::new(
            "invalid_arguments",
            format!("load_refs exceeds {TOOL_SEARCH_MAX_LOAD_REFS} entries"),
        ));
    }
    items.iter().map(agent_tool_ref_arg).collect()
}

fn agent_tool_ref_arg(value: &JsonValue) -> Result<proto::AgentToolRef, ProxyError> {
    let JsonValue::Object(object) = value else {
        return Err(ProxyError::new(
            "invalid_arguments",
            "ref entries must be objects",
        ));
    };
    let out = proto::AgentToolRef {
        system: string_field(object, "system")?,
        plugin: string_field(object, "plugin")?,
        operation: string_field(object, "operation")?,
        connection: string_field(object, "connection")?,
        instance: string_field(object, "instance")?,
        ..Default::default()
    };
    if out.system.is_empty()
        && out.plugin.is_empty()
        && out.operation.is_empty()
        && out.connection.is_empty()
        && out.instance.is_empty()
    {
        return Err(ProxyError::new(
            "invalid_arguments",
            "ref must include at least one selector field",
        ));
    }
    Ok(out)
}

fn string_field(object: &JsonMap<String, JsonValue>, key: &str) -> Result<String, ProxyError> {
    match object.get(key) {
        None | Some(JsonValue::Null) => Ok(String::new()),
        Some(JsonValue::String(value)) => Ok(value.trim().to_string()),
        Some(_) => Err(ProxyError::new(
            "invalid_arguments",
            format!("ref.{key} must be a string when provided"),
        )),
    }
}

fn tool_match_score(
    tool: &proto::ListedAgentTool,
    tokens: &[String],
    load_refs: &[proto::AgentToolRef],
) -> Option<usize> {
    if !load_refs.is_empty() {
        return load_refs
            .iter()
            .any(|requested| listed_tool_ref_matches(tool.r#ref.as_ref(), requested))
            .then_some(1);
    }
    if tokens.is_empty() {
        return Some(1);
    }
    let haystack = listed_tool_search_haystack(tool);
    let score = tokens
        .iter()
        .filter(|token| haystack.contains(token.as_str()))
        .count();
    (score > 0).then_some(score)
}

fn query_tokens(query: &str) -> Vec<String> {
    query
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-'))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn listed_tool_search_haystack(tool: &proto::ListedAgentTool) -> String {
    let mut values = vec![
        tool.mcp_name.as_str(),
        tool.title.as_str(),
        tool.description.as_str(),
    ];
    if let Some(ref_value) = tool.r#ref.as_ref() {
        values.extend([
            ref_value.system.as_str(),
            ref_value.plugin.as_str(),
            ref_value.operation.as_str(),
            ref_value.connection.as_str(),
            ref_value.instance.as_str(),
        ]);
    }
    values.join(" ").to_ascii_lowercase()
}

fn listed_tool_ref_matches(
    candidate: Option<&proto::AgentToolRef>,
    requested: &proto::AgentToolRef,
) -> bool {
    let Some(candidate) = candidate else {
        return false;
    };
    let mut any_field = false;
    for (requested, actual) in [
        (&requested.system, &candidate.system),
        (&requested.plugin, &candidate.plugin),
        (&requested.operation, &candidate.operation),
        (&requested.connection, &candidate.connection),
        (&requested.instance, &candidate.instance),
    ] {
        if requested.trim().is_empty() {
            continue;
        }
        any_field = true;
        if requested.trim() != actual.trim() {
            return false;
        }
    }
    any_field
}

fn listed_tool_json(tool: &proto::ListedAgentTool, include_schema: bool) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert(
        "mcp_name".to_string(),
        JsonValue::String(tool.mcp_name.clone()),
    );
    object.insert("title".to_string(), JsonValue::String(tool.title.clone()));
    object.insert(
        "description".to_string(),
        JsonValue::String(tool.description.clone()),
    );
    if let Some(ref_value) = tool.r#ref.as_ref() {
        object.insert("ref".to_string(), agent_tool_ref_json(ref_value));
    }
    if let Some(annotations) = tool.annotations.as_ref() {
        object.insert("annotations".to_string(), annotations_json(annotations));
    }
    if include_schema {
        object.insert("input_schema".to_string(), schema_value(&tool.input_schema));
        object.insert(
            "output_schema".to_string(),
            schema_value_or_null(&tool.output_schema),
        );
    }
    JsonValue::Object(object)
}

fn agent_tool_ref_json(ref_value: &proto::AgentToolRef) -> JsonValue {
    let mut object = JsonMap::new();
    insert_nonempty(&mut object, "system", &ref_value.system);
    insert_nonempty(&mut object, "plugin", &ref_value.plugin);
    insert_nonempty(&mut object, "operation", &ref_value.operation);
    insert_nonempty(&mut object, "connection", &ref_value.connection);
    insert_nonempty(&mut object, "instance", &ref_value.instance);
    JsonValue::Object(object)
}

fn annotations_json(annotations: &proto::OperationAnnotations) -> JsonValue {
    let mut object = JsonMap::new();
    insert_optional_bool(&mut object, "read_only_hint", annotations.read_only_hint);
    insert_optional_bool(
        &mut object,
        "destructive_hint",
        annotations.destructive_hint,
    );
    insert_optional_bool(&mut object, "idempotent_hint", annotations.idempotent_hint);
    insert_optional_bool(&mut object, "open_world_hint", annotations.open_world_hint);
    JsonValue::Object(object)
}

fn insert_nonempty(object: &mut JsonMap<String, JsonValue>, key: &str, value: &str) {
    if !value.trim().is_empty() {
        object.insert(key.to_string(), JsonValue::String(value.trim().to_string()));
    }
}

fn insert_optional_bool(object: &mut JsonMap<String, JsonValue>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        object.insert(key.to_string(), JsonValue::Bool(value));
    }
}

fn proxy_error_result(error: ProxyError) -> CallToolResult {
    json_result(
        json!({
            "ok": false,
            "error": {
                "code": error.code,
                "message": error.message,
            }
        }),
        true,
    )
}

fn json_result(value: JsonValue, is_error: bool) -> CallToolResult {
    let text = serde_json::to_string(&value).expect("serialize MCP proxy JSON result");
    if is_error {
        CallToolResult::error(vec![Content::text(text)])
    } else {
        CallToolResult::success(vec![Content::text(text)])
    }
}

fn schema_value(raw: &str) -> JsonValue {
    serde_json::from_str::<JsonValue>(raw)
        .ok()
        .filter(JsonValue::is_object)
        .unwrap_or_else(|| json!({"type": "object"}))
}

fn schema_value_or_null(raw: &str) -> JsonValue {
    if raw.trim().is_empty() {
        JsonValue::Null
    } else {
        schema_value(raw)
    }
}

fn json_object(value: JsonValue) -> JsonMap<String, JsonValue> {
    value.as_object().expect("JSON object").clone()
}

fn validate_mcp_name_text(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 128 {
        return Err(format!("invalid Gestalt MCP tool name {:?}", name));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err(format!("unsafe Gestalt MCP tool name {:?}", name));
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
