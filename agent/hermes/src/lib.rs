mod acp;
mod config;
mod mcp_bridge;
mod store;

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use acp::{AcpNotification, AcpProcess};
use config::HermesConfig;
use gestalt::proto::v1 as proto;
use mcp_bridge::McpBridgeHandle;
use prost_types::{Struct, value::Kind};
use serde_json::{Value as JsonValue, json};
use store::{
    BeginTurnResult, CreateSessionResult, Store, event_to_proto, json_to_value, session_to_proto,
    turn_to_proto,
};
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status};

const PROVIDER_DISPLAY_NAME: &str = "Hermes ACP Agent";
const PROVIDER_DESCRIPTION: &str = "Runs Hermes through the Agent Client Protocol.";
const PROVIDER_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Default)]
pub struct HermesAgentProvider {
    inner: Arc<ProviderInner>,
}

#[derive(Default)]
struct ProviderInner {
    name: RwLock<String>,
    config: RwLock<Option<HermesConfig>>,
    warnings: RwLock<Vec<String>>,
    store: Mutex<Store>,
    processes: Mutex<HashMap<String, Arc<AcpProcess>>>,
    mcp_bridges: Mutex<HashMap<String, McpBridgeHandle>>,
}

#[gestalt::async_trait]
impl gestalt::AgentProvider for HermesAgentProvider {
    async fn configure(
        &self,
        name: &str,
        config: serde_json::Map<String, JsonValue>,
    ) -> gestalt::Result<()> {
        let resolved = HermesConfig::from_json(config).map_err(gestalt::Error::bad_request)?;
        self.close().await?;
        *self.inner.name.write().await = if name.trim().is_empty() {
            "hermes".to_string()
        } else {
            name.trim().to_string()
        };
        let mut warnings = Vec::new();
        if let Some(warning) = resolved.hermes_version_warning().await {
            warnings.push(warning);
        }
        *self.inner.config.write().await = Some(resolved);
        *self.inner.warnings.write().await = warnings;
        Ok(())
    }

    fn metadata(&self) -> Option<gestalt::RuntimeMetadata> {
        Some(gestalt::RuntimeMetadata {
            name: "hermes".to_string(),
            display_name: PROVIDER_DISPLAY_NAME.to_string(),
            description: PROVIDER_DESCRIPTION.to_string(),
            version: PROVIDER_VERSION.to_string(),
        })
    }

    fn warnings(&self) -> Vec<String> {
        futures_like_block_on_warnings(self)
    }

    async fn close(&self) -> gestalt::Result<()> {
        let processes: Vec<Arc<AcpProcess>> = self
            .inner
            .processes
            .lock()
            .await
            .drain()
            .map(|(_, process)| process)
            .collect();
        for process in processes {
            process.kill().await;
        }
        let bridges: Vec<McpBridgeHandle> = self
            .inner
            .mcp_bridges
            .lock()
            .await
            .drain()
            .map(|(_, bridge)| bridge)
            .collect();
        for bridge in bridges {
            bridge.shutdown();
        }
        self.inner.store.lock().await.clear();
        *self.inner.config.write().await = None;
        self.inner.warnings.write().await.clear();
        Ok(())
    }
}

#[gestalt::async_trait]
impl proto::agent_provider_server::AgentProvider for HermesAgentProvider {
    async fn create_session(
        &self,
        request: Request<proto::CreateAgentProviderSessionRequest>,
    ) -> Result<Response<proto::AgentSession>, Status> {
        let req = request.into_inner();
        if req.session_id.trim().is_empty() {
            return Err(Status::invalid_argument("session_id is required"));
        }
        let config = self.require_config().await?;
        let provider_name = self.provider_name().await;
        let model = config.resolve_model(&req.model);
        if let Some(existing) = self
            .inner
            .store
            .lock()
            .await
            .existing_session_for_create(&req)
        {
            return Ok(Response::new(session_to_proto(existing, false)));
        }
        let token = config
            .fresh_access_token()
            .await
            .map_err(|err| Status::failed_precondition(redacted_token_error(err)))?;
        let acp = Arc::new(
            AcpProcess::spawn(&config, token.as_deref())
                .await
                .map_err(Status::unavailable)?,
        );
        let acp_session_result = async {
            acp.initialize(config.timeout).await?;
            let acp_session_id = acp
                .new_session(
                    config.working_directory.to_string_lossy().as_ref(),
                    Vec::new(),
                    config.timeout,
                )
                .await?;
            if config.should_set_model(&model) {
                acp.set_model(&acp_session_id, &model, config.timeout)
                    .await?;
            }
            Ok::<String, String>(acp_session_id)
        }
        .await;
        acp.kill().await;
        let acp_session_id = acp_session_result.map_err(Status::unavailable)?;

        let mut store = self.inner.store.lock().await;
        let session = match store.create_session(&req, &provider_name, model, acp_session_id) {
            Ok(CreateSessionResult::Created(session) | CreateSessionResult::Existing(session)) => {
                session
            }
            Err(err) => return Err(Status::invalid_argument(err)),
        };
        Ok(Response::new(session_to_proto(session, false)))
    }

    async fn get_session(
        &self,
        request: Request<proto::GetAgentProviderSessionRequest>,
    ) -> Result<Response<proto::AgentSession>, Status> {
        self.require_config().await?;
        let req = request.into_inner();
        let session = self
            .inner
            .store
            .lock()
            .await
            .get_session(&req.session_id)
            .ok_or_else(|| {
                Status::not_found(format!("agent session {:?} was not found", req.session_id))
            })?;
        Ok(Response::new(session_to_proto(session, false)))
    }

    async fn list_sessions(
        &self,
        request: Request<proto::ListAgentProviderSessionsRequest>,
    ) -> Result<Response<proto::ListAgentProviderSessionsResponse>, Status> {
        self.require_config().await?;
        let req = request.into_inner();
        if req.limit < 0 {
            return Err(Status::invalid_argument("limit must be non-negative"));
        }
        let subject_id = req
            .subject
            .as_ref()
            .map(|subject| subject.subject_id.trim().to_string())
            .unwrap_or_default();
        let sessions = self
            .inner
            .store
            .lock()
            .await
            .list_sessions(&req.session_ids, &subject_id, req.state, req.limit)
            .into_iter()
            .map(|session| session_to_proto(session, req.summary_only))
            .collect();
        Ok(Response::new(proto::ListAgentProviderSessionsResponse {
            sessions,
        }))
    }

    async fn update_session(
        &self,
        request: Request<proto::UpdateAgentProviderSessionRequest>,
    ) -> Result<Response<proto::AgentSession>, Status> {
        self.require_config().await?;
        let req = request.into_inner();
        let session = self
            .inner
            .store
            .lock()
            .await
            .update_session(&req.session_id, &req.client_ref, req.state, req.metadata)
            .ok_or_else(|| {
                Status::not_found(format!("agent session {:?} was not found", req.session_id))
            })?;
        Ok(Response::new(session_to_proto(session, false)))
    }

    async fn create_turn(
        &self,
        request: Request<proto::CreateAgentProviderTurnRequest>,
    ) -> Result<Response<proto::AgentTurn>, Status> {
        let req = request.into_inner();
        validate_turn_request(&req)?;
        let config = self.require_config().await?;
        let provider_name = self.provider_name().await;
        let model = {
            let store = self.inner.store.lock().await;
            let session = store.get_session(&req.session_id).ok_or_else(|| {
                Status::not_found(format!("agent session {:?} was not found", req.session_id))
            })?;
            config.resolve_model(if req.model.trim().is_empty() {
                &session.model
            } else {
                &req.model
            })
        };

        let turn = {
            let mut store = self.inner.store.lock().await;
            match store.begin_turn(&req, &provider_name, model) {
                Ok(BeginTurnResult::Created(turn)) => {
                    let worker = self.clone();
                    let turn_id = turn.id.clone();
                    tokio::spawn(async move {
                        worker.complete_turn(turn_id).await;
                    });
                    turn
                }
                Ok(BeginTurnResult::Existing(turn)) => turn,
                Err(err) if err.contains("active turn") => {
                    return Err(Status::failed_precondition(err));
                }
                Err(err) if err.contains("was not found") => return Err(Status::not_found(err)),
                Err(err) => return Err(Status::invalid_argument(err)),
            }
        };
        Ok(Response::new(turn_to_proto(turn, false)))
    }

    async fn get_turn(
        &self,
        request: Request<proto::GetAgentProviderTurnRequest>,
    ) -> Result<Response<proto::AgentTurn>, Status> {
        self.require_config().await?;
        let req = request.into_inner();
        let turn = self
            .inner
            .store
            .lock()
            .await
            .get_turn(&req.turn_id)
            .ok_or_else(|| {
                Status::not_found(format!("agent turn {:?} was not found", req.turn_id))
            })?;
        Ok(Response::new(turn_to_proto(turn, false)))
    }

    async fn list_turns(
        &self,
        request: Request<proto::ListAgentProviderTurnsRequest>,
    ) -> Result<Response<proto::ListAgentProviderTurnsResponse>, Status> {
        self.require_config().await?;
        let req = request.into_inner();
        if req.limit < 0 {
            return Err(Status::invalid_argument("limit must be non-negative"));
        }
        let subject_id = req
            .subject
            .as_ref()
            .map(|subject| subject.subject_id.trim().to_string())
            .unwrap_or_default();
        let turns = self
            .inner
            .store
            .lock()
            .await
            .list_turns(
                &req.session_id,
                &req.turn_ids,
                &subject_id,
                req.status,
                req.limit,
            )
            .into_iter()
            .map(|turn| turn_to_proto(turn, req.summary_only))
            .collect();
        Ok(Response::new(proto::ListAgentProviderTurnsResponse {
            turns,
        }))
    }

    async fn cancel_turn(
        &self,
        request: Request<proto::CancelAgentProviderTurnRequest>,
    ) -> Result<Response<proto::AgentTurn>, Status> {
        self.require_config().await?;
        let req = request.into_inner();
        let provider_name = self.provider_name().await;
        let turn = {
            let mut store = self.inner.store.lock().await;
            let before = store.get_turn(&req.turn_id).ok_or_else(|| {
                Status::not_found(format!("agent turn {:?} was not found", req.turn_id))
            })?;
            let turn = store
                .cancel_turn(&req.turn_id, &req.reason)
                .ok_or_else(|| {
                    Status::not_found(format!("agent turn {:?} was not found", req.turn_id))
                })?;
            if before.status != proto::AgentExecutionStatus::Canceled as i32
                && turn.status == proto::AgentExecutionStatus::Canceled as i32
            {
                store.append_event(
                    &req.turn_id,
                    "turn.canceled",
                    &provider_name,
                    json!({ "reason": req.reason }),
                    Some(proto::AgentTurnDisplay {
                        kind: "status".to_string(),
                        phase: "completed".to_string(),
                        text: "Turn canceled".to_string(),
                        ..Default::default()
                    }),
                );
            }
            turn
        };
        if let Some(process) = self.inner.processes.lock().await.get(&turn.id).cloned() {
            let acp_session_id = self
                .inner
                .store
                .lock()
                .await
                .get_session(&turn.session_id)
                .map(|session| session.acp_session_id)
                .unwrap_or_default();
            tokio::spawn(async move {
                process.cancel(&acp_session_id).await;
                process.kill().await;
            });
        }
        if let Some(bridge) = self.inner.mcp_bridges.lock().await.remove(&turn.id) {
            bridge.shutdown();
        }
        Ok(Response::new(turn_to_proto(turn, false)))
    }

    async fn list_turn_events(
        &self,
        request: Request<proto::ListAgentProviderTurnEventsRequest>,
    ) -> Result<Response<proto::ListAgentProviderTurnEventsResponse>, Status> {
        self.require_config().await?;
        let req = request.into_inner();
        if req.limit < 0 {
            return Err(Status::invalid_argument("limit must be non-negative"));
        }
        let events = self
            .inner
            .store
            .lock()
            .await
            .list_events(&req.turn_id, req.after_seq, req.limit)
            .into_iter()
            .map(event_to_proto)
            .collect();
        Ok(Response::new(proto::ListAgentProviderTurnEventsResponse {
            events,
        }))
    }

    async fn get_interaction(
        &self,
        request: Request<proto::GetAgentProviderInteractionRequest>,
    ) -> Result<Response<proto::AgentInteraction>, Status> {
        self.require_config().await?;
        let req = request.into_inner();
        Err(Status::not_found(format!(
            "agent interaction {:?} was not found",
            req.interaction_id
        )))
    }

    async fn list_interactions(
        &self,
        _request: Request<proto::ListAgentProviderInteractionsRequest>,
    ) -> Result<Response<proto::ListAgentProviderInteractionsResponse>, Status> {
        self.require_config().await?;
        Ok(Response::new(
            proto::ListAgentProviderInteractionsResponse {
                interactions: Vec::new(),
            },
        ))
    }

    async fn resolve_interaction(
        &self,
        request: Request<proto::ResolveAgentProviderInteractionRequest>,
    ) -> Result<Response<proto::AgentInteraction>, Status> {
        self.require_config().await?;
        let req = request.into_inner();
        Err(Status::not_found(format!(
            "agent interaction {:?} was not found",
            req.interaction_id
        )))
    }

    async fn get_capabilities(
        &self,
        _request: Request<proto::GetAgentProviderCapabilitiesRequest>,
    ) -> Result<Response<proto::AgentProviderCapabilities>, Status> {
        self.require_config().await?;
        Ok(Response::new(proto::AgentProviderCapabilities {
            streaming_text: true,
            tool_calls: true,
            parallel_tool_calls: false,
            structured_output: false,
            interactions: false,
            resumable_turns: false,
            reasoning_summaries: true,
            bounded_list_hydration: true,
            supported_tool_sources: vec![proto::AgentToolSourceMode::McpCatalog as i32],
            supports_session_start: false,
            supports_prepared_workspace: false,
        }))
    }
}

impl HermesAgentProvider {
    async fn require_config(&self) -> Result<HermesConfig, Status> {
        self.inner
            .config
            .read()
            .await
            .clone()
            .ok_or_else(|| Status::failed_precondition("provider has not been configured"))
    }

    async fn provider_name(&self) -> String {
        let name = self.inner.name.read().await.clone();
        if name.trim().is_empty() {
            "hermes".to_string()
        } else {
            name
        }
    }

    async fn complete_turn(&self, turn_id: String) {
        let result = self.run_turn(&turn_id).await;
        let provider_name = self.provider_name().await;
        if let Some(process) = self.inner.processes.lock().await.remove(&turn_id) {
            process.kill().await;
        }
        if let Some(bridge) = self.inner.mcp_bridges.lock().await.remove(&turn_id) {
            bridge.shutdown();
        }
        let mut store = self.inner.store.lock().await;
        let current = store.get_turn(&turn_id);
        if current
            .as_ref()
            .is_some_and(|turn| turn.status == proto::AgentExecutionStatus::Canceled as i32)
        {
            return;
        }
        match result {
            Ok(()) => {
                store.append_event(&turn_id, "turn.completed", &provider_name, json!({}), None);
                let _ = store.finish_turn(
                    &turn_id,
                    proto::AgentExecutionStatus::Succeeded as i32,
                    String::new(),
                );
            }
            Err(err) => {
                store.append_event(
                    &turn_id,
                    "turn.failed",
                    &provider_name,
                    json!({ "message": err }),
                    Some(proto::AgentTurnDisplay {
                        kind: "error".to_string(),
                        phase: "completed".to_string(),
                        text: err.clone(),
                        ..Default::default()
                    }),
                );
                let _ =
                    store.finish_turn(&turn_id, proto::AgentExecutionStatus::Failed as i32, err);
            }
        }
    }

    async fn run_turn(&self, turn_id: &str) -> Result<(), String> {
        let config = self
            .require_config()
            .await
            .map_err(|err| err.message().to_string())?;
        let provider_name = self.provider_name().await;
        let (session_id, acp_session_id, model, messages, tool_refs, tool_source, run_grant) = {
            let store = self.inner.store.lock().await;
            let turn = store
                .get_turn(turn_id)
                .ok_or_else(|| format!("agent turn {turn_id:?} was not found"))?;
            let session = store
                .get_session(&turn.session_id)
                .ok_or_else(|| format!("agent session {:?} was not found", turn.session_id))?;
            (
                turn.session_id,
                session.acp_session_id,
                turn.model,
                turn.messages,
                turn.tool_refs,
                turn.tool_source,
                turn.run_grant,
            )
        };
        let mcp_catalog_enabled = tool_source == proto::AgentToolSourceMode::McpCatalog as i32;
        if self.is_turn_canceled(turn_id).await {
            return Err("turn canceled".to_string());
        }
        let token = config
            .fresh_access_token()
            .await
            .map_err(redacted_token_error)?;
        if self.is_turn_canceled(turn_id).await {
            return Err("turn canceled".to_string());
        }
        let process = Arc::new(AcpProcess::spawn(&config, token.as_deref()).await?);
        self.inner
            .processes
            .lock()
            .await
            .insert(turn_id.to_string(), process.clone());
        let result = async {
            if self.is_turn_canceled(turn_id).await {
                return Err("turn canceled".to_string());
            }
            let _initialize_result = process.initialize(config.timeout).await?;
            let mcp_servers = if mcp_catalog_enabled && !tool_refs.is_empty() {
                let bridge = mcp_bridge::start_bridge(
                    session_id.clone(),
                    turn_id.to_string(),
                    run_grant.clone(),
                )
                .await?;
                let mcp_server = bridge.acp_server_config();
                self.inner
                    .mcp_bridges
                    .lock()
                    .await
                    .insert(turn_id.to_string(), bridge);
                vec![mcp_server]
            } else {
                Vec::new()
            };
            process
                .load_session(
                    config.working_directory.to_string_lossy().as_ref(),
                    &acp_session_id,
                    mcp_servers,
                    config.timeout,
                )
                .await?;
            if config.should_set_model(&model) {
                process
                    .set_model(&acp_session_id, &model, config.timeout)
                    .await?;
            }
            if self.is_turn_canceled(turn_id).await {
                return Err("turn canceled".to_string());
            }

            let prompt = messages_to_prompt(&messages)?;
            let prompt_process = process.clone();
            let prompt_session_id = acp_session_id.clone();
            let timeout = config.timeout;
            let mut prompt_task = tokio::spawn(async move {
                prompt_process
                    .prompt(&prompt_session_id, prompt, timeout)
                    .await
            });

            loop {
                tokio::select! {
                    prompt_result = &mut prompt_task => {
                        let prompt_result = prompt_result
                            .map_err(|err| format!("Hermes ACP prompt task failed: {err}"))??;
                        let stop_reason = prompt_result
                            .get("stopReason")
                            .or_else(|| prompt_result.get("stop_reason"))
                            .and_then(JsonValue::as_str)
                            .unwrap_or("end_turn");
                        if stop_reason == "cancelled" {
                            return Err("Hermes ACP turn was cancelled".to_string());
                        }
                        if let Some(err) = hermes_stderr_failure(&process.stderr().await) {
                            return Err(err);
                        }
                        return Ok(());
                    }
                    notification = process.next_notification() => {
                        match notification {
                            Some(AcpNotification::SessionUpdate(params)) => {
                                self.record_session_update(&provider_name, turn_id, &session_id, &acp_session_id, params).await;
                            }
                            Some(AcpNotification::ProtocolError(err) | AcpNotification::ChildExited(err)) => {
                                let stderr = process.stderr().await;
                                if stderr.trim().is_empty() {
                                    return Err(err);
                                }
                                return Err(format!("{err}; stderr: {}", stderr.trim()));
                            }
                            None => {
                                return Err("Hermes ACP notification stream closed".to_string());
                            }
                        }
                    }
                }
            }
        }.await;
        process.kill().await;
        if let Some(bridge) = self.inner.mcp_bridges.lock().await.remove(turn_id) {
            bridge.shutdown();
        }
        result
    }

    async fn is_turn_canceled(&self, turn_id: &str) -> bool {
        self.inner
            .store
            .lock()
            .await
            .get_turn(turn_id)
            .is_some_and(|turn| turn.status == proto::AgentExecutionStatus::Canceled as i32)
    }

    async fn record_session_update(
        &self,
        provider_name: &str,
        turn_id: &str,
        gestalt_session_id: &str,
        acp_session_id: &str,
        params: JsonValue,
    ) {
        let update_session_id = params
            .get("sessionId")
            .or_else(|| params.get("session_id"))
            .and_then(JsonValue::as_str)
            .unwrap_or_default();
        if !update_session_id.is_empty() && update_session_id != acp_session_id {
            self.inner.store.lock().await.append_event(
                turn_id,
                "acp.session_update.ignored",
                provider_name,
                json!({
                    "gestaltSessionId": gestalt_session_id,
                    "acpSessionId": acp_session_id,
                    "updateSessionId": update_session_id
                }),
                None,
            );
            return;
        }
        let update = params.get("update").cloned().unwrap_or(JsonValue::Null);
        let session_update = update
            .get("sessionUpdate")
            .or_else(|| update.get("session_update"))
            .and_then(JsonValue::as_str)
            .unwrap_or("unknown");

        match session_update {
            "agent_message_chunk" => {
                let text = content_text(&update);
                let mut store = self.inner.store.lock().await;
                store.append_output(turn_id, &text);
                store.append_event(
                    turn_id,
                    "agent.message.delta",
                    provider_name,
                    json!({ "text": text }),
                    Some(proto::AgentTurnDisplay {
                        kind: "text".to_string(),
                        phase: "delta".to_string(),
                        text,
                        ..Default::default()
                    }),
                );
            }
            "agent_thought_chunk" => {
                let text = content_text(&update);
                self.inner.store.lock().await.append_event(
                    turn_id,
                    "agent.reasoning.delta",
                    provider_name,
                    json!({ "text": text }),
                    Some(proto::AgentTurnDisplay {
                        kind: "reasoning".to_string(),
                        phase: "delta".to_string(),
                        text,
                        ..Default::default()
                    }),
                );
            }
            "tool_call" | "tool_call_update" => {
                let status = update
                    .get("status")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("in_progress");
                let phase = match status {
                    "completed" => "completed",
                    "failed" => "failed",
                    "pending" => "pending",
                    _ => "progress",
                };
                self.inner.store.lock().await.append_event(
                    turn_id,
                    if session_update == "tool_call" {
                        "tool.call"
                    } else {
                        "tool.call.update"
                    },
                    provider_name,
                    json!({ "update": update }),
                    Some(proto::AgentTurnDisplay {
                        kind: "tool".to_string(),
                        phase: phase.to_string(),
                        label: update
                            .get("title")
                            .and_then(JsonValue::as_str)
                            .unwrap_or("Hermes tool")
                            .to_string(),
                        r#ref: update
                            .get("toolCallId")
                            .or_else(|| update.get("tool_call_id"))
                            .and_then(JsonValue::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        input: update.get("rawInput").cloned().map(json_to_value),
                        output: update.get("rawOutput").cloned().map(json_to_value),
                        ..Default::default()
                    }),
                );
            }
            _ => {
                self.inner.store.lock().await.append_event(
                    turn_id,
                    "acp.session_update",
                    provider_name,
                    json!({ "update": update }),
                    Some(proto::AgentTurnDisplay {
                        kind: "status".to_string(),
                        phase: "progress".to_string(),
                        label: session_update.to_string(),
                        ..Default::default()
                    }),
                );
            }
        }
    }
}

fn hermes_stderr_failure(stderr: &str) -> Option<String> {
    let line = stderr.lines().rev().map(str::trim).find(|line| {
        line.contains("Non-retryable client error")
            || line.contains("Non-retryable error")
            || line.contains("PermissionDeniedError")
            || line.contains("AuthenticationError")
    })?;
    Some(format!(
        "Hermes reported a terminal error: {}",
        truncate_for_status(line, 700)
    ))
}

fn truncate_for_status(value: &str, max_chars: usize) -> String {
    let mut result = String::new();
    for ch in value.chars().take(max_chars) {
        result.push(ch);
    }
    if value.chars().count() > max_chars {
        result.push_str("...");
    }
    result
}

fn validate_turn_request(req: &proto::CreateAgentProviderTurnRequest) -> Result<(), Status> {
    if req.messages.is_empty() {
        return Err(Status::invalid_argument(
            "messages must contain at least one entry",
        ));
    }
    if !req.tools.is_empty() {
        return Err(Status::invalid_argument(
            "resolved tools are not supported by agent/hermes; use tool_source=MCP_CATALOG",
        ));
    }
    validate_mcp_catalog_tool_refs(&req.tool_refs)?;
    let tool_source = proto::AgentToolSourceMode::try_from(req.tool_source)
        .map_err(|_| Status::invalid_argument("unsupported tool_source for agent/hermes"))?;
    match tool_source {
        proto::AgentToolSourceMode::Unspecified => {
            if !req.tool_refs.is_empty() {
                return Err(Status::invalid_argument(
                    "tool_source=MCP_CATALOG is required when tool_refs are provided",
                ));
            }
        }
        proto::AgentToolSourceMode::McpCatalog => {
            if req.run_grant.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "run_grant is required when tool_source=MCP_CATALOG",
                ));
            }
        }
    }
    if req
        .response_schema
        .as_ref()
        .is_some_and(|schema| !schema.fields.is_empty())
    {
        return Err(Status::invalid_argument(
            "response_schema is not supported by agent/hermes",
        ));
    }
    if req
        .model_options
        .as_ref()
        .is_some_and(|options| !options.fields.is_empty())
    {
        return Err(Status::invalid_argument(
            "model_options are not supported by agent/hermes",
        ));
    }
    Ok(())
}

fn validate_mcp_catalog_tool_refs(refs: &[proto::AgentToolRef]) -> Result<(), Status> {
    for (index, tool_ref) in refs.iter().enumerate() {
        let system = tool_ref.system.trim();
        let plugin = tool_ref.plugin.trim();
        let operation = tool_ref.operation.trim();
        let connection = tool_ref.connection.trim();
        let instance = tool_ref.instance.trim();
        let title = tool_ref.title.trim();
        let description = tool_ref.description.trim();
        if system.is_empty() && plugin.is_empty() {
            return Err(Status::invalid_argument(format!(
                "tool_refs[{index}].plugin or system is required"
            )));
        }
        if !system.is_empty() && !plugin.is_empty() {
            return Err(Status::invalid_argument(format!(
                "tool_refs[{index}] must set exactly one of plugin or system"
            )));
        }
        if !system.is_empty() {
            if system != "workflow" {
                return Err(Status::invalid_argument(format!(
                    "tool_refs[{index}].system {system:?} is not supported"
                )));
            }
            if operation.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "tool_refs[{index}].operation is required for system refs"
                )));
            }
            if operation == "*" {
                return Err(Status::invalid_argument(format!(
                    "tool_refs[{index}].operation wildcard is not supported"
                )));
            }
            if !connection.is_empty()
                || !instance.is_empty()
                || !title.is_empty()
                || !description.is_empty()
            {
                return Err(Status::invalid_argument(format!(
                    "tool_refs[{index}] system refs cannot include connection, instance, title, or description"
                )));
            }
            continue;
        }
        if operation == "*" || connection == "*" || instance == "*" {
            return Err(Status::invalid_argument(format!(
                "tool_refs[{index}] wildcard fields are not supported"
            )));
        }
        if plugin == "*"
            && (!operation.is_empty()
                || !connection.is_empty()
                || !instance.is_empty()
                || !title.is_empty()
                || !description.is_empty())
        {
            return Err(Status::invalid_argument(format!(
                "tool_refs[{index}] global ref cannot include operation, connection, instance, title, or description"
            )));
        }
    }
    Ok(())
}

fn messages_to_prompt(messages: &[proto::AgentMessage]) -> Result<String, String> {
    let mut prompt = String::new();
    for (index, message) in messages.iter().enumerate() {
        writeln!(
            &mut prompt,
            r#"<message {} role="{}">"#,
            index + 1,
            message.role
        )
        .map_err(|err| err.to_string())?;
        if !message.text.is_empty() {
            prompt.push_str(&message.text);
            prompt.push('\n');
        }
        if !message.parts.is_empty() {
            let parts: Vec<JsonValue> = message.parts.iter().map(message_part_to_json).collect();
            let serialized = serde_json::to_string_pretty(&parts).map_err(|err| err.to_string())?;
            writeln!(&mut prompt, "<parts>{serialized}</parts>").map_err(|err| err.to_string())?;
        }
        writeln!(&mut prompt, "</message>").map_err(|err| err.to_string())?;
    }
    Ok(prompt)
}

fn message_part_to_json(part: &proto::AgentMessagePart) -> JsonValue {
    let part_type = proto::AgentMessagePartType::try_from(part.r#type)
        .unwrap_or(proto::AgentMessagePartType::Unspecified);
    match part_type {
        proto::AgentMessagePartType::Text => json!({ "type": "text", "text": part.text }),
        proto::AgentMessagePartType::Json => {
            json!({ "type": "json", "json": part.json.as_ref().map(struct_to_json).unwrap_or(JsonValue::Null) })
        }
        proto::AgentMessagePartType::ImageRef => json!({
            "type": "image_ref",
            "uri": part.image_ref.as_ref().map(|image| image.uri.as_str()).unwrap_or_default(),
            "mime_type": part.image_ref.as_ref().map(|image| image.mime_type.as_str()).unwrap_or_default()
        }),
        proto::AgentMessagePartType::ToolCall => match &part.tool_call {
            Some(tool_call) => json!({
                "type": "tool_call",
                "id": tool_call.id,
                "tool_id": tool_call.tool_id,
                "arguments": tool_call.arguments.as_ref().map(struct_to_json).unwrap_or(JsonValue::Null)
            }),
            None => json!({ "type": "tool_call" }),
        },
        proto::AgentMessagePartType::ToolResult => match &part.tool_result {
            Some(tool_result) => json!({
                "type": "tool_result",
                "tool_call_id": tool_result.tool_call_id,
                "status": tool_result.status,
                "content": tool_result.content,
                "output": tool_result.output.as_ref().map(struct_to_json).unwrap_or(JsonValue::Null)
            }),
            None => json!({ "type": "tool_result" }),
        },
        proto::AgentMessagePartType::Unspecified => json!({ "type": "unspecified" }),
    }
}

fn struct_to_json(value: &Struct) -> JsonValue {
    JsonValue::Object(
        value
            .fields
            .iter()
            .map(|(key, value)| (key.clone(), prost_value_to_json(value)))
            .collect(),
    )
}

fn prost_value_to_json(value: &prost_types::Value) -> JsonValue {
    match value.kind.as_ref() {
        Some(Kind::NullValue(_)) | None => JsonValue::Null,
        Some(Kind::NumberValue(value)) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Some(Kind::StringValue(value)) => JsonValue::String(value.clone()),
        Some(Kind::BoolValue(value)) => JsonValue::Bool(*value),
        Some(Kind::StructValue(value)) => struct_to_json(value),
        Some(Kind::ListValue(value)) => {
            JsonValue::Array(value.values.iter().map(prost_value_to_json).collect())
        }
    }
}

fn content_text(update: &JsonValue) -> String {
    update
        .get("content")
        .and_then(|content| content.get("text"))
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string()
}

fn redacted_token_error(err: String) -> String {
    format!("refresh access token: {err}")
}

fn futures_like_block_on_warnings(provider: &HermesAgentProvider) -> Vec<String> {
    match provider.inner.warnings.try_read() {
        Ok(warnings) => warnings.clone(),
        Err(_) => Vec::new(),
    }
}

gestalt::export_agent_provider!(constructor = HermesAgentProvider::default);
