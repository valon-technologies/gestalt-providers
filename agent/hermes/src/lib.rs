mod acp;
mod config;
mod mcp_bridge;
mod store;

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use acp::{AcpNotification, AcpProcess};
use config::HermesConfig;
use mcp_bridge::McpBridgeHandle;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use store::{
    BeginTurnResult, CreateSessionResult, Store, agent_session, agent_turn, agent_turn_event,
    session_readable_by,
};
use tokio::sync::{Mutex, RwLock};

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

    async fn create_session(
        &self,
        req: gestalt::CreateAgentProviderSessionRequest,
    ) -> gestalt::Result<gestalt::AgentSession> {
        if req.session_id.trim().is_empty() {
            return Err(gestalt::Error::bad_request("session_id is required"));
        }
        validate_session_tool_config(&req)?;
        let config = self.require_config().await?;
        let provider_name = self.provider_name().await;
        let model = config.resolve_model(&req.model);
        let subject_id = subject_id(req.subject.as_ref());
        if let Some(existing) = self
            .inner
            .store
            .lock()
            .await
            .existing_session_for_create(&req)
        {
            if !session_readable_by(&existing, &subject_id) {
                return Err(gestalt::Error::not_found(format!(
                    "agent session {:?} was not found",
                    req.session_id
                )));
            }
            return Ok(agent_session(existing, false));
        }
        let token = config
            .fresh_access_token()
            .await
            .map_err(|err| gestalt::Error::failed_precondition(redacted_token_error(err)))?;
        let acp = Arc::new(
            AcpProcess::spawn(&config, &token)
                .await
                .map_err(gestalt::Error::unavailable)?,
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
        let acp_session_id = acp_session_result.map_err(gestalt::Error::unavailable)?;

        let mut store = self.inner.store.lock().await;
        let session = match store.create_session(&req, &provider_name, model, acp_session_id) {
            Ok(CreateSessionResult::Created(session)) => session,
            Ok(CreateSessionResult::Existing(session)) => {
                if store
                    .get_session_if_readable(&session.id, &subject_id)
                    .is_none()
                {
                    return Err(gestalt::Error::not_found(format!(
                        "agent session {:?} was not found",
                        req.session_id
                    )));
                }
                session
            }
            Err(err) => return Err(gestalt::Error::bad_request(err)),
        };
        Ok(agent_session(session, false))
    }

    async fn get_session(
        &self,
        req: gestalt::GetAgentProviderSessionRequest,
    ) -> gestalt::Result<gestalt::AgentSession> {
        self.require_config().await?;
        let subject_id = subject_id(req.subject.as_ref());
        let session = self
            .inner
            .store
            .lock()
            .await
            .get_session_if_readable(&req.session_id, &subject_id)
            .ok_or_else(|| {
                gestalt::Error::not_found(format!(
                    "agent session {:?} was not found",
                    req.session_id
                ))
            })?;
        Ok(agent_session(session, false))
    }

    async fn list_sessions(
        &self,
        req: gestalt::ListAgentProviderSessionsRequest,
    ) -> gestalt::Result<gestalt::ListAgentProviderSessionsResponse> {
        self.require_config().await?;
        if req.limit < 0 {
            return Err(gestalt::Error::bad_request("limit must be non-negative"));
        }
        let subject_id = subject_id(req.subject.as_ref());
        let sessions = self
            .inner
            .store
            .lock()
            .await
            .list_sessions(&req.session_ids, &subject_id, req.state, req.limit)
            .into_iter()
            .map(|session| agent_session(session, req.summary_only))
            .collect();
        Ok(gestalt::ListAgentProviderSessionsResponse { sessions })
    }

    async fn update_session(
        &self,
        req: gestalt::UpdateAgentProviderSessionRequest,
    ) -> gestalt::Result<gestalt::AgentSession> {
        self.require_config().await?;
        let metadata = req.metadata.clone();
        let subject_id = subject_id(req.subject.as_ref());
        let session = {
            let mut store = self.inner.store.lock().await;
            if store.get_session(&req.session_id).is_none() {
                return Err(gestalt::Error::not_found(format!(
                    "agent session {:?} was not found",
                    req.session_id
                )));
            }
            if store
                .get_session_if_owner(&req.session_id, &subject_id)
                .is_none()
            {
                return Err(gestalt::Error::permission_denied(format!(
                    "agent session {:?} is not writable",
                    req.session_id
                )));
            }
            store
                .update_session(
                    &req.session_id,
                    &req.client_ref,
                    req.state,
                    metadata,
                    &subject_id,
                )
                .ok_or_else(|| {
                    gestalt::Error::not_found(format!(
                        "agent session {:?} was not found",
                        req.session_id
                    ))
                })?
        };
        Ok(agent_session(session, false))
    }

    async fn create_turn(
        &self,
        mut req: gestalt::CreateAgentProviderTurnRequest,
    ) -> gestalt::Result<gestalt::AgentTurn> {
        let config = self.require_config().await?;
        let provider_name = self.provider_name().await;
        let subject_id = subject_id(req.subject.as_ref());
        let (model, session_tool_source, session_tool_refs) = {
            let store = self.inner.store.lock().await;
            let session = store.get_session(&req.session_id).ok_or_else(|| {
                gestalt::Error::not_found(format!(
                    "agent session {:?} was not found",
                    req.session_id
                ))
            })?;
            if store
                .get_session_if_owner(&req.session_id, &subject_id)
                .is_none()
            {
                return Err(gestalt::Error::permission_denied(format!(
                    "agent session {:?} is not writable",
                    req.session_id
                )));
            }
            (
                config.resolve_model(if req.model.trim().is_empty() {
                    &session.model
                } else {
                    &req.model
                }),
                session.tool_source,
                session.tool_refs,
            )
        };
        let (tool_source, tool_refs) =
            effective_turn_tool_scope(&req, session_tool_source, &session_tool_refs)?;
        req.tool_source = tool_source;
        req.tool_refs = tool_refs;
        let request_context = req.context.clone();
        validate_turn_request(&req)?;

        let turn = {
            let mut store = self.inner.store.lock().await;
            match store.begin_turn(&req, &provider_name, model, &subject_id, request_context) {
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
                    return Err(gestalt::Error::failed_precondition(err));
                }
                Err(err) if err.contains("not writable") => {
                    return Err(gestalt::Error::permission_denied(err));
                }
                Err(err) if err.contains("was not found") => {
                    return Err(gestalt::Error::not_found(err));
                }
                Err(err) => return Err(gestalt::Error::bad_request(err)),
            }
        };
        Ok(agent_turn(turn, false))
    }

    async fn get_turn(
        &self,
        req: gestalt::GetAgentProviderTurnRequest,
    ) -> gestalt::Result<gestalt::AgentTurn> {
        self.require_config().await?;
        let subject_id = subject_id(req.subject.as_ref());
        let turn = self
            .inner
            .store
            .lock()
            .await
            .get_turn_if_readable(&req.turn_id, &subject_id)
            .ok_or_else(|| {
                gestalt::Error::not_found(format!("agent turn {:?} was not found", req.turn_id))
            })?;
        Ok(agent_turn(turn, false))
    }

    async fn list_turns(
        &self,
        req: gestalt::ListAgentProviderTurnsRequest,
    ) -> gestalt::Result<gestalt::ListAgentProviderTurnsResponse> {
        self.require_config().await?;
        if req.limit < 0 {
            return Err(gestalt::Error::bad_request("limit must be non-negative"));
        }
        let subject_id = subject_id(req.subject.as_ref());
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
            .map(|turn| agent_turn(turn, req.summary_only))
            .collect();
        Ok(gestalt::ListAgentProviderTurnsResponse { turns })
    }

    async fn cancel_turn(
        &self,
        req: gestalt::CancelAgentProviderTurnRequest,
    ) -> gestalt::Result<gestalt::AgentTurn> {
        self.require_config().await?;
        let provider_name = self.provider_name().await;
        let subject_id = subject_id(req.subject.as_ref());
        let turn = {
            let mut store = self.inner.store.lock().await;
            let before = store.get_turn(&req.turn_id).ok_or_else(|| {
                gestalt::Error::not_found(format!("agent turn {:?} was not found", req.turn_id))
            })?;
            if store.get_turn_if_owner(&req.turn_id, &subject_id).is_none() {
                return Err(gestalt::Error::permission_denied(format!(
                    "agent turn {:?} is not writable",
                    req.turn_id
                )));
            }
            let turn = store
                .cancel_turn(&req.turn_id, &req.reason, &subject_id)
                .ok_or_else(|| {
                    gestalt::Error::not_found(format!("agent turn {:?} was not found", req.turn_id))
                })?;
            if before.status != gestalt::AgentExecutionStatus::Canceled
                && turn.status == gestalt::AgentExecutionStatus::Canceled
            {
                store.append_event(
                    &req.turn_id,
                    "turn.canceled",
                    &provider_name,
                    json!({ "reason": req.reason }),
                    Some(gestalt::AgentTurnDisplay {
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
        Ok(agent_turn(turn, false))
    }

    async fn list_turn_events(
        &self,
        req: gestalt::ListAgentProviderTurnEventsRequest,
    ) -> gestalt::Result<gestalt::ListAgentProviderTurnEventsResponse> {
        self.require_config().await?;
        if req.limit < 0 {
            return Err(gestalt::Error::bad_request("limit must be non-negative"));
        }
        let subject_id = subject_id(req.subject.as_ref());
        let events = self
            .inner
            .store
            .lock()
            .await
            .list_events(&req.turn_id, req.after_seq, req.limit, &subject_id)
            .into_iter()
            .map(agent_turn_event)
            .collect();
        Ok(gestalt::ListAgentProviderTurnEventsResponse { events })
    }

    async fn get_interaction(
        &self,
        req: gestalt::GetAgentProviderInteractionRequest,
    ) -> gestalt::Result<gestalt::AgentInteraction> {
        self.require_config().await?;
        Err(gestalt::Error::not_found(format!(
            "agent interaction {:?} was not found",
            req.interaction_id
        )))
    }

    async fn list_interactions(
        &self,
        request: gestalt::ListAgentProviderInteractionsRequest,
    ) -> gestalt::Result<gestalt::ListAgentProviderInteractionsResponse> {
        self.require_config().await?;
        if !request.turn_id.trim().is_empty() {
            let subject_id = subject_id(request.subject.as_ref());
            if self
                .inner
                .store
                .lock()
                .await
                .get_turn_if_readable(&request.turn_id, &subject_id)
                .is_none()
            {
                return Ok(gestalt::ListAgentProviderInteractionsResponse {
                    interactions: Vec::new(),
                });
            }
        }
        Ok(gestalt::ListAgentProviderInteractionsResponse {
            interactions: Vec::new(),
        })
    }

    async fn resolve_interaction(
        &self,
        req: gestalt::ResolveAgentProviderInteractionRequest,
    ) -> gestalt::Result<gestalt::AgentInteraction> {
        self.require_config().await?;
        Err(gestalt::Error::not_found(format!(
            "agent interaction {:?} was not found",
            req.interaction_id
        )))
    }

    async fn get_capabilities(
        &self,
        _request: gestalt::GetAgentProviderCapabilitiesRequest,
    ) -> gestalt::Result<gestalt::AgentProviderCapabilities> {
        self.require_config().await?;
        Ok(gestalt::AgentProviderCapabilities {
            streaming_text: true,
            tool_calls: true,
            parallel_tool_calls: false,
            interactions: false,
            resumable_turns: false,
            reasoning_summaries: true,
            bounded_list_hydration: true,
            supported_tool_sources: vec![gestalt::AgentToolSourceMode::Catalog],
            supports_session_start: false,
            supports_prepared_workspace: false,
        })
    }
}

impl HermesAgentProvider {
    async fn require_config(&self) -> gestalt::Result<HermesConfig> {
        self.inner
            .config
            .read()
            .await
            .clone()
            .ok_or_else(|| gestalt::Error::failed_precondition("provider has not been configured"))
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
            .is_some_and(|turn| turn.status == gestalt::AgentExecutionStatus::Canceled)
        {
            return;
        }
        match result {
            Ok(()) => match current
                .as_ref()
                .ok_or_else(|| format!("agent turn {turn_id:?} was not found"))
                .and_then(turn_output_for)
            {
                Ok(output) => {
                    let event_data = turn_output_event_data(&output);
                    let _ = store.set_output(&turn_id, output);
                    store.append_event(
                        &turn_id,
                        "assistant.message",
                        &provider_name,
                        event_data,
                        None,
                    );
                    store.append_event(&turn_id, "turn.completed", &provider_name, json!({}), None);
                    let _ = store.finish_turn(
                        &turn_id,
                        gestalt::AgentExecutionStatus::Succeeded,
                        String::new(),
                    );
                }
                Err(err) => {
                    store.append_event(
                        &turn_id,
                        "turn.failed",
                        &provider_name,
                        json!({ "message": err }),
                        Some(gestalt::AgentTurnDisplay {
                            kind: "error".to_string(),
                            phase: "completed".to_string(),
                            text: err.clone(),
                            ..Default::default()
                        }),
                    );
                    let _ = store.finish_turn(&turn_id, gestalt::AgentExecutionStatus::Failed, err);
                }
            },
            Err(err) => {
                store.append_event(
                    &turn_id,
                    "turn.failed",
                    &provider_name,
                    json!({ "message": err }),
                    Some(gestalt::AgentTurnDisplay {
                        kind: "error".to_string(),
                        phase: "completed".to_string(),
                        text: err.clone(),
                        ..Default::default()
                    }),
                );
                let _ = store.finish_turn(&turn_id, gestalt::AgentExecutionStatus::Failed, err);
            }
        }
    }

    async fn run_turn(&self, turn_id: &str) -> Result<(), String> {
        let config = self
            .require_config()
            .await
            .map_err(|err| err.message().to_string())?;
        let provider_name = self.provider_name().await;
        let (
            session_id,
            acp_session_id,
            model,
            messages,
            output_request,
            tool_refs,
            tool_source,
            request_context,
        ) = {
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
                turn.output_request,
                turn.tool_refs,
                turn.tool_source,
                turn.request_context,
            )
        };
        let catalog_tools_enabled = tool_source == gestalt::AgentToolSourceMode::Catalog;
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
        let process = Arc::new(AcpProcess::spawn(&config, &token).await?);
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
            let mcp_servers = if catalog_tools_enabled && !tool_refs.is_empty() {
                let request_context = request_context
                    .clone()
                    .ok_or_else(|| "request context is required when tool_source=CATALOG".to_string())?;
                let bridge = mcp_bridge::start_bridge(
                    session_id.clone(),
                    turn_id.to_string(),
                    request_context,
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

            let prompt = messages_to_prompt(&messages, &output_request)?;
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
                        if let Some(err) = hermes_terminal_stderr_failure(&process).await {
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
            .is_some_and(|turn| turn.status == gestalt::AgentExecutionStatus::Canceled)
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
                    Some(gestalt::AgentTurnDisplay {
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
                    Some(gestalt::AgentTurnDisplay {
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
                    Some(gestalt::AgentTurnDisplay {
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
                        input: update.get("rawInput").cloned(),
                        output: update.get("rawOutput").cloned(),
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
                    Some(gestalt::AgentTurnDisplay {
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

async fn hermes_terminal_stderr_failure(process: &AcpProcess) -> Option<String> {
    if let Some(err) = hermes_stderr_failure(&process.stderr().await) {
        return Some(err);
    }
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    hermes_stderr_failure(&process.stderr().await)
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

fn validate_turn_request(req: &gestalt::CreateAgentProviderTurnRequest) -> gestalt::Result<()> {
    let has_object_fields = |value: Option<&JsonValue>| {
        value
            .and_then(JsonValue::as_object)
            .is_some_and(|object| !object.is_empty())
    };
    if req.messages.is_empty() {
        return Err(gestalt::Error::bad_request(
            "messages must contain at least one entry",
        ));
    }
    if !req.tools.is_empty() {
        return Err(gestalt::Error::bad_request(
            "resolved tools are not supported by agent/hermes; use tool_source=CATALOG",
        ));
    }
    if let gestalt::AgentOutput::Structured(output) = &req.output {
        validate_schema(&output.schema).map_err(gestalt::Error::bad_request)?;
    }
    validate_catalog_tool_refs(&req.tool_refs)?;
    match req.tool_source {
        gestalt::AgentToolSourceMode::Unspecified | gestalt::AgentToolSourceMode::None => {
            if !req.tool_refs.is_empty() {
                return Err(gestalt::Error::bad_request(
                    "tool_source=CATALOG is required when tool_refs are provided",
                ));
            }
        }
        gestalt::AgentToolSourceMode::Catalog => {
            if req.context.is_none() {
                return Err(gestalt::Error::bad_request(
                    "request context is required when tool_source=CATALOG",
                ));
            }
        }
    }
    if has_object_fields(req.model_options.as_ref()) {
        return Err(gestalt::Error::bad_request(
            "model_options are not supported by agent/hermes",
        ));
    }
    Ok(())
}

fn validate_session_tool_config(
    req: &gestalt::CreateAgentProviderSessionRequest,
) -> gestalt::Result<()> {
    match req.tools.as_ref().and_then(|tools| tools.source.as_ref()) {
        Some(gestalt::AgentToolConfigSource::None(_)) => Err(gestalt::Error::bad_request(
            "agent/hermes requires tools.catalog",
        )),
        Some(gestalt::AgentToolConfigSource::Catalog(catalog)) => {
            validate_catalog_tool_refs(&catalog.refs)
        }
        None => Ok(()),
    }
}

fn effective_turn_tool_scope(
    req: &gestalt::CreateAgentProviderTurnRequest,
    session_tool_source: gestalt::AgentToolSourceMode,
    session_tool_refs: &[gestalt::AgentToolRef],
) -> gestalt::Result<(gestalt::AgentToolSourceMode, Vec<gestalt::AgentToolRef>)> {
    if req.tool_source != gestalt::AgentToolSourceMode::Unspecified || !req.tool_refs.is_empty() {
        return Err(gestalt::Error::bad_request(
            "agent turn tools must be configured on the session",
        ));
    }
    if session_tool_source != gestalt::AgentToolSourceMode::Unspecified {
        return Ok((session_tool_source, session_tool_refs.to_vec()));
    }
    Ok((gestalt::AgentToolSourceMode::None, Vec::new()))
}

fn validate_catalog_tool_refs(refs: &[gestalt::AgentToolRef]) -> gestalt::Result<()> {
    for (index, tool_ref) in refs.iter().enumerate() {
        let system = tool_ref.system.trim();
        let app = tool_ref.app.trim();
        let operation = tool_ref.operation.trim();
        let connection = tool_ref.connection.trim();
        let instance = tool_ref.instance.trim();
        let title = tool_ref.title.trim();
        let description = tool_ref.description.trim();
        if system.is_empty() && app.is_empty() {
            return Err(gestalt::Error::bad_request(format!(
                "tool_refs[{index}].plugin or system is required"
            )));
        }
        if !system.is_empty() && !app.is_empty() {
            return Err(gestalt::Error::bad_request(format!(
                "tool_refs[{index}] must set exactly one of plugin or system"
            )));
        }
        if !system.is_empty() {
            if system != "workflow" {
                return Err(gestalt::Error::bad_request(format!(
                    "tool_refs[{index}].system {system:?} is not supported"
                )));
            }
            if operation.is_empty() {
                return Err(gestalt::Error::bad_request(format!(
                    "tool_refs[{index}].operation is required for system refs"
                )));
            }
            if operation == "*" {
                return Err(gestalt::Error::bad_request(format!(
                    "tool_refs[{index}].operation wildcard is not supported"
                )));
            }
            if !connection.is_empty()
                || !instance.is_empty()
                || !title.is_empty()
                || !description.is_empty()
            {
                return Err(gestalt::Error::bad_request(format!(
                    "tool_refs[{index}] system refs cannot include connection, instance, title, or description"
                )));
            }
            continue;
        }
        if operation == "*" || connection == "*" || instance == "*" {
            return Err(gestalt::Error::bad_request(format!(
                "tool_refs[{index}] wildcard fields are not supported"
            )));
        }
        if app == "*"
            && (!operation.is_empty()
                || !connection.is_empty()
                || !instance.is_empty()
                || !title.is_empty()
                || !description.is_empty())
        {
            return Err(gestalt::Error::bad_request(format!(
                "tool_refs[{index}] global ref cannot include operation, connection, instance, title, or description"
            )));
        }
    }
    Ok(())
}

fn turn_output_for(turn: &store::StoredTurn) -> Result<gestalt::AgentTurnOutput, String> {
    match &turn.output_request {
        gestalt::AgentOutput::Text(_) => Ok(gestalt::AgentTurnOutput::Text(
            gestalt::AgentTurnTextOutput {
                text: turn.output_buffer.clone(),
            },
        )),
        gestalt::AgentOutput::Structured(output) => {
            let value = structured_output_from_text(&turn.output_buffer, &output.schema)?;
            Ok(gestalt::AgentTurnOutput::Structured(
                gestalt::AgentTurnStructuredOutput {
                    text: turn.output_buffer.clone(),
                    value: Some(value),
                },
            ))
        }
    }
}

fn turn_output_event_data(output: &gestalt::AgentTurnOutput) -> JsonValue {
    match output {
        gestalt::AgentTurnOutput::Text(output) => json!({ "text": output.text }),
        gestalt::AgentTurnOutput::Structured(output) => {
            json!({ "text": output.text, "value": output.value })
        }
    }
}

fn validate_schema(schema: &JsonValue) -> Result<(), String> {
    let object = schema
        .as_object()
        .ok_or_else(|| "output.structured.schema must be a JSON schema object".to_string())?;
    if object.is_empty() || schema.get("type").and_then(JsonValue::as_str) != Some("object") {
        return Err(
            "output.structured.schema must be a non-empty JSON schema object with type 'object'"
                .to_string(),
        );
    }
    jsonschema::validator_for(schema)
        .map_err(|err| format!("invalid output.structured.schema: {err}"))?;
    Ok(())
}

fn structured_output_from_text(text: &str, schema: &JsonValue) -> Result<JsonValue, String> {
    validate_schema(schema)?;
    let value = parse_json_object(text)?;
    let validator = jsonschema::validator_for(schema)
        .map_err(|err| format!("invalid output.structured.schema: {err}"))?;
    validator
        .validate(&value)
        .map_err(|err| format!("structured output did not match output schema: {err}"))?;
    Ok(value)
}

fn parse_json_object(text: &str) -> Result<JsonValue, String> {
    if let Ok(value) = serde_json::from_str::<JsonValue>(text) {
        if value.is_object() {
            return Ok(value);
        }
    }
    for (start, ch) in text.char_indices() {
        if ch != '{' {
            continue;
        }
        let mut deserializer = serde_json::Deserializer::from_str(&text[start..]);
        if let Ok(value) = JsonValue::deserialize(&mut deserializer) {
            if value.is_object() {
                return Ok(value);
            }
        }
    }
    Err("structured output did not contain a JSON object".to_string())
}

fn messages_to_prompt(
    messages: &[gestalt::AgentMessage],
    output_request: &gestalt::AgentOutput,
) -> Result<String, String> {
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
    if let gestalt::AgentOutput::Structured(output) = output_request {
        let schema = serde_json::to_string(&output.schema).map_err(|err| err.to_string())?;
        writeln!(
            &mut prompt,
            "\n<gestalt_structured_output>\nReturn only one JSON object matching this JSON Schema. Do not wrap it in Markdown or include explanatory text.\n{schema}\n</gestalt_structured_output>"
        )
        .map_err(|err| err.to_string())?;
    }
    Ok(prompt)
}

fn message_part_to_json(part: &gestalt::AgentMessagePart) -> JsonValue {
    match part.r#type {
        gestalt::AgentMessagePartType::Text => json!({ "type": "text", "text": part.text }),
        gestalt::AgentMessagePartType::Json => {
            json!({ "type": "json", "json": part.json.as_ref().cloned().unwrap_or(JsonValue::Null) })
        }
        gestalt::AgentMessagePartType::ImageRef => json!({
            "type": "image_ref",
            "uri": part.image_ref.as_ref().map(|image| image.uri.as_str()).unwrap_or_default(),
            "mime_type": part.image_ref.as_ref().map(|image| image.mime_type.as_str()).unwrap_or_default()
        }),
        gestalt::AgentMessagePartType::ToolCall => match &part.tool_call {
            Some(tool_call) => json!({
                "type": "tool_call",
                "id": tool_call.id,
                "tool_id": tool_call.tool_id,
                "arguments": tool_call.arguments.as_ref().cloned().unwrap_or(JsonValue::Null)
            }),
            None => json!({ "type": "tool_call" }),
        },
        gestalt::AgentMessagePartType::ToolResult => match &part.tool_result {
            Some(tool_result) => json!({
                "type": "tool_result",
                "tool_call_id": tool_result.tool_call_id,
                "status": tool_result.status,
                "content": tool_result.content,
                "output": tool_result.output.as_ref().cloned().unwrap_or(JsonValue::Null)
            }),
            None => json!({ "type": "tool_result" }),
        },
        gestalt::AgentMessagePartType::Unspecified => json!({ "type": "unspecified" }),
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

fn subject_id(subject: Option<&gestalt::Subject>) -> String {
    subject
        .map(|subject| subject.id.trim().to_string())
        .unwrap_or_default()
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
