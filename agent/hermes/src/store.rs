use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use gestalt::{
    AgentExecutionStatus, AgentMessage, AgentOutput, AgentSession, AgentSessionState,
    AgentToolConfigSource, AgentTurn, AgentTurnDisplay, AgentTurnEvent, AgentTurnOutput,
    CreateAgentProviderSessionRequest, CreateAgentProviderTurnRequest, ListedAgentTool,
    proto::v1::RequestContext as GestaltRequestContext,
};

#[derive(Clone)]
pub struct StoredSession {
    pub id: String,
    pub provider_name: String,
    pub acp_session_id: String,
    pub model: String,
    pub client_ref: String,
    pub state: AgentSessionState,
    pub visibility: SessionVisibility,
    pub metadata: Option<serde_json::Value>,
    pub listed_tools: Vec<ListedAgentTool>,
    pub created_by_subject_id: String,
    pub created_at: Option<SystemTime>,
    pub updated_at: Option<SystemTime>,
    pub last_turn_at: Option<SystemTime>,
    pub active_turn_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionVisibility {
    Private,
    Company,
}

#[derive(Clone)]
pub struct StoredTurn {
    pub id: String,
    pub session_id: String,
    pub provider_name: String,
    pub model: String,
    pub status: AgentExecutionStatus,
    pub messages: Vec<AgentMessage>,
    pub output_request: AgentOutput,
    pub output_buffer: String,
    pub output: Option<AgentTurnOutput>,
    pub status_message: String,
    pub created_by_subject_id: String,
    pub created_at: Option<SystemTime>,
    pub started_at: Option<SystemTime>,
    pub completed_at: Option<SystemTime>,
    pub execution_ref: String,
    pub listed_tools: Vec<ListedAgentTool>,
    pub request_context: Option<GestaltRequestContext>,
}

#[derive(Clone)]
pub struct StoredEvent {
    pub id: String,
    pub turn_id: String,
    pub seq: i64,
    pub event_type: String,
    pub source: String,
    pub visibility: String,
    pub data: serde_json::Value,
    pub display: Option<AgentTurnDisplay>,
    pub created_at: Option<SystemTime>,
}

#[derive(Default)]
pub struct Store {
    sessions: HashMap<String, StoredSession>,
    session_idempotency: HashMap<String, String>,
    turns: HashMap<String, StoredTurn>,
    turn_idempotency: HashMap<(String, String), String>,
    events: HashMap<String, Vec<StoredEvent>>,
}

pub enum CreateSessionResult {
    Created(StoredSession),
    Existing(StoredSession),
}

pub enum BeginTurnResult {
    Created(StoredTurn),
    Existing(StoredTurn),
}

impl Store {
    pub fn clear(&mut self) {
        self.sessions.clear();
        self.session_idempotency.clear();
        self.turns.clear();
        self.turn_idempotency.clear();
        self.events.clear();
    }

    pub fn create_session(
        &mut self,
        req: &CreateAgentProviderSessionRequest,
        provider_name: &str,
        model: String,
        acp_session_id: String,
    ) -> CreateSessionResult {
        if let Some(existing) = self.existing_session_for_create(req) {
            return CreateSessionResult::Existing(existing);
        }

        let now = SystemTime::now();
        let listed_tools = session_tools(req);
        let session = StoredSession {
            id: uuid::Uuid::new_v4().to_string(),
            provider_name: provider_name.to_string(),
            acp_session_id,
            model,
            client_ref: req.client_ref.trim().to_string(),
            state: AgentSessionState::Active,
            visibility: session_visibility(
                req.metadata.as_ref(),
                req.created_by_subject_id.as_deref().unwrap_or_default(),
            ),
            metadata: req.metadata.clone(),
            listed_tools,
            created_by_subject_id: req.created_by_subject_id.clone().unwrap_or_default(),
            created_at: Some(now),
            updated_at: Some(now),
            last_turn_at: None,
            active_turn_id: None,
        };
        self.sessions.insert(session.id.clone(), session.clone());
        if let Some(key) = session_dedup_key(req) {
            self.session_idempotency.insert(key, session.id.clone());
        }
        CreateSessionResult::Created(session)
    }

    pub fn existing_session_for_create(
        &self,
        req: &CreateAgentProviderSessionRequest,
    ) -> Option<StoredSession> {
        let key = session_dedup_key(req)?;
        let existing_id = self.session_idempotency.get(&key)?;
        self.sessions.get(existing_id).cloned()
    }

    pub fn get_session(&self, id: &str) -> Option<StoredSession> {
        self.sessions.get(id.trim()).cloned()
    }

    pub fn get_session_if_readable(&self, id: &str, subject_id: &str) -> Option<StoredSession> {
        self.sessions
            .get(id.trim())
            .filter(|session| session_readable_by(session, subject_id))
            .cloned()
    }

    pub fn get_session_if_owner(&self, id: &str, subject_id: &str) -> Option<StoredSession> {
        self.sessions
            .get(id.trim())
            .filter(|session| session_owned_by(session, subject_id))
            .cloned()
    }

    pub fn list_sessions(
        &self,
        ids: &[String],
        subject_id: &str,
        state: AgentSessionState,
        limit: i32,
    ) -> Vec<StoredSession> {
        let requested: Vec<&str> = ids
            .iter()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .collect();
        let mut sessions: Vec<StoredSession> = if requested.is_empty() {
            self.sessions.values().cloned().collect()
        } else {
            requested
                .iter()
                .filter_map(|id| self.sessions.get(*id).cloned())
                .collect()
        };
        sessions.retain(|session| session_readable_by(session, subject_id));
        if state != AgentSessionState::Unspecified {
            sessions.retain(|session| session.state == state);
        }
        sessions.sort_by(|left, right| {
            let left_ts = left.last_turn_at.as_ref().or(left.updated_at.as_ref());
            let right_ts = right.last_turn_at.as_ref().or(right.updated_at.as_ref());
            timestamp_key(right_ts).cmp(&timestamp_key(left_ts))
        });
        if limit > 0 {
            sessions.truncate(limit as usize);
        }
        sessions
    }

    pub fn update_session(
        &mut self,
        id: &str,
        client_ref: &str,
        state: AgentSessionState,
        metadata: Option<serde_json::Value>,
        subject_id: &str,
    ) -> Option<StoredSession> {
        let session = self.sessions.get_mut(id.trim())?;
        if !session_owned_by(session, subject_id) {
            return None;
        }
        if !client_ref.trim().is_empty() {
            session.client_ref = client_ref.trim().to_string();
        }
        if state != AgentSessionState::Unspecified {
            session.state = state;
        }
        if metadata.is_some() {
            session.metadata = metadata;
        }
        session.updated_at = Some(SystemTime::now());
        Some(session.clone())
    }

    pub fn begin_turn(
        &mut self,
        req: &CreateAgentProviderTurnRequest,
        provider_name: &str,
        model: String,
        subject_id: &str,
        listed_tools: Vec<ListedAgentTool>,
        request_context: Option<GestaltRequestContext>,
    ) -> Result<BeginTurnResult, String> {
        let turn_id = req.turn_id.trim();
        let session_id = req.session_id.trim();
        if turn_id.is_empty() {
            return Err("turn_id is required".to_string());
        }
        if session_id.is_empty() {
            return Err("session_id is required".to_string());
        }
        let session = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("agent session {session_id:?} was not found"))?;
        if !session_owned_by(session, subject_id) {
            return Err(format!("agent session {session_id:?} is not writable"));
        }

        if let Some(existing) = self.turns.get(turn_id) {
            if existing.session_id != session_id {
                return Err(format!(
                    "turn_id {turn_id:?} already exists for another session"
                ));
            }
            return Ok(BeginTurnResult::Existing(existing.clone()));
        }
        let idempotency_key = req.idempotency_key.trim();
        if !idempotency_key.is_empty() {
            let key = (session_id.to_string(), idempotency_key.to_string());
            if let Some(existing_id) = self.turn_idempotency.get(&key) {
                if let Some(existing) = self.turns.get(existing_id) {
                    return Ok(BeginTurnResult::Existing(existing.clone()));
                }
            }
        }
        if let Some(active) = &session.active_turn_id {
            return Err(format!(
                "session {session_id:?} already has active turn {active:?}"
            ));
        }

        let now = SystemTime::now();
        let turn = StoredTurn {
            id: turn_id.to_string(),
            session_id: session_id.to_string(),
            provider_name: provider_name.to_string(),
            model,
            status: AgentExecutionStatus::Running,
            messages: req.messages.clone(),
            output_request: req.output.clone(),
            output_buffer: String::new(),
            output: None,
            status_message: String::new(),
            created_by_subject_id: req.created_by_subject_id.clone().unwrap_or_default(),
            created_at: Some(now),
            started_at: Some(now),
            completed_at: None,
            execution_ref: req.execution_ref.trim().to_string(),
            listed_tools,
            request_context,
        };
        self.turns.insert(turn.id.clone(), turn.clone());
        if !idempotency_key.is_empty() {
            self.turn_idempotency.insert(
                (session_id.to_string(), idempotency_key.to_string()),
                turn.id.clone(),
            );
        }
        session.active_turn_id = Some(turn.id.clone());
        session.last_turn_at = Some(now);
        session.updated_at = Some(now);
        self.append_event(
            turn.id.as_str(),
            "turn.started",
            provider_name,
            serde_json::json!({ "model": turn.model }),
            None,
        );
        Ok(BeginTurnResult::Created(turn))
    }

    pub fn get_turn(&self, id: &str) -> Option<StoredTurn> {
        self.turns.get(id.trim()).cloned()
    }

    pub fn get_turn_if_readable(&self, id: &str, subject_id: &str) -> Option<StoredTurn> {
        self.turns
            .get(id.trim())
            .filter(|turn| {
                self.sessions
                    .get(&turn.session_id)
                    .is_some_and(|session| session_readable_by(session, subject_id))
            })
            .cloned()
    }

    pub fn get_turn_if_owner(&self, id: &str, subject_id: &str) -> Option<StoredTurn> {
        self.turns
            .get(id.trim())
            .filter(|turn| {
                self.sessions
                    .get(&turn.session_id)
                    .is_some_and(|session| session_owned_by(session, subject_id))
            })
            .cloned()
    }

    pub fn list_turns(
        &self,
        session_id: &str,
        ids: &[String],
        subject_id: &str,
        status: AgentExecutionStatus,
        limit: i32,
    ) -> Vec<StoredTurn> {
        let requested: Vec<&str> = ids
            .iter()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .collect();
        let mut turns: Vec<StoredTurn> = if !requested.is_empty() {
            requested
                .iter()
                .filter_map(|id| self.turns.get(*id).cloned())
                .collect()
        } else if !session_id.trim().is_empty() {
            self.turns
                .values()
                .filter(|turn| turn.session_id == session_id.trim())
                .cloned()
                .collect()
        } else {
            self.turns.values().cloned().collect()
        };
        turns.retain(|turn| {
            self.sessions
                .get(&turn.session_id)
                .is_some_and(|session| session_readable_by(session, subject_id))
        });
        if status != AgentExecutionStatus::Unspecified {
            turns.retain(|turn| turn.status == status);
        }
        turns.sort_by(|left, right| {
            timestamp_key(right.created_at.as_ref()).cmp(&timestamp_key(left.created_at.as_ref()))
        });
        if limit > 0 {
            turns.truncate(limit as usize);
        }
        turns
    }

    pub fn append_output(&mut self, turn_id: &str, text: &str) {
        if let Some(turn) = self.turns.get_mut(turn_id) {
            turn.output_buffer.push_str(text);
        }
    }

    pub fn set_output(&mut self, turn_id: &str, output: AgentTurnOutput) -> Option<StoredTurn> {
        let turn = self.turns.get_mut(turn_id.trim())?;
        turn.output = Some(output);
        Some(turn.clone())
    }

    pub fn finish_turn(
        &mut self,
        turn_id: &str,
        status: AgentExecutionStatus,
        status_message: String,
    ) -> Option<StoredTurn> {
        let now = SystemTime::now();
        let turn = self.turns.get_mut(turn_id.trim())?;
        if is_terminal(turn.status) {
            return Some(turn.clone());
        }
        turn.status = status;
        turn.status_message = status_message;
        turn.completed_at = Some(now);
        if let Some(session) = self.sessions.get_mut(&turn.session_id) {
            if session.active_turn_id.as_deref() == Some(turn_id.trim()) {
                session.active_turn_id = None;
            }
            session.updated_at = Some(now);
        }
        Some(turn.clone())
    }

    pub fn cancel_turn(
        &mut self,
        turn_id: &str,
        reason: &str,
        subject_id: &str,
    ) -> Option<StoredTurn> {
        let turn = self.turns.get(turn_id.trim())?;
        let session = self.sessions.get(&turn.session_id)?;
        if !session_owned_by(session, subject_id) {
            return None;
        }
        self.finish_turn(
            turn_id,
            AgentExecutionStatus::Canceled,
            reason.trim().to_string(),
        )
    }

    pub fn append_event(
        &mut self,
        turn_id: &str,
        event_type: &str,
        source: &str,
        data: serde_json::Value,
        display: Option<AgentTurnDisplay>,
    ) -> StoredEvent {
        let events = self.events.entry(turn_id.to_string()).or_default();
        let seq = events.len() as i64 + 1;
        let event = StoredEvent {
            id: format!("{turn_id}:{seq}"),
            turn_id: turn_id.to_string(),
            seq,
            event_type: event_type.to_string(),
            source: source.to_string(),
            visibility: "public".to_string(),
            data,
            display,
            created_at: Some(SystemTime::now()),
        };
        events.push(event.clone());
        event
    }

    pub fn list_events(
        &self,
        turn_id: &str,
        after_seq: i64,
        limit: i32,
        subject_id: &str,
    ) -> Vec<StoredEvent> {
        if !self
            .turns
            .get(turn_id.trim())
            .and_then(|turn| self.sessions.get(&turn.session_id))
            .is_some_and(|session| session_readable_by(session, subject_id))
        {
            return Vec::new();
        }
        let mut events: Vec<StoredEvent> = self
            .events
            .get(turn_id.trim())
            .into_iter()
            .flat_map(|events| events.iter())
            .filter(|event| event.seq > after_seq)
            .cloned()
            .collect();
        if limit > 0 {
            events.truncate(limit as usize);
        }
        events
    }
}

pub fn agent_session(session: StoredSession, summary_only: bool) -> AgentSession {
    AgentSession {
        id: session.id,
        provider_name: session.provider_name,
        model: session.model,
        client_ref: session.client_ref,
        state: session.state,
        metadata: if summary_only { None } else { session.metadata },
        created_by_subject_id: Some(session.created_by_subject_id),
        created_at: session.created_at,
        updated_at: session.updated_at,
        last_turn_at: session.last_turn_at,
    }
}

pub fn agent_turn(turn: StoredTurn, summary_only: bool) -> AgentTurn {
    AgentTurn {
        id: turn.id,
        session_id: turn.session_id,
        provider_name: turn.provider_name,
        model: turn.model,
        status: turn.status,
        messages: if summary_only {
            Vec::new()
        } else {
            turn.messages
        },
        output: if summary_only { None } else { turn.output },
        status_message: turn.status_message,
        created_by_subject_id: Some(turn.created_by_subject_id),
        created_at: turn.created_at,
        started_at: turn.started_at,
        completed_at: turn.completed_at,
        execution_ref: turn.execution_ref,
    }
}

pub fn agent_turn_event(event: StoredEvent) -> AgentTurnEvent {
    AgentTurnEvent {
        id: event.id,
        turn_id: event.turn_id,
        seq: event.seq,
        r#type: event.event_type,
        source: event.source,
        visibility: event.visibility,
        data: Some(event.data),
        created_at: event.created_at,
        display: event.display,
    }
}

fn is_terminal(status: AgentExecutionStatus) -> bool {
    status == AgentExecutionStatus::Succeeded
        || status == AgentExecutionStatus::Failed
        || status == AgentExecutionStatus::Canceled
}

fn session_visibility(
    metadata: Option<&serde_json::Value>,
    created_by_subject_id: &str,
) -> SessionVisibility {
    if is_slack_agent_session_metadata(metadata) && is_managed_subject_id(created_by_subject_id) {
        SessionVisibility::Company
    } else {
        SessionVisibility::Private
    }
}

/// Session idempotency keys are scoped per subject: replays only match when
/// both the idempotency key and created_by_subject_id match. Empty keys never
/// dedup.
fn session_dedup_key(req: &CreateAgentProviderSessionRequest) -> Option<String> {
    let key = req.idempotency_key.trim();
    if key.is_empty() {
        return None;
    }
    let subject = req.created_by_subject_id.as_deref().unwrap_or_default();
    Some(format!("{}\u{1f}{}", subject.trim(), key))
}

fn session_tools(req: &CreateAgentProviderSessionRequest) -> Vec<ListedAgentTool> {
    let Some(AgentToolConfigSource::Catalog(catalog)) =
        req.tools.as_ref().and_then(|tools| tools.source.as_ref())
    else {
        unreachable!("Hermes sessions are validated before storage and require tools.catalog");
    };
    catalog.tools.clone()
}

fn is_slack_agent_session_metadata(metadata: Option<&serde_json::Value>) -> bool {
    let Some(slack) = metadata.and_then(|metadata| metadata.get("slack")) else {
        return false;
    };
    non_empty_json_string(slack.get("team_id"))
        && non_empty_json_string(slack.get("channel_id"))
        && non_empty_json_string(slack.get("root_message_ts"))
        && non_empty_json_string(slack.get("session_ref"))
}

fn non_empty_json_string(value: Option<&serde_json::Value>) -> bool {
    value
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
}

fn is_managed_subject_id(subject_id: &str) -> bool {
    subject_id.trim().starts_with("service_account:")
}

pub(crate) fn session_readable_by(session: &StoredSession, subject_id: &str) -> bool {
    let subject_id = subject_id.trim();
    if subject_id.is_empty() {
        return false;
    }
    session_owned_by(session, subject_id) || session.visibility == SessionVisibility::Company
}

fn session_owned_by(session: &StoredSession, subject_id: &str) -> bool {
    let subject_id = subject_id.trim();
    !subject_id.is_empty() && session.created_by_subject_id.trim() == subject_id
}

fn timestamp_key(ts: Option<&SystemTime>) -> SystemTime {
    ts.copied().unwrap_or(UNIX_EPOCH)
}
