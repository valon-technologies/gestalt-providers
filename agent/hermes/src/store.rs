use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use gestalt::proto::v1 as proto;
use prost_types::{Struct, Timestamp};

#[derive(Clone)]
pub struct StoredSession {
    pub id: String,
    pub provider_name: String,
    pub acp_session_id: String,
    pub model: String,
    pub client_ref: String,
    pub state: i32,
    pub metadata: Option<Struct>,
    pub created_by: Option<proto::AgentActor>,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    pub last_turn_at: Option<Timestamp>,
    pub active_turn_id: Option<String>,
}

#[derive(Clone)]
pub struct StoredTurn {
    pub id: String,
    pub session_id: String,
    pub provider_name: String,
    pub model: String,
    pub status: i32,
    pub messages: Vec<proto::AgentMessage>,
    pub output_text: String,
    pub status_message: String,
    pub created_by: Option<proto::AgentActor>,
    pub created_at: Option<Timestamp>,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub execution_ref: String,
    pub tool_refs: Vec<proto::AgentToolRef>,
    pub tool_source: i32,
    pub run_grant: String,
}

#[derive(Clone)]
pub struct StoredEvent {
    pub id: String,
    pub turn_id: String,
    pub seq: i64,
    pub event_type: String,
    pub source: String,
    pub visibility: String,
    pub data: Option<Struct>,
    pub display: Option<proto::AgentTurnDisplay>,
    pub created_at: Option<Timestamp>,
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
        req: &proto::CreateAgentProviderSessionRequest,
        provider_name: &str,
        model: String,
        acp_session_id: String,
    ) -> Result<CreateSessionResult, String> {
        let session_id = req.session_id.trim();
        if session_id.is_empty() {
            return Err("session_id is required".to_string());
        }
        if let Some(existing) = self.sessions.get(session_id) {
            return Ok(CreateSessionResult::Existing(existing.clone()));
        }
        let idempotency_key = req.idempotency_key.trim();
        if !idempotency_key.is_empty() {
            if let Some(existing_id) = self.session_idempotency.get(idempotency_key) {
                if let Some(existing) = self.sessions.get(existing_id) {
                    return Ok(CreateSessionResult::Existing(existing.clone()));
                }
            }
        }

        let now = now_ts();
        let session = StoredSession {
            id: session_id.to_string(),
            provider_name: provider_name.to_string(),
            acp_session_id,
            model,
            client_ref: req.client_ref.trim().to_string(),
            state: proto::AgentSessionState::Active as i32,
            metadata: req.metadata.clone(),
            created_by: req.created_by.clone(),
            created_at: Some(now.clone()),
            updated_at: Some(now),
            last_turn_at: None,
            active_turn_id: None,
        };
        self.sessions.insert(session.id.clone(), session.clone());
        if !idempotency_key.is_empty() {
            self.session_idempotency
                .insert(idempotency_key.to_string(), session.id.clone());
        }
        Ok(CreateSessionResult::Created(session))
    }

    pub fn existing_session_for_create(
        &self,
        req: &proto::CreateAgentProviderSessionRequest,
    ) -> Option<StoredSession> {
        let session_id = req.session_id.trim();
        if let Some(existing) = self.sessions.get(session_id) {
            return Some(existing.clone());
        }
        let idempotency_key = req.idempotency_key.trim();
        if !idempotency_key.is_empty() {
            if let Some(existing_id) = self.session_idempotency.get(idempotency_key) {
                return self.sessions.get(existing_id).cloned();
            }
        }
        None
    }

    pub fn get_session(&self, id: &str) -> Option<StoredSession> {
        self.sessions.get(id.trim()).cloned()
    }

    pub fn list_sessions(
        &self,
        ids: &[String],
        subject_id: &str,
        state: i32,
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
        if !subject_id.is_empty() {
            sessions.retain(|session| {
                session
                    .created_by
                    .as_ref()
                    .is_some_and(|actor| actor.subject_id.trim() == subject_id)
            });
        }
        if state != 0 {
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
        state: i32,
        metadata: Option<Struct>,
    ) -> Option<StoredSession> {
        let session = self.sessions.get_mut(id.trim())?;
        if !client_ref.trim().is_empty() {
            session.client_ref = client_ref.trim().to_string();
        }
        if state != 0 {
            session.state = state;
        }
        if metadata.is_some() {
            session.metadata = metadata;
        }
        session.updated_at = Some(now_ts());
        Some(session.clone())
    }

    pub fn begin_turn(
        &mut self,
        req: &proto::CreateAgentProviderTurnRequest,
        provider_name: &str,
        model: String,
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

        let now = now_ts();
        let turn = StoredTurn {
            id: turn_id.to_string(),
            session_id: session_id.to_string(),
            provider_name: provider_name.to_string(),
            model,
            status: proto::AgentExecutionStatus::Running as i32,
            messages: req.messages.clone(),
            output_text: String::new(),
            status_message: String::new(),
            created_by: req.created_by.clone(),
            created_at: Some(now.clone()),
            started_at: Some(now.clone()),
            completed_at: None,
            execution_ref: req.execution_ref.trim().to_string(),
            tool_refs: req.tool_refs.clone(),
            tool_source: req.tool_source,
            run_grant: req.run_grant.trim().to_string(),
        };
        self.turns.insert(turn.id.clone(), turn.clone());
        if !idempotency_key.is_empty() {
            self.turn_idempotency.insert(
                (session_id.to_string(), idempotency_key.to_string()),
                turn.id.clone(),
            );
        }
        session.active_turn_id = Some(turn.id.clone());
        session.last_turn_at = Some(now.clone());
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

    pub fn list_turns(
        &self,
        session_id: &str,
        ids: &[String],
        subject_id: &str,
        status: i32,
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
        if !subject_id.is_empty() {
            turns.retain(|turn| {
                turn.created_by
                    .as_ref()
                    .is_some_and(|actor| actor.subject_id.trim() == subject_id)
            });
        }
        if status != 0 {
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
            turn.output_text.push_str(text);
        }
    }

    pub fn finish_turn(
        &mut self,
        turn_id: &str,
        status: i32,
        status_message: String,
    ) -> Option<StoredTurn> {
        let now = now_ts();
        let turn = self.turns.get_mut(turn_id.trim())?;
        if is_terminal(turn.status) {
            return Some(turn.clone());
        }
        turn.status = status;
        turn.status_message = status_message;
        turn.completed_at = Some(now.clone());
        if let Some(session) = self.sessions.get_mut(&turn.session_id) {
            if session.active_turn_id.as_deref() == Some(turn_id.trim()) {
                session.active_turn_id = None;
            }
            session.updated_at = Some(now);
        }
        Some(turn.clone())
    }

    pub fn cancel_turn(&mut self, turn_id: &str, reason: &str) -> Option<StoredTurn> {
        self.finish_turn(
            turn_id,
            proto::AgentExecutionStatus::Canceled as i32,
            reason.trim().to_string(),
        )
    }

    pub fn append_event(
        &mut self,
        turn_id: &str,
        event_type: &str,
        source: &str,
        data: serde_json::Value,
        display: Option<proto::AgentTurnDisplay>,
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
            data: json_to_struct(data),
            display,
            created_at: Some(now_ts()),
        };
        events.push(event.clone());
        event
    }

    pub fn list_events(&self, turn_id: &str, after_seq: i64, limit: i32) -> Vec<StoredEvent> {
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

pub fn session_to_proto(session: StoredSession, summary_only: bool) -> proto::AgentSession {
    proto::AgentSession {
        id: session.id,
        provider_name: session.provider_name,
        model: session.model,
        client_ref: session.client_ref,
        state: session.state,
        metadata: if summary_only { None } else { session.metadata },
        created_by: session.created_by,
        created_at: session.created_at,
        updated_at: session.updated_at,
        last_turn_at: session.last_turn_at,
    }
}

pub fn turn_to_proto(turn: StoredTurn, summary_only: bool) -> proto::AgentTurn {
    proto::AgentTurn {
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
        output_text: turn.output_text,
        structured_output: None,
        status_message: turn.status_message,
        created_by: turn.created_by,
        created_at: turn.created_at,
        started_at: turn.started_at,
        completed_at: turn.completed_at,
        execution_ref: turn.execution_ref,
    }
}

pub fn event_to_proto(event: StoredEvent) -> proto::AgentTurnEvent {
    proto::AgentTurnEvent {
        id: event.id,
        turn_id: event.turn_id,
        seq: event.seq,
        r#type: event.event_type,
        source: event.source,
        visibility: event.visibility,
        data: event.data,
        created_at: event.created_at,
        display: event.display,
    }
}

pub fn now_ts() -> Timestamp {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

fn is_terminal(status: i32) -> bool {
    status == proto::AgentExecutionStatus::Succeeded as i32
        || status == proto::AgentExecutionStatus::Failed as i32
        || status == proto::AgentExecutionStatus::Canceled as i32
}

fn timestamp_key(ts: Option<&Timestamp>) -> (i64, i32) {
    ts.map(|ts| (ts.seconds, ts.nanos)).unwrap_or_default()
}

pub fn json_to_struct(value: serde_json::Value) -> Option<Struct> {
    match value {
        serde_json::Value::Object(object) => {
            let fields = object
                .into_iter()
                .map(|(key, value)| (key, json_to_value(value)))
                .collect();
            Some(Struct { fields })
        }
        _ => None,
    }
}

pub fn json_to_value(value: serde_json::Value) -> prost_types::Value {
    use prost_types::value::Kind;
    prost_types::Value {
        kind: Some(match value {
            serde_json::Value::Null => Kind::NullValue(0),
            serde_json::Value::Bool(value) => Kind::BoolValue(value),
            serde_json::Value::Number(value) => {
                Kind::NumberValue(value.as_f64().unwrap_or_default())
            }
            serde_json::Value::String(value) => Kind::StringValue(value),
            serde_json::Value::Array(values) => Kind::ListValue(prost_types::ListValue {
                values: values.into_iter().map(json_to_value).collect(),
            }),
            serde_json::Value::Object(object) => Kind::StructValue(Struct {
                fields: object
                    .into_iter()
                    .map(|(key, value)| (key, json_to_value(value)))
                    .collect(),
            }),
        }),
    }
}
