use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use gestalt::AgentProvider as GestaltAgentProvider;
use gestalt::proto::v1 as proto;
use gestalt::proto::v1::agent_provider_server::AgentProvider as AgentProviderService;
use prost_types::{Struct, Value as ProstValue, value::Kind};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tempfile::TempDir;
use tonic::{Code, Request};

use gestalt_agent_hermes::HermesAgentProvider;

#[tokio::test]
async fn completes_turn_and_refreshes_adc_token_per_turn() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;

    let capabilities = provider
        .get_capabilities(Request::new(proto::GetAgentProviderCapabilitiesRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(capabilities.streaming_text);
    assert!(!capabilities.tool_calls);
    assert!(!capabilities.native_tool_search);
    assert!(capabilities.supported_tool_sources.is_empty());

    create_session(&provider).await;
    create_session(&provider).await;
    create_turn(&provider, "turn-1").await;
    let turn = wait_for_turn(&provider, "turn-1", proto::AgentExecutionStatus::Succeeded).await;
    assert_eq!(turn.output_text, "Hermes says hi");
    create_turn(&provider, "turn-1").await;

    create_turn(&provider, "turn-2").await;
    wait_for_turn(&provider, "turn-2", proto::AgentExecutionStatus::Succeeded).await;

    let events = provider
        .list_turn_events(Request::new(proto::ListAgentProviderTurnEventsRequest {
            turn_id: "turn-1".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .events;
    assert!(
        events
            .iter()
            .any(|event| event.r#type == "agent.reasoning.delta")
    );
    assert!(
        events
            .iter()
            .any(|event| event.r#type == "agent.message.delta")
    );
    assert!(
        events
            .iter()
            .any(|event| event.r#type == "tool.call.update")
    );

    let log = fixture.log_events();
    let starts: Vec<&JsonValue> = log
        .iter()
        .filter(|event| event["event"] == "start")
        .collect();
    assert_eq!(starts.len(), 3, "{log:?}");
    assert_eq!(starts[0]["token"], "token-1");
    assert_eq!(starts[1]["token"], "token-2");
    assert_eq!(starts[2]["token"], "token-3");
    assert_eq!(
        starts[0]["hermesHome"].as_str(),
        Some(fixture.hermes_home.path().to_string_lossy().as_ref())
    );
    assert_eq!(
        log.iter().filter(|event| event["event"] == "load").count(),
        2,
        "{log:?}"
    );
}

#[tokio::test]
async fn auto_approves_acp_permission_requests() {
    let fixture = Fixture::new("permission");
    let provider = fixture.configure_provider().await;

    create_session(&provider).await;
    create_turn(&provider, "turn-permission").await;
    wait_for_turn(
        &provider,
        "turn-permission",
        proto::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let permission_response = log
        .iter()
        .find(|event| event["event"] == "permission_response")
        .expect("permission response was logged");
    assert_eq!(
        permission_response["message"]["result"]["outcome"]["outcome"],
        "selected"
    );
    assert_eq!(
        permission_response["message"]["result"]["outcome"]["optionId"],
        "allow"
    );
}

#[tokio::test]
async fn fixed_profile_mode_skips_acp_model_switching() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider_with_model_switching(false).await;

    let session = create_session(&provider).await;
    assert_eq!(session.model, "kimi-k2.6");
    create_turn(&provider, "turn-fixed-profile").await;
    let turn = wait_for_turn(
        &provider,
        "turn-fixed-profile",
        proto::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn.output_text, "Hermes says hi");

    let log = fixture.log_events();
    assert_eq!(
        log.iter()
            .filter(|event| event["event"] == "set_model")
            .count(),
        0,
        "{log:?}"
    );
}

#[tokio::test]
async fn terminal_hermes_stderr_marks_turn_failed() {
    let fixture = Fixture::new("stderr-fail");
    let provider = fixture.configure_provider().await;

    create_session(&provider).await;
    create_turn(&provider, "turn-stderr-fail").await;
    let turn = wait_for_turn(
        &provider,
        "turn-stderr-fail",
        proto::AgentExecutionStatus::Failed,
    )
    .await;
    assert!(
        turn.status_message
            .contains("Hermes reported a terminal error"),
        "{}",
        turn.status_message
    );
    assert!(
        turn.status_message.contains("Non-retryable client error"),
        "{}",
        turn.status_message
    );
    assert_turn_event(&provider, "turn-stderr-fail", "turn.failed").await;
}

#[tokio::test]
async fn rejects_gestalt_tooling_and_structured_options() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;
    create_session(&provider).await;

    let err = provider
        .create_turn(Request::new(proto::CreateAgentProviderTurnRequest {
            turn_id: "turn-tools".to_string(),
            session_id: "session-1".to_string(),
            messages: vec![proto::AgentMessage {
                role: "user".to_string(),
                text: "hi".to_string(),
                ..Default::default()
            }],
            tool_source: proto::AgentToolSourceMode::McpCatalog as i32,
            tool_grant: "grant-1".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let err = provider
        .create_turn(Request::new(proto::CreateAgentProviderTurnRequest {
            turn_id: "turn-schema".to_string(),
            session_id: "session-1".to_string(),
            messages: vec![proto::AgentMessage {
                role: "user".to_string(),
                text: "hi".to_string(),
                ..Default::default()
            }],
            response_schema: Some(non_empty_struct()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn rejects_empty_session_id_without_spawning_hermes() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;

    let err = provider
        .create_session(Request::new(proto::CreateAgentProviderSessionRequest {
            session_id: "   ".to_string(),
            model: "kimi-k2.6".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        fixture.log_events().is_empty(),
        "Hermes should not start for invalid session ids"
    );
}

#[tokio::test]
async fn cancel_turn_sends_acp_cancel_and_marks_turn_canceled() {
    let fixture = Fixture::new("hang");
    let provider = fixture.configure_provider().await;
    create_session(&provider).await;
    create_turn(&provider, "turn-cancel").await;

    wait_for_log_event(&fixture.log_path, "prompt").await;
    let canceled = provider
        .cancel_turn(Request::new(proto::CancelAgentProviderTurnRequest {
            turn_id: "turn-cancel".to_string(),
            reason: "operator requested".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        canceled.status,
        proto::AgentExecutionStatus::Canceled as i32
    );
    wait_for_log_event(&fixture.log_path, "cancel").await;
    assert_turn_event(&provider, "turn-cancel", "turn.canceled").await;
}

#[tokio::test]
async fn cancel_before_acp_spawn_prevents_prompt() {
    let fixture = Fixture::new_with_delayed_turn_token("success");
    let provider = fixture.configure_provider().await;
    create_session(&provider).await;
    create_turn(&provider, "turn-early-cancel").await;

    let canceled = provider
        .cancel_turn(Request::new(proto::CancelAgentProviderTurnRequest {
            turn_id: "turn-early-cancel".to_string(),
            reason: "operator requested".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        canceled.status,
        proto::AgentExecutionStatus::Canceled as i32
    );
    tokio::time::sleep(Duration::from_millis(1_300)).await;

    let log = fixture.log_events();
    assert_eq!(
        log.iter().filter(|event| event["event"] == "start").count(),
        1,
        "{log:?}"
    );
    assert_eq!(
        log.iter()
            .filter(|event| event["event"] == "prompt")
            .count(),
        0,
        "{log:?}"
    );
    assert_turn_event(&provider, "turn-early-cancel", "turn.canceled").await;
}

#[tokio::test]
async fn required_hermes_home_overrides_extra_env() {
    let fixture = Fixture::new_with_hermes_home_override("success");
    let provider = fixture.configure_provider().await;
    create_session(&provider).await;

    let log = fixture.log_events();
    let start = log
        .iter()
        .find(|event| event["event"] == "start")
        .expect("start was logged");
    assert_eq!(
        start["hermesHome"].as_str(),
        Some(fixture.hermes_home.path().to_string_lossy().as_ref())
    );
}

struct Fixture {
    tmp: TempDir,
    hermes_home: TempDir,
    env_hermes_home_override: Option<TempDir>,
    log_path: PathBuf,
    token_script: PathBuf,
}

impl Fixture {
    fn new(mode: &str) -> Self {
        Self::new_internal(mode, false, false)
    }

    fn new_with_delayed_turn_token(mode: &str) -> Self {
        Self::new_internal(mode, true, false)
    }

    fn new_with_hermes_home_override(mode: &str) -> Self {
        Self::new_internal(mode, false, true)
    }

    fn new_internal(mode: &str, delay_turn_token: bool, override_hermes_home: bool) -> Self {
        let tmp = tempfile::tempdir().expect("tmp");
        let hermes_home = tempfile::tempdir().expect("hermes home");
        let env_hermes_home_override =
            override_hermes_home.then(|| tempfile::tempdir().expect("override hermes home"));
        let log_path = tmp.path().join("fake-acp.log");
        let token_counter = tmp.path().join("token-counter");
        let token_script = tmp.path().join("token.sh");
        write_token_script(&token_script, &token_counter, delay_turn_token)
            .expect("write token script");
        let mode_path = tmp.path().join("mode");
        fs::write(&mode_path, mode).expect("write mode");
        Self {
            tmp,
            hermes_home,
            env_hermes_home_override,
            log_path,
            token_script,
        }
    }

    async fn configure_provider(&self) -> HermesAgentProvider {
        self.configure_provider_with_model_switching(true).await
    }

    async fn configure_provider_with_model_switching(
        &self,
        model_switching_enabled: bool,
    ) -> HermesAgentProvider {
        let provider = HermesAgentProvider::default();
        let mut extra_env = serde_json::Map::new();
        extra_env.insert(
            "FAKE_ACP_LOG".to_string(),
            JsonValue::String(self.log_path.to_string_lossy().to_string()),
        );
        extra_env.insert(
            "FAKE_ACP_MODE".to_string(),
            JsonValue::String(fs::read_to_string(self.tmp.path().join("mode")).expect("mode")),
        );
        if let Some(override_home) = &self.env_hermes_home_override {
            extra_env.insert(
                "HERMES_HOME".to_string(),
                JsonValue::String(override_home.path().to_string_lossy().to_string()),
            );
        }
        let config = json_map(json!({
            "hermesHome": self.hermes_home.path().to_string_lossy(),
            "hermesCommand": env!("CARGO_BIN_EXE_fake-acp"),
            "hermesArgs": [],
            "workingDirectory": self.tmp.path().to_string_lossy(),
            "defaultModel": "kimi-k2.6",
            "accessTokenCommand": [
                self.token_script.to_string_lossy()
            ],
            "modelSwitchingEnabled": model_switching_enabled,
            "extraEnv": JsonValue::Object(extra_env),
            "autoApprovePermissions": true
        }));
        GestaltAgentProvider::configure(&provider, "hermes", config)
            .await
            .unwrap();
        provider
    }

    fn log_events(&self) -> Vec<JsonValue> {
        read_log_events(&self.log_path)
    }
}

async fn create_session(provider: &HermesAgentProvider) -> proto::AgentSession {
    provider
        .create_session(Request::new(proto::CreateAgentProviderSessionRequest {
            session_id: "session-1".to_string(),
            model: "kimi-k2.6".to_string(),
            created_by: Some(proto::AgentActor {
                subject_id: "user-1".to_string(),
                subject_kind: "human".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn create_turn(provider: &HermesAgentProvider, turn_id: &str) -> proto::AgentTurn {
    provider
        .create_turn(Request::new(proto::CreateAgentProviderTurnRequest {
            turn_id: turn_id.to_string(),
            session_id: "session-1".to_string(),
            messages: vec![proto::AgentMessage {
                role: "user".to_string(),
                text: "say hi".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn wait_for_turn(
    provider: &HermesAgentProvider,
    turn_id: &str,
    status: proto::AgentExecutionStatus,
) -> proto::AgentTurn {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let turn = provider
            .get_turn(Request::new(proto::GetAgentProviderTurnRequest {
                turn_id: turn_id.to_string(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        if turn.status == status as i32 {
            return turn;
        }
        assert!(
            Instant::now() < deadline,
            "turn {turn_id} did not reach {status:?}; current status {} message {:?}",
            turn.status,
            turn.status_message
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_log_event(log_path: &Path, event_name: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if read_log_events(log_path)
            .iter()
            .any(|event| event["event"] == event_name)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "log event {event_name:?} not observed"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn assert_turn_event(provider: &HermesAgentProvider, turn_id: &str, event_type: &str) {
    let events = provider
        .list_turn_events(Request::new(proto::ListAgentProviderTurnEventsRequest {
            turn_id: turn_id.to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .events;
    assert!(
        events.iter().any(|event| event.r#type == event_type),
        "event {event_type:?} not found in {events:?}"
    );
}

fn read_log_events(log_path: &Path) -> Vec<JsonValue> {
    fs::read_to_string(log_path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

fn json_map(value: JsonValue) -> JsonMap<String, JsonValue> {
    match value {
        JsonValue::Object(map) => map,
        _ => panic!("expected object"),
    }
}

fn non_empty_struct() -> Struct {
    Struct {
        fields: [(
            "type".to_string(),
            ProstValue {
                kind: Some(Kind::StringValue("object".to_string())),
            },
        )]
        .into_iter()
        .collect(),
    }
}

fn write_token_script(path: &Path, counter_path: &Path, delay_after_first: bool) -> io::Result<()> {
    let delay_block = if delay_after_first {
        "if [ \"$n\" -ge 1 ]; then sleep 1; fi\n"
    } else {
        ""
    };
    fs::write(
        path,
        format!(
            "#!/bin/sh\nn=$(cat '{}' 2>/dev/null || echo 0)\n{delay_block}n=$((n + 1))\necho \"$n\" > '{}'\necho \"token-$n\"\n",
            counter_path.display(),
            counter_path.display()
        ),
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)?;
    }
    Ok(())
}
