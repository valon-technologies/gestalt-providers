use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use gestalt::proto::v1::app_server::{App as AppRpc, AppServer as AppGrpcServer};
use gestalt::{
    AgentProvider as _, ENV_HOST_SERVICE_SOCKET, ENV_HOST_SERVICE_TOKEN, proto::v1 as proto,
};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue, json};
use tempfile::TempDir;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::Request;
use tonic::transport::Server;

use gestalt_agent_hermes::HermesAgentProvider;

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const OWNER_SUBJECT_ID: &str = "user-1";
const OTHER_SUBJECT_ID: &str = "user-2";
const SLACK_SUBJECT_ID: &str = "service_account:slack-bot";

#[tokio::test]
async fn completes_turn_and_refreshes_adc_token_per_turn() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;

    let capabilities = provider
        .get_capabilities(gestalt::GetAgentProviderCapabilitiesRequest {})
        .await
        .unwrap();
    assert!(capabilities.streaming_text);
    assert!(capabilities.tool_calls);
    assert!(capabilities.bounded_list_hydration);
    assert_eq!(
        capabilities.supported_tool_sources,
        vec![gestalt::AgentToolSourceMode::Catalog]
    );

    create_session(&provider).await;
    create_session(&provider).await;
    create_turn(&provider, "turn-1").await;
    let turn = wait_for_turn(
        &provider,
        "turn-1",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn_text(&turn), "Hermes says hi");
    create_turn(&provider, "turn-1").await;

    create_turn(&provider, "turn-2").await;
    wait_for_turn(
        &provider,
        "turn-2",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let events = provider
        .list_turn_events(gestalt::ListAgentProviderTurnEventsRequest {
            turn_id: "turn-1".to_string(),
            subject: Some(owner_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
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
        gestalt::AgentExecutionStatus::Succeeded,
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
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn_text(&turn), "Hermes says hi");

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
async fn catalog_turn_bridges_gestalt_tools_to_hermes() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-call");
    let app = TestAppService::default();
    let socket_path = fixture.tmp.path().join("app-host.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_mcp_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp").await;
    let turn = wait_for_turn(
        &provider,
        "turn-mcp",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn_text(&turn), "Hermes used Gestalt MCP");

    let invoke_requests = app.invoke_requests.lock().expect("invoke requests").clone();
    assert_eq!(invoke_requests.len(), 1);
    assert_eq!(invoke_requests[0].app, "linear");
    assert_eq!(invoke_requests[0].operation, "issues");
    assert_eq!(
        invoke_requests[0]
            .context
            .as_ref()
            .and_then(|context| context.subject.as_ref())
            .map(|subject| subject.id.as_str()),
        Some(OWNER_SUBJECT_ID)
    );
    let context = invoke_requests[0]
        .context
        .as_ref()
        .expect("request context");
    assert_eq!(
        context.caller.as_ref().map(|caller| caller.kind.as_str()),
        Some("agent")
    );
    assert_eq!(
        context.caller.as_ref().map(|caller| caller.name.as_str()),
        Some("hermes")
    );
    assert_eq!(
        context.agent.as_ref().map(|agent| (
            agent.provider_name.as_str(),
            agent.session_id.as_str(),
            agent.turn_id.as_str()
        )),
        Some(("hermes", "session-1", "turn-mcp"))
    );
    assert!(context.tool_refs_set);
    assert_eq!(context.tool_refs.len(), 1);
    assert_eq!(context.tool_refs[0].app, "linear");
    assert_eq!(context.tool_refs[0].operation, "issues");
    assert_eq!(context.tool_refs[0].credential_mode, "subject");
    assert_eq!(invoke_requests[0].connection, "primary");
    assert_eq!(invoke_requests[0].instance, "workspace-a");
    assert_eq!(invoke_requests[0].credential_mode, "subject");
    assert_eq!(
        invoke_requests[0].idempotency_key,
        "agent/hermes-mcp:turn-mcp:1:linear.issues"
    );
    assert_eq!(
        invoke_param_json(&invoke_requests[0])["query"].as_str(),
        Some("Ada Lovelace")
    );
    assert_eq!(
        app.relay_tokens.lock().expect("relay tokens").clone(),
        vec!["relay-token".to_string()]
    );

    let log = fixture.log_events();
    let load = log
        .iter()
        .find(|event| event["event"] == "load")
        .expect("session/load logged");
    let mcp_servers = load["params"]["mcpServers"]
        .as_array()
        .expect("mcpServers array");
    assert_eq!(mcp_servers.len(), 1);
    assert_eq!(mcp_servers[0]["type"], "http");
    assert_eq!(mcp_servers[0]["name"], "gestalt");
    assert!(
        mcp_servers[0]["url"]
            .as_str()
            .unwrap_or_default()
            .contains("/mcp-")
    );
    assert!(
        log.iter().any(|event| event["event"] == "mcp_result"),
        "{log:?}"
    );
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    assert_eq!(mcp_result["result"]["unauthorizedStatus"], 401);
    assert_eq!(
        mcp_tool_names(&mcp_result["result"]["list"]),
        vec![
            "gestalt_search_tools".to_string(),
            "gestalt_get_tool_schema".to_string(),
            "gestalt_call_tool".to_string(),
        ]
    );
    let call_schema = mcp_tool_schema(&mcp_result["result"]["list"], "gestalt_call_tool");
    assert!(
        call_schema.get("oneOf").is_none(),
        "proxy selector schema should avoid top-level oneOf: {call_schema}"
    );
    assert!(call_schema["properties"]["mcp_name"].is_object());
    assert!(call_schema["properties"]["ref"].is_object());
    assert!(mcp_result["result"]["list"]["result"]["nextCursor"].is_null());
    let search_payload = tool_call_payload(&mcp_result["result"]["search"]);
    assert_eq!(
        search_payload["tools"][0]["mcp_name"].as_str(),
        Some("linear.issues")
    );
    assert_eq!(
        mcp_result["result"]["call"]["result"]["content"][0]["text"].as_str(),
        Some("linear tickets")
    );

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn catalog_turn_inherits_session_tool_scope() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-call");
    let app = TestAppService::default();
    let socket_path = fixture.tmp.path().join("app-host.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_session_with_tools(
        &provider,
        "session-1",
        catalog_tool_config(vec![gestalt::AgentToolRef {
            app: "*".to_string(),
            credential_mode: "subject".to_string(),
            ..Default::default()
        }]),
    )
    .await;
    provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: "turn-session-tools".to_string(),
            session_id: "session-1".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "show me my linear tickets".to_string(),
                ..Default::default()
            }],
            output: gestalt::AgentOutput::text(),
            created_by_subject_id: Some(OWNER_SUBJECT_ID.to_string()),
            subject: Some(owner_subject()),
            context: Some(request_context_for_turn(
                OWNER_SUBJECT_ID,
                "session-1",
                "turn-session-tools",
            )),
            ..empty_turn_request()
        })
        .await
        .unwrap();
    let turn = wait_for_turn(
        &provider,
        "turn-session-tools",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn_text(&turn), "Hermes used Gestalt MCP");

    let invoke_requests = app.invoke_requests.lock().expect("invoke requests").clone();
    assert_eq!(invoke_requests.len(), 1);
    assert_eq!(
        invoke_requests[0].idempotency_key,
        "agent/hermes-mcp:turn-session-tools:1:linear.issues"
    );
    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn catalog_turn_does_not_invoke_apps_for_mcp_tool_listing() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-list-only");
    let app = TestAppService::default();
    let socket_path = fixture.tmp.path().join("app-host.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_mcp_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp-no-prefetch").await;
    let turn = wait_for_turn(
        &provider,
        "turn-mcp-no-prefetch",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn_text(&turn), "Hermes used Gestalt MCP");

    let invoke_requests = app.invoke_requests.lock().expect("invoke requests").clone();
    assert!(
        invoke_requests.is_empty(),
        "MCP bridge should not invoke apps during proxy tools/list: {invoke_requests:?}"
    );

    let log = fixture.log_events();
    let load = log
        .iter()
        .find(|event| event["event"] == "load")
        .expect("session/load logged");
    let mcp_servers = load["params"]["mcpServers"]
        .as_array()
        .expect("mcpServers array");
    assert_eq!(mcp_servers.len(), 1);
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    assert_eq!(
        mcp_tool_names(&mcp_result["result"]["list"]),
        vec![
            "gestalt_search_tools".to_string(),
            "gestalt_get_tool_schema".to_string(),
            "gestalt_call_tool".to_string(),
        ]
    );

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn catalog_turn_marks_target_app_error_status_as_tool_error() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-call");
    let app = TestAppService {
        invoke_status: 424,
        invoke_body: Some(r#"{"error":{"code":"reconnect_required","plugin":"linear"}}"#),
        ..Default::default()
    };
    let socket_path = fixture.tmp.path().join("app-host.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_mcp_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp-sentinel").await;
    let turn = wait_for_turn(
        &provider,
        "turn-mcp-sentinel",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn_text(&turn), "Hermes used Gestalt MCP");

    assert_eq!(
        app.invoke_requests.lock().expect("invoke requests").len(),
        1
    );

    let log = fixture.log_events();
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    assert_eq!(
        mcp_result["result"]["call"]["result"]["isError"],
        serde_json::Value::Bool(true)
    );
    let payload = tool_call_payload(&mcp_result["result"]["call"]);
    assert_eq!(
        payload["error"]["code"].as_str(),
        Some("target_tool_failed")
    );
    assert_eq!(payload["error"]["status"].as_i64(), Some(424));
    assert_eq!(
        payload["error"]["body"].as_str(),
        Some(r#"{"error":{"code":"reconnect_required","plugin":"linear"}}"#)
    );

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn catalog_proxy_gets_schema_by_returned_mcp_name() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-schema");
    let app = TestAppService::default();
    let socket_path = fixture.tmp.path().join("app-host-schema.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_mcp_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp-schema").await;
    wait_for_turn(
        &provider,
        "turn-mcp-schema",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    let schema_payload = tool_call_payload(&mcp_result["result"]["schema"]);
    assert_eq!(
        schema_payload["tool"]["mcp_name"].as_str(),
        Some("linear.issues")
    );
    assert!(schema_payload["tool"]["input_schema"]["properties"]["query"].is_object());
    assert!(
        app.invoke_requests
            .lock()
            .expect("invoke requests")
            .is_empty()
    );

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn catalog_proxy_rejects_ambiguous_ref_selectors() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-ambiguous-ref");
    let app = TestAppService::default();
    let socket_path = fixture.tmp.path().join("app-host-ambiguous.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_mcp_session_with_tools(
        &provider,
        vec![
            listed_tool("linear-list-a", "linear.issues", "Linear issues A"),
            listed_tool(
                "linear-list-b",
                "linear.issues.secondary",
                "Linear issues B",
            ),
        ],
    )
    .await;
    create_mcp_turn(&provider, "turn-mcp-ambiguous").await;
    wait_for_turn(
        &provider,
        "turn-mcp-ambiguous",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    let payload = assert_proxy_error(&mcp_result["result"]["schema"], "ambiguous_tool_ref");
    assert!(
        payload["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("mcp_name"),
        "{payload}"
    );

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn catalog_proxy_rejects_invalid_selectors_before_lookup() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-invalid-selector");
    let app = TestAppService::default();
    let socket_path = fixture.tmp.path().join("app-host-invalid-selector.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_mcp_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp-invalid-selector").await;
    wait_for_turn(
        &provider,
        "turn-mcp-invalid-selector",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    assert_proxy_error(&mcp_result["result"]["schema"], "invalid_selector");

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn catalog_proxy_searches_only_catalog_metadata() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-search-schema-only");
    let app = TestAppService::default();
    let socket_path = fixture.tmp.path().join("app-host-schema-only.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_mcp_session_with_tools(
        &provider,
        vec![gestalt::ListedAgentTool {
            id: "schema-only".to_string(),
            mcp_name: "neutral.tool".to_string(),
            title: "Neutral tool".to_string(),
            description: "No matching metadata".to_string(),
            input_schema:
                r#"{"type":"object","properties":{"schemaOnlySecret":{"type":"string"}}}"#
                    .to_string(),
            r#ref: Some(gestalt::AgentToolRef {
                app: "neutral".to_string(),
                operation: "lookup".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }],
    )
    .await;
    create_mcp_turn(&provider, "turn-mcp-schema-only").await;
    wait_for_turn(
        &provider,
        "turn-mcp-schema-only",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    let search_payload = tool_call_payload(&mcp_result["result"]["search"]);
    assert_eq!(search_payload["tools"].as_array().map(Vec::len), Some(0));
    assert_eq!(search_payload["candidates"].as_array().map(Vec::len), None);

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn catalog_proxy_ranks_matches_across_catalog_tools() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-search-only");
    let app = TestAppService::default();
    let socket_path = fixture.tmp.path().join("app-host-ranked-tools.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    let mut tools: Vec<_> = (0..20)
        .map(|index| gestalt::ListedAgentTool {
            id: format!("linear-partial-{index}"),
            mcp_name: format!("linear.partial.{index}"),
            title: "Linear catalog entry".to_string(),
            description: "Catalog entry visible to the user".to_string(),
            input_schema: r#"{"type":"object","properties":{"query":{"type":"string"}}}"#
                .to_string(),
            r#ref: Some(gestalt::AgentToolRef {
                app: "linear".to_string(),
                operation: format!("partial-{index}"),
                ..Default::default()
            }),
            ..Default::default()
        })
        .collect();
    tools.push(listed_tool(
        "linear-best",
        "linear.issues.best",
        "Linear issues",
    ));
    create_mcp_session_with_tools(&provider, tools).await;
    create_mcp_turn(&provider, "turn-mcp-ranked-pages").await;
    wait_for_turn(
        &provider,
        "turn-mcp-ranked-pages",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    let search_payload = tool_call_payload(&mcp_result["result"]["search"]);
    assert_eq!(
        search_payload["tools"][0]["mcp_name"].as_str(),
        Some("linear.issues.best"),
        "{search_payload}"
    );
    assert_eq!(search_payload["has_more"], JsonValue::Bool(true));

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn rejects_invalid_session_catalog_tools_without_spawning_hermes() {
    let fixture = Fixture::new("mcp-search-only");
    let provider = fixture.configure_provider().await;
    let unsafe_tool = listed_tool("unsafe", "unsafe tool", "Unsafe tool");
    let mut missing_ref = listed_tool("missing-ref", "linear.missing_ref", "Linear missing ref");
    missing_ref.r#ref = None;
    let mut wildcard_listed_ref = listed_tool("wildcard", "linear.wildcard", "Linear wildcard");
    wildcard_listed_ref.r#ref = Some(gestalt::AgentToolRef {
        app: "linear".to_string(),
        operation: "*".to_string(),
        ..Default::default()
    });
    let mut run_as_listed_ref = listed_tool("run-as", "linear.run_as", "Linear run as");
    run_as_listed_ref.r#ref.as_mut().expect("ref").run_as = Some(gestalt::Subject {
        id: "user:other".to_string(),
        ..Default::default()
    });

    for (name, tools) in [
        ("unsafe-name", vec![unsafe_tool]),
        (
            "duplicate-name",
            vec![
                listed_tool("linear-list-a", "linear.issues", "Linear issues A"),
                listed_tool("linear-list-b", "linear.issues", "Linear issues B"),
            ],
        ),
        ("missing-ref", vec![missing_ref]),
        ("wildcard-listed-ref", vec![wildcard_listed_ref]),
        ("run-as-listed-ref", vec![run_as_listed_ref]),
    ] {
        let err = provider
            .create_session(gestalt::CreateAgentProviderSessionRequest {
                session_id: format!("session-{name}"),
                model: "kimi-k2.6".to_string(),
                tools: Some(catalog_tool_config_with_tools(
                    vec![gestalt::AgentToolRef {
                        app: "*".to_string(),
                        credential_mode: "subject".to_string(),
                        ..Default::default()
                    }],
                    tools,
                )),
                created_by_subject_id: Some(OWNER_SUBJECT_ID.to_string()),
                subject: Some(owner_subject()),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.status(), Some(400), "{name}: {err}");
    }
    assert!(
        fixture.log_events().is_empty(),
        "Hermes should not start for invalid catalog tools"
    );
}

#[tokio::test]
async fn catalog_proxy_reports_input_cap_errors_without_invoking_apps() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-input-caps");
    let app = TestAppService::default();
    let socket_path = fixture.tmp.path().join("app-host-input-caps.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_mcp_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp-input-caps").await;
    wait_for_turn(
        &provider,
        "turn-mcp-input-caps",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    assert_proxy_error(&mcp_result["result"]["search"], "invalid_arguments");
    assert!(
        app.invoke_requests
            .lock()
            .expect("invoke requests")
            .is_empty()
    );

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn catalog_proxy_reports_app_invoke_errors_as_tool_errors() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-call");
    let app = TestAppService {
        invoke_error: Some("app invoke failed"),
        ..Default::default()
    };
    let socket_path = fixture.tmp.path().join("app-host-invoke-error.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_mcp_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp-execute-error").await;
    wait_for_turn(
        &provider,
        "turn-mcp-execute-error",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    let payload = assert_proxy_error(&mcp_result["result"]["call"], "invoke_tool_failed");
    assert!(
        payload["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("app invoke failed"),
        "{payload}"
    );

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn catalog_does_not_require_advertised_acp_http_mcp_support() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-call-no-cap");
    let app = TestAppService::default();
    let socket_path = fixture.tmp.path().join("app-host-no-cap.sock");
    let _socket_guard = EnvGuard::set(ENV_HOST_SERVICE_SOCKET, socket_path.as_os_str());
    let _token_guard = EnvGuard::set(ENV_HOST_SERVICE_TOKEN, "relay-token");
    let app_task = serve_app_host(socket_path, app.clone()).await;
    let provider = fixture.configure_provider().await;

    create_mcp_session(&provider).await;
    create_mcp_turn(&provider, "turn-no-cap").await;
    let turn = wait_for_turn(
        &provider,
        "turn-no-cap",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn_text(&turn), "Hermes used Gestalt MCP");
    let log = fixture.log_events();
    assert!(log.iter().any(|event| event["event"] == "mcp_result"));

    app_task.abort();
    let _ = app_task.await;
}

#[tokio::test]
async fn structured_output_turn_returns_validated_value() {
    let fixture = Fixture::new("structured-json");
    let provider = fixture.configure_provider().await;

    create_session(&provider).await;
    provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: "turn-structured".to_string(),
            session_id: "session-1".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "grade".to_string(),
                ..Default::default()
            }],
            output: gestalt::AgentOutput::Structured(gestalt::AgentStructuredOutput {
                schema: json!({
                    "type": "object",
                    "required": ["score", "reasoning"],
                    "properties": {
                        "score": {"type": "number"},
                        "reasoning": {"type": "string"}
                    }
                }),
            }),
            created_by_subject_id: Some(OWNER_SUBJECT_ID.to_string()),
            subject: Some(owner_subject()),
            context: Some(request_context(OWNER_SUBJECT_ID)),
            ..empty_turn_request()
        })
        .await
        .unwrap();
    let turn = wait_for_turn(
        &provider,
        "turn-structured",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    match turn.output {
        Some(gestalt::AgentTurnOutput::Structured(output)) => {
            assert_eq!(
                output.value,
                Some(json!({"score": 1, "reasoning": "correct"}))
            );
        }
        other => panic!("expected structured output, got {other:?}"),
    }
}

#[tokio::test]
async fn structured_output_turn_fails_invalid_json() {
    let fixture = Fixture::new("structured-invalid");
    let provider = fixture.configure_provider().await;

    create_session(&provider).await;
    provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: "turn-structured-invalid".to_string(),
            session_id: "session-1".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "grade".to_string(),
                ..Default::default()
            }],
            output: gestalt::AgentOutput::Structured(gestalt::AgentStructuredOutput {
                schema: json!({
                    "type": "object",
                    "required": ["score"],
                    "properties": {"score": {"type": "number"}}
                }),
            }),
            created_by_subject_id: Some(OWNER_SUBJECT_ID.to_string()),
            subject: Some(owner_subject()),
            context: Some(request_context(OWNER_SUBJECT_ID)),
            ..empty_turn_request()
        })
        .await
        .unwrap();
    let turn = wait_for_turn(
        &provider,
        "turn-structured-invalid",
        gestalt::AgentExecutionStatus::Failed,
    )
    .await;
    assert!(
        turn.status_message.contains("structured output"),
        "{}",
        turn.status_message
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
        gestalt::AgentExecutionStatus::Failed,
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
async fn rejects_unsupported_tool_and_model_options() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;
    create_session(&provider).await;

    let err = provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: "turn-resolved-tools".to_string(),
            session_id: "session-1".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "hi".to_string(),
                ..Default::default()
            }],
            output: gestalt::AgentOutput::text(),
            tools: vec![gestalt::ResolvedAgentTool {
                id: "tool-1".to_string(),
                name: "tool".to_string(),
                ..Default::default()
            }],
            created_by_subject_id: Some(OWNER_SUBJECT_ID.to_string()),
            subject: Some(owner_subject()),
            context: Some(request_context(OWNER_SUBJECT_ID)),
            ..empty_turn_request()
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(400));

    let err = provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: "turn-schema".to_string(),
            session_id: "session-1".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "hi".to_string(),
                ..Default::default()
            }],
            output: gestalt::AgentOutput::Structured(gestalt::AgentStructuredOutput {
                schema: json!({}),
            }),
            created_by_subject_id: Some(OWNER_SUBJECT_ID.to_string()),
            subject: Some(owner_subject()),
            context: Some(request_context(OWNER_SUBJECT_ID)),
            ..empty_turn_request()
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(400));

    let err = provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: "turn-model-options".to_string(),
            session_id: "session-1".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "hi".to_string(),
                ..Default::default()
            }],
            output: gestalt::AgentOutput::text(),
            model_options: Some(json!({ "type": "object" })),
            created_by_subject_id: Some(OWNER_SUBJECT_ID.to_string()),
            subject: Some(owner_subject()),
            context: Some(request_context(OWNER_SUBJECT_ID)),
            ..empty_turn_request()
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(400));
}

#[tokio::test]
async fn rejects_empty_session_id_without_spawning_hermes() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;

    let err = provider
        .create_session(gestalt::CreateAgentProviderSessionRequest {
            session_id: "   ".to_string(),
            model: "kimi-k2.6".to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(400));
    assert!(
        fixture.log_events().is_empty(),
        "Hermes should not start for invalid session ids"
    );
}

#[tokio::test]
async fn owner_can_read_and_mutate_private_session() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;
    create_session_with(
        &provider,
        "session-owner-private",
        OWNER_SUBJECT_ID,
        Some(owner_subject()),
        None,
    )
    .await;

    let session = provider
        .get_session(gestalt::GetAgentProviderSessionRequest {
            session_id: "session-owner-private".to_string(),
            subject: Some(owner_subject()),
        })
        .await
        .unwrap();
    assert_eq!(session.id, "session-owner-private");

    let updated = provider
        .update_session(gestalt::UpdateAgentProviderSessionRequest {
            session_id: "session-owner-private".to_string(),
            client_ref: "owner-ref".to_string(),
            subject: Some(owner_subject()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(updated.client_ref, "owner-ref");

    create_turn_in_session(&provider, "session-owner-private", "turn-owner-private").await;
    wait_for_turn(
        &provider,
        "turn-owner-private",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    let turns = provider
        .list_turns(gestalt::ListAgentProviderTurnsRequest {
            session_id: "session-owner-private".to_string(),
            subject: Some(owner_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
        .turns;
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].id, "turn-owner-private");
}

#[tokio::test]
async fn slack_metadata_makes_session_company_visible_at_create_only() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;
    create_session_with(
        &provider,
        "session-company",
        SLACK_SUBJECT_ID,
        Some(slack_subject()),
        Some(json!({
            "slack": {
                "team_id": "T1",
                "channel_id": "C1",
                "channel_type": "channel",
                "root_message_ts": "1712161829.000300",
                "session_ref": "slack:T1:C1:1712161829.000300"
            }
        })),
    )
    .await;
    create_session_with(
        &provider,
        "session-private-mixed",
        OWNER_SUBJECT_ID,
        Some(owner_subject()),
        None,
    )
    .await;

    let session = provider
        .get_session(gestalt::GetAgentProviderSessionRequest {
            session_id: "session-company".to_string(),
            subject: Some(other_subject()),
        })
        .await
        .unwrap();
    assert_eq!(session.id, "session-company");

    let sessions = provider
        .list_sessions(gestalt::ListAgentProviderSessionsRequest {
            subject: Some(other_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
        .sessions;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].id, "session-company");

    let missing_subject_err = provider
        .get_session(gestalt::GetAgentProviderSessionRequest {
            session_id: "session-company".to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(missing_subject_err.status(), Some(404));

    provider
        .update_session(gestalt::UpdateAgentProviderSessionRequest {
            session_id: "session-company".to_string(),
            metadata: Some(json!({ "note": "slack metadata removed" })),
            subject: Some(slack_subject()),
            ..Default::default()
        })
        .await
        .unwrap();
    provider
        .get_session(gestalt::GetAgentProviderSessionRequest {
            session_id: "session-company".to_string(),
            subject: Some(other_subject()),
        })
        .await
        .unwrap();

    create_turn_in_session_as(
        &provider,
        "session-company",
        "turn-company",
        SLACK_SUBJECT_ID,
        slack_subject(),
    )
    .await;
    wait_for_turn(
        &provider,
        "turn-company",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    provider
        .get_turn(gestalt::GetAgentProviderTurnRequest {
            turn_id: "turn-company".to_string(),
            subject: Some(other_subject()),
        })
        .await
        .unwrap();
    let turns = provider
        .list_turns(gestalt::ListAgentProviderTurnsRequest {
            session_id: "session-company".to_string(),
            subject: Some(other_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
        .turns;
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].id, "turn-company");
    let exact_turns = provider
        .list_turns(gestalt::ListAgentProviderTurnsRequest {
            turn_ids: vec!["turn-company".to_string()],
            subject: Some(other_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
        .turns;
    assert_eq!(exact_turns.len(), 1);
    assert_eq!(exact_turns[0].id, "turn-company");
    let events = provider
        .list_turn_events(gestalt::ListAgentProviderTurnEventsRequest {
            turn_id: "turn-company".to_string(),
            subject: Some(other_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
        .events;
    assert!(!events.is_empty());
}

#[tokio::test]
async fn private_sessions_hide_non_owner_reads_lists_turns_and_events() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;
    create_session_with(
        &provider,
        "session-private-hidden",
        OWNER_SUBJECT_ID,
        Some(owner_subject()),
        None,
    )
    .await;
    create_turn_in_session(&provider, "session-private-hidden", "turn-private-hidden").await;
    wait_for_turn(
        &provider,
        "turn-private-hidden",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let err = provider
        .get_session(gestalt::GetAgentProviderSessionRequest {
            session_id: "session-private-hidden".to_string(),
            subject: Some(other_subject()),
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(404));
    let err = provider
        .get_session(gestalt::GetAgentProviderSessionRequest {
            session_id: "session-private-hidden".to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(404));

    let sessions = provider
        .list_sessions(gestalt::ListAgentProviderSessionsRequest {
            subject: Some(other_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
        .sessions;
    assert!(sessions.is_empty());
    let sessions = provider
        .list_sessions(gestalt::ListAgentProviderSessionsRequest::default())
        .await
        .unwrap()
        .sessions;
    assert!(sessions.is_empty());

    let err = provider
        .get_turn(gestalt::GetAgentProviderTurnRequest {
            turn_id: "turn-private-hidden".to_string(),
            subject: Some(other_subject()),
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(404));
    let turns = provider
        .list_turns(gestalt::ListAgentProviderTurnsRequest {
            session_id: "session-private-hidden".to_string(),
            subject: Some(other_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
        .turns;
    assert!(turns.is_empty());
    let events = provider
        .list_turn_events(gestalt::ListAgentProviderTurnEventsRequest {
            turn_id: "turn-private-hidden".to_string(),
            subject: Some(other_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
        .events;
    assert!(events.is_empty());

    provider
        .update_session(gestalt::UpdateAgentProviderSessionRequest {
            session_id: "session-private-hidden".to_string(),
            metadata: Some(json!({
                "slack": {
                    "team_id": "T1",
                    "channel_id": "C1",
                    "session_ref": "slack:T1:C1"
                }
            })),
            subject: Some(owner_subject()),
            ..Default::default()
        })
        .await
        .unwrap();
    let err = provider
        .get_session(gestalt::GetAgentProviderSessionRequest {
            session_id: "session-private-hidden".to_string(),
            subject: Some(other_subject()),
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(404));

    create_session_with(
        &provider,
        "session-incomplete-slack",
        SLACK_SUBJECT_ID,
        Some(slack_subject()),
        Some(json!({ "slack": { "team_id": "T1", "channel_id": "C1" } })),
    )
    .await;
    let err = provider
        .get_session(gestalt::GetAgentProviderSessionRequest {
            session_id: "session-incomplete-slack".to_string(),
            subject: Some(other_subject()),
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(404));
}

#[tokio::test]
async fn non_owner_cannot_mutate_company_visible_session_or_turn() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;
    create_session_with(
        &provider,
        "session-company-write",
        SLACK_SUBJECT_ID,
        Some(slack_subject()),
        Some(json!({
            "slack": {
                "team_id": "T1",
                "channel_id": "C1",
                "root_message_ts": "1712161829.000300",
                "session_ref": "slack:T1:C1:1712161829.000300"
            }
        })),
    )
    .await;

    let err = provider
        .update_session(gestalt::UpdateAgentProviderSessionRequest {
            session_id: "session-company-write".to_string(),
            client_ref: "other-ref".to_string(),
            subject: Some(other_subject()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(403));

    let err = provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: "turn-other-write".to_string(),
            session_id: "session-company-write".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "hi".to_string(),
                ..Default::default()
            }],
            output: gestalt::AgentOutput::text(),
            subject: Some(other_subject()),
            created_by_subject_id: Some(OTHER_SUBJECT_ID.to_string()),
            ..empty_turn_request()
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(403));

    create_turn_in_session_as(
        &provider,
        "session-company-write",
        "turn-company-write-owner",
        SLACK_SUBJECT_ID,
        slack_subject(),
    )
    .await;
    let err = provider
        .cancel_turn(gestalt::CancelAgentProviderTurnRequest {
            turn_id: "turn-company-write-owner".to_string(),
            reason: "other subject requested".to_string(),
            subject: Some(other_subject()),
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(403));
    wait_for_turn(
        &provider,
        "turn-company-write-owner",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
}

#[tokio::test]
async fn cancel_turn_sends_acp_cancel_and_marks_turn_canceled() {
    let fixture = Fixture::new("hang");
    let provider = fixture.configure_provider().await;
    create_session(&provider).await;
    create_turn(&provider, "turn-cancel").await;

    wait_for_log_event(&fixture.log_path, "prompt").await;
    let canceled = provider
        .cancel_turn(gestalt::CancelAgentProviderTurnRequest {
            turn_id: "turn-cancel".to_string(),
            reason: "operator requested".to_string(),
            subject: Some(owner_subject()),
        })
        .await
        .unwrap();
    assert_eq!(canceled.status, gestalt::AgentExecutionStatus::Canceled);
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
        .cancel_turn(gestalt::CancelAgentProviderTurnRequest {
            turn_id: "turn-early-cancel".to_string(),
            reason: "operator requested".to_string(),
            subject: Some(owner_subject()),
        })
        .await
        .unwrap();
    assert_eq!(canceled.status, gestalt::AgentExecutionStatus::Canceled);
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

struct Fixture {
    tmp: TempDir,
    hermes_home: TempDir,
    log_path: PathBuf,
    token_script: PathBuf,
}

#[derive(Clone, Default)]
struct TestAppService {
    invoke_requests: Arc<StdMutex<Vec<proto::AppInvokeRequest>>>,
    relay_tokens: Arc<StdMutex<Vec<String>>>,
    invoke_status: i32,
    invoke_body: Option<&'static str>,
    invoke_error: Option<&'static str>,
}

#[tonic::async_trait]
impl AppRpc for TestAppService {
    async fn invoke(
        &self,
        request: Request<proto::AppInvokeRequest>,
    ) -> Result<tonic::Response<proto::OperationResult>, tonic::Status> {
        let relay_tokens = request
            .metadata()
            .get_all("x-gestalt-host-service-relay-token")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .map(str::to_string)
            .collect::<Vec<_>>();
        self.relay_tokens
            .lock()
            .expect("relay tokens")
            .extend(relay_tokens);
        let request = request.into_inner();
        self.invoke_requests
            .lock()
            .expect("invoke requests")
            .push(request);
        if let Some(message) = self.invoke_error {
            return Err(tonic::Status::internal(message));
        }
        Ok(tonic::Response::new(proto::OperationResult {
            status: if self.invoke_status == 0 {
                200
            } else {
                self.invoke_status
            },
            body: self.invoke_body.unwrap_or("linear tickets").to_string(),
            headers: Default::default(),
        }))
    }

    async fn invoke_graph_ql(
        &self,
        _request: Request<proto::AppInvokeGraphQlRequest>,
    ) -> Result<tonic::Response<proto::OperationResult>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "GraphQL invocation is not used by this test",
        ))
    }
}

fn listed_tool(id: &str, mcp_name: &str, title: &str) -> gestalt::ListedAgentTool {
    gestalt::ListedAgentTool {
        id: id.to_string(),
        mcp_name: mcp_name.to_string(),
        title: title.to_string(),
        description: "List Linear issues visible to the user".to_string(),
        input_schema: r#"{"type":"object","properties":{"query":{"type":"string"}}}"#.to_string(),
        annotations: Some(gestalt::AgentToolAnnotations {
            read_only_hint: Some(true),
            open_world_hint: Some(false),
            ..Default::default()
        }),
        r#ref: Some(gestalt::AgentToolRef {
            app: "linear".to_string(),
            operation: "issues".to_string(),
            connection: "primary".to_string(),
            instance: "workspace-a".to_string(),
            credential_mode: "subject".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn mcp_tool_names(list_result: &JsonValue) -> Vec<String> {
    list_result["result"]["tools"]
        .as_array()
        .into_iter()
        .flat_map(|tools| tools.iter())
        .filter_map(|tool| tool["name"].as_str().map(str::to_string))
        .collect()
}

fn mcp_tool_schema<'a>(list_result: &'a JsonValue, name: &str) -> &'a JsonValue {
    list_result["result"]["tools"]
        .as_array()
        .into_iter()
        .flat_map(|tools| tools.iter())
        .find(|tool| tool["name"].as_str() == Some(name))
        .and_then(|tool| {
            tool.get("inputSchema")
                .or_else(|| tool.get("input_schema"))
                .or_else(|| tool.get("input_schema_json"))
        })
        .unwrap_or_else(|| panic!("MCP list result missing schema for {name}: {list_result}"))
}

fn tool_call_payload(call_result: &JsonValue) -> JsonValue {
    let text = call_result["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool call result has no text content: {call_result}"));
    serde_json::from_str(text)
        .unwrap_or_else(|err| panic!("decode tool call payload {text:?}: {err}"))
}

fn invoke_param_json(request: &proto::AppInvokeRequest) -> JsonValue {
    request
        .params
        .as_ref()
        .map(prost_struct_to_json)
        .unwrap_or_else(|| JsonValue::Object(JsonMap::new()))
}

fn prost_struct_to_json(value: &prost_types::Struct) -> JsonValue {
    JsonValue::Object(
        value
            .fields
            .iter()
            .map(|(key, value)| (key.clone(), prost_value_to_json(value)))
            .collect(),
    )
}

fn prost_value_to_json(value: &prost_types::Value) -> JsonValue {
    use prost_types::value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullValue(_)) | None => JsonValue::Null,
        Some(Kind::NumberValue(value)) => JsonNumber::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Some(Kind::StringValue(value)) => JsonValue::String(value.clone()),
        Some(Kind::BoolValue(value)) => JsonValue::Bool(*value),
        Some(Kind::StructValue(value)) => prost_struct_to_json(value),
        Some(Kind::ListValue(value)) => {
            JsonValue::Array(value.values.iter().map(prost_value_to_json).collect())
        }
    }
}

fn assert_proxy_error(call_result: &JsonValue, code: &str) -> JsonValue {
    assert_eq!(
        call_result["result"]["isError"],
        JsonValue::Bool(true),
        "{call_result}"
    );
    let payload = tool_call_payload(call_result);
    assert_eq!(payload["ok"], JsonValue::Bool(false), "{payload}");
    assert_eq!(payload["error"]["code"].as_str(), Some(code), "{payload}");
    payload
}

struct EnvGuard {
    key: String,
    previous: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn set(key: &str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self {
            key: key.to_string(),
            previous,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        unsafe {
            if let Some(previous) = &self.previous {
                std::env::set_var(&self.key, previous);
            } else {
                std::env::remove_var(&self.key);
            }
        }
    }
}

impl Fixture {
    fn new(mode: &str) -> Self {
        Self::new_internal(mode, false)
    }

    fn new_with_delayed_turn_token(mode: &str) -> Self {
        Self::new_internal(mode, true)
    }

    fn new_internal(mode: &str, delay_turn_token: bool) -> Self {
        let tmp = tempfile::tempdir().expect("tmp");
        let hermes_home = tempfile::tempdir().expect("hermes home");
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
        let mode = fs::read_to_string(self.tmp.path().join("mode")).expect("mode");
        let config = json_map(json!({
            "hermesHome": self.hermes_home.path().to_string_lossy(),
            "hermesCommand": env!("CARGO_BIN_EXE_fake-acp"),
            "hermesArgs": [
                "--fake-acp-log",
                self.log_path.to_string_lossy(),
                "--fake-acp-mode",
                mode
            ],
            "workingDirectory": self.tmp.path().to_string_lossy(),
            "defaultModel": "kimi-k2.6",
            "accessTokenCommand": [
                self.token_script.to_string_lossy()
            ],
            "modelSwitchingEnabled": model_switching_enabled,
            "autoApprovePermissions": true
        }));
        gestalt::AgentProvider::configure(&provider, "hermes", config)
            .await
            .unwrap();
        provider
    }

    fn log_events(&self) -> Vec<JsonValue> {
        read_log_events(&self.log_path)
    }
}

async fn create_session(provider: &HermesAgentProvider) -> gestalt::AgentSession {
    create_session_with(
        provider,
        "session-1",
        OWNER_SUBJECT_ID,
        Some(owner_subject()),
        None,
    )
    .await
}

async fn create_session_with_tools(
    provider: &HermesAgentProvider,
    session_id: &str,
    tools: gestalt::AgentToolConfig,
) -> gestalt::AgentSession {
    provider
        .create_session(gestalt::CreateAgentProviderSessionRequest {
            session_id: session_id.to_string(),
            model: "kimi-k2.6".to_string(),
            tools: Some(tools),
            created_by_subject_id: Some(OWNER_SUBJECT_ID.to_string()),
            subject: Some(owner_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
}

fn catalog_tool_config(refs: Vec<gestalt::AgentToolRef>) -> gestalt::AgentToolConfig {
    catalog_tool_config_with_tools(
        refs,
        vec![listed_tool("linear-list", "linear.issues", "Linear issues")],
    )
}

fn catalog_tool_config_with_tools(
    refs: Vec<gestalt::AgentToolRef>,
    tools: Vec<gestalt::ListedAgentTool>,
) -> gestalt::AgentToolConfig {
    gestalt::AgentToolConfig {
        source: Some(gestalt::AgentToolConfigSource::Catalog(
            gestalt::AgentCatalogToolConfig { refs, tools },
        )),
    }
}

fn listed_tool_refs(tools: &[gestalt::ListedAgentTool]) -> Vec<gestalt::AgentToolRef> {
    tools.iter().filter_map(|tool| tool.r#ref.clone()).collect()
}

async fn create_session_with(
    provider: &HermesAgentProvider,
    session_id: &str,
    owner_subject_id: &str,
    subject: Option<gestalt::Subject>,
    metadata: Option<JsonValue>,
) -> gestalt::AgentSession {
    provider
        .create_session(gestalt::CreateAgentProviderSessionRequest {
            session_id: session_id.to_string(),
            model: "kimi-k2.6".to_string(),
            metadata,
            tools: Some(catalog_tool_config(vec![gestalt::AgentToolRef {
                app: "*".to_string(),
                credential_mode: "subject".to_string(),
                ..Default::default()
            }])),
            created_by_subject_id: Some(owner_subject_id.to_string()),
            subject,
            ..Default::default()
        })
        .await
        .unwrap()
}

async fn create_turn(provider: &HermesAgentProvider, turn_id: &str) -> gestalt::AgentTurn {
    create_turn_in_session(provider, "session-1", turn_id).await
}

async fn create_turn_in_session(
    provider: &HermesAgentProvider,
    session_id: &str,
    turn_id: &str,
) -> gestalt::AgentTurn {
    create_turn_in_session_as(
        provider,
        session_id,
        turn_id,
        OWNER_SUBJECT_ID,
        owner_subject(),
    )
    .await
}

async fn create_turn_in_session_as(
    provider: &HermesAgentProvider,
    session_id: &str,
    turn_id: &str,
    created_by_subject_id: &str,
    subject: gestalt::Subject,
) -> gestalt::AgentTurn {
    provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: turn_id.to_string(),
            session_id: session_id.to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "say hi".to_string(),
                ..Default::default()
            }],
            output: gestalt::AgentOutput::text(),
            created_by_subject_id: Some(created_by_subject_id.to_string()),
            subject: Some(subject),
            context: Some(request_context_for_turn(
                created_by_subject_id,
                session_id,
                turn_id,
            )),
            ..empty_turn_request()
        })
        .await
        .unwrap()
}

async fn create_mcp_session(provider: &HermesAgentProvider) -> gestalt::AgentSession {
    create_mcp_session_with_tools(
        provider,
        vec![listed_tool("linear-list", "linear.issues", "Linear issues")],
    )
    .await
}

async fn create_mcp_session_with_tools(
    provider: &HermesAgentProvider,
    tools: Vec<gestalt::ListedAgentTool>,
) -> gestalt::AgentSession {
    create_session_with_tools(
        provider,
        "session-1",
        catalog_tool_config_with_tools(listed_tool_refs(&tools), tools),
    )
    .await
}

async fn create_mcp_turn(provider: &HermesAgentProvider, turn_id: &str) -> gestalt::AgentTurn {
    provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: turn_id.to_string(),
            session_id: "session-1".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "show me my linear tickets".to_string(),
                ..Default::default()
            }],
            output: gestalt::AgentOutput::text(),
            created_by_subject_id: Some(OWNER_SUBJECT_ID.to_string()),
            subject: Some(owner_subject()),
            context: Some(request_context_for_turn(
                OWNER_SUBJECT_ID,
                "session-1",
                turn_id,
            )),
            ..empty_turn_request()
        })
        .await
        .unwrap()
}

fn owner_subject() -> gestalt::Subject {
    subject_context(OWNER_SUBJECT_ID)
}

fn slack_subject() -> gestalt::Subject {
    service_account_subject(SLACK_SUBJECT_ID)
}

fn other_subject() -> gestalt::Subject {
    subject_context(OTHER_SUBJECT_ID)
}

fn turn_text(turn: &gestalt::AgentTurn) -> &str {
    match turn.output.as_ref() {
        Some(gestalt::AgentTurnOutput::Text(output)) => output.text.as_str(),
        Some(gestalt::AgentTurnOutput::Structured(output)) => output.text.as_str(),
        None => "",
    }
}

fn empty_turn_request() -> gestalt::CreateAgentProviderTurnRequest {
    gestalt::CreateAgentProviderTurnRequest {
        turn_id: String::new(),
        session_id: String::new(),
        idempotency_key: String::new(),
        model: String::new(),
        messages: Vec::new(),
        tools: Vec::new(),
        output: gestalt::AgentOutput::text(),
        metadata: None,
        created_by_subject_id: None,
        execution_ref: String::new(),
        subject: None,
        model_options: None,
        timeout_seconds: 0,
        context: None,
    }
}

fn subject_context(subject_id: &str) -> gestalt::Subject {
    gestalt::Subject {
        id: subject_id.to_string(),
        credential_subject_id: subject_id.to_string(),
        ..Default::default()
    }
}

fn request_context(subject_id: &str) -> proto::RequestContext {
    request_context_for_turn(subject_id, "session-1", "")
}

fn request_context_for_turn(
    subject_id: &str,
    session_id: &str,
    turn_id: &str,
) -> proto::RequestContext {
    proto::RequestContext {
        subject: Some(proto::SubjectContext {
            id: subject_id.to_string(),
            credential_subject_id: subject_id.to_string(),
            ..Default::default()
        }),
        caller: Some(proto::ProviderContext {
            kind: "agent".to_string(),
            name: "hermes".to_string(),
        }),
        tool_refs: vec![proto::AgentToolRef {
            app: "linear".to_string(),
            operation: "issues".to_string(),
            connection: "primary".to_string(),
            instance: "workspace-a".to_string(),
            credential_mode: "subject".to_string(),
            ..Default::default()
        }],
        tool_refs_set: true,
        agent: Some(proto::AgentInvocationContext {
            provider_name: "hermes".to_string(),
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
        }),
        ..Default::default()
    }
}

fn service_account_subject(subject_id: &str) -> gestalt::Subject {
    gestalt::Subject {
        id: subject_id.to_string(),
        credential_subject_id: subject_id.to_string(),
        ..Default::default()
    }
}

async fn serve_app_host(socket_path: PathBuf, app: TestAppService) -> tokio::task::JoinHandle<()> {
    let listener = UnixListener::bind(&socket_path).expect("bind app host socket");
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(AppGrpcServer::new(app))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .expect("serve app host");
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    handle
}

async fn wait_for_turn(
    provider: &HermesAgentProvider,
    turn_id: &str,
    status: gestalt::AgentExecutionStatus,
) -> gestalt::AgentTurn {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let turn = provider
            .get_turn(gestalt::GetAgentProviderTurnRequest {
                turn_id: turn_id.to_string(),
                subject: Some(owner_subject()),
            })
            .await
            .unwrap();
        if turn.status == status {
            return turn;
        }
        assert!(
            Instant::now() < deadline,
            "turn {turn_id} did not reach {status:?}; current status {:?} message {:?}",
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
        .list_turn_events(gestalt::ListAgentProviderTurnEventsRequest {
            turn_id: turn_id.to_string(),
            subject: Some(owner_subject()),
            ..Default::default()
        })
        .await
        .unwrap()
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
