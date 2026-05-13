use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use gestalt::AgentProvider as _;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tempfile::TempDir;

use gestalt_agent_hermes::{AgentHostClient, HermesAgentProvider};

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        vec![gestalt::AgentToolSourceMode::McpCatalog]
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
    assert_eq!(turn.output_text, "Hermes says hi");
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
async fn mcp_catalog_turn_bridges_gestalt_tools_to_hermes() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-call");
    let host = TestAgentHostService::default();
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp").await;
    let turn = wait_for_turn(
        &provider,
        "turn-mcp",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn.output_text, "Hermes used Gestalt MCP");

    let list_requests = host.list_requests.lock().expect("list requests").clone();
    assert!(
        !list_requests.is_empty(),
        "expected Hermes to list Gestalt MCP tools"
    );
    assert_eq!(list_requests[0].session_id, "session-1");
    assert_eq!(list_requests[0].turn_id, "turn-mcp");
    assert_eq!(list_requests[0].run_grant, "grant-mcp");
    assert_eq!(list_requests[0].page_size, 100);

    let execute_requests = host
        .execute_requests
        .lock()
        .expect("execute requests")
        .clone();
    assert_eq!(execute_requests.len(), 1);
    assert_eq!(execute_requests[0].session_id, "session-1");
    assert_eq!(execute_requests[0].turn_id, "turn-mcp");
    assert_eq!(execute_requests[0].tool_id, "linear-list");
    assert_eq!(execute_requests[0].tool_call_id, "mcp-1");
    assert_eq!(execute_requests[0].run_grant, "grant-mcp");
    assert_eq!(
        execute_requests[0].idempotency_key,
        "agent/hermes-mcp:turn-mcp:1:linear.issues"
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
}

#[tokio::test]
async fn mcp_catalog_turn_does_not_prefetch_tools_before_mcp_use() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-list-only");
    let host = TestAgentHostService::default();
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp-no-prefetch").await;
    let turn = wait_for_turn(
        &provider,
        "turn-mcp-no-prefetch",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn.output_text, "Hermes used Gestalt MCP");

    let list_requests = host.list_requests.lock().expect("list requests").clone();
    assert!(
        list_requests.is_empty(),
        "MCP bridge should not list Gestalt tools during proxy tools/list: {list_requests:?}"
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
}

#[tokio::test]
async fn mcp_catalog_turn_marks_unavailable_sentinel_call_as_error() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-call");
    let host = TestAgentHostService {
        list_reconnect_sentinel: true,
        execute_status: 424,
        execute_body: Some(r#"{"error":{"code":"reconnect_required","plugin":"linear"}}"#),
        ..Default::default()
    };
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp-sentinel").await;
    let turn = wait_for_turn(
        &provider,
        "turn-mcp-sentinel",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn.output_text, "Hermes used Gestalt MCP");

    let execute_requests = host
        .execute_requests
        .lock()
        .expect("execute requests")
        .clone();
    assert_eq!(execute_requests.len(), 1);
    assert_eq!(execute_requests[0].tool_id, "linear-reconnect");

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
}

#[tokio::test]
async fn mcp_catalog_turn_preserves_empty_target_error_body() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-call");
    let host = TestAgentHostService {
        execute_status: 500,
        execute_body: Some(""),
        ..Default::default()
    };
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp-empty-error").await;
    wait_for_turn(
        &provider,
        "turn-mcp-empty-error",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    let payload = tool_call_payload(&mcp_result["result"]["call"]);
    assert_eq!(
        mcp_result["result"]["call"]["result"]["isError"],
        serde_json::Value::Bool(true)
    );
    assert_eq!(payload["error"]["status"].as_i64(), Some(500));
    assert_eq!(payload["error"]["body"].as_str(), Some(""));
}

#[tokio::test]
async fn mcp_catalog_proxy_gets_schema_by_returned_mcp_name() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-schema");
    let host = TestAgentHostService::default();
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
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
        host.execute_requests
            .lock()
            .expect("execute requests")
            .is_empty()
    );
}

#[tokio::test]
async fn mcp_catalog_proxy_rejects_ambiguous_ref_selectors() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-ambiguous-ref");
    let host = TestAgentHostService {
        ambiguous_refs: true,
        ..Default::default()
    };
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
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
}

#[tokio::test]
async fn mcp_catalog_proxy_rejects_invalid_selectors_before_lookup() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-invalid-selector");
    let host = TestAgentHostService::default();
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
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
}

#[tokio::test]
async fn mcp_catalog_proxy_searches_only_catalog_metadata() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-search-schema-only");
    let host = TestAgentHostService {
        schema_only_tool: true,
        ..Default::default()
    };
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
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
}

#[tokio::test]
async fn mcp_catalog_proxy_ranks_matches_across_pages() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-search-only");
    let host = TestAgentHostService {
        ranked_pages: true,
        ..Default::default()
    };
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
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
}

#[tokio::test]
async fn mcp_catalog_proxy_reports_cursor_and_page_errors_as_tool_errors() {
    let _env_lock = ENV_LOCK.lock().await;
    for (name, host, code) in [
        (
            "repeated",
            TestAgentHostService {
                repeated_cursor: true,
                ..Default::default()
            },
            "repeated_cursor",
        ),
        (
            "page-cap",
            TestAgentHostService {
                endless_pages: true,
                ..Default::default()
            },
            "page_limit_exceeded",
        ),
    ] {
        let fixture = Fixture::new("mcp-search-only");
        let provider = fixture
            .configure_provider_with_agent_host(host.clone())
            .await;

        create_session(&provider).await;
        create_mcp_turn(&provider, &format!("turn-mcp-{name}")).await;
        wait_for_turn(
            &provider,
            &format!("turn-mcp-{name}"),
            gestalt::AgentExecutionStatus::Succeeded,
        )
        .await;

        let log = fixture.log_events();
        let mcp_result = log
            .iter()
            .find(|event| event["event"] == "mcp_result")
            .expect("mcp result logged");
        assert_proxy_error(&mcp_result["result"]["search"], code);
    }
}

#[tokio::test]
async fn mcp_catalog_proxy_reports_list_rpc_errors_as_tool_errors() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-search-only");
    let host = TestAgentHostService {
        list_error: Some("catalog unavailable"),
        ..Default::default()
    };
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
    create_mcp_turn(&provider, "turn-mcp-list-error").await;
    wait_for_turn(
        &provider,
        "turn-mcp-list-error",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let mcp_result = log
        .iter()
        .find(|event| event["event"] == "mcp_result")
        .expect("mcp result logged");
    let payload = assert_proxy_error(&mcp_result["result"]["search"], "list_tools_failed");
    assert!(
        payload["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("catalog unavailable"),
        "{payload}"
    );
}

#[tokio::test]
async fn mcp_catalog_proxy_reports_invalid_catalog_tools_as_tool_errors() {
    let _env_lock = ENV_LOCK.lock().await;
    for (name, host) in [
        (
            "unsafe-name",
            TestAgentHostService {
                unsafe_mcp_name: true,
                ..Default::default()
            },
        ),
        (
            "missing-id",
            TestAgentHostService {
                missing_tool_id: true,
                ..Default::default()
            },
        ),
        (
            "duplicate-name",
            TestAgentHostService {
                duplicate_mcp_name: true,
                ..Default::default()
            },
        ),
    ] {
        let fixture = Fixture::new("mcp-search-only");
        let provider = fixture
            .configure_provider_with_agent_host(host.clone())
            .await;

        create_session(&provider).await;
        create_mcp_turn(&provider, &format!("turn-mcp-{name}")).await;
        wait_for_turn(
            &provider,
            &format!("turn-mcp-{name}"),
            gestalt::AgentExecutionStatus::Succeeded,
        )
        .await;

        let log = fixture.log_events();
        let mcp_result = log
            .iter()
            .find(|event| event["event"] == "mcp_result")
            .expect("mcp result logged");
        assert_proxy_error(&mcp_result["result"]["search"], "invalid_catalog_tool");
    }
}

#[tokio::test]
async fn mcp_catalog_proxy_reports_input_cap_errors_without_listing_tools() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-input-caps");
    let host = TestAgentHostService::default();
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
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
    assert!(host.list_requests.lock().expect("list requests").is_empty());
}

#[tokio::test]
async fn mcp_catalog_proxy_reports_execute_rpc_errors_as_tool_errors() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-call");
    let host = TestAgentHostService {
        execute_error: Some("agent host execute failed"),
        ..Default::default()
    };
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
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
    let payload = assert_proxy_error(&mcp_result["result"]["call"], "execute_tool_failed");
    assert!(
        payload["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("agent host execute failed"),
        "{payload}"
    );
}

#[tokio::test]
async fn mcp_catalog_does_not_require_advertised_acp_http_mcp_support() {
    let _env_lock = ENV_LOCK.lock().await;
    let fixture = Fixture::new("mcp-call-no-cap");
    let host = TestAgentHostService::default();
    let provider = fixture
        .configure_provider_with_agent_host(host.clone())
        .await;

    create_session(&provider).await;
    create_mcp_turn(&provider, "turn-no-cap").await;
    let turn = wait_for_turn(
        &provider,
        "turn-no-cap",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;
    assert_eq!(turn.output_text, "Hermes used Gestalt MCP");
    let log = fixture.log_events();
    assert!(log.iter().any(|event| event["event"] == "mcp_result"));
}

#[tokio::test]
async fn explicit_no_tool_turn_allows_run_grant_without_mcp_servers() {
    let fixture = Fixture::new("success");
    let provider = fixture.configure_provider().await;

    create_session(&provider).await;
    provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: "turn-no-tools-with-grant".to_string(),
            session_id: "session-1".to_string(),
            run_grant: "grant-no-tools".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "say hi".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    wait_for_turn(
        &provider,
        "turn-no-tools-with-grant",
        gestalt::AgentExecutionStatus::Succeeded,
    )
    .await;

    let log = fixture.log_events();
    let load = log
        .iter()
        .find(|event| event["event"] == "load")
        .expect("session/load logged");
    assert_eq!(
        load["params"]["mcpServers"].as_array().map(Vec::len),
        Some(0),
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
            tools: vec![gestalt::ResolvedAgentTool {
                id: "tool-1".to_string(),
                name: "tool".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(400));

    let err = provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: "turn-missing-grant".to_string(),
            session_id: "session-1".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "hi".to_string(),
                ..Default::default()
            }],
            tool_source: gestalt::AgentToolSourceMode::McpCatalog,
            tool_refs: vec![gestalt::AgentToolRef {
                plugin: "*".to_string(),
                ..Default::default()
            }],
            ..Default::default()
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
            response_schema: Some(json!({ "type": "object" })),
            ..Default::default()
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
            model_options: Some(json!({ "type": "object" })),
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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

#[derive(Clone, Default)]
struct TestAgentHostService {
    list_requests: Arc<StdMutex<Vec<gestalt::AgentHostListToolsInput>>>,
    execute_requests: Arc<StdMutex<Vec<gestalt::AgentHostExecuteToolInput>>>,
    list_reconnect_sentinel: bool,
    list_error: Option<&'static str>,
    repeated_cursor: bool,
    endless_pages: bool,
    ambiguous_refs: bool,
    unsafe_mcp_name: bool,
    missing_tool_id: bool,
    duplicate_mcp_name: bool,
    schema_only_tool: bool,
    ranked_pages: bool,
    execute_status: i32,
    execute_body: Option<&'static str>,
    execute_error: Option<&'static str>,
}

#[gestalt::async_trait]
impl AgentHostClient for TestAgentHostService {
    async fn list_tools_for_turn(
        &self,
        request: gestalt::AgentHostListToolsInput,
    ) -> Result<gestalt::ListAgentToolsResponse, String> {
        self.list_requests
            .lock()
            .expect("list requests")
            .push(request.clone());
        if let Some(message) = self.list_error {
            return Err(message.to_string());
        }
        if self.repeated_cursor {
            return Ok(gestalt::ListAgentToolsResponse {
                tools: Vec::new(),
                next_page_token: "same-cursor".to_string(),
            });
        }
        if self.endless_pages {
            let count = self.list_requests.lock().expect("list requests").len();
            return Ok(gestalt::ListAgentToolsResponse {
                tools: Vec::new(),
                next_page_token: format!("page-{count}"),
            });
        }
        if self.ranked_pages {
            if request.page_token.trim().is_empty() {
                let tools = (0..20)
                    .map(|index| gestalt::ListedAgentTool {
                        id: format!("linear-partial-{index}"),
                        mcp_name: format!("linear.partial.{index}"),
                        title: "Linear catalog entry".to_string(),
                        description: "Catalog entry visible to the user".to_string(),
                        input_schema:
                            r#"{"type":"object","properties":{"query":{"type":"string"}}}"#
                                .to_string(),
                        r#ref: Some(gestalt::AgentToolRef {
                            plugin: "linear".to_string(),
                            operation: format!("partial-{index}"),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                    .collect();
                return Ok(gestalt::ListAgentToolsResponse {
                    tools,
                    next_page_token: "page-2".to_string(),
                });
            }
            return Ok(gestalt::ListAgentToolsResponse {
                tools: vec![listed_tool(
                    "linear-best",
                    "linear.issues.best",
                    "Linear issues",
                )],
                next_page_token: String::new(),
            });
        }
        Ok(gestalt::ListAgentToolsResponse {
            tools: if request.page_token.trim().is_empty() {
                if self.list_reconnect_sentinel {
                    vec![gestalt::ListedAgentTool {
                        id: "linear-reconnect".to_string(),
                        mcp_name: "linear__reconnect_required".to_string(),
                        title: "linear reconnect required".to_string(),
                        description: "linear credentials expired or refresh failed".to_string(),
                        input_schema:
                            r#"{"type":"object","properties":{},"additionalProperties":false}"#
                                .to_string(),
                        annotations: Some(gestalt::AgentToolAnnotations {
                            read_only_hint: Some(true),
                            open_world_hint: Some(false),
                            ..Default::default()
                        }),
                        r#ref: Some(gestalt::AgentToolRef {
                            plugin: "linear".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]
                } else if self.ambiguous_refs {
                    vec![
                        listed_tool("linear-list-a", "linear.issues", "Linear issues A"),
                        listed_tool(
                            "linear-list-b",
                            "linear.issues.secondary",
                            "Linear issues B",
                        ),
                    ]
                } else if self.unsafe_mcp_name {
                    vec![gestalt::ListedAgentTool {
                        id: "unsafe".to_string(),
                        mcp_name: "unsafe tool".to_string(),
                        title: "Unsafe tool".to_string(),
                        description: "Unsafe MCP name".to_string(),
                        input_schema: r#"{"type":"object"}"#.to_string(),
                        r#ref: Some(gestalt::AgentToolRef {
                            plugin: "linear".to_string(),
                            operation: "issues".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]
                } else if self.missing_tool_id {
                    let mut tool = listed_tool("", "linear.issues", "Linear issues");
                    tool.id.clear();
                    vec![tool]
                } else if self.duplicate_mcp_name {
                    vec![
                        listed_tool("linear-list-a", "linear.issues", "Linear issues A"),
                        listed_tool("linear-list-b", "linear.issues", "Linear issues B"),
                    ]
                } else if self.schema_only_tool {
                    vec![gestalt::ListedAgentTool {
                        id: "schema-only".to_string(),
                        mcp_name: "neutral.tool".to_string(),
                        title: "Neutral tool".to_string(),
                        description: "No matching metadata".to_string(),
                        input_schema:
                            r#"{"type":"object","properties":{"schemaOnlySecret":{"type":"string"}}}"#
                                .to_string(),
                        r#ref: Some(gestalt::AgentToolRef {
                            plugin: "neutral".to_string(),
                            operation: "lookup".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]
                } else {
                    vec![gestalt::ListedAgentTool {
                        id: "linear-list".to_string(),
                        mcp_name: "linear.issues".to_string(),
                        title: "Linear issues".to_string(),
                        description: "List Linear issues visible to the user".to_string(),
                        input_schema:
                            r#"{"type":"object","properties":{"query":{"type":"string"}}}"#
                                .to_string(),
                        annotations: Some(gestalt::AgentToolAnnotations {
                            read_only_hint: Some(true),
                            open_world_hint: Some(false),
                            ..Default::default()
                        }),
                        r#ref: Some(gestalt::AgentToolRef {
                            plugin: "linear".to_string(),
                            operation: "issues".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]
                }
            } else {
                Vec::new()
            },
            next_page_token: String::new(),
        })
    }

    async fn execute_tool_for_turn(
        &self,
        request: gestalt::AgentHostExecuteToolInput,
    ) -> Result<gestalt::ExecuteAgentToolResponse, String> {
        self.execute_requests
            .lock()
            .expect("execute requests")
            .push(request);
        if let Some(message) = self.execute_error {
            return Err(message.to_string());
        }
        Ok(gestalt::ExecuteAgentToolResponse {
            status: if self.execute_status == 0 {
                200
            } else {
                self.execute_status
            },
            body: self.execute_body.unwrap_or("linear tickets").to_string(),
        })
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
            plugin: "linear".to_string(),
            operation: "issues".to_string(),
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

    async fn configure_provider_with_agent_host(
        &self,
        host: TestAgentHostService,
    ) -> HermesAgentProvider {
        self.configure_provider_inner(true, Some(Arc::new(host)))
            .await
    }

    async fn configure_provider_with_model_switching(
        &self,
        model_switching_enabled: bool,
    ) -> HermesAgentProvider {
        self.configure_provider_inner(model_switching_enabled, None)
            .await
    }

    async fn configure_provider_inner(
        &self,
        model_switching_enabled: bool,
        host: Option<Arc<dyn AgentHostClient>>,
    ) -> HermesAgentProvider {
        let provider = match host {
            Some(host) => HermesAgentProvider::with_agent_host_for_tests(host),
            None => HermesAgentProvider::default(),
        };
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
    provider
        .create_session(gestalt::CreateAgentProviderSessionRequest {
            session_id: "session-1".to_string(),
            model: "kimi-k2.6".to_string(),
            created_by: Some(gestalt::AgentActor {
                subject_id: "user-1".to_string(),
                subject_kind: "human".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap()
}

async fn create_turn(provider: &HermesAgentProvider, turn_id: &str) -> gestalt::AgentTurn {
    provider
        .create_turn(gestalt::CreateAgentProviderTurnRequest {
            turn_id: turn_id.to_string(),
            session_id: "session-1".to_string(),
            messages: vec![gestalt::AgentMessage {
                role: "user".to_string(),
                text: "say hi".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap()
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
            tool_source: gestalt::AgentToolSourceMode::McpCatalog,
            tool_refs: vec![gestalt::AgentToolRef {
                plugin: "*".to_string(),
                ..Default::default()
            }],
            run_grant: "grant-mcp".to_string(),
            ..Default::default()
        })
        .await
        .unwrap()
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
                ..Default::default()
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
