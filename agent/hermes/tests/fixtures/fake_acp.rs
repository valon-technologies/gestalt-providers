use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};

use serde_json::{Value, json};

fn main() {
    let mode = env::var("FAKE_ACP_MODE").unwrap_or_else(|_| "success".to_string());
    log_event(json!({
        "event": "start",
        "token": env::var("OPENAI_API_KEY").unwrap_or_default(),
        "hermesHome": env::var("HERMES_HOME").unwrap_or_default(),
    }));

    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    while let Some(Ok(line)) = lines.next() {
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if request.get("result").is_some() || request.get("error").is_some() {
            log_event(json!({"event": "client_response", "message": request}));
            continue;
        }
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match method {
            "initialize" => respond(
                &request,
                json!({"protocolVersion": 1, "agentCapabilities": {"loadSession": true}}),
            ),
            "session/new" => {
                log_event(json!({"event": "new", "params": request.get("params")}));
                respond(&request, json!({"sessionId": "acp-session-1"}));
            }
            "session/load" => {
                log_event(json!({"event": "load", "params": request.get("params")}));
                respond(&request, json!({}));
            }
            "session/set_model" => {
                log_event(json!({"event": "set_model", "params": request.get("params")}));
                respond(&request, json!({}));
            }
            "session/cancel" => {
                log_event(json!({"event": "cancel", "params": request.get("params")}));
                respond(&request, json!({}));
            }
            "session/prompt" => {
                log_event(json!({"event": "prompt", "params": request.get("params")}));
                if mode == "permission" {
                    request_permission();
                    if let Some(Ok(response)) = lines.next() {
                        let response: Value =
                            serde_json::from_str(&response).unwrap_or(Value::Null);
                        log_event(json!({"event": "permission_response", "message": response}));
                    }
                }
                if mode == "hang" {
                    while let Some(Ok(cancel_line)) = lines.next() {
                        let cancel_request: Value =
                            serde_json::from_str(&cancel_line).unwrap_or(Value::Null);
                        if cancel_request.get("method").and_then(Value::as_str)
                            == Some("session/cancel")
                        {
                            log_event(
                                json!({"event": "cancel", "params": cancel_request.get("params")}),
                            );
                            respond(&cancel_request, json!({}));
                            respond(&request, json!({"stopReason": "cancelled"}));
                            break;
                        }
                    }
                    continue;
                }
                if mode == "stderr-fail" {
                    update("agent_thought_chunk", "thinking");
                    eprintln!("Non-retryable client error: Error code: 403 - permission denied");
                    respond(&request, json!({"stopReason": "end_turn"}));
                    continue;
                }
                update("agent_thought_chunk", "thinking");
                update("agent_message_chunk", "Hermes says hi");
                tool_update("tool-call-1", "completed");
                if mode == "wrong-session" {
                    update_for_session("other-session", "agent_message_chunk", "ignored");
                }
                respond(&request, json!({"stopReason": "end_turn"}));
            }
            _ => respond_error(&request, -32601, "method not found"),
        }
    }
}

fn request_permission() {
    emit(json!({
        "jsonrpc": "2.0",
        "id": "permission-1",
        "method": "session/request_permission",
        "params": {
            "sessionId": "acp-session-1",
            "options": [
                {"kind": "allow_always", "optionId": "allow", "name": "Allow"}
            ],
            "toolCall": {
                "sessionUpdate": "tool_call",
                "toolCallId": "tool-call-1",
                "title": "Fake tool"
            }
        }
    }));
}

fn update(kind: &str, text: &str) {
    update_for_session("acp-session-1", kind, text);
}

fn update_for_session(session_id: &str, kind: &str, text: &str) {
    emit(json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": session_id,
            "update": {
                "sessionUpdate": kind,
                "content": {"type": "text", "text": text}
            }
        }
    }));
}

fn tool_update(tool_call_id: &str, status: &str) {
    emit(json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": "acp-session-1",
            "update": {
                "sessionUpdate": "tool_call_update",
                "toolCallId": tool_call_id,
                "title": "Fake tool",
                "status": status,
                "rawInput": {"ok": true},
                "rawOutput": {"done": true}
            }
        }
    }));
}

fn respond(request: &Value, result: Value) {
    emit(json!({
        "jsonrpc": "2.0",
        "id": request.get("id").cloned().unwrap_or(Value::Null),
        "result": result
    }));
}

fn respond_error(request: &Value, code: i64, message: &str) {
    emit(json!({
        "jsonrpc": "2.0",
        "id": request.get("id").cloned().unwrap_or(Value::Null),
        "error": {"code": code, "message": message}
    }));
}

fn emit(value: Value) {
    println!("{}", serde_json::to_string(&value).expect("serialize"));
    io::stdout().flush().expect("flush stdout");
}

fn log_event(value: Value) {
    let Ok(path) = env::var("FAKE_ACP_LOG") else {
        return;
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open fake acp log");
    writeln!(
        file,
        "{}",
        serde_json::to_string(&value).expect("serialize")
    )
    .expect("write fake acp log");
}
