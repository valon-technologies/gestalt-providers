use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Read, Write};
use std::net::TcpStream;

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
    let mut mcp_server: Option<Value> = None;
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
            "initialize" => {
                let mut agent_capabilities = json!({
                    "loadSession": true,
                    "mcpCapabilities": {
                        "http": true,
                        "sse": false
                    }
                });
                if mode == "mcp-call-no-cap" {
                    agent_capabilities
                        .as_object_mut()
                        .expect("agent capabilities object")
                        .remove("mcpCapabilities");
                }
                respond(
                    &request,
                    json!({
                        "protocolVersion": 1,
                        "agentCapabilities": agent_capabilities
                    }),
                )
            }
            "session/new" => {
                log_event(json!({"event": "new", "params": request.get("params")}));
                respond(&request, json!({"sessionId": "acp-session-1"}));
            }
            "session/load" => {
                log_event(json!({"event": "load", "params": request.get("params")}));
                mcp_server = request
                    .get("params")
                    .and_then(|params| params.get("mcpServers"))
                    .and_then(Value::as_array)
                    .and_then(|servers| servers.first())
                    .cloned();
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
                if mode == "mcp-call" || mode == "mcp-call-no-cap" {
                    match exercise_mcp_server(mcp_server.as_ref()) {
                        Ok(result) => {
                            log_event(json!({"event": "mcp_result", "result": result}));
                            update("agent_message_chunk", "Hermes used Gestalt MCP");
                            tool_update("mcp-call-1", "completed");
                        }
                        Err(err) => {
                            log_event(json!({"event": "mcp_error", "message": err}));
                            tool_update("mcp-call-1", "failed");
                        }
                    }
                    respond(&request, json!({"stopReason": "end_turn"}));
                    continue;
                }
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

fn exercise_mcp_server(server: Option<&Value>) -> Result<Value, String> {
    let server = server.ok_or_else(|| "missing MCP server".to_string())?;
    let url = server
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing MCP server url".to_string())?;
    let auth_header = server
        .get("headers")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|headers| headers.iter())
        .find(|header| header.get("name").and_then(Value::as_str) == Some("Authorization"))
        .and_then(|header| header.get("value").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    let initialize_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "fake-acp", "version": "1.0"}
        }
    });
    let unauthorized_status = mcp_post_status(url, "Bearer wrong-token", initialize_body.clone())?;
    let initialize = mcp_post(url, &auth_header, initialize_body)?;
    let listed = mcp_post(
        url,
        &auth_header,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }),
    )?;
    let tool_name = listed
        .pointer("/result/tools/0/name")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("no MCP tool in list response: {listed}"))?;
    let called = mcp_post(
        url,
        &auth_header,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": {"query": "Ada Lovelace"}
            }
        }),
    )?;
    Ok(json!({
        "initialize": initialize,
        "unauthorizedStatus": unauthorized_status,
        "list": listed,
        "call": called
    }))
}

fn mcp_post(url: &str, auth_header: &str, body: Value) -> Result<Value, String> {
    let (status, body) = mcp_post_raw(url, auth_header, body)?;
    if status != 200 {
        return Err(format!(
            "MCP HTTP request failed with status {status}; body: {body}"
        ));
    }
    serde_json::from_str(&body).map_err(|err| format!("decode MCP response {body:?}: {err}"))
}

fn mcp_post_status(url: &str, auth_header: &str, body: Value) -> Result<u16, String> {
    let (status, _) = mcp_post_raw(url, auth_header, body)?;
    Ok(status)
}

fn mcp_post_raw(url: &str, auth_header: &str, body: Value) -> Result<(u16, String), String> {
    let (host, port, path) = parse_http_url(url)?;
    let serialized = serde_json::to_string(&body).map_err(|err| err.to_string())?;
    let mut stream = TcpStream::connect((host.as_str(), port))
        .map_err(|err| format!("connect MCP server {url}: {err}"))?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nAuthorization: {auth_header}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nMcp-Protocol-Version: 2025-06-18\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{serialized}",
        serialized.len()
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|err| format!("write MCP request: {err}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|err| format!("read MCP response: {err}"))?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("invalid MCP HTTP response: {response}"))?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|raw| raw.parse::<u16>().ok())
        .ok_or_else(|| format!("invalid MCP HTTP status line: {headers}"))?;
    Ok((status, body.to_string()))
}

fn parse_http_url(url: &str) -> Result<(String, u16, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("only http MCP URLs are supported in fake ACP: {url}"))?;
    let (authority, path) = rest
        .split_once('/')
        .ok_or_else(|| format!("MCP URL is missing path: {url}"))?;
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| format!("MCP URL is missing port: {url}"))?;
    let port = port
        .parse::<u16>()
        .map_err(|err| format!("invalid MCP URL port {port:?}: {err}"))?;
    Ok((host.to_string(), port, format!("/{path}")))
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
