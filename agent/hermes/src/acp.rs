use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value as JsonValue, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time;

use crate::config::HermesConfig;

#[derive(Clone, Debug)]
pub enum AcpNotification {
    SessionUpdate(JsonValue),
    ProtocolError(String),
    ChildExited(String),
}

#[derive(Clone)]
pub struct AcpProcess {
    inner: Arc<AcpProcessInner>,
}

#[derive(Clone, Debug, Default)]
pub struct AcpInitializeResult;

struct AcpProcessInner {
    writer: Arc<Mutex<ChildStdin>>,
    child: Mutex<Option<Child>>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Result<JsonValue, String>>>>>,
    next_id: AtomicU64,
    notifications: Mutex<mpsc::Receiver<AcpNotification>>,
    stderr: Arc<Mutex<String>>,
}

impl AcpProcess {
    pub async fn spawn(config: &HermesConfig, token: Option<&str>) -> Result<Self, String> {
        let mut command = Command::new(&config.hermes_command);
        command
            .args(&config.hermes_args)
            .current_dir(&config.working_directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in &config.extra_env {
            command.env(key, value);
        }
        command.env("HERMES_HOME", &config.hermes_home);
        if let Some(token) = token {
            command.env(&config.access_token_env_var, token);
        }

        let mut child = command.spawn().map_err(|err| {
            format!(
                "spawn Hermes ACP command `{}`: {err}",
                config.hermes_command
            )
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "Hermes ACP stdin was not available".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "Hermes ACP stdout was not available".to_string())?;
        let stderr = child.stderr.take();

        let (notifications_tx, notifications_rx) = mpsc::channel(256);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let writer = Arc::new(Mutex::new(stdin));
        let stderr_buffer = Arc::new(Mutex::new(String::new()));
        let process = Self {
            inner: Arc::new(AcpProcessInner {
                writer,
                child: Mutex::new(Some(child)),
                pending,
                next_id: AtomicU64::new(1),
                notifications: Mutex::new(notifications_rx),
                stderr: stderr_buffer.clone(),
            }),
        };

        spawn_stdout_reader(
            stdout,
            process.inner.pending.clone(),
            process.inner.writer.clone(),
            notifications_tx.clone(),
        );
        if let Some(stderr) = stderr {
            spawn_stderr_reader(stderr, stderr_buffer);
        }

        Ok(process)
    }

    pub async fn stderr(&self) -> String {
        self.inner.stderr.lock().await.clone()
    }

    pub async fn kill(&self) {
        if let Some(mut child) = self.inner.child.lock().await.take() {
            let _ = child.kill().await;
        }
        self.fail_all_pending("Hermes ACP process was killed").await;
    }

    pub async fn initialize(
        &self,
        timeout: std::time::Duration,
    ) -> Result<AcpInitializeResult, String> {
        let result = self
            .request(
                "initialize",
                json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "auth": { "terminal": false },
                        "fs": { "readTextFile": false, "writeTextFile": false },
                        "terminal": false
                    },
                    "clientInfo": {
                        "name": "gestalt-agent-hermes",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
                timeout,
            )
            .await?;
        let _ = result;
        Ok(AcpInitializeResult)
    }

    pub async fn new_session(
        &self,
        cwd: &str,
        mcp_servers: Vec<JsonValue>,
        timeout: std::time::Duration,
    ) -> Result<String, String> {
        let result = self
            .request(
                "session/new",
                json!({
                    "cwd": cwd,
                    "mcpServers": mcp_servers
                }),
                timeout,
            )
            .await?;
        session_id_from_result(&result)
    }

    pub async fn load_session(
        &self,
        cwd: &str,
        session_id: &str,
        mcp_servers: Vec<JsonValue>,
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        self.request(
            "session/load",
            json!({
                "cwd": cwd,
                "sessionId": session_id,
                "mcpServers": mcp_servers
            }),
            timeout,
        )
        .await
        .map(|_| ())
    }

    pub async fn set_model(
        &self,
        session_id: &str,
        model: &str,
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        if model.trim().is_empty() {
            return Ok(());
        }
        self.request(
            "session/set_model",
            json!({
                "sessionId": session_id,
                "modelId": model
            }),
            timeout,
        )
        .await
        .map(|_| ())
    }

    pub async fn prompt(
        &self,
        session_id: &str,
        prompt: String,
        timeout: std::time::Duration,
    ) -> Result<JsonValue, String> {
        self.request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }),
            timeout,
        )
        .await
    }

    pub async fn cancel(&self, session_id: &str) {
        let _ = self
            .request(
                "session/cancel",
                json!({
                    "sessionId": session_id
                }),
                std::time::Duration::from_secs(2),
            )
            .await;
    }

    pub async fn next_notification(&self) -> Option<AcpNotification> {
        self.inner.notifications.lock().await.recv().await
    }

    async fn request(
        &self,
        method: &str,
        params: JsonValue,
        timeout: std::time::Duration,
    ) -> Result<JsonValue, String> {
        let id = self
            .inner
            .next_id
            .fetch_add(1, Ordering::Relaxed)
            .to_string();
        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().await.insert(id.clone(), tx);
        let payload = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });
        if let Err(err) = write_json_line(&self.inner.writer, &payload).await {
            let _ = self.inner.pending.lock().await.remove(&id);
            return Err(err);
        }
        time::timeout(timeout, rx)
            .await
            .map_err(|_| format!("ACP request {method} timed out"))?
            .map_err(|_| format!("ACP request {method} response channel closed"))?
    }

    async fn fail_all_pending(&self, message: &str) {
        let pending: Vec<oneshot::Sender<Result<JsonValue, String>>> = self
            .inner
            .pending
            .lock()
            .await
            .drain()
            .map(|(_, tx)| tx)
            .collect();
        for tx in pending {
            let _ = tx.send(Err(message.to_string()));
        }
    }
}

fn spawn_stdout_reader(
    stdout: tokio::process::ChildStdout,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Result<JsonValue, String>>>>>,
    writer: Arc<Mutex<ChildStdin>>,
    notifications_tx: mpsc::Sender<AcpNotification>,
) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let message: JsonValue = match serde_json::from_str(&line) {
                Ok(value) => value,
                Err(err) => {
                    let _ = notifications_tx
                        .send(AcpNotification::ProtocolError(format!(
                            "invalid JSON from Hermes ACP: {err}"
                        )))
                        .await;
                    fail_pending(&pending, "invalid JSON from Hermes ACP").await;
                    return;
                }
            };
            if message.get("method").is_some() && message.get("id").is_some() {
                handle_server_request(&writer, message).await;
                continue;
            }
            if let Some(id) = message.get("id") {
                let key = id_to_key(id);
                if let Some(tx) = pending.lock().await.remove(&key) {
                    let result = if let Some(error) = message.get("error") {
                        Err(format!("ACP error response: {error}"))
                    } else {
                        Ok(message.get("result").cloned().unwrap_or(JsonValue::Null))
                    };
                    let _ = tx.send(result);
                }
                continue;
            }
            if message.get("method").and_then(JsonValue::as_str) == Some("session/update") {
                let params = message.get("params").cloned().unwrap_or(JsonValue::Null);
                let _ = notifications_tx
                    .send(AcpNotification::SessionUpdate(params))
                    .await;
            }
        }
        let _ = notifications_tx
            .send(AcpNotification::ChildExited(
                "Hermes ACP stdout closed".to_string(),
            ))
            .await;
        fail_pending(&pending, "Hermes ACP stdout closed").await;
    });
}

fn spawn_stderr_reader(stderr: tokio::process::ChildStderr, stderr_buffer: Arc<Mutex<String>>) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut buffer = stderr_buffer.lock().await;
            if buffer.len() < 16_384 {
                buffer.push_str(&line);
                buffer.push('\n');
            }
        }
    });
}

async fn handle_server_request(writer: &Arc<Mutex<ChildStdin>>, message: JsonValue) {
    let id = message.get("id").cloned().unwrap_or(JsonValue::Null);
    match message.get("method").and_then(JsonValue::as_str) {
        Some("session/request_permission") => {
            let result = permission_response(message.get("params").unwrap_or(&JsonValue::Null));
            let _ = write_json_line(
                writer,
                &json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": result
                }),
            )
            .await;
        }
        _ => {
            let _ = write_json_line(
                writer,
                &json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {
                        "code": -32601,
                        "message": "method not found"
                    }
                }),
            )
            .await;
        }
    }
}

fn permission_response(params: &JsonValue) -> JsonValue {
    let options = params
        .get("options")
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    let selected = options
        .iter()
        .find(|option| option.get("kind").and_then(JsonValue::as_str) == Some("allow_always"))
        .or_else(|| {
            options
                .iter()
                .find(|option| option.get("kind").and_then(JsonValue::as_str) == Some("allow_once"))
        })
        .or_else(|| options.first());
    if let Some(option) = selected {
        if let Some(option_id) = option.get("optionId").and_then(JsonValue::as_str) {
            return json!({
                "outcome": {
                    "outcome": "selected",
                    "optionId": option_id
                }
            });
        }
    }
    json!({
        "outcome": {
            "outcome": "cancelled"
        }
    })
}

async fn write_json_line(
    writer: &Arc<Mutex<ChildStdin>>,
    payload: &JsonValue,
) -> Result<(), String> {
    let mut writer = writer.lock().await;
    let mut serialized =
        serde_json::to_vec(payload).map_err(|err| format!("serialize JSON-RPC payload: {err}"))?;
    serialized.push(b'\n');
    writer
        .write_all(&serialized)
        .await
        .map_err(|err| format!("write Hermes ACP stdin: {err}"))?;
    writer
        .flush()
        .await
        .map_err(|err| format!("flush Hermes ACP stdin: {err}"))
}

async fn fail_pending(
    pending: &Arc<Mutex<HashMap<String, oneshot::Sender<Result<JsonValue, String>>>>>,
    message: &str,
) {
    let senders: Vec<_> = pending.lock().await.drain().map(|(_, tx)| tx).collect();
    for tx in senders {
        let _ = tx.send(Err(message.to_string()));
    }
}

fn id_to_key(id: &JsonValue) -> String {
    match id {
        JsonValue::String(value) => value.clone(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::Null => "null".to_string(),
        other => other.to_string(),
    }
}

fn session_id_from_result(result: &JsonValue) -> Result<String, String> {
    result
        .get("sessionId")
        .or_else(|| result.get("session_id"))
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("ACP session/new response did not contain sessionId: {result}"))
}
