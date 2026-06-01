use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::process::Command;
use tokio::time;

const DEFAULT_HERMES_COMMAND: &str = "hermes";
const DEFAULT_TIMEOUT_SECONDS: f64 = 600.0;
const TOKEN_COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
const MIN_HERMES_VERSION: (u64, u64, u64) = (0, 12, 0);

#[derive(Clone, Debug)]
pub struct HermesConfig {
    pub hermes_home: PathBuf,
    pub hermes_command: String,
    pub hermes_args: Vec<String>,
    pub working_directory: PathBuf,
    pub default_model: String,
    pub model_switching_enabled: bool,
    pub timeout: Duration,
    pub access_token_command: Vec<String>,
}

impl HermesConfig {
    pub fn from_json(raw: JsonMap<String, JsonValue>) -> Result<Self, String> {
        if raw
            .get("autoApprovePermissions")
            .and_then(JsonValue::as_bool)
            .is_some_and(|value| !value)
        {
            return Err("autoApprovePermissions must be true".to_string());
        }

        let hermes_home = trimmed_text(raw.get("hermesHome"))
            .ok_or_else(|| "hermesHome is required".to_string())
            .map(PathBuf::from)?;
        if !hermes_home.is_dir() {
            return Err("hermesHome must be an existing directory".to_string());
        }

        let working_directory = match trimmed_text(raw.get("workingDirectory")) {
            Some(value) => {
                let path = PathBuf::from(value);
                if !path.is_dir() {
                    return Err("workingDirectory must be an existing directory".to_string());
                }
                path
            }
            None => std::env::current_dir()
                .map_err(|err| format!("resolve current working directory: {err}"))?,
        };

        let timeout_seconds = match raw.get("timeoutSeconds") {
            Some(JsonValue::Number(value)) => value
                .as_f64()
                .ok_or_else(|| "timeoutSeconds must be a number".to_string())?,
            Some(JsonValue::String(value)) if !value.trim().is_empty() => value
                .trim()
                .parse::<f64>()
                .map_err(|_| "timeoutSeconds must be a number".to_string())?,
            Some(JsonValue::Null) | None => DEFAULT_TIMEOUT_SECONDS,
            _ => return Err("timeoutSeconds must be a number".to_string()),
        };
        if timeout_seconds <= 0.0 {
            return Err("timeoutSeconds must be positive".to_string());
        }

        let access_token_command = string_list(
            raw.get("accessTokenCommand"),
            &[
                "gcloud",
                "auth",
                "application-default",
                "print-access-token",
            ],
            "accessTokenCommand",
        )?;
        if access_token_command.is_empty() || access_token_command[0].trim().is_empty() {
            return Err("accessTokenCommand must start with a command".to_string());
        }

        Ok(Self {
            hermes_home,
            hermes_command: trimmed_text(raw.get("hermesCommand"))
                .unwrap_or_else(|| DEFAULT_HERMES_COMMAND.to_string()),
            hermes_args: string_list(
                raw.get("hermesArgs"),
                &["acp", "--accept-hooks"],
                "hermesArgs",
            )?,
            working_directory,
            default_model: trimmed_text(raw.get("defaultModel")).unwrap_or_default(),
            model_switching_enabled: optional_bool(raw.get("modelSwitchingEnabled"))?
                .unwrap_or(true),
            timeout: Duration::from_secs_f64(timeout_seconds),
            access_token_command,
        })
    }

    pub fn resolve_model(&self, requested: &str) -> String {
        let requested = requested.trim();
        if requested.is_empty() {
            self.default_model.clone()
        } else {
            requested.to_string()
        }
    }

    pub fn should_set_model(&self, model: &str) -> bool {
        self.model_switching_enabled && !model.trim().is_empty()
    }

    pub async fn fresh_access_token(&self) -> Result<String, String> {
        let mut command = Command::new(&self.access_token_command[0]);
        command
            .args(&self.access_token_command[1..])
            .current_dir(&self.working_directory)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = time::timeout(TOKEN_COMMAND_TIMEOUT, command.output())
            .await
            .map_err(|_| "accessTokenCommand timed out".to_string())?
            .map_err(|err| format!("run accessTokenCommand: {err}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let suffix = if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            };
            return Err(format!(
                "accessTokenCommand exited with status {}{suffix}",
                output.status
            ));
        }
        let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if token.is_empty() {
            return Err("accessTokenCommand produced empty stdout".to_string());
        }
        Ok(token)
    }

    pub async fn hermes_version_warning(&self) -> Option<String> {
        let output = time::timeout(
            Duration::from_secs(5),
            Command::new(&self.hermes_command)
                .arg("--version")
                .current_dir(&self.working_directory)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output(),
        )
        .await
        .ok()?
        .ok()?;
        if !output.status.success() {
            return Some(format!(
                "unable to check Hermes version with `{}`",
                self.hermes_command
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let version = parse_hermes_version(&stdout)?;
        if version < MIN_HERMES_VERSION {
            Some(format!(
                "Hermes Agent v{}.{}.{} or newer is expected; found v{}.{}.{}",
                MIN_HERMES_VERSION.0,
                MIN_HERMES_VERSION.1,
                MIN_HERMES_VERSION.2,
                version.0,
                version.1,
                version.2
            ))
        } else {
            None
        }
    }
}

fn trimmed_text(raw: Option<&JsonValue>) -> Option<String> {
    let value = raw?.as_str()?.trim().to_string();
    if value.is_empty() { None } else { Some(value) }
}

fn optional_bool(raw: Option<&JsonValue>) -> Result<Option<bool>, String> {
    match raw {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Bool(value)) => Ok(Some(*value)),
        _ => Err("modelSwitchingEnabled must be a boolean".to_string()),
    }
}

fn string_list(
    raw: Option<&JsonValue>,
    default: &[&str],
    field: &str,
) -> Result<Vec<String>, String> {
    match raw {
        None | Some(JsonValue::Null) => {
            Ok(default.iter().map(|value| (*value).to_string()).collect())
        }
        Some(JsonValue::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| format!("{field} must be a list of strings"))
            })
            .collect(),
        _ => Err(format!("{field} must be a list of strings")),
    }
}

fn parse_hermes_version(raw: &str) -> Option<(u64, u64, u64)> {
    let marker = raw.split_whitespace().find(|part| part.starts_with('v'))?;
    let marker = marker.trim_start_matches('v');
    let mut parts = marker.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    Some((major, minor, patch))
}
