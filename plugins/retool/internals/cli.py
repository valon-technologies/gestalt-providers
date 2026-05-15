from __future__ import annotations

import base64
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import shutil
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from typing import Any

DEFAULT_TIMEOUT_SECONDS = 120
RETOOL_CLI_PACKAGE = "retool-cli@1.0.29"
_CAPTURE_EXCLUDED_PARTS = {
    "inputs",
    ".git",
    ".npm",
    ".cache",
    "__pycache__",
    "node_modules",
}


def run_retool_cli(
    arguments: Sequence[str],
    *,
    stdin: str = "",
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    extra_files: Mapping[str, bytes] | None = None,
    extra_env: Mapping[str, str] | None = None,
    keyring_credentials: Mapping[str, Any] | None = None,
    redact_values: Sequence[str] = (),
) -> dict[str, Any]:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    normalized_args = [_require_argument(arg) for arg in arguments]
    command_prefix = _resolve_retool_command()
    command = [*command_prefix, *normalized_args]

    with tempfile.TemporaryDirectory(prefix="gestalt-retool-") as temp_root:
        temp_path = Path(temp_root)
        work_dir = temp_path / "work"
        home_dir = temp_path / "home"
        cache_dir = temp_path / "npm-cache"
        xdg_cache_dir = temp_path / "xdg-cache"
        work_dir.mkdir()
        home_dir.mkdir()
        cache_dir.mkdir()
        xdg_cache_dir.mkdir()

        _write_extra_files(work_dir, extra_files or {})
        env = _subprocess_env(
            home_dir=home_dir,
            cache_dir=cache_dir,
            xdg_cache_dir=xdg_cache_dir,
            extra_env=extra_env or {},
        )
        if keyring_credentials is not None:
            _install_keyring_shim(temp_path, env, keyring_credentials)

        try:
            completed = subprocess.run(
                command,
                cwd=work_dir,
                env=env,
                input=stdin,
                text=True,
                capture_output=True,
                timeout=timeout_seconds,
                check=False,
            )
            stdout = completed.stdout or ""
            stderr = completed.stderr or ""
            exit_code = completed.returncode
        except subprocess.TimeoutExpired as err:
            stdout = _coerce_output(err.stdout)
            stderr = (
                f"Retool CLI timed out after {timeout_seconds} seconds."
                + ("\n" + _coerce_output(err.stderr) if err.stderr else "")
            )
            exit_code = -1

        redacted_stdout = _redact_text(stdout, redact_values)
        redacted_stderr = _redact_text(stderr, redact_values)

        return {
            "command": _redacted_command(command, redact_values),
            "exit_code": exit_code,
            "stdout": redacted_stdout,
            "stderr": redacted_stderr,
            "files": _capture_files(work_dir),
            "parsed_json": _parse_json(redacted_stdout),
        }


def _resolve_retool_command() -> list[str]:
    override = os.environ.get("RETOOL_CLI_COMMAND", "").strip()
    if override:
        return shlex.split(override)

    retool = shutil.which("retool")
    if retool:
        return [retool]

    npx = shutil.which("npx")
    if npx:
        return [npx, "--yes", "--package", RETOOL_CLI_PACKAGE, "--", "retool"]

    raise RuntimeError(
        "Retool CLI requires Node.js/npm. Install retool-cli or provide RETOOL_CLI_COMMAND."
    )


def _subprocess_env(
    *,
    home_dir: Path,
    cache_dir: Path,
    xdg_cache_dir: Path,
    extra_env: Mapping[str, str],
) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "HOME": str(home_dir),
            "NPM_CONFIG_CACHE": str(cache_dir),
            "XDG_CACHE_HOME": str(xdg_cache_dir),
            "NO_COLOR": "1",
            "PUPPETEER_SKIP_DOWNLOAD": "true",
        }
    )
    env.update({key: value for key, value in extra_env.items() if value})
    return env


def _redacted_command(command: Sequence[str], redact_values: Sequence[str]) -> list[str]:
    secrets = {value for value in redact_values if value}
    return ["[redacted]" if value in secrets else value for value in command]


def _redact_text(text: str, redact_values: Sequence[str]) -> str:
    redacted = text
    for secret in {value for value in redact_values if value}:
        redacted = redacted.replace(secret, "[redacted]")
    return redacted


def _install_keyring_shim(
    temp_path: Path,
    env: dict[str, str],
    credentials: Mapping[str, Any],
) -> None:
    shim_path = temp_path / "keyring-shim.cjs"
    shim_path.write_text(_KEYRING_SHIM, encoding="utf-8")

    preload = f"--require={shim_path}"
    existing_node_options = env.get("NODE_OPTIONS", "").strip()
    env["NODE_OPTIONS"] = f"{preload} {existing_node_options}".strip()
    env["RETOOL_CLI_KEYRING_PASSWORD"] = json.dumps(
        dict(credentials),
        separators=(",", ":"),
    )


def _write_extra_files(work_dir: Path, files: Mapping[str, bytes]) -> None:
    for relative_path, content in files.items():
        target = _safe_work_path(work_dir, relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)


def _safe_work_path(work_dir: Path, relative_path: str) -> Path:
    raw = relative_path.strip().replace("\\", "/")
    if not raw:
        raise ValueError("file path is required")
    path = PurePosixPath(raw)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"unsafe file path: {relative_path}")
    return work_dir / Path(*path.parts)


def _capture_files(work_dir: Path) -> list[dict[str, Any]]:
    return _capture_tree(work_dir, PurePosixPath("."))


def _capture_tree(root: Path, prefix: PurePosixPath) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(root)
        if any(part in _CAPTURE_EXCLUDED_PARTS for part in relative.parts):
            continue
        data = path.read_bytes()
        files.append(_file_record(prefix / PurePosixPath(*relative.parts), data))
    return files


def _file_record(path: PurePosixPath, data: bytes) -> dict[str, Any]:
    try:
        text = data.decode("utf-8")
        return {
            "path": _normalize_output_path(path),
            "encoding": "utf-8",
            "content": text,
            "content_base64": "",
            "size_bytes": len(data),
        }
    except UnicodeDecodeError:
        return {
            "path": _normalize_output_path(path),
            "encoding": "base64",
            "content": "",
            "content_base64": base64.b64encode(data).decode("ascii"),
            "size_bytes": len(data),
        }


def _normalize_output_path(path: PurePosixPath) -> str:
    text = path.as_posix()
    if text.startswith("./"):
        return text[2:]
    return text


def _parse_json(stdout: str) -> Any | None:
    stripped = stdout.strip()
    if not stripped or stripped[0] not in "[{":
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None


def _coerce_output(value: bytes | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return value.decode("utf-8", errors="replace")


def _require_argument(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("CLI arguments must be strings")
    if value == "":
        raise ValueError("CLI arguments cannot be empty")
    return value


_KEYRING_SHIM = """
const Module = require("module");
let password = process.env.RETOOL_CLI_KEYRING_PASSWORD || "";
const originalLoad = Module._load;

class Entry {
  constructor(_service, _account) {}

  getPassword() {
    return password;
  }

  setPassword(value) {
    password = value || "";
  }

  deletePassword() {
    password = "";
  }
}

Module._load = function patchedLoad(request, parent, isMain) {
  if (request === "@napi-rs/keyring") {
    return { Entry };
  }
  return originalLoad.apply(this, arguments);
};
""".lstrip()
