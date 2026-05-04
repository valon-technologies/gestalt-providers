from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import gestalt
from google.protobuf import json_format

from .config import get_github_config
from .operations import (
    GitHubCreatePullRequestReviewRequest,
    GitHubListPullRequestFilesRequest,
    GitHubPullRequestRequest,
    GitHubPullRequestReviewComment,
    create_pull_request_review,
    get_pull_request,
    list_pull_request_files,
    pull_request_file_summary,
    pull_request_review_summary,
    pull_request_summary,
)

DEFAULT_AGENT_PROVIDER = "claude"
DEFAULT_MODEL = "claude-opus-4-7"
DEFAULT_MAX_COMMENTS = 10
DEFAULT_MAX_FILES = 50
DEFAULT_MAX_PATCH_CHARS = 80_000
DEFAULT_TURN_TIMEOUT_MS = 180_000
DEFAULT_POLL_INTERVAL_MS = 1_000
MAX_COMMENT_BODY_CHARS = 1_200

DEFAULT_SYSTEM_PROMPT = " ".join(
    [
        "Review the pull request diff for concrete correctness, reliability,",
        "security, or data-loss bugs.",
        "Only report issues that can be anchored to added lines in the provided diff.",
        "Do not report style nits, speculative risks, missing tests, or broad",
        "architectural preferences.",
        "Return structured findings only. If there are no concrete line-anchored",
        "issues, return an empty findings array.",
    ]
)


@dataclass(frozen=True, slots=True)
class ReviewSettings:
    agent_provider: str
    model: str
    system_prompt: str
    max_comments: int
    max_files: int
    max_patch_chars: int
    changed_lines_only: bool
    dry_run: bool
    turn_timeout_ms: int
    poll_interval_ms: int


@dataclass(frozen=True, slots=True)
class PullRequestSubject:
    owner: str
    repo: str
    repository: str
    pull_number: int
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class PullRequestFile:
    filename: str
    status: str
    additions: int
    deletions: int
    changes: int
    patch: str
    patch_truncated: bool = False


@dataclass(frozen=True, slots=True)
class ReviewFinding:
    path: str
    line: int
    body: str
    side: str = ""
    severity: str = ""


@dataclass(frozen=True, slots=True)
class ValidatedFinding:
    path: str
    line: int
    body: str


def review_pull_request(input: Any, req: gestalt.Request) -> dict[str, Any]:
    settings = normalize_review_settings(input)
    signal = latest_github_signal(req.workflow)
    if not signal:
        return skipped_result("missing_github_signal")

    if str(signal.get("github_event", "")).strip() != "pull_request":
        return skipped_result("unsupported_event")
    if str(signal.get("github_action", "")).strip() not in {
        "opened",
        "synchronize",
        "reopened",
        "ready_for_review",
    }:
        return skipped_result("unsupported_action")

    subject = pull_request_subject(signal)
    if subject is None:
        return skipped_result("missing_pull_request_subject")

    pull_request = get_pull_request(
        GitHubPullRequestRequest(
            owner=subject.owner,
            repo=subject.repo,
            pull_number=subject.pull_number,
            installation_id=subject.installation_id,
        ),
        subject=req.subject,
    )
    raw_files = list_pull_request_files(
        GitHubListPullRequestFilesRequest(
            owner=subject.owner,
            repo=subject.repo,
            pull_number=subject.pull_number,
            per_page=100,
            page=1,
            installation_id=subject.installation_id,
        ),
        subject=req.subject,
    )
    files = review_files(raw_files, settings)
    if not files:
        return review_result(subject, posted=False, comments=0, reason="no_files")

    line_index = build_line_index(files, settings)
    agent_findings = ask_agent_for_findings(
        req,
        settings,
        signal=signal,
        subject=subject,
        pull_request=pull_request_summary(pull_request),
        files=files,
    )
    findings, dropped = validate_findings(agent_findings, line_index, settings)
    if not findings:
        return review_result(
            subject,
            posted=False,
            comments=0,
            reason="no_valid_findings",
            dropped_findings=dropped,
        )

    review: Mapping[str, Any] | None = None
    if not settings.dry_run:
        review = create_pull_request_review(
            GitHubCreatePullRequestReviewRequest(
                owner=subject.owner,
                repo=subject.repo,
                pull_number=subject.pull_number,
                installation_id=subject.installation_id,
                commit_id=str(pull_request_summary(pull_request).get("head_sha", "")),
                body=(
                    f"Automated review found {len(findings)} concrete "
                    f"issue{'' if len(findings) == 1 else 's'}."
                ),
                comments=tuple(
                    GitHubPullRequestReviewComment(
                        path=finding.path,
                        line=finding.line,
                        side="RIGHT",
                        body=finding.body,
                    )
                    for finding in findings
                ),
            ),
            subject=req.subject,
        )

    result = review_result(
        subject,
        posted=not settings.dry_run,
        comments=len(findings),
        dropped_findings=dropped,
    )
    if settings.dry_run:
        result["dry_run"] = True
    if review:
        result["review"] = pull_request_review_summary(review)
    return result


def normalize_review_settings(input: Any) -> ReviewSettings:
    config = get_github_config()
    return ReviewSettings(
        agent_provider=string_setting(
            input, "agentProvider", config.agent_provider or DEFAULT_AGENT_PROVIDER
        ),
        model=string_setting(input, "model", config.agent_model or DEFAULT_MODEL),
        system_prompt=string_setting(input, "systemPrompt", DEFAULT_SYSTEM_PROMPT),
        max_comments=bounded_int_setting(input, "maxComments", DEFAULT_MAX_COMMENTS, 1, 25),
        max_files=bounded_int_setting(input, "maxFiles", DEFAULT_MAX_FILES, 1, 100),
        max_patch_chars=bounded_int_setting(
            input, "maxPatchChars", DEFAULT_MAX_PATCH_CHARS, 4_000, 200_000
        ),
        changed_lines_only=bool_setting(input, "changedLinesOnly", True),
        dry_run=bool_setting(input, "dryRun", False),
        turn_timeout_ms=bounded_int_setting(
            input, "turnTimeoutMs", DEFAULT_TURN_TIMEOUT_MS, 10_000, 600_000
        ),
        poll_interval_ms=bounded_int_setting(
            input, "pollIntervalMs", DEFAULT_POLL_INTERVAL_MS, 250, 10_000
        ),
    )


def string_setting(input: Any, key: str, fallback: str) -> str:
    value = input_value(input, key)
    return value.strip() if isinstance(value, str) and value.strip() else fallback


def bool_setting(input: Any, key: str, fallback: bool) -> bool:
    value = input_value(input, key)
    return value if isinstance(value, bool) else fallback


def bounded_int_setting(input: Any, key: str, fallback: int, minimum: int, maximum: int) -> int:
    value = input_value(input, key)
    parsed = value if isinstance(value, int) and not isinstance(value, bool) else fallback
    return max(minimum, min(maximum, parsed))


def input_value(input: Any, key: str) -> Any:
    if isinstance(input, Mapping):
        return input.get(key)
    return getattr(input, key, None)


def latest_github_signal(workflow: Mapping[str, Any]) -> dict[str, Any] | None:
    signals = workflow.get("signals")
    if not isinstance(signals, list):
        return None
    for item in reversed(signals):
        signal = object_value(item)
        payload = object_value(signal.get("payload") if signal else None)
        if payload and payload.get("github_event"):
            return payload
    return None


def pull_request_subject(signal: Mapping[str, Any]) -> PullRequestSubject | None:
    repository = string_value(
        nested_value(signal, "repository", "full_name")
        or nested_value(signal, "summary", "repository")
        or nested_value(signal, "agent_request", "subject", "repository")
    )
    pull_number = int_value(
        nested_value(signal, "summary", "number")
        or nested_value(signal, "agent_request", "subject", "number")
        or nested_value(signal, "agent_request", "pull_request", "number")
    )
    if not repository or "/" not in repository or pull_number <= 0:
        return None
    owner, repo = repository.split("/", 1)
    if not owner or not repo:
        return None
    return PullRequestSubject(
        owner=owner,
        repo=repo,
        repository=repository,
        pull_number=pull_number,
        installation_id=int_value(nested_value(signal, "installation", "id")),
    )


def review_files(
    raw_files: Sequence[Mapping[str, Any]], settings: ReviewSettings
) -> list[PullRequestFile]:
    files: list[PullRequestFile] = []
    for raw in raw_files[: settings.max_files]:
        summary = pull_request_file_summary(raw)
        patch = string_value(summary.get("patch"))
        filename = string_value(summary.get("filename"))
        if not filename or not patch:
            continue
        files.append(
            PullRequestFile(
                filename=filename,
                status=string_value(summary.get("status")),
                additions=int_value(summary.get("additions")),
                deletions=int_value(summary.get("deletions")),
                changes=int_value(summary.get("changes")),
                patch=patch,
                patch_truncated=bool(summary.get("patch_truncated")),
            )
        )
    return files


def ask_agent_for_findings(
    req: gestalt.Request,
    settings: ReviewSettings,
    *,
    signal: Mapping[str, Any],
    subject: PullRequestSubject,
    pull_request: Mapping[str, Any],
    files: Sequence[PullRequestFile],
) -> list[ReviewFinding]:
    manager = req.agent_manager()
    metadata = {
        "source": "github.reviewPullRequest",
        "repository": subject.repository,
        "pullNumber": subject.pull_number,
        "deliveryId": string_value(signal.get("delivery_id")),
    }
    idempotency_base = ":".join(
        [
            "github-review",
            subject.repository,
            str(subject.pull_number),
            string_value(pull_request.get("head_sha")),
            string_value(signal.get("delivery_id")),
        ]
    )
    session_request = gestalt.AgentManagerCreateSessionRequest(
        provider_name=settings.agent_provider,
        model=settings.model,
        client_ref=f"{subject.repository}#{subject.pull_number}",
        idempotency_key=f"{idempotency_base}:session",
    )
    session_request.metadata.update(metadata)
    session = manager.create_session(session_request)

    turn_request = gestalt.AgentManagerCreateTurnRequest(
        session_id=session.id,
        model=settings.model,
        messages=[
            gestalt.AgentMessage(role="system", text=settings.system_prompt),
            gestalt.AgentMessage(
                role="user", text=review_prompt(subject, pull_request, files, settings)
            ),
        ],
        idempotency_key=f"{idempotency_base}:turn",
    )
    turn_request.response_schema.update(review_response_schema())
    turn_request.metadata.update(metadata)
    turn = manager.create_turn(turn_request)
    turn = wait_for_turn(manager, turn, settings)

    if turn.status != gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED:
        raise RuntimeError(
            f"agent turn {turn.id} finished with status {turn.status}: "
            f"{turn.status_message}"
        )

    structured = struct_to_dict(getattr(turn, "structured_output", None))
    fallback = {} if structured else parse_json_object(str(getattr(turn, "output_text", "")))
    findings = (structured or fallback).get("findings")
    if not isinstance(findings, list):
        return []
    return [finding for item in findings for finding in normalize_finding(item)]


def wait_for_turn(manager: Any, turn: Any, settings: ReviewSettings) -> Any:
    deadline = time.monotonic() + settings.turn_timeout_ms / 1000
    while turn.status in {
        gestalt.AGENT_EXECUTION_STATUS_UNSPECIFIED,
        gestalt.AGENT_EXECUTION_STATUS_PENDING,
        gestalt.AGENT_EXECUTION_STATUS_RUNNING,
    }:
        if time.monotonic() >= deadline:
            raise RuntimeError(f"agent turn {turn.id} timed out")
        time.sleep(settings.poll_interval_ms / 1000)
        turn = manager.get_turn(gestalt.AgentManagerGetTurnRequest(turn_id=turn.id))
    return turn


def review_prompt(
    subject: PullRequestSubject,
    pull_request: Mapping[str, Any],
    files: Sequence[PullRequestFile],
    settings: ReviewSettings,
) -> str:
    return json.dumps(
        {
            "task": "Return findings for concrete bugs on added RIGHT-side diff lines only.",
            "repository": subject.repository,
            "pull_number": subject.pull_number,
            "pull_request": dict(pull_request),
            "files": [
                {
                    "filename": file.filename,
                    "status": file.status,
                    "additions": file.additions,
                    "deletions": file.deletions,
                    "changes": file.changes,
                    "patch_truncated": file.patch_truncated,
                    "patch": bounded_text(file.patch, settings.max_patch_chars),
                }
                for file in files
            ],
            "output_contract": {
                "findings": [
                    {
                        "path": "exact changed file path",
                        "line": "new-file line number from an added line in the diff",
                        "body": "specific review comment explaining the bug and suggested fix",
                        "severity": "optional: critical|high|medium|low",
                    }
                ]
            },
        },
        indent=2,
    )


def review_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "path": {"type": "string"},
                        "line": {"type": "integer"},
                        "side": {"type": "string", "enum": ["RIGHT"]},
                        "body": {"type": "string"},
                        "severity": {"type": "string"},
                    },
                    "required": ["path", "line", "body"],
                },
            }
        },
        "required": ["findings"],
    }


def build_line_index(
    files: Sequence[PullRequestFile], settings: ReviewSettings
) -> dict[str, set[int]]:
    return {
        file.filename: changed_right_lines(file.patch, settings.changed_lines_only)
        for file in files
    }


def changed_right_lines(patch: str, changed_lines_only: bool) -> set[int]:
    lines: set[int] = set()
    new_line = 0
    for raw_line in patch.splitlines():
        if raw_line.startswith("@@"):
            _, new_line = hunk_start_lines(raw_line)
            continue
        if new_line <= 0:
            continue
        if raw_line.startswith(("+++", "---")):
            continue
        if raw_line.startswith("+"):
            lines.add(new_line)
            new_line += 1
            continue
        if raw_line.startswith("-"):
            continue
        if not changed_lines_only:
            lines.add(new_line)
        new_line += 1
    return lines


def hunk_start_lines(line: str) -> tuple[int, int]:
    parts = line.split(" ")
    if len(parts) < 3:
        return 0, 0
    return parse_hunk_start(parts[1], "-"), parse_hunk_start(parts[2], "+")


def parse_hunk_start(part: str, prefix: str) -> int:
    if not part.startswith(prefix):
        return 0
    value = part[1:].split(",", 1)[0]
    return int_value(value)


def validate_findings(
    raw_findings: Sequence[ReviewFinding],
    line_index: Mapping[str, set[int]],
    settings: ReviewSettings,
) -> tuple[list[ValidatedFinding], int]:
    findings: list[ValidatedFinding] = []
    dropped = 0
    seen: set[tuple[str, int, str]] = set()
    for raw in raw_findings:
        path = raw.path.strip()
        body = bounded_text(raw.body.strip(), MAX_COMMENT_BODY_CHARS)
        valid_lines = line_index.get(path, set())
        key = (path, raw.line, body)
        if (
            not path
            or raw.line <= 0
            or not body
            or raw.side.upper() == "LEFT"
            or raw.line not in valid_lines
            or key in seen
        ):
            dropped += 1
            continue
        seen.add(key)
        findings.append(
            ValidatedFinding(path=path, line=raw.line, body=format_comment_body(raw, body))
        )
        if len(findings) >= settings.max_comments:
            dropped += len(raw_findings) - (len(findings) + dropped)
            break
    return findings, max(0, dropped)


def normalize_finding(value: Any) -> list[ReviewFinding]:
    item = object_value(value)
    if not item:
        return []
    path = string_value(item.get("path"))
    line = int_value(item.get("line"))
    body = string_value(item.get("body"))
    if not path or line <= 0 or not body:
        return []
    return [
        ReviewFinding(
            path=path,
            line=line,
            body=body,
            side=string_value(item.get("side")),
            severity=string_value(item.get("severity")),
        )
    ]


def format_comment_body(raw: ReviewFinding, body: str) -> str:
    severity = raw.severity.strip().lower()
    if severity in {"critical", "high", "medium", "low"}:
        return f"[{severity}] {body}"
    return body


def review_result(
    subject: PullRequestSubject,
    *,
    posted: bool,
    comments: int,
    reason: str = "",
    dropped_findings: int = 0,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": True,
        "posted": posted,
        "comments": comments,
        "repository": subject.repository,
        "pullNumber": subject.pull_number,
    }
    if reason:
        result["reason"] = reason
    if dropped_findings:
        result["droppedFindings"] = dropped_findings
    return result


def skipped_result(reason: str) -> dict[str, Any]:
    return {
        "ok": True,
        "skipped": True,
        "posted": False,
        "comments": 0,
        "reason": reason,
    }


def nested_value(value: Mapping[str, Any], *keys: str) -> Any:
    current: Any = value
    for key in keys:
        current = object_value(current)
        if current is None:
            return None
        current = current.get(key)
    return current


def object_value(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def string_value(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def int_value(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
        return int(value.strip())
    return 0


def bounded_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return f"{value[: max(0, max_chars - 16)].rstrip()}\n...[truncated]"


def parse_json_object(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def struct_to_dict(value: Any) -> dict[str, Any]:
    if value is None or not getattr(value, "fields", None):
        return {}
    return json_format.MessageToDict(value)
