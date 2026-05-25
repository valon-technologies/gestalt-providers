from __future__ import annotations

import hashlib
import json
import re
import time
import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import gestalt

from .client import bot_identity_or_none
from .config import SELF_FIX_BRANCH_COMMIT, get_github_config
from .constants import BOT_COMMIT_FILES_OPERATION
from .errors import GitHubAPIError, GitHubAuthorizationError, GitHubConfigError
from .manual_trigger import (
    app_mention_body_matches,
    manual_command_body_matches,
)
from .operations import (
    GitHubCheckRunOutput,
    GitHubCommitRequest,
    GitHubCreateCheckRunRequest,
    GitHubCreatePullRequestReviewRequest,
    GitHubFileChange,
    GitHubFileContentRequest,
    GitHubListPullRequestReviewThreadsRequest,
    GitHubListPullRequestFilesRequest,
    GitHubPullRequestRequest,
    GitHubPullRequestReviewComment,
    GitHubResolvePullRequestReviewThreadRequest,
    GitHubUpdateCheckRunRequest,
    check_run_summary,
    commit_files,
    commit_result_dict,
    create_check_run,
    create_pull_request_review,
    get_file_text_at_ref,
    get_pull_request,
    list_pull_request_review_threads,
    list_pull_request_files,
    non_empty_external_identity,
    pull_request_file_summary,
    pull_request_review_summary,
    pull_request_summary,
    resolve_pull_request_review_thread,
    update_check_run,
)

DEFAULT_AGENT_PROVIDER = "claude"
DEFAULT_MODEL = "claude-opus-4-7"
DEFAULT_MAX_COMMENTS = 10
DEFAULT_MAX_FILES = 50
DEFAULT_MAX_PATCH_CHARS = 80_000
DEFAULT_TURN_TIMEOUT_MS = 180_000
DEFAULT_POLL_INTERVAL_MS = 1_000
MAX_COMMENT_BODY_CHARS = 1_200
REVIEW_FINDING_SOURCE = "github.reviewPullRequest"
REVIEW_FINDING_MARKER_RE = re.compile(
    r"<!--\s*gestalt:github-review-finding\s+v(?P<version>[12])\s+"
    r"fingerprint=(?P<fingerprint>\S+)"
    r"(?:\s+stable_fingerprint=(?P<stable_fingerprint>\S+))?"
    r"\s+source=(?P<source>\S+)\s*-->"
)
REVIEW_FINDING_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")
AUTO_RESOLVE_MAX_THREAD_PAGES = 10
REVIEW_OUTPUT_CONTRACT = "github.pull_request_review.findings.v1"
REVIEW_OUTPUT_KEYS = frozenset(("findings",))
SELF_FIX_OUTPUT_CONTRACT = "github.pull_request_review.self_fix.v1"
SELF_FIX_OUTPUT_KEYS = frozenset(("commit_message", "files"))
SUPPORTED_PULL_REQUEST_ACTIONS = frozenset(
    ("opened", "synchronize", "reopened", "ready_for_review")
)

DEFAULT_SYSTEM_PROMPT = " ".join(
    [
        "Review the pull request diff for concrete correctness, reliability,",
        "security, or data-loss bugs.",
        "Only report issues that can be anchored to RIGHT-side lines allowed by",
        "the review prompt's line_policy.",
        "Do not report style nits, speculative risks, missing tests, or broad",
        "architectural preferences.",
        "Return only a valid JSON object with a top-level findings array and no",
        "other top-level keys. If there are no concrete line-anchored issues,",
        "return an empty findings array.",
    ]
)

SELF_FIX_SYSTEM_PROMPT = " ".join(
    [
        "Fix concrete pull request review findings using the provided full file",
        "contents.",
        "Only edit files included in the self-fix prompt.",
        "Return only a valid JSON object with top-level commit_message and files",
        "keys. If a safe fix is not clear, return an empty files array.",
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
    auto_resolve_stale_findings: bool
    check_run_name: str
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


@dataclass(frozen=True, slots=True)
class ReviewFixFile:
    path: str
    content: str


@dataclass(frozen=True, slots=True)
class ReviewFileContent:
    path: str
    content: str


@dataclass(frozen=True, slots=True)
class ReviewFindingMarker:
    fingerprint: str
    stable_fingerprint: str
    source: str


@dataclass(frozen=True, slots=True)
class ReviewFindingFingerprints:
    fingerprint: str
    stable_fingerprint: str


def review_pull_request(input: Any, req: gestalt.Request) -> dict[str, Any]:
    settings = normalize_review_settings(input)
    signal = latest_github_signal(req.workflow)
    if not signal:
        return skipped_result("missing_github_signal")

    unsupported_reason = unsupported_review_signal_reason(signal)
    if unsupported_reason:
        return skipped_result(unsupported_reason)

    subject = pull_request_subject(signal)
    if subject is None:
        return skipped_result("missing_pull_request_subject")

    check_run: Mapping[str, Any] | None = signal_review_check_run(signal)
    identity_kwargs = request_external_identity_kwargs(req)
    try:
        pull_request = get_pull_request(
            GitHubPullRequestRequest(
                owner=subject.owner,
                repo=subject.repo,
                pull_number=subject.pull_number,
                installation_id=subject.installation_id,
            ),
            subject=req.subject,
            **identity_kwargs,
        )
        pull_summary = pull_request_summary(pull_request)
        if check_run is None and not settings.dry_run:
            check_run = create_review_check_run(subject, req, settings, pull_summary)
        if pull_request.get("draft") is True:
            result = review_result(
                subject, posted=False, comments=0, reason="draft_pull_request"
            )
            result["skipped"] = True
            completed_check_run = complete_review_check_run(
                check_run,
                subject,
                req,
                conclusion="skipped",
                title="Review skipped",
                summary="Gestalt review skipped this draft pull request.",
            )
            add_check_run_result(result, completed_check_run or check_run)
            return result

        return _review_pull_request_after_fetch(
            settings=settings,
            signal=signal,
            subject=subject,
            req=req,
            pull_summary=pull_summary,
            check_run=check_run,
        )
    except Exception:
        complete_review_check_run(
            check_run,
            subject,
            req,
            conclusion="failure",
            title="Review failed",
            summary="Gestalt review failed before it could complete.",
        )
        raise


def _review_pull_request_after_fetch(
    *,
    settings: ReviewSettings,
    signal: Mapping[str, Any],
    subject: PullRequestSubject,
    req: gestalt.Request,
    pull_summary: Mapping[str, Any],
    check_run: Mapping[str, Any] | None,
) -> dict[str, Any]:
    identity_kwargs = request_external_identity_kwargs(req)
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
        **identity_kwargs,
    )
    files = review_files(raw_files, settings)
    if not files:
        result = review_result(subject, posted=False, comments=0, reason="no_files")
        completed_check_run = complete_review_check_run(
            check_run,
            subject,
            req,
            conclusion="success",
            title="No reviewable files",
            summary="Gestalt review found no reviewable changed files.",
        )
        add_check_run_result(result, completed_check_run or check_run)
        return result

    line_index = build_line_index(files, settings)
    agent_findings = ask_agent_for_findings(
        req,
        settings,
        signal=signal,
        subject=subject,
        pull_request=pull_summary,
        files=files,
    )
    findings, dropped = validate_findings(agent_findings, line_index, settings)
    current_fingerprints = [
        review_finding_fingerprints(subject, finding) for finding in findings
    ]
    current_fingerprint_set = fingerprint_marker_values(current_fingerprints)
    existing = existing_review_findings(
        subject,
        req,
        enabled=not settings.dry_run,
    )
    duplicate_fingerprints = set(existing["fingerprints"])
    postable_findings: list[tuple[ValidatedFinding, ReviewFindingFingerprints]] = []
    suppressed = 0
    for finding, fingerprints in zip(findings, current_fingerprints, strict=True):
        if (
            fingerprints.fingerprint in duplicate_fingerprints
            or fingerprints.stable_fingerprint in duplicate_fingerprints
        ):
            suppressed += 1
            continue
        postable_findings.append((finding, fingerprints))

    if not findings:
        resolution = auto_resolve_stale_findings(
            subject,
            req,
            current_fingerprints=current_fingerprint_set,
            enabled=settings.auto_resolve_stale_findings and not settings.dry_run,
        )
        result = review_result(
            subject,
            posted=False,
            comments=0,
            reason="no_valid_findings",
            dropped_findings=dropped,
            resolved_threads=resolution["resolvedThreads"],
            skipped_resolution_reasons=resolution["skippedResolutionReasons"],
        )
        completed_check_run = complete_review_check_run(
            check_run,
            subject,
            req,
            conclusion="success",
            title="No findings",
            summary="Gestalt review found no concrete line-anchored issues.",
        )
        add_check_run_result(result, completed_check_run or check_run)
        return result

    self_fix = (
        attempt_review_self_fix(
            req,
            settings,
            signal=signal,
            subject=subject,
            pull_request=pull_summary,
            findings=[finding for finding, _fingerprints in postable_findings],
        )
        if postable_findings
        else {}
    )
    if self_fix.get("status") == "committed":
        result = review_result(
            subject,
            posted=False,
            comments=0,
            dropped_findings=dropped,
            suppressed_findings=suppressed,
        )
        add_self_fix_result(result, self_fix)
        completed_check_run = complete_review_check_run(
            check_run,
            subject,
            req,
            conclusion="success",
            title="Fix committed",
            summary="Gestalt committed a same-PR fix for this review.",
        )
        add_check_run_result(
            result,
            object_value(self_fix.get("checkRun")) or completed_check_run or check_run,
        )
        return result

    review: Mapping[str, Any] | None = None
    if postable_findings and not settings.dry_run:
        review = create_pull_request_review(
            GitHubCreatePullRequestReviewRequest(
                owner=subject.owner,
                repo=subject.repo,
                pull_number=subject.pull_number,
                installation_id=subject.installation_id,
                commit_id=str(pull_summary.get("head_sha", "")),
                body=(
                    f"Automated review found {len(postable_findings)} new concrete "
                    f"issue{'' if len(postable_findings) == 1 else 's'}."
                ),
                comments=tuple(
                    GitHubPullRequestReviewComment(
                        path=finding.path,
                        line=finding.line,
                        side="RIGHT",
                        body=review_comment_body_with_marker(
                            finding.body,
                            fingerprints,
                        ),
                    )
                    for finding, fingerprints in postable_findings
                ),
            ),
            subject=req.subject,
            **identity_kwargs,
        )
    resolution = auto_resolve_stale_findings(
        subject,
        req,
        current_fingerprints=current_fingerprint_set,
        enabled=settings.auto_resolve_stale_findings and not settings.dry_run,
    )

    result = review_result(
        subject,
        posted=bool(review),
        comments=len(postable_findings),
        dropped_findings=dropped,
        suppressed_findings=suppressed,
        resolved_threads=resolution["resolvedThreads"],
        skipped_resolution_reasons=resolution["skippedResolutionReasons"],
    )
    if not postable_findings and suppressed:
        result["reason"] = "duplicate_findings"
    if settings.dry_run:
        result["dry_run"] = True
    if review:
        result["review"] = pull_request_review_summary(review)
    add_self_fix_result(result, self_fix)
    completed_check_run = complete_review_check_run(
        check_run,
        subject,
        req,
        conclusion="success",
        title="Review complete",
        summary=(
            f"Gestalt review found {len(findings)} concrete "
            f"issue{'' if len(findings) == 1 else 's'}."
        ),
    )
    add_check_run_result(result, completed_check_run or check_run)
    return result


def normalize_review_settings(input: Any) -> ReviewSettings:
    config = get_github_config()
    return ReviewSettings(
        agent_provider=string_setting(
            input, "agentProvider", config.agent_provider or DEFAULT_AGENT_PROVIDER
        ),
        model=string_setting(input, "model", config.agent_model or DEFAULT_MODEL),
        system_prompt=string_setting(input, "systemPrompt", DEFAULT_SYSTEM_PROMPT),
        max_comments=bounded_int_setting(
            input, "maxComments", DEFAULT_MAX_COMMENTS, 1, 25
        ),
        max_files=bounded_int_setting(input, "maxFiles", DEFAULT_MAX_FILES, 1, 100),
        max_patch_chars=bounded_int_setting(
            input, "maxPatchChars", DEFAULT_MAX_PATCH_CHARS, 4_000, 200_000
        ),
        changed_lines_only=bool_setting(input, "changedLinesOnly", True),
        dry_run=bool_setting(input, "dryRun", False),
        auto_resolve_stale_findings=bool_setting(
            input, "autoResolveStaleFindings", True
        ),
        check_run_name=string_setting(input, "checkRunName", "Gestalt Review"),
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


def bounded_int_setting(
    input: Any, key: str, fallback: int, minimum: int, maximum: int
) -> int:
    value = input_value(input, key)
    parsed = (
        value if isinstance(value, int) and not isinstance(value, bool) else fallback
    )
    return max(minimum, min(maximum, parsed))


def input_value(input: Any, key: str) -> Any:
    if isinstance(input, Mapping):
        return input.get(key)
    return getattr(input, key, None)


def request_external_identity_kwargs(req: gestalt.Request) -> dict[str, Any]:
    external_identity = non_empty_external_identity(
        getattr(req, "external_identity", None)
    )
    return (
        {"external_identity": external_identity}
        if external_identity is not None
        else {}
    )


def create_review_check_run(
    subject: PullRequestSubject,
    req: gestalt.Request,
    settings: ReviewSettings,
    pull_request: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    head_sha = string_value(pull_request.get("head_sha"))
    if not head_sha:
        raise ValueError("review check run requires pull request head SHA")
    return create_check_run(
        GitHubCreateCheckRunRequest(
            owner=subject.owner,
            repo=subject.repo,
            name=settings.check_run_name,
            head_sha=head_sha,
            status="in_progress",
            output=GitHubCheckRunOutput(
                title="Review running",
                summary="Gestalt is reviewing this pull request.",
            ),
            installation_id=subject.installation_id,
        ),
        subject=req.subject,
        **request_external_identity_kwargs(req),
    )


def complete_review_check_run(
    check_run: Mapping[str, Any] | None,
    subject: PullRequestSubject,
    req: gestalt.Request,
    *,
    conclusion: str,
    title: str,
    summary: str,
) -> Mapping[str, Any] | None:
    if check_run is None:
        return None
    check_run_id = int_value(check_run.get("id"))
    if check_run_id <= 0:
        return None
    return update_check_run(
        GitHubUpdateCheckRunRequest(
            owner=subject.owner,
            repo=subject.repo,
            check_run_id=check_run_id,
            conclusion=conclusion,
            completed_at=utc_timestamp(),
            output=GitHubCheckRunOutput(title=title, summary=summary),
            installation_id=subject.installation_id,
        ),
        subject=req.subject,
        **request_external_identity_kwargs(req),
    )


def add_check_run_result(
    result: dict[str, Any], check_run: Mapping[str, Any] | None
) -> None:
    if check_run is not None:
        result["checkRun"] = check_run_summary(check_run)


def add_self_fix_result(result: dict[str, Any], self_fix: Mapping[str, Any]) -> None:
    if self_fix:
        result["selfFix"] = dict(self_fix)


def attempt_review_self_fix(
    req: gestalt.Request,
    settings: ReviewSettings,
    *,
    signal: Mapping[str, Any],
    subject: PullRequestSubject,
    pull_request: Mapping[str, Any],
    findings: Sequence[ValidatedFinding],
) -> dict[str, Any]:
    if not review_self_fix_enabled(req, signal):
        return {}
    if settings.dry_run:
        return self_fix_result("dry_run")
    reason = review_self_fix_unsafe_reason(pull_request, findings)
    if reason:
        return self_fix_result(reason)

    try:
        file_contents = review_self_fix_file_contents(
            req,
            settings,
            subject=subject,
            pull_request=pull_request,
            findings=findings,
        )
        replacements = ask_agent_for_fix(
            req,
            settings,
            signal=signal,
            subject=subject,
            pull_request=pull_request,
            findings=findings,
            files=file_contents,
        )
        changes = validate_review_fix_replacements(
            replacements,
            file_contents=file_contents,
        )
        if not changes:
            return self_fix_result("no_valid_edits")
        commit = commit_files(
            GitHubCommitRequest(
                owner=subject.owner,
                repo=subject.repo,
                message=review_self_fix_commit_message(),
                files=tuple(
                    GitHubFileChange(path=change.path, content=change.content)
                    for change in changes
                ),
                branch=string_value(pull_request.get("head_ref")),
                base_branch=string_value(pull_request.get("base_ref")),
                installation_id=subject.installation_id,
                expected_head_sha=string_value(pull_request.get("head_sha")),
            ),
            subject=req.subject,
            pull_request_permissions=False,
            **request_external_identity_kwargs(req),
        )
    except (
        ValueError,
        RuntimeError,
        GitHubAPIError,
        GitHubAuthorizationError,
        GitHubConfigError,
    ) as err:
        return self_fix_result(f"skipped: {err}")

    result = self_fix_result("committed")
    result["commit"] = commit_result_dict(commit)
    try:
        latest_check_run = create_review_self_fix_check_run(
            req,
            settings,
            subject=subject,
            commit_sha=commit.commit_sha,
            file_count=len(changes),
        )
    except (
        ValueError,
        GitHubAPIError,
        GitHubAuthorizationError,
        GitHubConfigError,
    ) as err:
        result["check_run_error"] = str(err)
        latest_check_run = None
    if latest_check_run is not None:
        result["checkRun"] = check_run_summary(latest_check_run)
    return result


def review_self_fix_enabled(req: gestalt.Request, signal: Mapping[str, Any]) -> bool:
    if not review_tool_refs_allow_operation(
        review_agent_tool_refs(req, signal),
        plugin="github",
        operation=BOT_COMMIT_FILES_OPERATION,
    ):
        return False
    action = object_value(nested_value(signal, "webhook_policy", "action")) or {}
    preferences = (
        object_value(nested_value(signal, "webhook_policy", "action_preferences")) or {}
    )
    effective = object_value(preferences.get("effective")) or {}
    allow_self_fix = effective.get("allow_self_fix", action.get("allow_self_fix"))
    self_fix_mode = string_value(
        effective.get("self_fix_mode", action.get("self_fix_mode"))
    )
    return bool(allow_self_fix) and self_fix_mode == SELF_FIX_BRANCH_COMMIT


def review_policy_tool_ref_operations(signal: Mapping[str, Any]) -> list[str]:
    operations: list[str] = []
    seen: set[str] = set()
    for item in nested_value(signal, "webhook_policy", "tool_refs") or []:
        operation = str(item).strip()
        if not operation or operation in seen:
            continue
        seen.add(operation)
        operations.append(operation)
    return operations


def review_policy_tool_refs(signal: Mapping[str, Any]) -> list[Any]:
    return [
        gestalt.AgentToolRef(app="github", operation=operation)
        for operation in review_policy_tool_ref_operations(signal)
    ]


def review_agent_tool_refs(
    req: gestalt.Request,
    signal: Mapping[str, Any],
) -> list[Any]:
    if bool(getattr(req, "tool_refs_set", False)):
        return list(getattr(req, "tool_refs", ()) or ())
    return review_policy_tool_refs(signal)


def review_tool_refs_allow_operation(
    tool_refs: Sequence[Any],
    *,
    plugin: str,
    operation: str,
) -> bool:
    for ref in tool_refs:
        if str(getattr(ref, "system", "") or "").strip():
            continue
        ref_plugin = str(
            getattr(ref, "app", "") or getattr(ref, "plugin", "") or ""
        ).strip()
        ref_operation = str(getattr(ref, "operation", "") or "").strip()
        if ref_plugin in (plugin, "*") and ref_operation in (operation, ""):
            return True
    return False


def review_self_fix_unsafe_reason(
    pull_request: Mapping[str, Any], findings: Sequence[ValidatedFinding]
) -> str:
    if not findings:
        return "no_findings"
    if pull_request.get("head_repo_is_base_repo") is not True:
        return "head_repo_not_writable"
    head_ref = string_value(pull_request.get("head_ref"))
    base_ref = string_value(pull_request.get("base_ref"))
    head_sha = string_value(pull_request.get("head_sha"))
    if not head_ref or not base_ref or not head_sha:
        return "missing_head_metadata"
    if head_ref == base_ref:
        return "head_is_base_branch"
    return ""


def review_self_fix_file_contents(
    req: gestalt.Request,
    settings: ReviewSettings,
    *,
    subject: PullRequestSubject,
    pull_request: Mapping[str, Any],
    findings: Sequence[ValidatedFinding],
) -> list[ReviewFileContent]:
    head_sha = string_value(pull_request.get("head_sha"))
    paths = sorted({finding.path for finding in findings if finding.path.strip()})
    files: list[ReviewFileContent] = []
    for path in paths:
        content = get_file_text_at_ref(
            GitHubFileContentRequest(
                owner=subject.owner,
                repo=subject.repo,
                path=path,
                ref=head_sha,
                installation_id=subject.installation_id,
                max_bytes=settings.max_patch_chars,
            ),
            subject=req.subject,
            **request_external_identity_kwargs(req),
        )
        files.append(ReviewFileContent(path=path, content=content))
    return files


def validate_review_fix_replacements(
    replacements: Sequence[ReviewFixFile],
    *,
    file_contents: Sequence[ReviewFileContent],
) -> list[ReviewFixFile]:
    original = {file.path: file.content for file in file_contents}
    changes: list[ReviewFixFile] = []
    for item in replacements:
        if item.path not in original:
            raise ValueError(f"{item.path}: self-fix can only edit finding files")
        if item.content != original[item.path]:
            changes.append(item)
    return changes


def create_review_self_fix_check_run(
    req: gestalt.Request,
    settings: ReviewSettings,
    *,
    subject: PullRequestSubject,
    commit_sha: str,
    file_count: int,
) -> Mapping[str, Any] | None:
    if not commit_sha:
        return None
    return create_check_run(
        GitHubCreateCheckRunRequest(
            owner=subject.owner,
            repo=subject.repo,
            name=settings.check_run_name,
            head_sha=commit_sha,
            conclusion="success",
            output=GitHubCheckRunOutput(
                title="Fix committed",
                summary=(
                    "Gestalt committed a same-PR fix "
                    f"touching {file_count} file{'' if file_count == 1 else 's'}."
                ),
            ),
            installation_id=subject.installation_id,
        ),
        subject=req.subject,
        **request_external_identity_kwargs(req),
    )


def review_self_fix_commit_message() -> str:
    return "Apply Gestalt review fixes"


def self_fix_result(reason: str) -> dict[str, Any]:
    status = "committed" if reason == "committed" else "skipped"
    return {"status": status, "reason": reason}


def signal_review_check_run(signal: Mapping[str, Any]) -> Mapping[str, Any] | None:
    check_run = object_value(signal.get("review_check_run"))
    return check_run if check_run and int_value(check_run.get("id")) > 0 else None


def utc_timestamp() -> str:
    return (
        dt.datetime.now(dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


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


def unsupported_review_signal_reason(signal: Mapping[str, Any]) -> str:
    event = string_value(signal.get("github_event"))
    action = string_value(signal.get("github_action"))
    if event == "pull_request":
        return "" if action in SUPPORTED_PULL_REQUEST_ACTIONS else "unsupported_action"
    if event != "issue_comment":
        return "unsupported_event"
    if action != "created":
        return "unsupported_action"
    issue = object_value(nested_value(signal, "agent_request", "issue")) or {}
    if issue.get("is_pull_request") is not True:
        return "not_pull_request_comment"
    body = string_value(nested_value(signal, "agent_request", "comment", "body"))
    require_app_mention = bool(
        nested_value(signal, "webhook_policy", "trigger", "require_app_mention")
    )
    if require_app_mention:
        return "" if app_mention_body_matches(body) else "app_mention_mismatch"
    commands = [
        str(command)
        for command in nested_value(
            signal, "webhook_policy", "trigger", "manual_commands"
        )
        or []
        if str(command).strip()
    ]
    if not commands:
        return "missing_manual_command"
    match_mode = string_value(
        nested_value(signal, "webhook_policy", "trigger", "manual_command_match")
    )
    if not manual_command_body_matches(
        body,
        commands,
        match_mode=match_mode,
    ):
        return "manual_command_mismatch"
    return ""


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
    session_request = gestalt.AgentCreateSession(
        provider_name=settings.agent_provider,
        model=settings.model,
        client_ref=f"{subject.repository}#{subject.pull_number}",
        metadata=metadata,
        idempotency_key=f"{idempotency_base}:session",
    )
    with req.agent() as agent:
        try:
            session = agent.create_session(session_request)
        except Exception as err:
            raise RuntimeError(f"review agent session request failed: {err}") from err

        turn_request = gestalt.AgentCreateTurn(
            session_id=session.id,
            model=settings.model,
            messages=[
                gestalt.AgentMessage(role="system", text=settings.system_prompt),
                gestalt.AgentMessage(
                    role="user",
                    text=review_prompt(subject, pull_request, files, settings),
                ),
            ],
            tool_refs=review_agent_tool_refs(req, signal),
            tool_source=gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
            metadata=metadata,
            idempotency_key=f"{idempotency_base}:turn",
        )
        try:
            turn = agent.create_turn(turn_request)
            turn = wait_for_turn(agent, turn, settings)
        except Exception as err:
            raise RuntimeError(f"review agent turn request failed: {err}") from err

        if turn.status != gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED:
            raise RuntimeError(
                f"agent turn {turn.id} finished with status {turn.status}: "
                f"{turn.status_message}"
            )

        findings = parse_review_findings_output(str(getattr(turn, "output_text", "")))
        return [finding for item in findings for finding in normalize_finding(item)]


def ask_agent_for_fix(
    req: gestalt.Request,
    settings: ReviewSettings,
    *,
    signal: Mapping[str, Any],
    subject: PullRequestSubject,
    pull_request: Mapping[str, Any],
    findings: Sequence[ValidatedFinding],
    files: Sequence[ReviewFileContent],
) -> list[ReviewFixFile]:
    metadata = {
        "source": "github.reviewPullRequest.selfFix",
        "repository": subject.repository,
        "pullNumber": subject.pull_number,
        "deliveryId": string_value(signal.get("delivery_id")),
    }
    idempotency_base = ":".join(
        [
            "github-review-fix",
            subject.repository,
            str(subject.pull_number),
            string_value(pull_request.get("head_sha")),
            string_value(signal.get("delivery_id")),
        ]
    )
    session_request = gestalt.AgentCreateSession(
        provider_name=settings.agent_provider,
        model=settings.model,
        client_ref=f"{subject.repository}#{subject.pull_number}:self-fix",
        metadata=metadata,
        idempotency_key=f"{idempotency_base}:session",
    )
    with req.agent() as agent:
        try:
            session = agent.create_session(session_request)
        except Exception as err:
            raise RuntimeError(f"self-fix agent session request failed: {err}") from err

        turn_request = gestalt.AgentCreateTurn(
            session_id=session.id,
            model=settings.model,
            messages=[
                gestalt.AgentMessage(role="system", text=SELF_FIX_SYSTEM_PROMPT),
                gestalt.AgentMessage(
                    role="user",
                    text=self_fix_prompt(
                        subject,
                        pull_request,
                        findings=findings,
                        files=files,
                        settings=settings,
                    ),
                ),
            ],
            tool_refs=review_agent_tool_refs(req, signal),
            tool_source=gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
            metadata=metadata,
            idempotency_key=f"{idempotency_base}:turn",
        )
        try:
            turn = agent.create_turn(turn_request)
            turn = wait_for_turn(agent, turn, settings)
        except Exception as err:
            raise RuntimeError(f"self-fix agent turn request failed: {err}") from err

        if turn.status != gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED:
            raise RuntimeError(
                f"agent turn {turn.id} finished with status {turn.status}: "
                f"{turn.status_message}"
            )

        return parse_review_fix_output(str(getattr(turn, "output_text", "")))


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
        turn = manager.get_turn(gestalt.AgentGetTurn(turn_id=turn.id))
    return turn


def review_prompt(
    subject: PullRequestSubject,
    pull_request: Mapping[str, Any],
    files: Sequence[PullRequestFile],
    settings: ReviewSettings,
) -> str:
    return json.dumps(
        {
            "task": (
                "Return findings for concrete bugs on RIGHT-side diff lines allowed "
                "by output_contract.line_policy."
            ),
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
                "contract": REVIEW_OUTPUT_CONTRACT,
                "format": (
                    "Return exactly one JSON object with no Markdown wrapper and no "
                    "top-level keys other than findings."
                ),
                "line_policy": line_policy(settings),
                "empty_response": {"findings": []},
                "schema": {
                    "findings": [
                        {
                            "path": "exact changed file path",
                            "line": (
                                "new-file RIGHT-side line number allowed by the "
                                "line_policy"
                            ),
                            "body": (
                                "specific review comment explaining the bug and "
                                "suggested fix"
                            ),
                            "severity": "optional: critical|high|medium|low",
                        }
                    ]
                },
            },
        },
        indent=2,
    )


def self_fix_prompt(
    subject: PullRequestSubject,
    pull_request: Mapping[str, Any],
    *,
    findings: Sequence[ValidatedFinding],
    files: Sequence[ReviewFileContent],
    settings: ReviewSettings,
) -> str:
    return json.dumps(
        {
            "task": (
                "Return full-file replacements that narrowly fix the listed "
                "review findings. Only edit provided files. Return an empty files "
                "array if a safe fix is not clear."
            ),
            "repository": subject.repository,
            "pull_number": subject.pull_number,
            "pull_request": dict(pull_request),
            "findings": [
                {"path": finding.path, "line": finding.line, "body": finding.body}
                for finding in findings
            ],
            "files": [
                {
                    "path": file.path,
                    "content": bounded_text(file.content, settings.max_patch_chars),
                }
                for file in files
            ],
            "output_contract": {
                "contract": SELF_FIX_OUTPUT_CONTRACT,
                "format": (
                    "Return exactly one JSON object with no Markdown wrapper and no "
                    "top-level keys other than commit_message and files."
                ),
                "empty_response": {"commit_message": "", "files": []},
                "schema": {
                    "commit_message": "optional concise Git commit subject",
                    "files": [
                        {
                            "path": "exact path of one provided file",
                            "content": "complete UTF-8 replacement file content",
                        }
                    ],
                },
            },
        },
        indent=2,
    )


def line_policy(settings: ReviewSettings) -> str:
    if settings.changed_lines_only:
        return "Use only added RIGHT-side lines from the provided diff."
    return "Use RIGHT-side lines that are present in the provided diff."


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
            ValidatedFinding(
                path=path, line=raw.line, body=format_comment_body(raw, body)
            )
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


def review_finding_fingerprints(
    subject: PullRequestSubject, finding: ValidatedFinding
) -> ReviewFindingFingerprints:
    exact_payload = {
        "source": REVIEW_FINDING_SOURCE,
        "repository": subject.repository,
        "pull_number": subject.pull_number,
        "path": finding.path.strip().lstrip("/"),
        "side": "RIGHT",
        "line": finding.line,
        "body": finding.body,
    }
    stable_payload = {
        "source": REVIEW_FINDING_SOURCE,
        "repository": subject.repository,
        "pull_number": subject.pull_number,
        "path": finding.path.strip().lstrip("/"),
        "side": "RIGHT",
        "body": normalized_finding_body(finding.body),
    }
    return ReviewFindingFingerprints(
        fingerprint=sha256_payload(exact_payload),
        stable_fingerprint=sha256_payload(stable_payload),
    )


def review_finding_fingerprint(
    subject: PullRequestSubject, finding: ValidatedFinding
) -> str:
    return review_finding_fingerprints(subject, finding).fingerprint


def fingerprint_marker_values(
    fingerprints: Sequence[ReviewFindingFingerprints],
) -> set[str]:
    values: set[str] = set()
    for item in fingerprints:
        values.add(item.fingerprint)
        values.add(item.stable_fingerprint)
    return values


def normalized_finding_body(body: str) -> str:
    return " ".join(body.casefold().split())


def sha256_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def review_comment_body_with_marker(
    body: str, fingerprints: ReviewFindingFingerprints
) -> str:
    return f"{body}\n\n{review_comment_marker(fingerprints)}"


def review_comment_marker(fingerprints: ReviewFindingFingerprints | str) -> str:
    if isinstance(fingerprints, str):
        return (
            "<!-- gestalt:github-review-finding v1 "
            f"fingerprint={fingerprints} source={REVIEW_FINDING_SOURCE} -->"
        )
    return (
        "<!-- gestalt:github-review-finding v2 "
        f"fingerprint={fingerprints.fingerprint} "
        f"stable_fingerprint={fingerprints.stable_fingerprint} "
        f"source={REVIEW_FINDING_SOURCE} -->"
    )


def existing_review_findings(
    subject: PullRequestSubject,
    req: gestalt.Request,
    *,
    enabled: bool,
) -> dict[str, Any]:
    fingerprints: set[str] = set()
    skipped_reasons: list[dict[str, str]] = []
    if not enabled:
        return {"fingerprints": fingerprints, "skippedReasons": skipped_reasons}

    identity = bot_identity_or_none()
    bot_login = identity.login.strip().lower() if identity is not None else ""
    if not bot_login:
        skipped_reasons.append({"threadId": "", "reason": "missing_bot_identity"})
        return {"fingerprints": fingerprints, "skippedReasons": skipped_reasons}

    for thread, reason in provider_review_threads(subject, req):
        thread_id = string_value(thread.get("id"))
        if reason:
            skipped_reasons.append({"threadId": thread_id, "reason": reason})
            continue
        marker, marker_reason = provider_marker_for_duplicate_suppression(
            thread, bot_login=bot_login
        )
        if marker is None:
            skipped_reasons.append({"threadId": thread_id, "reason": marker_reason})
            continue
        fingerprints.add(marker.fingerprint)
        fingerprints.add(marker.stable_fingerprint)
    return {"fingerprints": fingerprints, "skippedReasons": skipped_reasons}


def provider_review_threads(
    subject: PullRequestSubject, req: gestalt.Request
) -> list[tuple[Mapping[str, Any], str]]:
    results: list[tuple[Mapping[str, Any], str]] = []
    after = ""
    identity_kwargs = request_external_identity_kwargs(req)
    for _page in range(AUTO_RESOLVE_MAX_THREAD_PAGES):
        try:
            response = list_pull_request_review_threads(
                GitHubListPullRequestReviewThreadsRequest(
                    owner=subject.owner,
                    repo=subject.repo,
                    pull_number=subject.pull_number,
                    first=100,
                    after=after,
                    comments_first=20,
                    installation_id=subject.installation_id,
                ),
                subject=req.subject,
                **identity_kwargs,
            )
        except Exception as err:
            results.append(({}, f"list_failed: {err}"))
            break

        raw_threads = response.get("threads")
        threads = raw_threads if isinstance(raw_threads, list) else []
        for thread in threads:
            if isinstance(thread, dict):
                results.append((thread, ""))

        page_info = object_value(response.get("pageInfo")) or {}
        if not bool(page_info.get("hasNextPage")):
            break
        after = string_value(page_info.get("endCursor"))
        if not after:
            results.append(({}, "missing_next_page_cursor"))
            break
    else:
        results.append(({}, "thread_page_limit_reached"))
    return results


def provider_marker_for_duplicate_suppression(
    thread: Mapping[str, Any], *, bot_login: str
) -> tuple[ReviewFindingMarker | None, str]:
    if bool(thread.get("isResolved")):
        return None, "already_resolved"
    comments = thread_comments(thread)
    if not comments:
        return None, "missing_marker"
    author_login = string_value(comments[0].get("authorLogin")).lower()
    if author_login != bot_login:
        return None, "not_bot_authored"
    return provider_marker_from_first_comment(comments[0])


def thread_comments(thread: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw_comments = thread.get("comments")
    if not isinstance(raw_comments, list):
        return []
    return [comment for comment in raw_comments if isinstance(comment, dict)]


def has_malformed_thread_comment(thread: Mapping[str, Any]) -> bool:
    raw_comments = thread.get("comments")
    return isinstance(raw_comments, list) and any(
        not isinstance(comment, dict) for comment in raw_comments
    )


def auto_resolve_stale_findings(
    subject: PullRequestSubject,
    req: gestalt.Request,
    *,
    current_fingerprints: set[str],
    enabled: bool,
) -> dict[str, Any]:
    resolved_threads: list[str] = []
    skipped_reasons: list[dict[str, str]] = []
    if not enabled:
        return {
            "resolvedThreads": resolved_threads,
            "skippedResolutionReasons": skipped_reasons,
        }

    identity = bot_identity_or_none()
    bot_login = identity.login.strip().lower() if identity is not None else ""
    if not bot_login:
        skipped_reasons.append({"threadId": "", "reason": "missing_bot_identity"})
        return {
            "resolvedThreads": resolved_threads,
            "skippedResolutionReasons": skipped_reasons,
        }

    after = ""
    identity_kwargs = request_external_identity_kwargs(req)
    for _page in range(AUTO_RESOLVE_MAX_THREAD_PAGES):
        try:
            response = list_pull_request_review_threads(
                GitHubListPullRequestReviewThreadsRequest(
                    owner=subject.owner,
                    repo=subject.repo,
                    pull_number=subject.pull_number,
                    first=100,
                    after=after,
                    comments_first=20,
                    installation_id=subject.installation_id,
                ),
                subject=req.subject,
                **identity_kwargs,
            )
        except Exception as err:
            skipped_reasons.append({"threadId": "", "reason": f"list_failed: {err}"})
            break

        raw_threads = response.get("threads")
        threads = raw_threads if isinstance(raw_threads, list) else []
        for thread in threads:
            if not isinstance(thread, dict):
                continue
            thread_id = string_value(thread.get("id"))
            if not thread_id:
                skipped_reasons.append({"threadId": "", "reason": "missing_thread_id"})
                continue
            decision = review_thread_resolution_decision(
                thread,
                bot_login=bot_login,
                current_fingerprints=current_fingerprints,
            )
            if decision:
                skipped_reasons.append({"threadId": thread_id, "reason": decision})
                continue
            try:
                resolved = resolve_pull_request_review_thread(
                    GitHubResolvePullRequestReviewThreadRequest(
                        owner=subject.owner,
                        repo=subject.repo,
                        pull_number=subject.pull_number,
                        thread_id=thread_id,
                        installation_id=subject.installation_id,
                    ),
                    subject=req.subject,
                    **identity_kwargs,
                )
            except Exception as err:
                skipped_reasons.append(
                    {"threadId": thread_id, "reason": f"resolve_failed: {err}"}
                )
                continue
            resolved_threads.append(string_value(resolved.get("id")) or thread_id)

        page_info = object_value(response.get("pageInfo")) or {}
        if not bool(page_info.get("hasNextPage")):
            break
        after = string_value(page_info.get("endCursor"))
        if not after:
            skipped_reasons.append(
                {"threadId": "", "reason": "missing_next_page_cursor"}
            )
            break
    else:
        skipped_reasons.append({"threadId": "", "reason": "thread_page_limit_reached"})

    return {
        "resolvedThreads": resolved_threads,
        "skippedResolutionReasons": skipped_reasons,
    }


def review_thread_resolution_decision(
    thread: Mapping[str, Any],
    *,
    bot_login: str,
    current_fingerprints: set[str],
) -> str:
    if bool(thread.get("isResolved")):
        return "already_resolved"
    if not bool(thread.get("viewerCanResolve")):
        return "viewer_cannot_resolve"
    if bool(thread.get("commentsTruncated")):
        return "comments_truncated"

    if has_malformed_thread_comment(thread):
        return "malformed_comment"
    comments = thread_comments(thread)
    if not comments:
        return "missing_marker"
    for comment in comments:
        author_login = string_value(comment.get("authorLogin")).lower()
        if author_login != bot_login:
            return "human_reply"

    marker, reason = provider_marker_from_first_comment(comments[0])
    if marker is None:
        return reason
    if (
        marker.fingerprint in current_fingerprints
        or marker.stable_fingerprint in current_fingerprints
    ):
        return "current_finding"
    return ""


def provider_marker_from_first_comment(
    comment: Mapping[str, Any],
) -> tuple[ReviewFindingMarker | None, str]:
    body = string_value(comment.get("body"))
    match = REVIEW_FINDING_MARKER_RE.search(body)
    if match is None:
        return None, "missing_marker"
    fingerprint = match.group("fingerprint")
    source = match.group("source")
    stable_fingerprint = match.group("stable_fingerprint") or fingerprint
    if not REVIEW_FINDING_FINGERPRINT_RE.fullmatch(fingerprint):
        return None, "malformed_marker"
    if not REVIEW_FINDING_FINGERPRINT_RE.fullmatch(stable_fingerprint):
        return None, "malformed_marker"
    if source != REVIEW_FINDING_SOURCE:
        return None, "wrong_marker_source"
    return ReviewFindingMarker(
        fingerprint=fingerprint,
        stable_fingerprint=stable_fingerprint,
        source=source,
    ), ""


def review_result(
    subject: PullRequestSubject,
    *,
    posted: bool,
    comments: int,
    reason: str = "",
    dropped_findings: int = 0,
    suppressed_findings: int = 0,
    resolved_threads: Sequence[str] = (),
    skipped_resolution_reasons: Sequence[Mapping[str, str]] = (),
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": True,
        "posted": posted,
        "comments": comments,
        "repository": subject.repository,
        "pullNumber": subject.pull_number,
        "resolvedThreads": list(resolved_threads),
        "skippedResolutionReasons": [
            dict(reason) for reason in skipped_resolution_reasons
        ],
    }
    if reason:
        result["reason"] = reason
    if dropped_findings:
        result["droppedFindings"] = dropped_findings
    if suppressed_findings:
        result["suppressedFindings"] = suppressed_findings
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


def parse_review_findings_output(value: str) -> list[Any]:
    parsed = parse_single_review_output_object(value)
    extra_keys = sorted(set(parsed) - REVIEW_OUTPUT_KEYS)
    if extra_keys:
        raise RuntimeError(
            "agent output JSON must not include top-level keys other than findings"
        )
    findings = parsed.get("findings")
    if not isinstance(findings, list):
        raise RuntimeError("agent output JSON must include a findings array")
    return findings


def parse_review_fix_output(value: str) -> list[ReviewFixFile]:
    parsed = parse_single_review_output_object(value)
    extra_keys = sorted(set(parsed) - SELF_FIX_OUTPUT_KEYS)
    if extra_keys:
        raise RuntimeError(
            "agent output JSON must not include top-level keys other than "
            "commit_message and files"
        )
    raw_files = parsed.get("files")
    if not isinstance(raw_files, list):
        raise RuntimeError("agent output JSON must include a files array")
    files: list[ReviewFixFile] = []
    seen: set[str] = set()
    for item in raw_files:
        data = object_value(item)
        if data is None:
            raise RuntimeError("agent output files must be objects")
        extra_file_keys = sorted(set(data) - {"path", "content"})
        if extra_file_keys:
            raise RuntimeError(
                "agent output file objects must not include keys other than "
                f"path and content: {', '.join(extra_file_keys)}"
            )
        path = string_value(data.get("path")).lstrip("/")
        content = data.get("content")
        if not path or not isinstance(content, str):
            raise RuntimeError("agent output files require path and content")
        if path in seen:
            raise RuntimeError(f"agent output duplicated file path {path}")
        seen.add(path)
        files.append(ReviewFixFile(path=path, content=content))
    return files


def parse_single_review_output_object(value: str) -> dict[str, Any]:
    text: str = (value or "").strip()
    if not text:
        raise RuntimeError("agent returned empty review output")

    parsed = parse_json_exact(text)
    if parsed is None:
        raise RuntimeError("agent review output must be exactly one JSON object")
    return require_review_output_object(parsed)


def parse_json_exact(value: str) -> Any | None:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def require_review_output_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError("agent output JSON must be an object with a findings array")
    return value
