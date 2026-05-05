from __future__ import annotations

import hashlib
import json
from typing import Any

import gestalt
from google.protobuf import struct_pb2 as _struct_pb2

from .config import (
    GitHubWebhookPolicy,
    GitHubWorkflowPluginTarget,
    WEBHOOK_DEDUPE_CI_INCIDENT,
    WEBHOOK_DEDUPE_DELIVERY,
    WEBHOOK_DEDUPE_PR_HEAD,
    WEBHOOK_DEDUPE_PULL_REQUEST,
    WEBHOOK_INLINE_FINDINGS_ONLY,
    WEBHOOK_TIMELINE_ACTIONABLE_ONLY,
    WEBHOOK_TRIGGER_EVERY_DELIVERY,
    WEBHOOK_TRIGGER_MANUAL_ONLY,
    WEBHOOK_TRIGGER_ONCE_PER_CI_INCIDENT,
    WEBHOOK_TRIGGER_ONCE_PER_HEAD_SHA,
    WEBHOOK_TRIGGER_ONCE_PER_PR,
    effective_policy_operations,
    get_github_config,
)
from .constants import (
    BOT_ADD_LABELS_OPERATION,
    BOT_ADD_REACTION_OPERATION,
    BOT_COMMIT_FILES_OPERATION,
    BOT_CREATE_ISSUE_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_OPERATION,
    BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
    BOT_GET_PULL_REQUEST_OPERATION,
    BOT_LIST_PULL_REQUEST_FILES_OPERATION,
    BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION,
    BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
    BOT_OPEN_PULL_REQUEST_OPERATION,
    BOT_REMOVE_LABELS_OPERATION,
    BOT_REQUEST_REVIEWERS_OPERATION,
    BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION,
    DEFAULT_AGENT_SYSTEM_PROMPT,
    GITHUB_WORKFLOW_SIGNAL_NAME,
    MAX_AGENT_USER_PROMPT_CHARS,
)
from .webhook import bounded_text
from .workflow_dispatch import workflow_signal_data

struct_pb2: Any = _struct_pb2


def build_workflow_signal_or_start_request(
    payload: dict[str, Any],
    summary: dict[str, Any],
    policy: GitHubWebhookPolicy | None = None,
) -> Any:
    idempotency_key = agent_turn_idempotency_key(payload, summary, policy)
    request = gestalt.WorkflowManagerSignalOrStartRunRequest(
        provider_name=workflow_provider(policy),
        workflow_key=agent_session_ref(summary, policy),
        idempotency_key=idempotency_key,
        target=workflow_target(summary, policy),
        signal=gestalt.WorkflowSignal(
            name=GITHUB_WORKFLOW_SIGNAL_NAME,
            idempotency_key=idempotency_key,
        ),
    )
    request.signal.payload.CopyFrom(workflow_signal_payload(payload, summary, policy))
    request.signal.metadata.CopyFrom(agent_turn_metadata(summary, policy))
    return request


def workflow_provider(policy: GitHubWebhookPolicy | None) -> str:
    config = get_github_config()
    if policy is not None and policy.workflow_provider:
        return policy.workflow_provider
    return config.workflow_provider


def workflow_target(
    summary: dict[str, Any], policy: GitHubWebhookPolicy | None = None
) -> Any:
    if policy is not None and policy.workflow_target is not None:
        return workflow_plugin_target(policy.workflow_target)
    return workflow_agent_target(summary, policy)


def workflow_plugin_target(target: GitHubWorkflowPluginTarget) -> Any:
    plugin = gestalt.BoundWorkflowPluginTarget(
        plugin_name=target.plugin_name,
        operation=target.operation,
        connection=target.connection,
        instance=target.instance,
    )
    plugin.input.CopyFrom(dict_to_struct(target.input))
    return gestalt.BoundWorkflowTarget(plugin=plugin)


def workflow_agent_target(
    summary: dict[str, Any], policy: GitHubWebhookPolicy | None = None
) -> Any:
    model_options = agent_model_options(policy)
    agent = gestalt.BoundWorkflowAgentTarget(
        provider_name=agent_provider(policy),
        model=agent_model(policy),
        prompt=workflow_agent_prompt(),
        messages=[
            gestalt.AgentMessage(role="system", text=agent_system_prompt(policy)),
        ],
        tool_refs=agent_tool_refs(policy),
    )
    agent.metadata.CopyFrom(agent_session_metadata(summary, policy))
    if model_options:
        target_options = getattr(agent, "model_options", None)
        if target_options is None:
            target_options = agent.provider_options
        target_options.CopyFrom(dict_to_struct(model_options))
    return gestalt.BoundWorkflowTarget(agent=agent)


def workflow_signal_payload(
    payload: dict[str, Any],
    summary: dict[str, Any],
    policy: GitHubWebhookPolicy | None = None,
) -> Any:
    data = workflow_signal_data(payload, summary, policy)
    if policy is not None:
        metadata = policy_metadata(policy, summary)
        data["webhook_policy"] = metadata
        agent_request = data.get("agent_request")
        if isinstance(agent_request, dict):
            agent_request["policy"] = metadata
    agent_request = data.get("agent_request")
    if not isinstance(agent_request, dict):
        agent_request = {}
        data["agent_request"] = agent_request
    agent_request["user_prompt"] = agent_user_prompt(agent_request, summary, policy)
    return dict_to_struct(data)


def workflow_agent_prompt() -> str:
    return "\n".join(
        [
            "Handle GitHub App webhooks delivered in the final workflow signal batch.",
            "Each signal payload includes summary and compact agent_request fields.",
            "Use agent_request.user_prompt as the current GitHub request.",
        ]
    )


def agent_tool_refs(policy: GitHubWebhookPolicy | None = None) -> list[Any]:
    return [
        gestalt.AgentToolRef(plugin="github", operation=operation)
        for operation in agent_operations(policy)
    ]


def agent_operations(policy: GitHubWebhookPolicy | None = None) -> tuple[str, ...]:
    if policy is not None:
        return effective_policy_operations(policy)
    return (
        BOT_COMMIT_FILES_OPERATION,
        BOT_OPEN_PULL_REQUEST_OPERATION,
        BOT_CREATE_PULL_REQUEST_OPERATION,
    )


def agent_session_metadata(
    summary: dict[str, Any], policy: GitHubWebhookPolicy | None = None
) -> Any:
    metadata = {
        key: summary[key]
        for key in (
            "installation_id",
            "repository",
            "repository_owner",
            "repository_name",
            "number",
            "pull_request_numbers",
            "check_run_id",
            "check_suite_id",
            "workflow_run_id",
            "delivery_id",
            "head_ref",
            "head_sha",
            "base_ref",
            "base_sha",
        )
        if key in summary
    }
    metadata["session_ref"] = agent_session_ref(summary, policy)
    if policy is not None:
        metadata["policy"] = policy_metadata(policy, summary)
    return dict_to_struct({"github": metadata})


def agent_turn_metadata(
    summary: dict[str, Any], policy: GitHubWebhookPolicy | None = None
) -> Any:
    metadata = dict(summary)
    metadata["session_ref"] = agent_session_ref(summary, policy)
    if policy is not None:
        metadata["policy"] = policy_metadata(policy, summary)
    return dict_to_struct({"github": metadata})


def agent_provider(policy: GitHubWebhookPolicy | None) -> str:
    config = get_github_config()
    if policy is not None and policy.agent_provider:
        return policy.agent_provider
    return config.agent_provider


def agent_model(policy: GitHubWebhookPolicy | None) -> str:
    config = get_github_config()
    if policy is not None and policy.agent_model:
        return policy.agent_model
    return config.agent_model


def agent_model_options(policy: GitHubWebhookPolicy | None) -> dict[str, Any]:
    config = get_github_config()
    if policy is not None and policy.agent_model_options is not None:
        return policy.agent_model_options
    return config.agent_model_options


def agent_system_prompt(policy: GitHubWebhookPolicy | None = None) -> str:
    config = get_github_config()
    prompt = ""
    if policy is not None and policy.agent_system_prompt:
        prompt = policy.agent_system_prompt
    elif config.agent_system_prompt:
        prompt = config.agent_system_prompt
    parts: list[str] = [DEFAULT_AGENT_SYSTEM_PROMPT]
    operation_guidance = agent_operation_guidance(policy)
    if operation_guidance:
        parts.append(operation_guidance)
    if prompt:
        parts.append(prompt.strip())
    return "\n\n".join(parts)


def agent_operation_guidance(policy: GitHubWebhookPolicy | None = None) -> str:
    operations = set(agent_operations(policy))
    lines: list[str] = []
    if (
        BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION in operations
        and BOT_LIST_PULL_REQUEST_FILES_OPERATION in operations
    ):
        if BOT_GET_PULL_REQUEST_OPERATION in operations:
            lines.append(
                "Use bot.getPullRequest and bot.listPullRequestFiles to inspect "
                "pull request metadata and diff patches before using "
                "bot.createPullRequestReview for inline review comments."
            )
        else:
            lines.append(
                "Use bot.listPullRequestFiles to inspect diff patches before "
                "using bot.createPullRequestReview for inline review comments."
            )
    elif BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION in operations:
        lines.append(
            "Use bot.createPullRequestReview for inline file/line pull request "
            "review comments."
        )
    if BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION in operations:
        lines.append(
            "Use bot.createPullRequestConversationComment for pull request "
            "timeline comments."
        )
    if BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION in operations:
        lines.append(
            "Use bot.listPullRequestReviewThreads to inspect existing inline "
            "review threads."
        )
    if BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION in operations:
        lines.append(
            "Use bot.listCheckSuiteCheckRuns to expand check suite webhooks into "
            "their individual check runs before diagnosing CI failures."
        )
    if BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION in operations:
        lines.append(
            "Use bot.resolvePullRequestReviewThread only when a review thread is "
            "known to be stale and safe to resolve."
        )
    if BOT_ADD_REACTION_OPERATION in operations:
        lines.append(
            "Use bot.addReaction to react to issues, pull requests, issue comments, "
            "or pull request review comments."
        )
    if BOT_ADD_LABELS_OPERATION in operations:
        lines.append("Use bot.addLabels for configured issue or pull request labels.")
    if BOT_REMOVE_LABELS_OPERATION in operations:
        lines.append(
            "Use bot.removeLabels for configured issue or pull request label removals."
        )
    if BOT_REQUEST_REVIEWERS_OPERATION in operations:
        lines.append(
            "Use bot.requestReviewers to request GitHub users or team slugs as pull "
            "request reviewers."
        )
    if BOT_CREATE_ISSUE_COMMENT_OPERATION in operations:
        lines.append("Use bot.createIssueComment only for issue comments.")
    if policy is not None:
        if policy.comments.timeline_policy == WEBHOOK_TIMELINE_ACTIONABLE_ONLY:
            lines.append(
                "Only create pull request timeline or issue comments for concrete "
                "actionable findings; do not comment just to acknowledge the event."
            )
        if policy.comments.inline_policy == WEBHOOK_INLINE_FINDINGS_ONLY:
            lines.append(
                "Only create inline review comments for concrete line-anchored "
                "findings."
            )
        if policy.comments.suppress_stale_head:
            lines.append(
                "CI signals whose head SHA is no longer the pull request head are "
                "suppressed before this agent runs."
            )
    return "\n".join(lines)


def agent_user_prompt(
    agent_request: dict[str, Any],
    summary: dict[str, Any],
    policy: GitHubWebhookPolicy | None = None,
) -> str:
    lines = [
        "GitHub App webhook:",
        f"installation_id: {summary.get('installation_id', '')}",
        f"event_type: {summary.get('event_type', '')}",
        f"repository: {summary.get('repository', '')}",
        f"action: {summary.get('action', '')}",
        f"sender: {summary.get('sender', '')}",
    ]
    if policy is not None:
        lines.append(f"policy_id: {policy.id}")
        lines.append(f"policy_mode: {policy.action_mode}")
        lines.append(f"trigger_frequency: {policy.trigger.frequency}")
        lines.append(f"dedupe_scope: {policy.dedupe.scope}")
        lines.append(f"timeline_policy: {policy.comments.timeline_policy}")
        lines.append(f"inline_policy: {policy.comments.inline_policy}")
        lines.append(f"suppress_stale_head: {policy.comments.suppress_stale_head}")
        operations = agent_operations(policy)
        if operations:
            lines.append(f"available_operations: {', '.join(operations)}")
    if "number" in summary:
        lines.append(f"number: {summary['number']}")
    if "pull_request_numbers" in summary:
        lines.append(f"pull_request_numbers: {summary['pull_request_numbers']}")
    for key in ("check_run_id", "check_suite_id", "workflow_run_id"):
        if key in summary:
            lines.append(f"{key}: {summary[key]}")
    subject = agent_request.get("subject")
    if isinstance(subject, dict) and subject.get("html_url"):
        lines.append(f"url: {subject['html_url']}")
    for key in ("pull_request", "issue", "comment", "review"):
        value = agent_request.get(key)
        if isinstance(value, dict):
            lines.extend(_prompt_section(key, value))
    ref_lines = _ref_prompt_lines(agent_request)
    if ref_lines:
        lines.extend(["", "ref:"] + ref_lines)
    return bounded_text("\n".join(lines), MAX_AGENT_USER_PROMPT_CHARS)


def _prompt_section(name: str, value: dict[str, Any]) -> list[str]:
    lines = ["", f"{name}:"]
    for key in (
        "number",
        "title",
        "state",
        "html_url",
        "head_ref",
        "base_ref",
        "id",
        "user",
        "body",
    ):
        nested = value.get(key)
        if nested not in ("", 0, None):
            lines.append(f"{key}: {nested}")
    return lines


def _ref_prompt_lines(agent_request: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for key in (
        "ref",
        "base_ref",
        "before",
        "after",
        "compare",
        "ref_type",
        "created",
        "deleted",
        "forced",
    ):
        if key in agent_request:
            lines.append(f"{key}: {agent_request[key]}")
    head_commit = agent_request.get("head_commit")
    if isinstance(head_commit, dict):
        for key in ("id", "message", "url"):
            value = head_commit.get(key)
            if value:
                lines.append(f"head_commit.{key}: {value}")
    return lines


def agent_session_ref(
    summary: dict[str, Any], policy: GitHubWebhookPolicy | None = None
) -> str:
    if policy is not None:
        scoped_ref = policy_dedupe_session_ref(summary, policy)
        if scoped_ref:
            return scoped_ref
    ref = legacy_agent_session_ref(summary)
    if policy is None:
        return ref
    return f"{ref}:policy:{policy.id}"


def legacy_agent_session_ref(summary: dict[str, Any]) -> str:
    installation_id = summary.get("installation_id", "")
    repo = summary.get("repository", "")
    event_ref = ci_event_session_ref(summary, installation_id, repo)
    if event_ref:
        return event_ref
    number = summary.get("number", "")
    if repo and number:
        return f"github:{installation_id}:{repo}:{number}"
    if repo:
        return f"github:{installation_id}:{repo}"
    return f"github:{installation_id}"


def ci_event_session_ref(
    summary: dict[str, Any], installation_id: Any, repo: Any
) -> str:
    if not repo:
        return ""
    event_type = str(summary.get("event_type", "")).strip()
    if event_type not in ("check_run", "check_suite", "workflow_run"):
        return ""
    event_id = summary.get(f"{event_type}_id", "")
    if event_id:
        return f"github:{installation_id}:{repo}:{event_type}:{event_id}"
    delivery_id = str(summary.get("delivery_id", "")).strip()
    if delivery_id:
        return f"github:{installation_id}:{repo}:{event_type}:{delivery_id}"
    payload_sha256 = str(summary.get("payload_sha256", "")).strip()
    if payload_sha256:
        return f"github:{installation_id}:{repo}:{event_type}:payload:{payload_sha256}"
    return f"github:{installation_id}:{repo}:{event_type}:unknown"


def policy_dedupe_session_ref(
    summary: dict[str, Any], policy: GitHubWebhookPolicy
) -> str:
    if policy.dedupe.scope == WEBHOOK_DEDUPE_DELIVERY:
        return ""
    installation_id = summary.get("installation_id", "")
    repo = str(summary.get("repository", "")).strip()
    if not repo:
        return ""
    pr_ref = pull_request_ref(summary)
    if policy.dedupe.scope == WEBHOOK_DEDUPE_PULL_REQUEST and pr_ref:
        return f"github:{installation_id}:{repo}:{pr_ref}:policy:{policy.id}"
    head_sha = str(summary.get("head_sha", "")).strip()
    if policy.dedupe.scope == WEBHOOK_DEDUPE_PR_HEAD and pr_ref and head_sha:
        return (
            f"github:{installation_id}:{repo}:{pr_ref}:head:{head_sha}:"
            f"policy:{policy.id}"
        )
    if policy.dedupe.scope == WEBHOOK_DEDUPE_CI_INCIDENT and pr_ref and head_sha:
        return (
            f"github:{installation_id}:{repo}:ci:{pr_ref}:head:{head_sha}:"
            f"policy:{policy.id}"
        )
    return ""


def pull_request_ref(summary: dict[str, Any]) -> str:
    values = summary.get("pull_request_numbers")
    numbers = pull_request_numbers_value(values)
    if numbers:
        if len(numbers) == 1:
            return f"pr:{numbers[0]}"
        return "prs:" + ",".join(str(item) for item in numbers)
    if not event_number_is_pull_request(summary):
        return ""
    number = positive_int_value(summary.get("number"))
    return f"pr:{number}" if number > 0 else ""


def pull_request_numbers_value(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    numbers: list[int] = []
    seen: set[int] = set()
    for item in value:
        number = positive_int_value(item)
        if number <= 0 or number in seen:
            continue
        seen.add(number)
        numbers.append(number)
    return numbers


def event_number_is_pull_request(summary: dict[str, Any]) -> bool:
    return str(summary.get("event_type", "")).strip() in (
        "pull_request",
        "pull_request_review",
        "pull_request_review_comment",
    )


def positive_int_value(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        item = int(value)
        return item if item > 0 else 0
    return 0


def agent_turn_idempotency_key(
    payload: dict[str, Any],
    summary: dict[str, Any],
    policy: GitHubWebhookPolicy | None = None,
) -> str:
    if policy is not None:
        policy_key = policy_frequency_idempotency_key(summary, policy)
        if policy_key:
            return policy_key
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    repo = summary.get("repository", "")
    event_type = summary.get("event_type", "")
    action = summary.get("action", "")
    if policy is not None:
        return f"github:event:{repo}:policy:{policy.id}:{event_type}:{action}:{digest}"
    return f"github:event:{repo}:{event_type}:{action}:{digest}"


def policy_frequency_idempotency_key(
    summary: dict[str, Any], policy: GitHubWebhookPolicy
) -> str:
    frequency = policy.trigger.frequency
    if frequency in (WEBHOOK_TRIGGER_EVERY_DELIVERY, WEBHOOK_TRIGGER_MANUAL_ONLY):
        return ""
    installation_id = summary.get("installation_id", "")
    repo = str(summary.get("repository", "")).strip()
    if not repo:
        return ""
    pr_ref = pull_request_ref(summary)
    if frequency == WEBHOOK_TRIGGER_ONCE_PER_PR and pr_ref:
        return (
            f"github:trigger:{frequency}:{installation_id}:{repo}:{pr_ref}:"
            f"policy:{policy.id}"
        )
    head_sha = str(summary.get("head_sha", "")).strip()
    if frequency == WEBHOOK_TRIGGER_ONCE_PER_HEAD_SHA and pr_ref and head_sha:
        return (
            f"github:trigger:{frequency}:{installation_id}:{repo}:{pr_ref}:"
            f"head:{head_sha}:policy:{policy.id}"
        )
    if frequency == WEBHOOK_TRIGGER_ONCE_PER_CI_INCIDENT and pr_ref and head_sha:
        return (
            f"github:trigger:{frequency}:{installation_id}:{repo}:ci:{pr_ref}:"
            f"head:{head_sha}:policy:{policy.id}"
        )
    return ""


def policy_metadata(
    policy: GitHubWebhookPolicy, summary: dict[str, Any] | None = None
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "id": policy.id,
        "mode": policy.action_mode,
        "tool_refs": list(effective_policy_operations(policy)),
        "trigger": {
            "frequency": policy.trigger.frequency,
            "include_drafts": policy.trigger.include_drafts,
            "manual_commands": list(policy.trigger.manual_commands),
        },
        "dedupe": {"scope": policy.dedupe.scope},
        "comments": {
            "timeline_policy": policy.comments.timeline_policy,
            "inline_policy": policy.comments.inline_policy,
            "suppress_stale_head": policy.comments.suppress_stale_head,
        },
    }
    if summary is not None:
        metadata["canonical"] = policy_canonical_metadata(summary, policy)
    return metadata


def policy_canonical_metadata(
    summary: dict[str, Any], policy: GitHubWebhookPolicy
) -> dict[str, Any]:
    metadata = {
        "workflow_key": agent_session_ref(summary, policy),
        "dedupe_scope": policy.dedupe.scope,
        "trigger_frequency": policy.trigger.frequency,
    }
    idempotency_key = policy_frequency_idempotency_key(summary, policy)
    if idempotency_key:
        metadata["idempotency_key"] = idempotency_key
    return metadata


def dict_to_struct(data: dict[str, Any]) -> Any:
    struct = struct_pb2.Struct()
    struct.update(data)
    return struct
