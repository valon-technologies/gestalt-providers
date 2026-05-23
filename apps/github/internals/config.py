from __future__ import annotations

import os
import json
import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, cast

import gestalt

from .constants import (
    BOT_COMMIT_FILES_OPERATION,
    BOT_CREATE_ISSUE_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_OPERATION,
    BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
    BOT_OPEN_PULL_REQUEST_OPERATION,
    BOT_OPERATION_ORDER,
    DEFAULT_WEBHOOK_EVENTS,
    DEFAULT_POLICY_OPERATIONS_BY_MODE,
    GITHUB_DEFAULT_API_BASE_URL,
    GITHUB_DEFAULT_GRAPHQL_BASE_URL,
    GITHUB_DEFAULT_WEB_BASE_URL,
    REVIEW_PULL_REQUEST_OPERATION,
    WEBHOOK_POLICY_ACTION_MODES,
    WEBHOOK_POLICY_OBSERVE_MODE,
)


_POLICY_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_STORE_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")

WEBHOOK_TRIGGER_EVERY_DELIVERY = "every_delivery"
WEBHOOK_TRIGGER_ONCE_PER_PR = "once_per_pr"
WEBHOOK_TRIGGER_ONCE_PER_HEAD_SHA = "once_per_head_sha"
WEBHOOK_TRIGGER_ONCE_PER_CI_INCIDENT = "once_per_ci_incident"
WEBHOOK_TRIGGER_MANUAL_ONLY = "manual_only"
WEBHOOK_TRIGGER_FREQUENCIES = (
    WEBHOOK_TRIGGER_EVERY_DELIVERY,
    WEBHOOK_TRIGGER_ONCE_PER_PR,
    WEBHOOK_TRIGGER_ONCE_PER_HEAD_SHA,
    WEBHOOK_TRIGGER_ONCE_PER_CI_INCIDENT,
    WEBHOOK_TRIGGER_MANUAL_ONLY,
)

WEBHOOK_MANUAL_COMMAND_CONTAINS = "contains"
WEBHOOK_MANUAL_COMMAND_EXACT = "exact"
WEBHOOK_MANUAL_COMMAND_MATCH_MODES = (
    WEBHOOK_MANUAL_COMMAND_CONTAINS,
    WEBHOOK_MANUAL_COMMAND_EXACT,
)

WEBHOOK_DEDUPE_DELIVERY = "delivery"
WEBHOOK_DEDUPE_PULL_REQUEST = "pull_request"
WEBHOOK_DEDUPE_PR_HEAD = "pr_head"
WEBHOOK_DEDUPE_CI_INCIDENT = "ci_incident"
WEBHOOK_DEDUPE_SCOPES = (
    WEBHOOK_DEDUPE_DELIVERY,
    WEBHOOK_DEDUPE_PULL_REQUEST,
    WEBHOOK_DEDUPE_PR_HEAD,
    WEBHOOK_DEDUPE_CI_INCIDENT,
)

WEBHOOK_TIMELINE_ALLOW = "allow"
WEBHOOK_TIMELINE_NEVER = "never"
WEBHOOK_TIMELINE_ACTIONABLE_ONLY = "actionable_only"
WEBHOOK_TIMELINE_POLICIES = (
    WEBHOOK_TIMELINE_ALLOW,
    WEBHOOK_TIMELINE_NEVER,
    WEBHOOK_TIMELINE_ACTIONABLE_ONLY,
)

WEBHOOK_INLINE_ALLOW = "allow"
WEBHOOK_INLINE_NEVER = "never"
WEBHOOK_INLINE_FINDINGS_ONLY = "findings_only"
WEBHOOK_INLINE_POLICIES = (
    WEBHOOK_INLINE_ALLOW,
    WEBHOOK_INLINE_NEVER,
    WEBHOOK_INLINE_FINDINGS_ONLY,
)

WEBHOOK_PREFERENCE_SUBJECT_PULL_REQUEST_AUTHOR = "pull_request_author"
WEBHOOK_PREFERENCE_SUBJECT_COMMENT_AUTHOR = "comment_author"
WEBHOOK_PREFERENCE_SUBJECT_SENDER = "sender"
WEBHOOK_PREFERENCE_SUBJECTS = (
    WEBHOOK_PREFERENCE_SUBJECT_PULL_REQUEST_AUTHOR,
    WEBHOOK_PREFERENCE_SUBJECT_COMMENT_AUTHOR,
    WEBHOOK_PREFERENCE_SUBJECT_SENDER,
)

SELF_FIX_DISABLED = "disabled"
SELF_FIX_SUGGEST = "suggest"
SELF_FIX_BRANCH_COMMIT = "branch_commit"
SELF_FIX_PULL_REQUEST = "pull_request"
SELF_FIX_MODES = (
    SELF_FIX_DISABLED,
    SELF_FIX_SUGGEST,
    SELF_FIX_BRANCH_COMMIT,
    SELF_FIX_PULL_REQUEST,
)

ACTION_PREFERENCES_FAILURE_CONFIG_DEFAULT = "config_default"
ACTION_PREFERENCES_FAILURE_MODES = (ACTION_PREFERENCES_FAILURE_CONFIG_DEFAULT,)


@dataclass(frozen=True, slots=True)
class GitHubWebhookPolicyMatch:
    events: tuple[str, ...] = ()
    actions: tuple[str, ...] = ()
    statuses: tuple[str, ...] = ()
    conclusions: tuple[str, ...] = ()
    repositories: tuple[str, ...] = ()
    branches: tuple[str, ...] = ()
    check_names: tuple[str, ...] = ()
    workflow_names: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GitHubWorkflowAppTarget:
    app_name: str
    operation: str
    connection: str = ""
    instance: str = ""
    credential_mode: str = ""
    input: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GitHubWebhookTrigger:
    frequency: str = WEBHOOK_TRIGGER_EVERY_DELIVERY
    include_drafts: bool = True
    manual_commands: tuple[str, ...] = ()
    manual_command_match: str = WEBHOOK_MANUAL_COMMAND_CONTAINS
    require_app_mention: bool = False


@dataclass(frozen=True, slots=True)
class GitHubWebhookDedupe:
    scope: str = WEBHOOK_DEDUPE_DELIVERY


@dataclass(frozen=True, slots=True)
class GitHubWebhookComments:
    timeline_policy: str = WEBHOOK_TIMELINE_ALLOW
    inline_policy: str = WEBHOOK_INLINE_ALLOW
    suppress_stale_head: bool = False


@dataclass(frozen=True, slots=True)
class GitHubWebhookPolicy:
    id: str
    display_name: str = ""
    description: str = ""
    match: GitHubWebhookPolicyMatch = field(default_factory=GitHubWebhookPolicyMatch)
    trigger: GitHubWebhookTrigger = field(default_factory=GitHubWebhookTrigger)
    dedupe: GitHubWebhookDedupe = field(default_factory=GitHubWebhookDedupe)
    comments: GitHubWebhookComments = field(default_factory=GitHubWebhookComments)
    workflow_provider: str = ""
    agent_provider: str = ""
    agent_model: str = ""
    agent_system_prompt: str = ""
    agent_model_options: dict[str, Any] | None = None
    workflow_target: GitHubWorkflowAppTarget | None = None
    action_mode: str = WEBHOOK_POLICY_OBSERVE_MODE
    allow_code_review_comments: bool = True
    allow_self_fix: bool = True
    self_fix_mode: str = SELF_FIX_DISABLED
    allowed_operations: tuple[str, ...] = ()
    action_preference_subject: str = ""
    action_preferences: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class GitHubActionPreferencesConfig:
    enabled: bool = False
    indexeddb_provider: str = ""
    store: str = ""
    failure_mode: str = ACTION_PREFERENCES_FAILURE_CONFIG_DEFAULT


@dataclass(frozen=True, slots=True)
class GitHubAppConfig:
    app_id: str = ""
    private_key: str = ""
    private_key_path: str = ""
    api_base_url: str = GITHUB_DEFAULT_API_BASE_URL
    graphql_base_url: str = GITHUB_DEFAULT_GRAPHQL_BASE_URL
    web_base_url: str = GITHUB_DEFAULT_WEB_BASE_URL
    webhook_events: tuple[str, ...] = DEFAULT_WEBHOOK_EVENTS
    webhook_events_configured: bool = False
    webhook_policies: tuple[GitHubWebhookPolicy, ...] = ()
    workflow_provider: str = ""
    ignore_bot_sender: bool = True
    agent_provider: str = ""
    agent_model: str = ""
    agent_system_prompt: str = ""
    agent_model_options: dict[str, Any] = field(default_factory=dict)
    action_preferences: GitHubActionPreferencesConfig = field(
        default_factory=GitHubActionPreferencesConfig
    )


@dataclass(frozen=True, slots=True)
class GitHubBotIdentity:
    name: str
    login: str
    user_id: str
    email: str
    slug: str = ""


@dataclass(frozen=True, slots=True)
class GitHubUserIdentity:
    name: str
    login: str
    user_id: str
    email: str


_github_config = GitHubAppConfig()
_github_bot_identity: GitHubBotIdentity | None = None


def configure_from_mapping(
    config: dict[str, Any], provider_name: str = "github"
) -> GitHubAppConfig:
    global _github_bot_identity, _github_config

    _github_config = github_config_from_mapping(config, provider_name=provider_name)
    _github_bot_identity = None
    return _github_config


def get_github_config() -> GitHubAppConfig:
    return _github_config


def get_cached_bot_identity() -> GitHubBotIdentity | None:
    return _github_bot_identity


def set_cached_bot_identity(identity: GitHubBotIdentity) -> None:
    global _github_bot_identity

    _github_bot_identity = identity


def github_config_from_mapping(
    config: dict[str, Any], provider_name: str = "github"
) -> GitHubAppConfig:
    app_id = (
        config_string(config, "appId", "app_id")
        or os.environ.get("GITHUB_APP_ID", "").strip()
    )
    private_key = config_string(
        config, "appPrivateKey", "privateKey", "app_private_key", "private_key"
    )
    private_key_env = config_string(
        config,
        "appPrivateKeyEnv",
        "privateKeyEnv",
        "app_private_key_env",
        "private_key_env",
    )
    if not private_key:
        env_name = private_key_env or "GITHUB_APP_PRIVATE_KEY"
        private_key = os.environ.get(env_name, "").strip()

    private_key_path = config_string(
        config, "appPrivateKeyPath", "privateKeyPath", "app_private_key_path"
    )
    if not private_key_path:
        private_key_path = os.environ.get("GITHUB_APP_PRIVATE_KEY_PATH", "").strip()

    webhook_events = config_string_list(config, "webhookEvents", "webhook_events")
    workflow_provider = workflow_config_string(config, "provider")
    action_preferences = parse_action_preferences_config(config, provider_name)
    webhook_policies = parse_webhook_policies(config)
    api_base_url = (
        config_string(config, "apiBaseUrl", "api_base_url")
        or GITHUB_DEFAULT_API_BASE_URL
    ).rstrip("/")
    graphql_base_url = derive_graphql_base_url(
        config_string(config, "graphqlBaseUrl", "graphql_base_url"),
        api_base_url,
    )
    web_base_url = (
        config_string(config, "webBaseUrl", "web_base_url")
        or GITHUB_DEFAULT_WEB_BASE_URL
    ).rstrip("/")

    return GitHubAppConfig(
        app_id=app_id,
        private_key=normalize_private_key(private_key),
        private_key_path=private_key_path,
        api_base_url=api_base_url,
        graphql_base_url=graphql_base_url,
        web_base_url=web_base_url,
        webhook_events=tuple(
            event.lower()
            for event in (
                webhook_events
                if webhook_events is not None
                else list(DEFAULT_WEBHOOK_EVENTS)
            )
        ),
        webhook_events_configured=webhook_events is not None,
        webhook_policies=webhook_policies,
        workflow_provider=workflow_provider,
        ignore_bot_sender=config_bool(
            config, "ignoreBotSender", "ignore_bot_sender", default=True
        ),
        agent_provider=agent_config_string(config, "provider"),
        agent_model=agent_config_string(config, "model"),
        agent_system_prompt=agent_config_string(
            config, "systemPrompt", "system_prompt", "prompt"
        ),
        agent_model_options=agent_config_dict(config, "modelOptions", "model_options"),
        action_preferences=action_preferences,
    )


def parse_action_preferences_config(
    config: dict[str, Any], provider_name: str
) -> GitHubActionPreferencesConfig:
    if "actionPreferences" not in config and "action_preferences" not in config:
        return GitHubActionPreferencesConfig()
    action_preferences = config.get(
        "actionPreferences", config.get("action_preferences")
    )
    if not isinstance(action_preferences, dict):
        raise ValueError("actionPreferences must be an object")
    pref_config = cast(dict[str, Any], action_preferences)
    indexeddb_provider = config_string(
        pref_config, "indexeddb", "indexeddbProvider", "indexeddb_provider"
    )
    store = config_string(pref_config, "store") or derive_action_preferences_store_name(
        provider_name
    )
    if not _STORE_NAME_RE.fullmatch(store):
        raise ValueError(f"actionPreferences.store must match {_STORE_NAME_RE.pattern}")
    failure_mode = enum_string(
        pref_config,
        "failureMode",
        "actionPreferences.failureMode",
        ACTION_PREFERENCES_FAILURE_MODES,
        ACTION_PREFERENCES_FAILURE_CONFIG_DEFAULT,
        "failure_mode",
    )
    return GitHubActionPreferencesConfig(
        enabled=True,
        indexeddb_provider=indexeddb_provider,
        store=store,
        failure_mode=failure_mode,
    )


def derive_action_preferences_store_name(provider_name: str) -> str:
    raw = provider_name.strip() or "github"
    slug = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_") or "github"
    return f"{slug}_action_preferences"


def derive_graphql_base_url(explicit: str, api_base_url: str) -> str:
    value = explicit.strip().rstrip("/")
    if value:
        return value
    if api_base_url == GITHUB_DEFAULT_API_BASE_URL:
        return GITHUB_DEFAULT_GRAPHQL_BASE_URL
    if api_base_url.rstrip("/").endswith("/api/v3"):
        parsed = urllib.parse.urlparse(api_base_url)
        graphql_path = parsed.path.rstrip("/")[: -len("/api/v3")] + "/api/graphql"
        return urllib.parse.urlunparse(
            parsed._replace(path=graphql_path, params="", query="", fragment="")
        ).rstrip("/")
    return api_base_url.rstrip("/") + "/graphql"


def parse_webhook_policies(config: dict[str, Any]) -> tuple[GitHubWebhookPolicy, ...]:
    raw = config.get("webhookPolicies", config.get("webhook_policies"))
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError("webhookPolicies must be a list")

    policies: list[GitHubWebhookPolicy] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"webhookPolicies[{index}] must be an object")
        policy_config = cast(dict[str, Any], item)
        policy_id = config_string(policy_config, "id")
        if not policy_id:
            raise ValueError(f"webhookPolicies[{index}].id is required")
        if not _POLICY_ID_RE.fullmatch(policy_id):
            raise ValueError(
                f"webhookPolicies[{index}].id must match {_POLICY_ID_RE.pattern}"
            )
        if policy_id in seen_ids:
            raise ValueError(f"duplicate webhook policy id {policy_id!r}")
        seen_ids.add(policy_id)

        match_config = config_dict(policy_config, "match")
        trigger = parse_policy_trigger(policy_config, index)
        dedupe = parse_policy_dedupe(policy_config, index)
        comments = parse_policy_comments(policy_config, index)
        workflow_config = policy_config_object(policy_config, "workflow", index)
        workflow_target = parse_policy_workflow_target(workflow_config, index)
        validate_policy_comments_for_workflow_target(workflow_target, comments, index)
        agent_config = config_dict(policy_config, "agent")
        action_config = config_dict(policy_config, "action")
        action_mode = (
            config_string(action_config, "mode") or WEBHOOK_POLICY_OBSERVE_MODE
        )
        if action_mode not in WEBHOOK_POLICY_ACTION_MODES:
            raise ValueError(
                f"webhookPolicies[{index}].action.mode must be one of "
                + ", ".join(WEBHOOK_POLICY_ACTION_MODES)
            )
        allow_code_review_comments = optional_bool(
            action_config,
            "allowCodeReviewComments",
            "allow_code_review_comments",
            path=f"webhookPolicies[{index}].action.allowCodeReviewComments",
            default=True,
        )
        allow_self_fix = optional_bool(
            action_config,
            "allowSelfFix",
            "allow_self_fix",
            path=f"webhookPolicies[{index}].action.allowSelfFix",
            default=True,
        )
        self_fix_mode = enum_string(
            action_config,
            "selfFixMode",
            f"webhookPolicies[{index}].action.selfFixMode",
            SELF_FIX_MODES,
            SELF_FIX_DISABLED,
            "self_fix_mode",
        )
        action_preference_subject = enum_string(
            action_config,
            "preferenceSubject",
            f"webhookPolicies[{index}].action.preferenceSubject",
            WEBHOOK_PREFERENCE_SUBJECTS,
            "",
            "preference_subject",
        )

        allowed_operations = policy_allowed_operations(
            action_config, action_mode, index
        )
        validate_policy_action_gates_for_workflow_target(
            workflow_target, allow_code_review_comments, index
        )
        model_options = (
            config_dict(agent_config, "modelOptions", "model_options")
            if "modelOptions" in agent_config or "model_options" in agent_config
            else None
        )
        policies.append(
            GitHubWebhookPolicy(
                id=policy_id,
                display_name=config_string(
                    policy_config, "displayName", "display_name"
                ),
                description=config_string(policy_config, "description"),
                match=GitHubWebhookPolicyMatch(
                    events=lower_string_tuple(match_config, "events"),
                    actions=lower_string_tuple(match_config, "actions"),
                    statuses=lower_string_tuple(match_config, "statuses"),
                    conclusions=lower_string_tuple(match_config, "conclusions"),
                    repositories=string_tuple(match_config, "repositories"),
                    branches=string_tuple(match_config, "branches"),
                    check_names=string_tuple(match_config, "checkNames", "check_names"),
                    workflow_names=string_tuple(
                        match_config, "workflowNames", "workflow_names"
                    ),
                ),
                trigger=trigger,
                dedupe=dedupe,
                comments=comments,
                workflow_provider=config_string(workflow_config, "provider"),
                agent_provider=config_string(agent_config, "provider"),
                agent_model=config_string(agent_config, "model"),
                agent_system_prompt=config_string(
                    agent_config, "systemPrompt", "system_prompt", "prompt"
                ),
                agent_model_options=model_options,
                workflow_target=workflow_target,
                action_mode=action_mode,
                allow_code_review_comments=allow_code_review_comments,
                allow_self_fix=allow_self_fix,
                self_fix_mode=self_fix_mode,
                allowed_operations=allowed_operations,
                action_preference_subject=action_preference_subject,
            )
        )
    return tuple(policies)


def parse_policy_trigger(
    policy_config: dict[str, Any], policy_index: int
) -> GitHubWebhookTrigger:
    trigger_config = policy_config_object(policy_config, "trigger", policy_index)
    frequency = enum_string(
        trigger_config,
        "frequency",
        f"webhookPolicies[{policy_index}].trigger.frequency",
        WEBHOOK_TRIGGER_FREQUENCIES,
        WEBHOOK_TRIGGER_EVERY_DELIVERY,
    )
    include_drafts = optional_bool(
        trigger_config,
        "includeDrafts",
        "include_drafts",
        path=f"webhookPolicies[{policy_index}].trigger.includeDrafts",
        default=True,
    )
    manual_commands = string_tuple(trigger_config, "manualCommands", "manual_commands")
    manual_command_match = enum_string(
        trigger_config,
        "manualCommandMatch",
        f"webhookPolicies[{policy_index}].trigger.manualCommandMatch",
        WEBHOOK_MANUAL_COMMAND_MATCH_MODES,
        WEBHOOK_MANUAL_COMMAND_CONTAINS,
        "manual_command_match",
    )
    require_app_mention = optional_bool(
        trigger_config,
        "requireAppMention",
        "require_app_mention",
        path=f"webhookPolicies[{policy_index}].trigger.requireAppMention",
        default=False,
    )
    if manual_commands and require_app_mention:
        raise ValueError(
            f"webhookPolicies[{policy_index}].trigger.requireAppMention cannot "
            "be combined with trigger.manualCommands"
        )
    if (
        frequency == WEBHOOK_TRIGGER_MANUAL_ONLY
        and not manual_commands
        and not require_app_mention
    ):
        raise ValueError(
            f"webhookPolicies[{policy_index}].trigger.manualCommands or "
            "trigger.requireAppMention is required when trigger.frequency is "
            "manual_only"
        )
    return GitHubWebhookTrigger(
        frequency=frequency,
        include_drafts=include_drafts,
        manual_commands=manual_commands,
        manual_command_match=manual_command_match,
        require_app_mention=require_app_mention,
    )


def parse_policy_dedupe(
    policy_config: dict[str, Any], policy_index: int
) -> GitHubWebhookDedupe:
    dedupe_config = policy_config_object(policy_config, "dedupe", policy_index)
    return GitHubWebhookDedupe(
        scope=enum_string(
            dedupe_config,
            "scope",
            f"webhookPolicies[{policy_index}].dedupe.scope",
            WEBHOOK_DEDUPE_SCOPES,
            WEBHOOK_DEDUPE_DELIVERY,
        )
    )


def parse_policy_comments(
    policy_config: dict[str, Any], policy_index: int
) -> GitHubWebhookComments:
    comments_config = policy_config_object(policy_config, "comments", policy_index)
    return GitHubWebhookComments(
        timeline_policy=enum_string(
            comments_config,
            "timelinePolicy",
            f"webhookPolicies[{policy_index}].comments.timelinePolicy",
            WEBHOOK_TIMELINE_POLICIES,
            WEBHOOK_TIMELINE_ALLOW,
            "timeline_policy",
        ),
        inline_policy=enum_string(
            comments_config,
            "inlinePolicy",
            f"webhookPolicies[{policy_index}].comments.inlinePolicy",
            WEBHOOK_INLINE_POLICIES,
            WEBHOOK_INLINE_ALLOW,
            "inline_policy",
        ),
        suppress_stale_head=optional_bool(
            comments_config,
            "suppressStaleHead",
            "suppress_stale_head",
            path=f"webhookPolicies[{policy_index}].comments.suppressStaleHead",
            default=False,
        ),
    )


def effective_policy_operations(policy: GitHubWebhookPolicy) -> tuple[str, ...]:
    operations = list(policy.allowed_operations)
    if policy.comments.timeline_policy == WEBHOOK_TIMELINE_NEVER:
        operations = [
            operation
            for operation in operations
            if operation
            not in {
                BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
                BOT_CREATE_ISSUE_COMMENT_OPERATION,
            }
        ]
    if policy.comments.inline_policy == WEBHOOK_INLINE_NEVER:
        operations = [
            operation
            for operation in operations
            if operation != BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION
        ]
    if not policy.allow_code_review_comments:
        operations = [
            operation
            for operation in operations
            if operation != BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION
        ]
    if not policy.allow_self_fix or policy.self_fix_mode in {
        SELF_FIX_DISABLED,
        SELF_FIX_SUGGEST,
    }:
        operations = [
            operation
            for operation in operations
            if operation
            not in {
                BOT_COMMIT_FILES_OPERATION,
                BOT_OPEN_PULL_REQUEST_OPERATION,
                BOT_CREATE_PULL_REQUEST_OPERATION,
            }
        ]
    elif policy.self_fix_mode == SELF_FIX_BRANCH_COMMIT:
        operations = [
            operation
            for operation in operations
            if operation
            not in {
                BOT_OPEN_PULL_REQUEST_OPERATION,
                BOT_CREATE_PULL_REQUEST_OPERATION,
            }
        ]
    return tuple(operations)


def _parse_workflow_app_call_config(
    app_config: dict[str, Any], path: str
) -> GitHubWorkflowAppTarget:
    if "credential_mode" in app_config:
        raise ValueError(f"{path}.credential_mode is not supported; use credentialMode")

    input_value: dict[str, Any] = {}
    input_path = f"{path}.input"
    if "input" in app_config:
        input_config = app_config.get("input")
        if not isinstance(input_config, dict):
            raise ValueError(f"{input_path} must be an object")
        input_value = dict(input_config)
    validate_struct_compatible(input_value, input_path)
    credential_mode = optional_string(
        app_config, "credentialMode", f"{path}.credentialMode"
    ).lower()
    if credential_mode not in ("", "none", "user"):
        raise ValueError(
            f'{path}.credentialMode "{credential_mode}" is not supported'
        )

    return GitHubWorkflowAppTarget(
        app_name=required_string(app_config, "name", f"{path}.name"),
        operation=required_string(app_config, "operation", f"{path}.operation"),
        connection=optional_string(app_config, "connection", f"{path}.connection"),
        instance=optional_string(app_config, "instance", f"{path}.instance"),
        credential_mode=credential_mode,
        input=input_value,
    )


def parse_policy_workflow_target(
    workflow_config: dict[str, Any], policy_index: int
) -> GitHubWorkflowAppTarget | None:
    if "target" not in workflow_config:
        return None
    target_path = f"webhookPolicies[{policy_index}].workflow.target"
    target_config = required_config_object(workflow_config, "target", target_path)

    steps_config = target_config.get("steps")
    steps_path = f"{target_path}.steps"
    if not isinstance(steps_config, list) or not steps_config:
        raise ValueError(f"{steps_path} must be a non-empty array")
    for index, step_config in enumerate(steps_config):
        step_path = f"{steps_path}[{index}]"
        if not isinstance(step_config, dict):
            raise ValueError(f"{step_path} must be an object")
        step_map = cast(dict[str, Any], step_config)
        app_config = step_map.get("app")
        if app_config is None:
            continue
        if not isinstance(app_config, dict):
            raise ValueError(f"{step_path}.app must be an object")
        return _parse_workflow_app_call_config(app_config, f"{step_path}.app")
    raise ValueError(f"{steps_path} must include an app step")


def validate_policy_comments_for_workflow_target(
    target: GitHubWorkflowAppTarget | None,
    comments: GitHubWebhookComments,
    policy_index: int,
) -> None:
    if target is None:
        return
    if (
        target.app_name == "github"
        and target.operation == REVIEW_PULL_REQUEST_OPERATION
        and comments.inline_policy == WEBHOOK_INLINE_NEVER
    ):
        raise ValueError(
            f"webhookPolicies[{policy_index}].comments.inlinePolicy cannot be "
            "never when workflow.target uses github.reviewPullRequest"
        )


def validate_policy_action_gates_for_workflow_target(
    target: GitHubWorkflowAppTarget | None,
    allow_code_review_comments: bool,
    policy_index: int,
) -> None:
    if target is None:
        return
    if (
        target.app_name == "github"
        and target.operation == REVIEW_PULL_REQUEST_OPERATION
        and not allow_code_review_comments
    ):
        raise ValueError(
            f"webhookPolicies[{policy_index}].action.allowCodeReviewComments cannot "
            "be false when workflow.target uses github.reviewPullRequest"
        )


def policy_config_object(
    config: dict[str, Any], key: str, policy_index: int
) -> dict[str, Any]:
    if key not in config:
        return {}
    value = config.get(key)
    if isinstance(value, dict):
        return dict(value)
    raise ValueError(f"webhookPolicies[{policy_index}].{key} must be an object")


def required_config_object(
    config: dict[str, Any], key: str, path: str
) -> dict[str, Any]:
    if key not in config:
        raise ValueError(f"{path} is required")
    value = config.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be an object")
    return dict(value)


def required_string(config: dict[str, Any], key: str, path: str) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path} is required")
    return value.strip()


def optional_string(config: dict[str, Any], key: str, path: str) -> str:
    if key not in config:
        return ""
    value = config.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{path} must be a string")
    return value.strip()


def optional_bool(config: dict[str, Any], *keys: str, path: str, default: bool) -> bool:
    for key in keys:
        if key not in config:
            continue
        value = config.get(key)
        if isinstance(value, bool):
            return value
        raise ValueError(f"{path} must be a boolean")
    return default


def enum_string(
    config: dict[str, Any],
    key: str,
    path: str,
    values: tuple[str, ...],
    default: str,
    *aliases: str,
) -> str:
    value = config_string(config, key, *aliases)
    if not value:
        return default
    if value not in values:
        raise ValueError(f"{path} must be one of {', '.join(values)}")
    return value


def validate_struct_compatible(input_value: dict[str, Any], path: str) -> None:
    try:
        converter = getattr(gestalt, "json_from_native", None)
        normalized = (
            converter(input_value, path=path)
            if callable(converter)
            else json.loads(json.dumps(input_value, allow_nan=False))
        )
    except Exception as err:
        raise ValueError(f"{path} must be JSON-compatible") from err
    if not isinstance(normalized, dict):
        raise ValueError(f"{path} must be JSON-compatible")


def policy_allowed_operations(
    action_config: dict[str, Any], action_mode: str, policy_index: int
) -> tuple[str, ...]:
    configured = config_string_list(
        action_config, "allowedOperations", "allowed_operations"
    )
    if configured is None:
        configured = list(DEFAULT_POLICY_OPERATIONS_BY_MODE[action_mode])

    unknown = sorted(
        {operation for operation in configured if operation not in BOT_OPERATION_ORDER}
    )
    if unknown:
        raise ValueError(
            f"webhookPolicies[{policy_index}].action.allowedOperations contains "
            f"unknown operation(s): {', '.join(unknown)}"
        )

    configured_set = set(configured)
    return tuple(
        operation for operation in BOT_OPERATION_ORDER if operation in configured_set
    )


def lower_string_tuple(config: dict[str, Any], *keys: str) -> tuple[str, ...]:
    return tuple(item.lower() for item in string_tuple(config, *keys))


def string_tuple(config: dict[str, Any], *keys: str) -> tuple[str, ...]:
    return tuple(config_string_list(config, *keys) or [])


def config_string(config: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = config.get(key)
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, int):
            return str(value)
    return ""


def config_dict(config: dict[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = config.get(key)
        if isinstance(value, dict):
            return dict(value)
    return {}


def workflow_config_string(config: dict[str, Any], *keys: str) -> str:
    workflow = root_workflow_config(config)
    return config_string(workflow, *keys)


def root_workflow_config(config: dict[str, Any]) -> dict[str, Any]:
    if "workflow" not in config:
        return {}
    workflow = config.get("workflow")
    if not isinstance(workflow, dict):
        raise ValueError("workflow must be an object")
    workflow_config = dict(workflow)
    if "target" in workflow_config:
        raise ValueError(
            "workflow.target is not supported; configure "
            "webhookPolicies[].workflow.target instead"
        )
    return workflow_config


def agent_config_string(config: dict[str, Any], *keys: str) -> str:
    agent = config_dict(config, "agent")
    return config_string(agent, *keys)


def agent_config_dict(config: dict[str, Any], *keys: str) -> dict[str, Any]:
    agent = config_dict(config, "agent")
    return config_dict(agent, *keys)


def config_string_list(config: dict[str, Any], *keys: str) -> list[str] | None:
    for key in keys:
        if key not in config:
            continue
        value = config.get(key)
        if isinstance(value, list):
            return [
                item.strip() for item in value if isinstance(item, str) and item.strip()
            ]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
    return None


def config_bool(config: dict[str, Any], *keys: str, default: bool) -> bool:
    for key in keys:
        value = config.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off"}:
                return False
    return default


def normalize_private_key(value: str) -> str:
    value = value.strip()
    if "\\n" in value and "\n" not in value:
        value = value.replace("\\n", "\n")
    return value
