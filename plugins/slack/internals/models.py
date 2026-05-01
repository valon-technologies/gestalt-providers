from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from types import TracebackType
from typing import Any, Literal, Protocol, TypeAlias


class SlackCallbackType(StrEnum):
    URL_VERIFICATION = "url_verification"
    EVENT_CALLBACK = "event_callback"


class SlackEventType(StrEnum):
    APP_MENTION = "app_mention"
    MESSAGE = "message"
    ASSISTANT_THREAD_STARTED = "assistant_thread_started"
    ASSISTANT_THREAD_CONTEXT_CHANGED = "assistant_thread_context_changed"


class SlackChannelType(StrEnum):
    IM = "im"
    MPIM = "mpim"


SUPPORTED_EVENT_TYPES = frozenset(event.value for event in SlackEventType)
DIRECT_MESSAGE_CHANNEL_TYPES = frozenset(
    channel.value for channel in (SlackChannelType.IM, SlackChannelType.MPIM)
)
ASSISTANT_THREAD_EVENT_TYPES = frozenset(
    {
        SlackEventType.ASSISTANT_THREAD_STARTED.value,
        SlackEventType.ASSISTANT_THREAD_CONTEXT_CHANGED.value,
    }
)

SlackInteractionActionStyle: TypeAlias = Literal["", "primary", "danger"]


@dataclass(frozen=True, slots=True)
class SlackSuggestedPrompt:
    title: str
    message: str

    def as_slack_payload(self) -> dict[str, str]:
        return {"title": self.title, "message": self.message}


@dataclass(frozen=True, slots=True)
class SlackInteractionAction:
    action_id: str
    label: str
    value: str
    style: SlackInteractionActionStyle = ""


@dataclass(frozen=True, slots=True)
class SlackAgentEvent:
    callback_type: str
    event_type: str
    event_id: str
    team_id: str
    user_id: str
    channel_id: str
    channel_type: str
    text: str
    message_ts: str
    thread_ts: str
    reply_thread_ts: str
    files: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class SlackAgentRouteMatch:
    team_ids: tuple[str, ...] = ()
    channel_ids: tuple[str, ...] = ()
    channel_types: tuple[str, ...] = ()
    event_types: tuple[str, ...] = ()
    user_ids: tuple[str, ...] = ()

    def matches(self, event: SlackAgentEvent) -> bool:
        if self.team_ids and event.team_id not in self.team_ids:
            return False
        if self.channel_ids and event.channel_id not in self.channel_ids:
            return False
        if self.channel_types and event.channel_type.lower() not in self.channel_types:
            return False
        if self.event_types and event.event_type.lower() not in self.event_types:
            return False
        if self.user_ids and event.user_id not in self.user_ids:
            return False
        return True


@dataclass(frozen=True, slots=True)
class SlackAgentRoute:
    id: str = ""
    match: SlackAgentRouteMatch = field(default_factory=SlackAgentRouteMatch)
    agent_provider: str = ""
    agent_model: str = ""
    agent_system_prompt: str = ""
    agent_provider_options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SlackBotConfig:
    token: str = ""


@dataclass(frozen=True, slots=True)
class SlackAssistantConfig:
    enabled: bool = False
    status: str = "thinking..."
    loading_messages: tuple[str, ...] = ()
    icon_emoji: str = ""
    icon_url: str = ""
    username: str = ""
    suggested_prompts_title: str = ""
    suggested_prompts: tuple[SlackSuggestedPrompt, ...] = ()


@dataclass(frozen=True, slots=True)
class SlackAcknowledgementConfig:
    reaction: str = ""


@dataclass(frozen=True, slots=True)
class SlackWorkflowConfig:
    provider_name: str = ""


@dataclass(frozen=True, slots=True)
class SlackAgentConfig:
    plugin_name: str = "slack"
    bot: SlackBotConfig = field(default_factory=SlackBotConfig)
    assistant: SlackAssistantConfig = field(default_factory=SlackAssistantConfig)
    acknowledgement: SlackAcknowledgementConfig = field(
        default_factory=SlackAcknowledgementConfig
    )
    workflow: SlackWorkflowConfig = field(default_factory=SlackWorkflowConfig)
    agent_provider: str = ""
    agent_model: str = ""
    agent_system_prompt: str = ""
    agent_provider_options: dict[str, Any] = field(default_factory=dict)
    routes: tuple[SlackAgentRoute, ...] = ()


@dataclass(frozen=True, slots=True)
class SlackReplyRef:
    team_id: str
    channel_id: str
    message_ts: str
    reply_thread_ts: str
    event_id: str
    subject_id: str
    expires_at: int
    user_id: str = ""
    channel_type: str = ""
    route_id: str = ""


@dataclass(frozen=True, slots=True)
class SlackInteractionRef:
    team_id: str
    channel_id: str
    channel_type: str
    message_ts: str
    reply_thread_ts: str
    workflow_key: str
    reply_ref: str
    subject_id: str
    user_id: str
    route_id: str
    action_id: str
    action_value: str
    expires_at: int


class WorkflowManager(Protocol):
    def __enter__(self) -> WorkflowManager:
        ...

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> bool | None:
        ...

    def signal_or_start_run(self, request: Any) -> Any:
        ...


class WorkflowManagerFactory(Protocol):
    def __call__(self) -> WorkflowManager:
        ...
