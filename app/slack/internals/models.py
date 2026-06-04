from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal, TypeAlias


class SlackCallbackType(StrEnum):
    URL_VERIFICATION = "url_verification"
    EVENT_CALLBACK = "event_callback"


class SlackEventType(StrEnum):
    APP_MENTION = "app_mention"
    MESSAGE = "message"
    ASSISTANT_THREAD_STARTED = "assistant_thread_started"
    ASSISTANT_THREAD_CONTEXT_CHANGED = "assistant_thread_context_changed"


class SlackChannelType(StrEnum):
    CHANNEL = "channel"
    GROUP = "group"
    IM = "im"
    MPIM = "mpim"
    APP_HOME = "app_home"


SUPPORTED_EVENT_TYPES = frozenset(event.value for event in SlackEventType)
SLACK_MESSAGE_EVENT_TYPE_BY_CHANNEL_TYPE = {
    SlackChannelType.CHANNEL.value: "message.channels",
    SlackChannelType.GROUP.value: "message.groups",
    SlackChannelType.IM.value: "message.im",
    SlackChannelType.MPIM.value: "message.mpim",
    SlackChannelType.APP_HOME.value: "message.app_home",
}
SUPPORTED_SLACK_EVENT_TYPES = frozenset(
    {
        SlackEventType.APP_MENTION.value,
        SlackEventType.ASSISTANT_THREAD_STARTED.value,
        SlackEventType.ASSISTANT_THREAD_CONTEXT_CHANGED.value,
        *SLACK_MESSAGE_EVENT_TYPE_BY_CHANNEL_TYPE.values(),
    }
)
SUPPORTED_AGENT_ROUTE_EVENT_TYPES = SUPPORTED_EVENT_TYPES | SUPPORTED_SLACK_EVENT_TYPES
SUPPORTED_AGENT_ROUTE_THREAD_MATCHES = frozenset({"any", "root", "reply"})
DIRECT_MESSAGE_CHANNEL_TYPES = frozenset(
    channel.value
    for channel in (
        SlackChannelType.IM,
        SlackChannelType.MPIM,
        SlackChannelType.APP_HOME,
    )
)
ASSISTANT_THREAD_EVENT_TYPES = frozenset(
    {
        SlackEventType.ASSISTANT_THREAD_STARTED.value,
        SlackEventType.ASSISTANT_THREAD_CONTEXT_CHANGED.value,
    }
)

SlackInteractionActionStyle: TypeAlias = Literal["", "primary", "danger"]


def event_type_for_event(event_type: str, channel_type: str) -> str:
    normalized_event_type = event_type.strip().lower()
    if normalized_event_type == SlackEventType.MESSAGE:
        return SLACK_MESSAGE_EVENT_TYPE_BY_CHANNEL_TYPE.get(
            channel_type.strip().lower(), ""
        )
    if normalized_event_type in SUPPORTED_SLACK_EVENT_TYPES:
        return normalized_event_type
    return ""


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
    client_msg_id: str = ""
    addressed_to_bot: bool = False
    assistant_context_present: bool = False
    bot_id: str = ""
    bot_user_id: str = ""
    context_channel_id: str = ""
    files: tuple[dict[str, Any], ...] = ()
    is_bot_event: bool = False
    subtype: str = ""

    @property
    def is_thread_reply(self) -> bool:
        return bool(self.thread_ts and self.thread_ts != self.message_ts)

    @property
    def is_thread_root(self) -> bool:
        return not self.is_thread_reply


@dataclass(frozen=True, slots=True)
class SlackAgentRouteMatch:
    team_ids: tuple[str, ...] = ()
    channel_ids: tuple[str, ...] = ()
    channel_types: tuple[str, ...] = ()
    event_types: tuple[str, ...] = ()
    user_ids: tuple[str, ...] = ()
    bot_ids: tuple[str, ...] = ()
    include_bot_events: bool = False
    addressed_to_bot: bool | None = None
    subtypes: tuple[str, ...] | None = None
    thread: str = "any"

    def matches(self, event: SlackAgentEvent) -> bool:
        if self.team_ids and event.team_id not in self.team_ids:
            return False
        event_channel_ids = tuple(
            dict.fromkeys(
                channel_id
                for channel_id in (event.channel_id, event.context_channel_id)
                if channel_id
            )
        )
        if self.channel_ids and not any(
            channel_id in self.channel_ids for channel_id in event_channel_ids
        ):
            return False
        if self.channel_types and event.channel_type.lower() not in self.channel_types:
            return False
        if self.event_types:
            event_types = frozenset(value.strip().lower() for value in self.event_types)
            slack_event_type = event_type_for_event(
                event.event_type, event.channel_type
            )
            if (
                event.event_type.lower() not in event_types
                and slack_event_type not in event_types
            ):
                return False
        if self.subtypes is not None:
            subtype = event.subtype.lower()
            if not self.subtypes:
                if subtype:
                    return False
            elif subtype not in self.subtypes:
                return False
        if self.thread == "root":
            if not event.is_thread_root:
                return False
        elif self.thread == "reply":
            if not event.is_thread_reply:
                return False
        elif self.thread != "any":
            return False
        if (
            self.addressed_to_bot is not None
            and event.addressed_to_bot != self.addressed_to_bot
        ):
            return False
        if self.user_ids and event.user_id not in self.user_ids:
            return False
        if self.bot_ids and event.bot_id not in self.bot_ids:
            return False
        if event.is_bot_event and not self.include_bot_events and not self.bot_ids:
            return False
        return True

    def explicitly_matches_slack_message_event(self, event: SlackAgentEvent) -> bool:
        if event.event_type != SlackEventType.MESSAGE:
            return False
        if not self.event_types:
            return False
        event_type = event_type_for_event(event.event_type, event.channel_type)
        event_types = frozenset(value.strip().lower() for value in self.event_types)
        return bool(event_type and event_type in event_types)


@dataclass(frozen=True, slots=True)
class SlackAgentRoute:
    id: str = ""
    match: SlackAgentRouteMatch = field(default_factory=SlackAgentRouteMatch)
    run_as_subject_id: str = ""
    workflow: SlackWorkflowConfig | None = None
    assistant: SlackAssistantConfig | None = None
    acknowledgement: SlackAcknowledgementConfig | None = None
    thread_context: SlackThreadContextConfig | None = None


@dataclass(frozen=True, slots=True)
class SlackEventPublishCallback:
    callback_type: str
    event_type: str
    event_id: str
    team_id: str
    enterprise_id: str
    api_app_id: str
    event_context: str
    user_id: str
    bot_id: str
    channel_id: str
    channel_type: str
    subtype: str
    text: str
    message_ts: str
    event_ts: str
    thread_ts: str
    files: tuple[dict[str, Any], ...] = ()
    is_bot_event: bool = False


@dataclass(frozen=True, slots=True)
class SlackEventPublishRouteMatch:
    team_ids: tuple[str, ...] = ()
    channel_ids: tuple[str, ...] = ()
    channel_types: tuple[str, ...] = ()
    event_types: tuple[str, ...] = ()
    subtypes: tuple[str, ...] | None = None
    user_ids: tuple[str, ...] = ()
    bot_ids: tuple[str, ...] = ()
    include_bot_events: bool = False

    def matches(self, event: SlackEventPublishCallback) -> bool:
        if self.team_ids and event.team_id not in self.team_ids:
            return False
        if self.channel_ids and event.channel_id not in self.channel_ids:
            return False
        if self.channel_types and event.channel_type.lower() not in self.channel_types:
            return False
        if self.event_types and event.event_type.lower() not in self.event_types:
            return False
        if self.subtypes is not None:
            if not self.subtypes:
                if event.subtype:
                    return False
            elif event.subtype.lower() not in self.subtypes:
                return False
        if self.user_ids and event.user_id not in self.user_ids:
            return False
        if self.bot_ids and event.bot_id not in self.bot_ids:
            return False
        if event.is_bot_event and not self.include_bot_events and not self.bot_ids:
            return False
        return True


@dataclass(frozen=True, slots=True)
class SlackEventPublishRoute:
    id: str = ""
    match: SlackEventPublishRouteMatch = field(
        default_factory=SlackEventPublishRouteMatch
    )
    workflow_provider: str = ""
    workflow_event_type: str = "slack.event.received"
    source: str = "slack"
    subject: str = ""


@dataclass(frozen=True, slots=True)
class SlackEventPublishConfig:
    routes: tuple[SlackEventPublishRoute, ...] = ()


@dataclass(frozen=True, slots=True)
class SlackEventsConfig:
    publish: SlackEventPublishConfig = field(default_factory=SlackEventPublishConfig)


@dataclass(frozen=True, slots=True)
class SlackBotConfig:
    token: str = ""
    user_id: str = ""


@dataclass(frozen=True, slots=True)
class SlackAssistantConfig:
    enabled: bool = False
    enabled_configured: bool = False
    status: str = "thinking..."
    loading_messages: tuple[str, ...] = ()
    icon_emoji: str = ""
    icon_url: str = ""
    username: str = ""
    suggested_prompts_title: str = ""
    suggested_prompts: tuple[SlackSuggestedPrompt, ...] = ()


@dataclass(frozen=True, slots=True)
class SlackAcknowledgementConfig:
    enabled: bool = True
    reaction: str = ""


@dataclass(frozen=True, slots=True)
class SlackWorkflowConfig:
    provider_name: str = ""
    key_template: str = ""
    definition_id: str = ""


@dataclass(frozen=True, slots=True)
class SlackThreadContextConfig:
    enabled: bool = True
    max_messages: int = 200
    include_user_info: bool = False
    include_bots: bool = True
    include_files: bool = True
    include_file_content: bool = False
    include_image_data: bool = False
    max_file_bytes: int = 200_000


@dataclass(frozen=True, slots=True)
class SlackAgentConfig:
    app_name: str = "slack"
    bot: SlackBotConfig = field(default_factory=SlackBotConfig)
    events: SlackEventsConfig = field(default_factory=SlackEventsConfig)
    assistant: SlackAssistantConfig = field(default_factory=SlackAssistantConfig)
    acknowledgement: SlackAcknowledgementConfig = field(
        default_factory=SlackAcknowledgementConfig
    )
    workflow: SlackWorkflowConfig = field(default_factory=SlackWorkflowConfig)
    thread_context: SlackThreadContextConfig = field(
        default_factory=SlackThreadContextConfig
    )
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
    client_msg_id: str = ""
    workflow_key: str = ""


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
