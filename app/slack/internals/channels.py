import hashlib
import time

from .client import SlackAPIError, SlackClientError, slack_get
from .helpers import bool_field, map_field, string_field

CHANNEL_NAME_CACHE_TTL_SECONDS = 600.0
CHANNEL_NAME_CACHE_MAX_ENTRIES = 1024

_cache: dict[tuple[str, str], tuple[float, str]] = {}


def channel_name(token: str, channel: str) -> str:
    """Return the name of a public or private channel, or "" if unavailable.

    DMs, group DMs, and failed lookups return "" so callers can treat the name
    as optional display information. Failures are not cached. The cache is
    keyed by token so one caller's access never serves a name to another.
    """
    if not channel:
        return ""
    key = (hashlib.sha256(token.encode("utf-8")).hexdigest(), channel)
    now = time.monotonic()
    cached = _cache.get(key)
    if cached is not None and cached[0] > now:
        return cached[1]

    try:
        data = slack_get("conversations.info", {"channel": channel}, token)
    except (SlackAPIError, SlackClientError):
        return ""

    info = map_field(data, "channel")
    is_named = (
        bool_field(info, "is_channel") is True or bool_field(info, "is_group") is True
    ) and not (bool_field(info, "is_im") is True or bool_field(info, "is_mpim") is True)
    name = string_field(info, "name") if is_named else ""

    if len(_cache) >= CHANNEL_NAME_CACHE_MAX_ENTRIES:
        _cache.clear()
    _cache[key] = (now + CHANNEL_NAME_CACHE_TTL_SECONDS, name)
    return name


def clear_channel_name_cache() -> None:
    _cache.clear()
