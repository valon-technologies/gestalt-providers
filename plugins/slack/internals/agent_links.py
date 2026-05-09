"""Helpers for Gestalt agent-session UI links."""

from __future__ import annotations

import urllib.parse

AGENT_SESSION_ROUTE = "/agents"
AGENT_SESSION_QUERY_PARAM = "session"
AGENT_TURN_QUERY_PARAM = "turn"


def agent_session_href(session_id: str, turn_id: str | None = None) -> str:
    """Return the canonical relative UI href for an agent session or turn."""

    params: dict[str, str] = {}
    if session_id:
        params[AGENT_SESSION_QUERY_PARAM] = session_id
    if turn_id:
        params[AGENT_TURN_QUERY_PARAM] = turn_id
    query = urllib.parse.urlencode(params)
    return f"{AGENT_SESSION_ROUTE}?{query}" if query else AGENT_SESSION_ROUTE


def agent_session_url(
    public_base_url: str,
    session_id: str,
    turn_id: str | None = None,
) -> str:
    """Return an absolute Gestalt UI URL for an agent session or turn.

    The authorization relationship decides whether the recipient may open the
    linked session; this helper only centralizes the stable UI locator.
    """

    base_url = public_base_url.strip().rstrip("/")
    return f"{base_url}{agent_session_href(session_id, turn_id)}"
