from __future__ import annotations

import gestalt

from .store_records import StoredSession, StoredTurn, StoredTurnEvent
from .subject_id import agent_actor_from_created_by_subject_id


def agent_session(session: StoredSession, *, summary_only: bool = False) -> gestalt.AgentSession:
    return gestalt.AgentSession(
        id=session.session_id,
        provider_name=session.provider_name,
        model=session.model,
        client_ref=session.client_ref,
        state=session.state,
        metadata=None if summary_only else session.metadata,
        created_by=agent_actor_from_created_by_subject_id(session.created_by_subject_id),
        created_at=session.created_at,
        updated_at=session.updated_at,
        last_turn_at=session.last_turn_at,
    )


def agent_turn(turn: StoredTurn, *, summary_only: bool = False) -> gestalt.AgentTurn:
    return gestalt.AgentTurn(
        id=turn.turn_id,
        session_id=turn.session_id,
        provider_name=turn.provider_name,
        model=turn.model,
        status=turn.status,
        messages=[] if summary_only else turn.messages,
        output=None if summary_only else turn.output,
        status_message=turn.status_message,
        execution_ref=turn.execution_ref,
        created_by=agent_actor_from_created_by_subject_id(turn.created_by_subject_id),
        created_at=turn.created_at,
        started_at=turn.started_at,
        completed_at=turn.completed_at,
    )


def agent_turn_event(event: StoredTurnEvent) -> gestalt.AgentTurnEvent:
    return gestalt.AgentTurnEvent(
        id=event.event_id,
        turn_id=event.turn_id,
        seq=event.seq,
        type=event.event_type,
        source=event.source,
        visibility=event.visibility,
        data=event.data,
        created_at=event.created_at,
    )
