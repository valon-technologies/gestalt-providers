from typing import Any, cast

from gestalt.gen.v1 import agent_pb2 as _agent_pb2

agent_pb2: Any = cast(Any, _agent_pb2)

_STATUS_NAME_PAIRS = (
    ("AGENT_RUN_STATUS_UNSPECIFIED", "AGENT_EXECUTION_STATUS_UNSPECIFIED"),
    ("AGENT_RUN_STATUS_PENDING", "AGENT_EXECUTION_STATUS_PENDING"),
    ("AGENT_RUN_STATUS_RUNNING", "AGENT_EXECUTION_STATUS_RUNNING"),
    ("AGENT_RUN_STATUS_SUCCEEDED", "AGENT_EXECUTION_STATUS_SUCCEEDED"),
    ("AGENT_RUN_STATUS_FAILED", "AGENT_EXECUTION_STATUS_FAILED"),
    ("AGENT_RUN_STATUS_CANCELED", "AGENT_EXECUTION_STATUS_CANCELED"),
    ("AGENT_RUN_STATUS_WAITING_FOR_INPUT", "AGENT_EXECUTION_STATUS_WAITING_FOR_INPUT"),
)


for legacy_name, current_name in _STATUS_NAME_PAIRS:
    legacy_value = getattr(agent_pb2, legacy_name, None)
    current_value = getattr(agent_pb2, current_name, None)
    if legacy_value is None and current_value is not None:
        setattr(agent_pb2, legacy_name, current_value)
    if current_value is None and legacy_value is not None:
        setattr(agent_pb2, current_name, legacy_value)
