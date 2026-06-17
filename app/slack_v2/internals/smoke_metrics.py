from __future__ import annotations

from opentelemetry import metrics

_runs_counter = None


def record_smoke_run(*, app_id: str = "") -> None:
    global _runs_counter
    if _runs_counter is None:
        meter = metrics.get_meter("gestalt_providers.slack_v2")
        _runs_counter = meter.create_counter(
            "gestalt_providers.slack_v2.smoke_runs",
            unit="{run}",
            description="Number of times the slack_v2 smoke workflow debug endpoint ran.",
        )
    attrs: dict[str, str] = {}
    if app_id.strip():
        attrs["slack.app_id"] = app_id.strip()
    _runs_counter.add(1, attrs)
