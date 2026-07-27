#!/usr/bin/env python3
"""Capture real GitHub App webhook deliveries as committed fixtures.

Requires GITHUB_APP_ID and GITHUB_APP_PRIVATE_KEY (or Secret Manager access via
gcloud). Writes JSON files to the directory passed via --output-dir.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any

from internals.client import create_app_jwt, github_json

FRONT_PORCH = "valon-technologies/front-porch"

TARGETS: list[tuple[str, str, str | None]] = [
    ("pull_request", "opened", None),
    ("pull_request", "synchronize", None),
    ("pull_request", "closed", None),
    ("check_run", "completed", "deploy-fp-prod"),
    ("check_run", "completed", None),
    ("check_suite", "completed", None),
    ("workflow_run", "completed", None),
    ("issue_comment", "created", "peach-deploy-tracking"),
]


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def list_deliveries(per_page: int = 100) -> list[dict[str, Any]]:
    response = github_json("GET", f"/app/hook/deliveries?per_page={per_page}", create_app_jwt())
    return response if isinstance(response, list) else []


def get_delivery(delivery_id: int) -> dict[str, Any]:
    response = github_json("GET", f"/app/hook/deliveries/{delivery_id}", create_app_jwt())
    if not isinstance(response, dict):
        raise RuntimeError(f"unexpected delivery payload for id={delivery_id}")
    return response


def delivery_matches(
    delivery: dict[str, Any],
    *,
    event: str,
    action: str,
    name_hint: str | None,
) -> bool:
    if delivery.get("event") != event:
        return False
    if delivery.get("action") != action:
        return False
    if int(delivery.get("status_code") or 0) < 200 or int(delivery.get("status_code") or 0) >= 300:
        return False

    body = delivery.get("request", {}).get("payload")
    if not isinstance(body, dict):
        return False

    repo = body.get("repository") or {}
    full_name = repo.get("full_name") if isinstance(repo, dict) else None
    if full_name and full_name != FRONT_PORCH:
        return False

    if name_hint == "deploy-fp-prod":
        check_run = body.get("check_run") or {}
        if not isinstance(check_run, dict):
            return False
        return str(check_run.get("name") or "") == "deploy-fp-prod"

    if name_hint == "peach-deploy-tracking":
        comment = body.get("comment") or {}
        if not isinstance(comment, dict):
            return False
        body_text = str(comment.get("body") or "")
        return "peach-deploy-tracking" in body_text

    return True


def capture_fixture(delivery: dict[str, Any], filename: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename
    request = delivery.get("request") if isinstance(delivery.get("request"), dict) else {}
    payload = request.get("payload")
    headers = request.get("headers") if isinstance(request.get("headers"), dict) else {}

    fixture = {
        "delivery_id": delivery.get("id"),
        "event": delivery.get("event"),
        "action": delivery.get("action"),
        "status_code": delivery.get("status_code"),
        "throttled_at": delivery.get("throttled_at"),
        "delivered_at": delivery.get("delivered_at"),
        "redelivery": delivery.get("redelivery"),
        "headers": headers,
        "payload": payload,
    }
    path.write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture GitHub webhook delivery fixtures")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write fixture JSON files (e.g. toolshed ci-cd tests/fixtures/webhooks)",
    )
    parser.add_argument("--per-page", type=int, default=100, help="Deliveries to scan")
    parser.add_argument("--dry-run", action="store_true", help="Print matches without writing")
    args = parser.parse_args()

    deliveries = list_deliveries(args.per_page)
    print(f"Scanned {len(deliveries)} hook deliveries")

    captured = 0
    for event, action, hint in TARGETS:
        suffix = f".{slugify(hint)}" if hint else ""
        filename = f"{event}.{action}{suffix}.json"
        target_path = args.output_dir / filename
        if target_path.exists():
            print(f"skip existing {filename}")
            continue

        match_id: int | None = None
        for summary in deliveries:
            delivery_id = summary.get("id")
            if not isinstance(delivery_id, int):
                continue
            full = get_delivery(delivery_id)
            if delivery_matches(full, event=event, action=action, name_hint=hint):
                match_id = delivery_id
                if args.dry_run:
                    print(
                        f"would capture {filename} from delivery {delivery_id} "
                        f"(status={full.get('status_code')}, throttled_at={full.get('throttled_at')})"
                    )
                else:
                    path = capture_fixture(full, filename, args.output_dir)
                    print(f"captured {path.name} from delivery {delivery_id}")
                captured += 1
                break
            time.sleep(0.05)

        if match_id is None:
            print(f"MISSING {event}.{action}{suffix}")

    return 0 if captured > 0 or args.dry_run else 1


if __name__ == "__main__":
    raise SystemExit(main())
