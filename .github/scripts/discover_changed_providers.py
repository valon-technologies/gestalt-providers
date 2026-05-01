#!/usr/bin/env python3
"""Discover which provider validation and UI jobs CI should run.

Reads the changed file list (via ``git diff base..head`` for ``pull_request``
events, or treats every change as in-scope for ``push`` events), derives the
Go inter-provider dependency graph from ``go.mod`` ``replace`` directives, and
produces a payload describing exactly which downstream jobs need to run.

The payload is emitted as JSON on stdout and, when ``GITHUB_OUTPUT`` /
``GITHUB_STEP_SUMMARY`` are set in the environment, also written there for
consumption by the workflow.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable

PACKAGE_ROOTS: tuple[str, ...] = (
    "agent",
    "runtime",
    "plugins",
    "auth",
    "authorization",
    "external_credentials",
    "s3",
    "indexeddb",
    "workflow",
    "cache",
    "secrets",
    "ui",
)

# Workflows that drive CI behavior. Edits to any other workflow file (e.g. the
# release workflows) do not affect CI matrix selection.
CI_WORKFLOW_FILES: frozenset[str] = frozenset({
    ".github/workflows/ci.yml",
})

# Scripts referenced by ci.yml. Edits to any other script under
# ``.github/scripts/`` are release-only and don't affect CI matrix selection.
CI_SCRIPTS: frozenset[str] = frozenset({
    ".github/scripts/checkout_gestalt_ref.sh",
    ".github/scripts/setup_test_env.sh",
    ".github/scripts/validate_python_plugin.py",
    ".github/scripts/write_provider_release_metadata.py",
    ".github/scripts/discover_changed_providers.py",
})

# Provider trees whose contents the UI E2E job exercises directly. Any change
# under one of these dirs forces ``run_ui_e2e``.
UI_E2E_PROVIDER_TREES: tuple[str, ...] = (
    "ui/default",
    "indexeddb/relationaldb",
    "external_credentials/default",
)

# Provider trees whose contents the integration test target exercises. Any
# change under one of these dirs forces ``run_integration``.
INTEGRATION_PROVIDER_TREES: tuple[str, ...] = (
    "integration",
    "external_credentials/default",
    "indexeddb/relationaldb",
    "workflow/indexeddb",
)

REPLACE_RE = re.compile(
    r"^\s*replace\s+github\.com/valon-technologies/gestalt-providers/(\S+)\s*=>\s*(\S+)",
    re.MULTILINE,
)


def _needs_sibling_gestalt(plugin_dir: Path) -> bool:
    """A plugin needs a sibling gestalt checkout if its go.mod still uses a
    local-replace directive for the gestalt SDK (i.e., it has not yet been
    migrated to a published `require v0.0.1-alpha.N` pin)."""
    gomod = plugin_dir / "go.mod"
    if not gomod.is_file():
        return False
    try:
        text = gomod.read_text(encoding="utf-8")
    except Exception:
        return False
    return bool(
        re.search(
            r"^\s*replace\s+github\.com/valon-technologies/gestalt/sdk/go\s*=>",
            text,
            re.MULTILINE,
        )
    )


def discover_plugins(repo: Path) -> list[dict]:
    """Return the list of plugin matrix entries for the repo."""
    plugins: list[dict] = []
    for root_name in PACKAGE_ROOTS:
        root = repo / root_name
        if not root.is_dir():
            continue
        for manifest in sorted(root.glob("*/manifest.yaml")):
            plugin_dir = manifest.parent
            plugins.append(
                {
                    "dir": plugin_dir.relative_to(repo).as_posix(),
                    "name": f"{root_name}/{plugin_dir.name}",
                    "root": root_name,
                    "has_go": any(p.suffix == ".go" for p in plugin_dir.glob("*.go")),
                    "has_python": (plugin_dir / "pyproject.toml").is_file(),
                    "has_rust": (plugin_dir / "Cargo.toml").is_file(),
                    "needs_sibling_gestalt": _needs_sibling_gestalt(plugin_dir),
                }
            )
    return plugins


def collect_go_modules(repo: Path) -> dict[str, set[str]]:
    """Map every in-repo Go module dir to the set of in-repo Go module dirs it
    depends on (resolved from ``replace`` directives)."""
    forward: dict[str, set[str]] = {}
    candidate_modules: set[str] = set()
    for gomod in repo.rglob("go.mod"):
        if any(part in {".git", "node_modules"} for part in gomod.parts):
            continue
        candidate_modules.add(gomod.parent.relative_to(repo).as_posix())

    for gomod in repo.rglob("go.mod"):
        if any(part in {".git", "node_modules"} for part in gomod.parts):
            continue
        m_dir = gomod.parent.relative_to(repo).as_posix()
        deps: set[str] = set()
        for match in REPLACE_RE.finditer(gomod.read_text(encoding="utf-8")):
            rel_path = match.group(2)
            target = (gomod.parent / rel_path).resolve()
            try:
                rel_target = target.relative_to(repo).as_posix()
            except ValueError:
                continue
            if rel_target in candidate_modules:
                deps.add(rel_target)
        forward[m_dir] = deps
    return forward


def owning_module(path: str, modules: Iterable[str]) -> str | None:
    """Return the deepest module dir that owns ``path``, or ``None``."""
    matches = [m for m in modules if path == m or path.startswith(m + "/")]
    if not matches:
        return None
    return max(matches, key=len)


def transitive_targets(start: str, forward: dict[str, set[str]]) -> set[str]:
    """Return ``{start}`` plus everything reachable from it in ``forward``."""
    seen: set[str] = set()
    stack = [start]
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(forward.get(node, ()))
    return seen


def build_consumers(
    plugins: list[dict],
    forward: dict[str, set[str]],
) -> dict[str, set[str]]:
    """Map every Go module dir to the set of plugin dirs that transitively
    depend on it (including the plugin's own owning module)."""
    modules = set(forward.keys())
    consumers: dict[str, set[str]] = {m: set() for m in modules}
    for plugin in plugins:
        plugin_module = owning_module(plugin["dir"], modules)
        if plugin_module is None:
            continue
        for reachable in transitive_targets(plugin_module, forward):
            consumers.setdefault(reachable, set()).add(plugin["dir"])
    return consumers


def is_under(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix + "/")


def compute_payload(
    repo: Path,
    event_name: str,
    changed: list[str] | None,
) -> dict:
    """Pure function: given the repo state, event, and changed file list,
    return the discovery payload."""
    plugins = discover_plugins(repo)
    plugin_dirs = {p["dir"] for p in plugins}
    forward = collect_go_modules(repo)
    consumers = build_consumers(plugins, forward)
    modules = set(forward.keys())

    selected: set[str] = set()
    run_ui_lint = False
    run_ui_tests = False
    run_ui_e2e = False
    run_integration = False
    run_all = event_name != "pull_request"
    reasons: list[str] = []

    if run_all:
        reasons.append(f"event={event_name}: running everything")
    else:
        files = changed or []
        for path in files:
            if not path:
                continue

            if path in CI_WORKFLOW_FILES or path in CI_SCRIPTS:
                run_all = True
                reasons.append(f"{path}: CI-critical file -> run everything")
                break
            if path.startswith(".github/workflows/") or path.startswith(".github/scripts/"):
                reasons.append(f"{path}: release-only or unrelated -> ignored")
                continue
            if path.startswith(".github/"):
                reasons.append(f"{path}: meta file -> ignored")
                continue

            if is_under(path, "ui"):
                if not run_ui_lint:
                    reasons.append(f"{path}: triggers UI lint/tests/e2e")
                run_ui_lint = True
                run_ui_tests = True
                run_ui_e2e = True

            for tree in UI_E2E_PROVIDER_TREES:
                if is_under(path, tree) and not run_ui_e2e:
                    run_ui_e2e = True
                    reasons.append(f"{path}: triggers UI e2e ({tree})")
                    break

            for tree in INTEGRATION_PROVIDER_TREES:
                if is_under(path, tree) and not run_integration:
                    run_integration = True
                    reasons.append(f"{path}: triggers integration tests ({tree})")
                    break

            parts = Path(path).parts
            if not parts or parts[0] not in PACKAGE_ROOTS:
                reasons.append(f"{path}: outside provider roots -> ignored")
                continue

            handled = False

            if len(parts) >= 2:
                candidate = f"{parts[0]}/{parts[1]}"
                if candidate in plugin_dirs:
                    selected.add(candidate)
                    plugin_module = owning_module(candidate, modules)
                    if plugin_module is not None:
                        extras = consumers.get(plugin_module, set()) - {candidate}
                        for cons in extras:
                            selected.add(cons)
                        if extras:
                            reasons.append(
                                f"{path}: selects {candidate} + Go consumers "
                                f"{sorted(extras)}"
                            )
                        else:
                            reasons.append(f"{path}: selects {candidate}")
                    else:
                        reasons.append(f"{path}: selects {candidate}")
                    handled = True

            if not handled:
                file_module = owning_module(path, modules)
                if file_module is not None:
                    cons = consumers.get(file_module, set())
                    selected.update(cons)
                    if cons:
                        reasons.append(
                            f"{path}: shared module {file_module} -> consumers "
                            f"{sorted(cons)}"
                        )
                    else:
                        reasons.append(
                            f"{path}: shared module {file_module} has no plugin consumers"
                        )

    if run_all:
        out_plugins = list(plugins)
        run_ui_lint = True
        run_ui_tests = True
        run_ui_e2e = True
        run_integration = True
    else:
        out_plugins = [p for p in plugins if p["dir"] in selected]
        if not reasons:
            reasons.append("no relevant changes detected")

    return {
        "plugins": out_plugins,
        "count": len(out_plugins),
        "scope": "all" if run_all else "changed",
        "run_validate": "true" if out_plugins else "false",
        "run_ui_lint": "true" if run_ui_lint else "false",
        "run_ui_tests": "true" if run_ui_tests else "false",
        "run_ui_e2e": "true" if run_ui_e2e else "false",
        "run_integration": "true" if run_integration else "false",
        "reasons": reasons,
    }


def changed_files_from_git(base: str, head: str) -> list[str]:
    out = subprocess.check_output(
        ["git", "diff", "--name-only", f"{base}..{head}"],
        text=True,
    )
    return [line for line in out.splitlines() if line.strip()]


def write_github_outputs(payload: dict) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as fh:
            fh.write(f"plugins<<EOF\n{json.dumps(payload['plugins'])}\nEOF\n")
            for key in (
                "count",
                "scope",
                "run_validate",
                "run_ui_lint",
                "run_ui_tests",
                "run_ui_e2e",
                "run_integration",
            ):
                fh.write(f"{key}={payload[key]}\n")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        lines = ["## CI discovery", "", f"- scope: `{payload['scope']}`"]
        lines.append(f"- plugin count: `{payload['count']}`")
        lines.append(f"- run_validate: `{payload['run_validate']}`")
        lines.append(f"- run_ui_lint: `{payload['run_ui_lint']}`")
        lines.append(f"- run_ui_tests: `{payload['run_ui_tests']}`")
        lines.append(f"- run_ui_e2e: `{payload['run_ui_e2e']}`")
        if payload["reasons"]:
            lines.append("")
            lines.append("### Reasons")
            for reason in payload["reasons"]:
                lines.append(f"- {reason}")
        if payload["plugins"]:
            lines.append("")
            lines.append("### Plugins selected")
            for plugin in payload["plugins"]:
                lines.append(f"- {plugin['name']}")
        with open(summary_path, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--head-sha", default="")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args(argv)

    repo = Path(args.repo_root).resolve()

    changed: list[str] | None = None
    if args.event_name == "pull_request":
        if not args.base_sha or not args.head_sha:
            print(
                "ERROR: --base-sha and --head-sha are required for pull_request events",
                file=sys.stderr,
            )
            return 2
        changed = changed_files_from_git(args.base_sha, args.head_sha)

    payload = compute_payload(repo, args.event_name, changed)
    json.dump(payload, sys.stdout)
    sys.stdout.write("\n")
    write_github_outputs(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
