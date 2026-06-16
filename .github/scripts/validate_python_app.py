#!/usr/bin/env python3

from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path

SKIP_DIRS = {".venv", "dist", "__pycache__"}


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: validate_python_app.py <app-dir>", file=sys.stderr)
        return 2

    app_dir = Path(sys.argv[1]).resolve()
    pyproject_path = app_dir / "pyproject.toml"
    if not pyproject_path.is_file():
        print(f"expected {pyproject_path} to exist", file=sys.stderr)
        return 2

    config = tomllib.loads(pyproject_path.read_text())
    has_dev_group = "dev" in config.get("dependency-groups", {})
    package_names = declared_packages(config)
    ci_config = config.get("tool", {}).get("gestalt", {}).get("ci", {})
    vulture_ignore_names = ci_config.get("vulture-ignore-names", [])
    if not isinstance(vulture_ignore_names, list) or not all(
        isinstance(name, str) for name in vulture_ignore_names
    ):
        raise SystemExit("tool.gestalt.ci.vulture-ignore-names must be a list of strings")

    sync_cmd = ["uv", "sync", "--frozen"]
    run_cmd = ["uv", "run", "--no-sync"]
    if has_dev_group:
        sync_cmd.extend(["--group", "dev"])
        run_cmd.extend(["--group", "dev"])

    run(sync_cmd, cwd=app_dir, step="install dependencies")
    install_local_gestalt_sdk(app_dir, package_names)

    if "ruff" in package_names:
        run([*run_cmd, "ruff", "check", "."], cwd=app_dir, step="ruff lint")

    if "ty" in package_names:
        run([*run_cmd, "ty", "check", "."], cwd=app_dir, step="ty type check")

    source_paths = python_source_paths(app_dir)
    if "vulture" in package_names and source_paths:
        cmd = [*run_cmd, "vulture", *source_paths]
        if vulture_ignore_names:
            cmd.extend(["--ignore-names", ",".join(vulture_ignore_names)])
        run(cmd, cwd=app_dir, step="vulture unused-code check")

    run_tests(app_dir, package_names, run_cmd)
    return 0


def run_tests(app_dir: Path, package_names: set[str], run_cmd: list[str]) -> None:
    tests_dir = app_dir / "tests"
    top_level_tests = sorted(
        path.name
        for path in app_dir.glob("test*.py")
        if path.is_file()
    )

    if "pytest" in package_names:
        test_targets: list[str] = []
        if tests_dir.is_dir():
            test_targets.append("tests")
        test_targets.extend(top_level_tests)
        if test_targets:
            run([*run_cmd, "pytest", *test_targets], cwd=app_dir, step="pytest")
        return

    if tests_dir.is_dir():
        run(
            [*run_cmd, "python", "-m", "unittest", "discover", "-s", "tests", "-p", "test*.py"],
            cwd=app_dir,
            step="unittest discovery",
        )
        return

    for test_file in top_level_tests:
        module_name = Path(test_file).stem
        run([*run_cmd, "python", "-m", "unittest", module_name], cwd=app_dir, step="unittest")


def python_source_paths(app_dir: Path) -> list[str]:
    return sorted(
        str(path.relative_to(app_dir))
        for path in app_dir.rglob("*.py")
        if not should_skip(path, app_dir)
    )


def should_skip(path: Path, app_dir: Path) -> bool:
    relative_parts = path.relative_to(app_dir).parts
    return any(part in SKIP_DIRS or part.endswith(".egg-info") for part in relative_parts)


def declared_packages(config: dict) -> set[str]:
    packages: set[str] = set()

    for dep in config.get("project", {}).get("dependencies", []):
        name = dependency_name(dep)
        if name:
            packages.add(name)

    for group in config.get("dependency-groups", {}).values():
        if not isinstance(group, list):
            continue
        for dep in group:
            name = dependency_name(dep)
            if name:
                packages.add(name)

    return packages


def install_local_gestalt_sdk(app_dir: Path, package_names: set[str]) -> None:
    if "gestalt-sdk" not in package_names:
        return

    gestalt_checkout = os.environ.get("GESTALT_CHECKOUT")
    if not gestalt_checkout:
        return

    sdk_dir = Path(gestalt_checkout).resolve() / "sdk" / "python"
    if not (sdk_dir / "pyproject.toml").is_file():
        raise SystemExit(f"expected local Gestalt Python SDK at {sdk_dir}")

    python_bin = app_dir / ".venv" / "bin" / "python"
    run(
        ["uv", "pip", "install", "--python", str(python_bin), "--reinstall", str(sdk_dir)],
        cwd=app_dir,
        step="install local Gestalt Python SDK",
    )


def dependency_name(dep: object) -> str | None:
    if not isinstance(dep, str):
        return None
    match = re.match(r"\s*([A-Za-z0-9_.-]+)", dep)
    if not match:
        return None
    return match.group(1).lower().replace("_", "-")


def run(cmd: list[str], *, cwd: Path, step: str) -> None:
    print(f"+ {shlex.join(cmd)}", flush=True)
    try:
        subprocess.run(cmd, cwd=cwd, check=True)
    except subprocess.CalledProcessError as exc:
        command = shlex.join(cmd)
        print(
            f"\nValidation step failed: {step}\n"
            f"Working directory: {cwd}\n"
            f"Exit code: {exc.returncode}\n"
            f"To reproduce locally: cd {shlex.quote(str(cwd))} && {command}",
            file=sys.stderr,
        )
        raise SystemExit(exc.returncode) from None


if __name__ == "__main__":
    raise SystemExit(main())
