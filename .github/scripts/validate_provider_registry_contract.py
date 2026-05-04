#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
from typing import Any

import yaml


INDEX_SCHEMA_NAME = "gestaltd-provider-index"
INDEX_SCHEMA_VERSION = 1
CATALOG_SCHEMA_NAME = "gestaltd-provider-catalog"
CATALOG_SCHEMA_VERSION = 1
SOURCE_PREFIX = "github.com/valon-technologies/gestalt-providers/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate provider registry files through gestaltd CLI contract commands."
    )
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument("--gestaltd", default="gestaltd", help="Path to gestaltd")
    return parser.parse_args()


def load_yaml(path: pathlib.Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def file_url(path: pathlib.Path) -> str:
    return urllib.parse.urlunparse(("file", "", str(path), "", "", ""))


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.returncode != 0:
        print("$ " + " ".join(cmd), file=sys.stderr)
        print(result.stdout, file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        raise SystemExit(result.returncode)
    return result.stdout


def require_package(packages: dict[str, Any], suffix: str) -> str:
    source = SOURCE_PREFIX + suffix
    if source not in packages:
        raise SystemExit(f"provider index missing {source}")
    return source


def semver_sort_key(version: str) -> tuple[tuple[int, int, int], int, tuple[Any, ...], str]:
    public, _, _build = version.partition("+")
    core, _, prerelease = public.partition("-")
    parts = core.split(".")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        return ((0, 0, 0), 0, (), version)
    nums = tuple(int(part) for part in parts)
    pre_key: tuple[Any, ...] = ()
    if prerelease:
        pre_key = tuple(
            (0, int(part)) if part.isdigit() else (1, part)
            for part in prerelease.split(".")
        )
    return (nums, 0 if prerelease else 1, pre_key, version)


def is_prerelease_version(version: str) -> bool:
    public, _, _build = version.partition("+")
    return "-" in public


def latest_installable_version(versions: dict[str, Any]) -> str | None:
    ordered_versions = sorted(versions, key=semver_sort_key, reverse=True)
    for version in ordered_versions:
        entry = versions.get(version) or {}
        if not bool(entry.get("yanked")) and not is_prerelease_version(version):
            return version
    for version in ordered_versions:
        entry = versions.get(version) or {}
        if not bool(entry.get("yanked")):
            return version
    return None


def validate_latest_installable_selection_rules() -> None:
    if (
        latest_installable_version(
            {
                "1.2.0": {"yanked": True},
                "1.1.0-alpha.1": {},
                "1.0.0": {},
            }
        )
        != "1.0.0"
    ):
        raise SystemExit("latestInstallableVersion must prefer non-yanked stable releases")
    if (
        latest_installable_version(
            {
                "1.2.0": {"yanked": True},
                "1.1.0-alpha.2": {},
                "1.1.0-alpha.1": {},
            }
        )
        != "1.1.0-alpha.2"
    ):
        raise SystemExit("latestInstallableVersion must fall back to non-yanked prereleases")
    if latest_installable_version({"1.0.0": {"yanked": True}}) is not None:
        raise SystemExit("latestInstallableVersion must be null when all releases are yanked")


def validate_catalog(index: dict[str, Any], catalog: dict[str, Any]) -> None:
    validate_latest_installable_selection_rules()
    if index.get("schema") != INDEX_SCHEMA_NAME:
        raise SystemExit("provider-index.yaml has wrong schema")
    if int(index.get("schemaVersion") or 0) != INDEX_SCHEMA_VERSION:
        raise SystemExit("provider-index.yaml has wrong schemaVersion")
    if catalog.get("schema") != CATALOG_SCHEMA_NAME:
        raise SystemExit("registry/catalog.json has wrong schema")
    if int(catalog.get("schemaVersion") or 0) != CATALOG_SCHEMA_VERSION:
        raise SystemExit("registry/catalog.json has wrong schemaVersion")
    packages = index.get("packages") or {}
    providers = catalog.get("providers") or []
    if not isinstance(packages, dict) or not packages:
        raise SystemExit("provider-index.yaml has no packages")
    if not isinstance(providers, list) or not providers:
        raise SystemExit("registry/catalog.json has no providers")
    provider_by_package = {provider.get("package"): provider for provider in providers}
    for source, package in packages.items():
        if source not in provider_by_package:
            raise SystemExit(f"catalog missing indexed provider {source}")
        versions = package.get("versions") or {}
        catalog_versions = provider_by_package[source].get("versions") or []
        if len(catalog_versions) != len(versions):
            raise SystemExit(f"catalog version count for {source} does not match index")
        expected_latest = latest_installable_version(versions)
        actual_latest = provider_by_package[source].get("latestInstallableVersion")
        if actual_latest != expected_latest:
            raise SystemExit(
                f"catalog latestInstallableVersion for {source} is {actual_latest!r}, "
                f"want {expected_latest!r}"
            )
    generic_versions = [
        (source, version)
        for source, package in packages.items()
        for version, entry in (package.get("versions") or {}).items()
        if "generic" in (entry.get("platforms") or [])
    ]
    if not generic_versions:
        raise SystemExit("provider index does not include a generic artifact target")
    ui_default = provider_by_package.get(SOURCE_PREFIX + "ui/default")
    if not ui_default:
        raise SystemExit("catalog missing ui/default")
    if (ui_default.get("configTarget") or {}).get("requiredSet", {}).get("path") != "/":
        raise SystemExit("catalog ui/default configTarget must require path=/")


def write_base_config(config_path: pathlib.Path, index_path: pathlib.Path) -> None:
    config_path.write_text(
        "\n".join(
            [
                "apiVersion: gestaltd.config/v5",
                "providerRepositories:",
                "  local:",
                f"    url: {file_url(index_path)}",
                "plugins: {}",
                "providers: {}",
                "runtime:",
                "  providers: {}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def validate_cli(
    gestaltd: str, config_path: pathlib.Path, packages: dict[str, Any]
) -> None:
    package_names = sorted(packages)
    search_output = run([gestaltd, "provider", "search", "--repo", "local", "--config", str(config_path)])
    if SOURCE_PREFIX + "plugins/slack" not in search_output:
        raise SystemExit("provider search output did not include plugins/slack")
    for source in package_names:
        run([gestaltd, "provider", "info", "--repo", "local", "--config", str(config_path), source])
        run(
            [
                gestaltd,
                "provider",
                "add",
                "--dry-run",
                "--repo",
                "local",
                "--config",
                str(config_path),
                source,
            ]
        )


def validate_mutations(
    gestaltd: str, config_path: pathlib.Path, packages: dict[str, Any]
) -> None:
    plugin = require_package(packages, "plugins/httpbin")
    indexeddb = require_package(packages, "indexeddb/mongodb")
    ui = require_package(packages, "ui/default")
    run(
        [
            gestaltd,
            "provider",
            "add",
            "--repo",
            "local",
            "--config",
            str(config_path),
            "--name",
            "registry-plugin",
            "--no-lock",
            plugin,
        ]
    )
    run(
        [
            gestaltd,
            "provider",
            "add",
            "--repo",
            "local",
            "--config",
            str(config_path),
            "--kind",
            "indexeddb",
            "--name",
            "registry-indexeddb",
            "--no-lock",
            indexeddb,
        ]
    )
    run(
        [
            gestaltd,
            "provider",
            "add",
            "--repo",
            "local",
            "--config",
            str(config_path),
            "--kind",
            "ui",
            "--name",
            "registry-ui",
            "--set",
            "path=/",
            "--no-lock",
            ui,
        ]
    )
    mutated = load_yaml(config_path)
    if (
        (((mutated.get("plugins") or {}).get("registry-plugin") or {}).get("source") or {}).get("package")
        != plugin
    ):
        raise SystemExit("plugin provider add did not write top-level plugins entry")
    if (
        ((((mutated.get("providers") or {}).get("indexeddb") or {}).get("registry-indexeddb") or {}).get("source") or {}).get("package")
        != indexeddb
    ):
        raise SystemExit("indexeddb provider add did not write providers.indexeddb entry")
    ui_entry = (((mutated.get("providers") or {}).get("ui") or {}).get("registry-ui") or {})
    if (ui_entry.get("source") or {}).get("package") != ui:
        raise SystemExit("ui provider add did not write providers.ui entry")
    if ui_entry.get("path") != "/":
        raise SystemExit("ui provider add did not write path=/")


def main() -> int:
    args = parse_args()
    repo_root = pathlib.Path(args.repo_root).resolve()
    gestaltd = shutil.which(args.gestaltd) or args.gestaltd
    if not pathlib.Path(gestaltd).is_file():
        raise SystemExit(f"gestaltd not found: {args.gestaltd}")
    index_path = repo_root / "provider-index.yaml"
    catalog_path = repo_root / "registry/catalog.json"
    index = load_yaml(index_path)
    catalog = load_json(catalog_path)
    validate_catalog(index, catalog)
    packages = index["packages"]
    with tempfile.TemporaryDirectory() as tmp:
        config_path = pathlib.Path(tmp) / "gestalt.yaml"
        write_base_config(config_path, index_path)
        validate_cli(gestaltd, config_path, packages)
        validate_mutations(gestaltd, config_path, packages)
    return 0


if __name__ == "__main__":
    sys.exit(main())
