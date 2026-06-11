#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
import xml.etree.ElementTree as ET
from typing import Any

import yaml


INDEX_SCHEMA_NAME = "gestaltd-provider-index"
INDEX_SCHEMA_VERSION = 1
CATALOG_SCHEMA_NAME = "gestaltd-provider-catalog"
CATALOG_SCHEMA_VERSION = 1
REPOSITORY = "valon-technologies/gestalt-providers"
SOURCE_PREFIX = "github.com/valon-technologies/gestalt-providers/"
RAW_URL_PREFIX = f"https://raw.githubusercontent.com/{REPOSITORY}/main/"
BLOB_URL_PREFIX = f"https://github.com/{REPOSITORY}/blob/main/"
SVG_NAMESPACE = "http://www.w3.org/2000/svg"
DOC_PATH_PATTERN = re.compile(r"^/(?:[a-z0-9][a-z0-9-]*/)?$")
DOC_ESM_PATTERN = re.compile(r"^\s*(?:import|export)\s+")
DOC_MDX_JSX_PATTERN = re.compile(r"^\s*</?[A-Z][A-Za-z0-9]*(?:\s|>|/>)")
DOC_HTML_PATTERN = re.compile(r"^\s*</?[a-z][a-z0-9-]*(?:\s|>|/>)")


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


def validate_icon_assets(repo_root: pathlib.Path, catalog: dict[str, Any]) -> None:
    providers = catalog.get("providers") or []
    for provider in providers:
        icon_url = provider.get("iconUrl")
        if not icon_url:
            continue
        package = provider.get("package") or "<unknown>"
        if not isinstance(icon_url, str) or not icon_url.startswith(RAW_URL_PREFIX):
            raise SystemExit(f"catalog iconUrl for {package} must point at {RAW_URL_PREFIX}")
        relative_path = urllib.parse.unquote(icon_url.removeprefix(RAW_URL_PREFIX))
        icon_path = repo_root / relative_path
        if not icon_path.is_file():
            raise SystemExit(f"catalog iconUrl for {package} points at missing file {relative_path}")
        if icon_path.suffix != ".svg":
            continue
        try:
            root = ET.parse(icon_path).getroot()
        except ET.ParseError as error:
            raise SystemExit(f"catalog SVG icon for {package} is invalid XML: {relative_path}: {error}")
        if root.tag != f"{{{SVG_NAMESPACE}}}svg":
            raise SystemExit(
                f"catalog SVG icon for {package} must declare xmlns=\"{SVG_NAMESPACE}\": {relative_path}"
            )


def relative_path_from_raw_url(url: str, field: str, package: str) -> str:
    if not isinstance(url, str) or not url.startswith(RAW_URL_PREFIX):
        raise SystemExit(f"catalog {field} for {package} must point at {RAW_URL_PREFIX}")
    return urllib.parse.unquote(url.removeprefix(RAW_URL_PREFIX))


def relative_path_from_blob_url(url: str, field: str, package: str) -> str:
    if not isinstance(url, str) or not url.startswith(BLOB_URL_PREFIX):
        raise SystemExit(f"catalog {field} for {package} must point at {BLOB_URL_PREFIX}")
    return urllib.parse.unquote(url.removeprefix(BLOB_URL_PREFIX))


def validate_doc_source_text(path: pathlib.Path, package: str) -> None:
    in_code_fence = False
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            in_code_fence = not in_code_fence
            continue
        if in_code_fence:
            continue
        if DOC_ESM_PATTERN.match(line):
            raise SystemExit(
                f"catalog docs for {package} use unsupported MDX import/export at {path}:{line_number}"
            )
        if DOC_MDX_JSX_PATTERN.match(line):
            raise SystemExit(
                f"catalog docs for {package} use unsupported MDX JSX at {path}:{line_number}"
            )
        if DOC_HTML_PATTERN.match(line):
            raise SystemExit(
                f"catalog docs for {package} use unsupported raw HTML at {path}:{line_number}"
            )


def validate_provider_docs(repo_root: pathlib.Path, catalog: dict[str, Any]) -> None:
    providers = catalog.get("providers") or []
    for provider in providers:
        package = provider.get("package") or "<unknown>"
        package_path = provider.get("packagePath")
        docs = provider.get("docs") or []
        if not docs:
            continue
        if not isinstance(package_path, str) or not package_path:
            raise SystemExit(f"catalog docs for {package} require packagePath")
        if not isinstance(docs, list):
            raise SystemExit(f"catalog docs for {package} must be a list")
        seen_paths: set[str] = set()
        for doc in docs:
            if not isinstance(doc, dict):
                raise SystemExit(f"catalog docs for {package} entries must be mappings")
            doc_path = doc.get("path")
            if (
                not isinstance(doc_path, str)
                or not DOC_PATH_PATTERN.fullmatch(doc_path)
                or ".." in pathlib.PurePosixPath(doc_path).parts
                or "?" in doc_path
                or "#" in doc_path
            ):
                raise SystemExit(f"catalog docs path for {package} is invalid: {doc_path!r}")
            if doc_path in seen_paths:
                raise SystemExit(f"catalog docs for {package} duplicate path {doc_path!r}")
            seen_paths.add(doc_path)

            title = doc.get("title")
            if not isinstance(title, str) or not title.strip():
                raise SystemExit(f"catalog docs for {package} path {doc_path} must have a title")

            source_path = doc.get("sourcePath")
            if not isinstance(source_path, str) or not source_path:
                raise SystemExit(f"catalog docs for {package} path {doc_path} must have sourcePath")
            source = pathlib.PurePosixPath(source_path)
            if source.is_absolute() or ".." in source.parts:
                raise SystemExit(f"catalog docs for {package} sourcePath is invalid: {source_path!r}")
            if not source_path.startswith(f"{package_path}/"):
                raise SystemExit(
                    f"catalog docs for {package} sourcePath {source_path!r} is outside {package_path!r}"
                )
            if source.suffix not in {".md", ".mdx"}:
                raise SystemExit(f"catalog docs for {package} sourcePath must be .md or .mdx: {source_path}")

            raw_path = relative_path_from_raw_url(doc.get("rawUrl"), "docs rawUrl", package)
            if raw_path != source_path:
                raise SystemExit(
                    f"catalog docs rawUrl for {package} points at {raw_path!r}, want {source_path!r}"
                )
            edit_path = relative_path_from_blob_url(doc.get("editUrl"), "docs editUrl", package)
            if edit_path != source_path:
                raise SystemExit(
                    f"catalog docs editUrl for {package} points at {edit_path!r}, want {source_path!r}"
                )
            source_file = repo_root / source_path
            if not source_file.is_file():
                raise SystemExit(
                    f"catalog docs for {package} point at missing file {source_path}"
                )
            validate_doc_source_text(source_file, package)


def validate_catalog(repo_root: pathlib.Path, index: dict[str, Any], catalog: dict[str, Any]) -> None:
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
    validate_icon_assets(repo_root, catalog)
    validate_provider_docs(repo_root, catalog)


def write_base_config(config_path: pathlib.Path, index_path: pathlib.Path) -> None:
    config_path.write_text(
        "\n".join(
            [
                "apiVersion: gestaltd.config/v6",
                "providerRepositories:",
                "  local:",
                f"    url: {file_url(index_path)}",
                "apps: {}",
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
    if SOURCE_PREFIX + "app/slack" not in search_output:
        raise SystemExit("provider search output did not include app/slack")
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
    plugin = require_package(packages, "app/ashby")
    indexeddb = require_package(packages, "indexeddb/relationaldb")
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
            "registry-app",
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
        (((mutated.get("apps") or {}).get("registry-app") or {}).get("source") or {}).get("package")
        != plugin
    ):
        raise SystemExit("app provider add did not write top-level apps entry")
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
    validate_catalog(repo_root, index, catalog)
    packages = index["packages"]
    with tempfile.TemporaryDirectory() as tmp:
        config_path = pathlib.Path(tmp) / "gestalt.yaml"
        write_base_config(config_path, index_path)
        validate_cli(gestaltd, config_path, packages)
        validate_mutations(gestaltd, config_path, packages)
    return 0


if __name__ == "__main__":
    sys.exit(main())
