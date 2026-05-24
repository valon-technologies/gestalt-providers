#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import yaml


INDEX_SCHEMA_NAME = "gestaltd-provider-index"
INDEX_SCHEMA_VERSION = 1
CATALOG_SCHEMA_NAME = "gestaltd-provider-catalog"
CATALOG_SCHEMA_VERSION = 1
RELEASE_SCHEMA_NAME = "gestaltd-provider-release"
RELEASE_SCHEMA_VERSION = 1
REPOSITORY = "valon-technologies/gestalt-providers"
SOURCE_PREFIX = f"github.com/{REPOSITORY}/"
PACKAGE_ROOTS = (
    "agent",
    "runtime",
    "app",
    "auth",
    "authorization",
    "externalcredentials",
    "s3",
    "indexeddb",
    "workflow",
    "cache",
    "secrets",
    "ui",
)
GENERIC_PLATFORM = "generic"
PLATFORM_ORDER = {
    "darwin/amd64": 0,
    "darwin/arm64": 1,
    "linux/amd64": 2,
    "linux/arm64": 3,
    "linux/arm": 4,
    GENERIC_PLATFORM: 100,
}
TAG_PATTERN = re.compile(
    r"^(?P<dir>.+)/v(?P<version>\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)$"
)
DOC_EXTENSIONS = (".mdx", ".md")
DOC_SLUG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")


@dataclass(frozen=True)
class Manifest:
    package_dir: str
    manifest_path: str
    source: str
    version: str
    kind: str
    display_name: str
    description: str
    icon_file: str


@dataclass(frozen=True)
class Release:
    package_dir: str
    source: str
    version: str
    kind: str
    runtime: str
    platforms: tuple[str, ...]
    metadata_url: str


@dataclass(frozen=True)
class ProviderDoc:
    path: str
    title: str
    source_path: str
    raw_url: str
    edit_url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate provider-index.yaml and registry/catalog.json."
    )
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument("--output", default="provider-index.yaml", help="Provider index output")
    parser.add_argument(
        "--catalog-output",
        default="registry/catalog.json",
        help="Registry UI catalog output",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if generated files are not up to date",
    )
    parser.add_argument(
        "--refresh-releases",
        action="store_true",
        help="Rebuild the provider index from non-draft GitHub releases",
    )
    parser.add_argument(
        "--release-metadata",
        help="Local provider-release.yaml to upsert into the index",
    )
    parser.add_argument(
        "--release-manifest",
        help="Tagged manifest.yaml matching --release-metadata",
    )
    parser.add_argument(
        "--release-tag",
        help="Release tag, for example app/slack/v0.0.1-alpha.42",
    )
    parser.add_argument(
        "--package-dir",
        help="Provider directory, for example app/slack",
    )
    return parser.parse_args()


def load_yaml_file(path: pathlib.Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise SystemExit(f"{path}: invalid YAML: {exc}") from exc
    return data if data is not None else {}


def load_yaml_text(text: str, source: str) -> Any:
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise SystemExit(f"{source}: invalid YAML: {exc}") from exc
    return data if data is not None else {}


def normalize_kind(kind: Any) -> str:
    value = str(kind or "").strip().lower()
    aliases = {
        "auth": "authentication",
        "external_credentials": "externalcredentials",
        "externalcredentials": "externalcredentials",
        "external-credentials": "externalcredentials",
        "plugin": "app",  # legacy manifest kind alias; remove after downstream manifests migrate
    }
    return aliases.get(value, value)


def scalar(value: Any) -> str:
    return str(value or "").strip()


def manifest_from_data(data: Any, package_dir: str, manifest_path: str) -> Manifest:
    if not isinstance(data, dict):
        raise SystemExit(f"{manifest_path}: manifest must be a mapping")
    source = scalar(data.get("source"))
    version = scalar(data.get("version"))
    kind = normalize_kind(data.get("kind"))
    missing = [
        name
        for name, value in (
            ("source", source),
            ("version", version),
            ("kind", kind),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"{manifest_path}: missing {', '.join(missing)}")
    expected_source = SOURCE_PREFIX + package_dir
    if source != expected_source:
        raise SystemExit(
            f"{manifest_path}: source {source!r} does not match {expected_source!r}"
        )
    return Manifest(
        package_dir=package_dir,
        manifest_path=manifest_path,
        source=source,
        version=version,
        kind=kind,
        display_name=scalar(data.get("displayName")) or pathlib.PurePosixPath(package_dir).name,
        description=scalar(data.get("description")),
        icon_file=scalar(data.get("iconFile")),
    )


def read_manifest(path: pathlib.Path, repo_root: pathlib.Path) -> Manifest:
    package_dir = path.parent.relative_to(repo_root).as_posix()
    return manifest_from_data(load_yaml_file(path), package_dir, path.as_posix())


def discover_current_manifests(repo_root: pathlib.Path) -> dict[str, Manifest]:
    manifests: dict[str, Manifest] = {}
    for root_name in PACKAGE_ROOTS:
        root = repo_root / root_name
        if not root.is_dir():
            continue
        for manifest_path in sorted(root.glob("*/manifest.yaml")):
            manifest = read_manifest(manifest_path, repo_root)
            manifests[manifest.source] = manifest
    return manifests


def read_existing_index(path: pathlib.Path) -> dict[str, dict[str, Any]]:
    if not path.is_file():
        return {}
    data = load_yaml_file(path)
    if not isinstance(data, dict):
        raise SystemExit(f"{path}: provider index must be a mapping")
    if data.get("schema") != INDEX_SCHEMA_NAME:
        raise SystemExit(f"{path}: unsupported schema {data.get('schema')!r}")
    if int(data.get("schemaVersion") or 0) != INDEX_SCHEMA_VERSION:
        raise SystemExit(f"{path}: unsupported schemaVersion {data.get('schemaVersion')!r}")
    raw_packages = data.get("packages") or {}
    if not isinstance(raw_packages, dict):
        raise SystemExit(f"{path}: packages must be a mapping")
    packages: dict[str, dict[str, Any]] = {}
    for source, raw_package in raw_packages.items():
        source = scalar(source)
        if not source.startswith(SOURCE_PREFIX):
            raise SystemExit(f"{path}: package {source!r} does not start with {SOURCE_PREFIX!r}")
        package_dir = source.removeprefix(SOURCE_PREFIX)
        if package_dir.split("/", 1)[0] not in PACKAGE_ROOTS:
            continue
        if not isinstance(raw_package, dict):
            raise SystemExit(f"{path}: package {source!r} must be a mapping")
        raw_versions = raw_package.get("versions") or {}
        if not isinstance(raw_versions, dict):
            raise SystemExit(f"{path}: package {source!r} versions must be a mapping")
        versions: dict[str, dict[str, Any]] = {}
        for version, raw_entry in raw_versions.items():
            version = scalar(version)
            if not isinstance(raw_entry, dict):
                raise SystemExit(f"{path}: {source} {version} must be a mapping")
            metadata = scalar(raw_entry.get("metadata"))
            kind = normalize_kind(raw_entry.get("kind"))
            runtime = scalar(raw_entry.get("runtime"))
            platforms = raw_entry.get("platforms") or []
            if not isinstance(platforms, list):
                raise SystemExit(f"{path}: {source} {version} platforms must be a sequence")
            missing = [
                name
                for name, value in (
                    ("metadata", metadata),
                    ("kind", kind),
                    ("runtime", runtime),
                )
                if not value
            ]
            if missing:
                raise SystemExit(f"{path}: {source} {version} missing {', '.join(missing)}")
            validate_metadata_url(source, version, metadata, path.as_posix())
            versions[version] = {
                "metadata": metadata,
                "kind": kind,
                "runtime": runtime,
                "platforms": normalize_platforms(platforms),
            }
            if bool(raw_entry.get("yanked")):
                versions[version]["yanked"] = True
        packages[source] = {
            "displayName": scalar(raw_package.get("displayName"))
            or pathlib.PurePosixPath(source).name,
            "description": scalar(raw_package.get("description")),
            "versions": versions,
        }
    return packages


def validate_metadata_url(source: str, version: str, metadata: str, context: str) -> None:
    package_dir = source.removeprefix(SOURCE_PREFIX)
    expected_suffix = f"/releases/download/{package_dir}/v{version}/provider-release.yaml"
    parsed = urllib.parse.urlparse(metadata)
    if parsed.scheme not in {"http", "https", "file"}:
        raise SystemExit(f"{context}: {source} {version} metadata URL has invalid scheme")
    if parsed.scheme in {"http", "https"} and not metadata.endswith(expected_suffix):
        raise SystemExit(
            f"{context}: {source} {version} metadata URL must end with {expected_suffix!r}"
        )


def sort_platforms(platforms: Any) -> list[str]:
    unique = {scalar(platform) for platform in platforms if scalar(platform)}
    return sorted(
        unique,
        key=lambda platform: (PLATFORM_ORDER.get(platform, 50), platform),
    )


def normalize_platforms(platforms: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for platform in platforms:
        value = scalar(platform)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


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


def latest_installable_version(versions: dict[str, dict[str, Any]]) -> str | None:
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


def release_metadata_url_for_tag(tag: str) -> str:
    return f"https://github.com/{REPOSITORY}/releases/download/{tag}/provider-release.yaml"


def release_from_metadata(
    *,
    metadata_data: Any,
    metadata_source: str,
    manifest: Manifest,
    package_dir: str,
    tag: str,
    metadata_url: str,
) -> Release:
    match = TAG_PATTERN.match(tag)
    if match is None:
        raise SystemExit(f"{metadata_source}: release tag {tag!r} is not a provider tag")
    if match.group("dir") != package_dir:
        raise SystemExit(
            f"{metadata_source}: release tag package dir {match.group('dir')!r} does not match {package_dir!r}"
        )
    if not isinstance(metadata_data, dict):
        raise SystemExit(f"{metadata_source}: provider release metadata must be a mapping")
    if metadata_data.get("schema") != RELEASE_SCHEMA_NAME:
        raise SystemExit(f"{metadata_source}: unsupported schema {metadata_data.get('schema')!r}")
    if int(metadata_data.get("schemaVersion") or 0) != RELEASE_SCHEMA_VERSION:
        raise SystemExit(
            f"{metadata_source}: unsupported schemaVersion {metadata_data.get('schemaVersion')!r}"
        )
    source = scalar(metadata_data.get("package"))
    version = scalar(metadata_data.get("version"))
    kind = normalize_kind(metadata_data.get("kind"))
    runtime = scalar(metadata_data.get("runtime"))
    artifacts = metadata_data.get("artifacts") or {}
    if not isinstance(artifacts, dict) or not artifacts:
        raise SystemExit(f"{metadata_source}: artifacts must be a non-empty mapping")
    if source != manifest.source:
        raise SystemExit(
            f"{metadata_source}: package {source!r} does not match tagged manifest {manifest.source!r}"
        )
    if kind != manifest.kind:
        raise SystemExit(
            f"{metadata_source}: kind {kind!r} does not match tagged manifest {manifest.kind!r}"
        )
    tag_version = match.group("version")
    if version != tag_version:
        raise SystemExit(
            f"{metadata_source}: version {version!r} does not match release tag version {tag_version!r}"
        )
    if version != manifest.version:
        raise SystemExit(
            f"{metadata_source}: version {version!r} does not match tagged manifest {manifest.version!r}"
        )
    if not runtime:
        raise SystemExit(f"{metadata_source}: runtime is required")
    platforms = sort_platforms(artifacts.keys())
    if not platforms:
        raise SystemExit(f"{metadata_source}: artifacts must include at least one target")
    return Release(
        package_dir=package_dir,
        source=source,
        version=version,
        kind=kind,
        runtime=runtime,
        platforms=tuple(platforms),
        metadata_url=metadata_url,
    )


def yanked_status(
    previous: dict[str, dict[str, Any]], source: str, version: str
) -> bool:
    package = previous.get(source) or {}
    versions = package.get("versions") or {}
    entry = versions.get(version) or {}
    return bool(entry.get("yanked"))


def upsert_release(
    packages: dict[str, dict[str, Any]],
    previous: dict[str, dict[str, Any]],
    manifest: Manifest,
    release: Release,
) -> None:
    package = packages.setdefault(
        release.source,
        {"displayName": manifest.display_name, "description": manifest.description, "versions": {}},
    )
    package["displayName"] = manifest.display_name
    package["description"] = manifest.description
    versions = package.setdefault("versions", {})
    entry: dict[str, Any] = {
        "metadata": release.metadata_url,
        "kind": release.kind,
        "runtime": release.runtime,
        "platforms": list(release.platforms),
    }
    if yanked_status(previous, release.source, release.version):
        entry["yanked"] = True
    versions[release.version] = entry


def apply_current_manifest_metadata(
    packages: dict[str, dict[str, Any]], manifests: dict[str, Manifest]
) -> None:
    for source, manifest in manifests.items():
        package = packages.get(source)
        if package is None:
            continue
        package["displayName"] = manifest.display_name
        package["description"] = manifest.description


def tagged_manifest(repo_root: pathlib.Path, tag: str, package_dir: str) -> Manifest:
    manifest_path = f"{package_dir}/manifest.yaml"
    text = git_show(repo_root, f"{tag}:{manifest_path}")
    data = load_yaml_text(text, f"{tag}:{manifest_path}")
    return manifest_from_data(data, package_dir, f"{tag}:{manifest_path}")


def git_show(repo_root: pathlib.Path, spec: str) -> str:
    result = subprocess.run(
        ["git", "show", spec],
        cwd=repo_root,
        text=True,
        capture_output=True,
    )
    if result.returncode == 0:
        return result.stdout
    tag = spec.split(":", 1)[0]
    fetch = subprocess.run(
        ["git", "fetch", "--force", "origin", f"refs/tags/{tag}:refs/tags/{tag}"],
        cwd=repo_root,
        text=True,
        capture_output=True,
    )
    if fetch.returncode != 0:
        message = fetch.stderr.strip() or result.stderr.strip()
        raise SystemExit(f"unable to fetch tag {tag}: {message}")
    retry = subprocess.run(
        ["git", "show", spec],
        cwd=repo_root,
        text=True,
        capture_output=True,
    )
    if retry.returncode != 0:
        message = retry.stderr.strip() or result.stderr.strip()
        raise SystemExit(f"unable to read {spec}: {message}")
    return retry.stdout


def list_github_releases() -> list[dict[str, Any]]:
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    url = f"https://api.github.com/repos/{REPOSITORY}/releases?per_page=100"
    releases: list[dict[str, Any]] = []
    while url:
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                **({"Authorization": f"Bearer {token}"} if token else {}),
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
                link = response.headers.get("Link", "")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise SystemExit(f"GitHub releases API failed with {exc.code}: {body}") from exc
        if not isinstance(payload, list):
            raise SystemExit("GitHub releases API returned non-list payload")
        releases.extend(release for release in payload if isinstance(release, dict))
        url = next_link(link)
    return releases


def next_link(link_header: str) -> str:
    for part in link_header.split(","):
        url_part, _, rel_part = part.strip().partition(";")
        if 'rel="next"' not in rel_part:
            continue
        url_part = url_part.strip()
        if url_part.startswith("<") and url_part.endswith(">"):
            return url_part[1:-1]
    return ""


def fetch_url_text(url: str) -> str:
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/octet-stream",
            **({"Authorization": f"Bearer {token}"} if token else {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"{url}: download failed with {exc.code}: {body}") from exc


def refresh_github_releases(
    repo_root: pathlib.Path,
    previous: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    packages: dict[str, dict[str, Any]] = {}
    for release in list_github_releases():
        if release.get("draft"):
            continue
        tag = scalar(release.get("tag_name"))
        match = TAG_PATTERN.match(tag)
        if match is None or match.group("dir").split("/", 1)[0] not in PACKAGE_ROOTS:
            continue
        assets = release.get("assets") or []
        if not isinstance(assets, list):
            raise SystemExit(f"{tag}: release assets must be a list")
        asset = next(
            (
                candidate
                for candidate in assets
                if isinstance(candidate, dict) and candidate.get("name") == "provider-release.yaml"
            ),
            None,
        )
        if asset is None:
            print(f"warning: skipping {tag}; provider-release.yaml asset not found", file=sys.stderr)
            continue
        metadata_url = release_metadata_url_for_tag(tag)
        download_url = scalar(asset.get("browser_download_url")) or metadata_url
        package_dir = match.group("dir")
        manifest = tagged_manifest(repo_root, tag, package_dir)
        release_info = release_from_metadata(
            metadata_data=load_yaml_text(fetch_url_text(download_url), metadata_url),
            metadata_source=metadata_url,
            manifest=manifest,
            package_dir=package_dir,
            tag=tag,
            metadata_url=metadata_url,
        )
        upsert_release(packages, previous, manifest, release_info)
    return packages


def upsert_single_release(
    packages: dict[str, dict[str, Any]],
    previous: dict[str, dict[str, Any]],
    metadata_path: pathlib.Path,
    manifest_path: pathlib.Path,
    package_dir: str,
    tag: str,
) -> None:
    manifest = manifest_from_data(
        load_yaml_file(manifest_path),
        package_dir,
        manifest_path.as_posix(),
    )
    release = release_from_metadata(
        metadata_data=load_yaml_file(metadata_path),
        metadata_source=metadata_path.as_posix(),
        manifest=manifest,
        package_dir=package_dir,
        tag=tag,
        metadata_url=release_metadata_url_for_tag(tag),
    )
    upsert_release(packages, previous, manifest, release)


def yaml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def render_index(packages: dict[str, dict[str, Any]]) -> str:
    lines = [
        f"schema: {INDEX_SCHEMA_NAME}",
        f"schemaVersion: {INDEX_SCHEMA_VERSION}",
        "packages:",
    ]
    for source in sorted(packages):
        package = packages[source]
        lines.append(f"  {source}:")
        lines.append(f"    displayName: {yaml_string(scalar(package.get('displayName')))}")
        if scalar(package.get("description")):
            lines.append(f"    description: {yaml_string(scalar(package.get('description')))}")
        lines.append("    versions:")
        versions = package.get("versions") or {}
        for version in sorted(versions, key=semver_sort_key, reverse=True):
            entry = versions[version]
            lines.append(f"      {yaml_string(version)}:")
            lines.append(f"        metadata: {yaml_string(scalar(entry.get('metadata')))}")
            lines.append(f"        kind: {yaml_string(normalize_kind(entry.get('kind')))}")
            lines.append(f"        runtime: {yaml_string(scalar(entry.get('runtime')))}")
            lines.append("        platforms:")
            for platform in normalize_platforms(entry.get("platforms") or []):
                lines.append(f"          - {yaml_string(platform)}")
            if bool(entry.get("yanked")):
                lines.append("        yanked: true")
    return "\n".join(lines) + "\n"


def config_target(kind: str) -> dict[str, Any]:
    if kind == "app":
        return {"section": "app", "entryKind": "app"}
    if kind == "runtime":
        return {"section": "runtime.providers", "entryKind": "runtime"}
    if kind == "ui":
        return {
            "section": "providers.ui",
            "entryKind": "ui",
            "requiredSet": {"path": "/"},
        }
    if kind == "externalcredentials":
        return {"section": "providers.externalCredentials", "entryKind": kind}
    if kind in {
        "authentication",
        "authorization",
        "cache",
        "indexeddb",
        "s3",
        "secrets",
        "workflow",
        "agent",
    }:
        return {"section": f"providers.{kind}", "entryKind": kind}
    return {"section": f"providers.{kind}", "entryKind": kind}


def provider_registry_path(kind: str, package_dir: str) -> str:
    return f"/providers/{kind}/{pathlib.PurePosixPath(package_dir).name}/"


def repository_blob_url(path: str) -> str:
    return f"https://github.com/{REPOSITORY}/blob/main/{path}"


def repository_tree_url(path: str) -> str:
    return f"https://github.com/{REPOSITORY}/tree/main/{path}"


def repository_raw_url(path: str) -> str:
    return f"https://raw.githubusercontent.com/{REPOSITORY}/main/{path}"


def title_from_filename(path: pathlib.Path) -> str:
    if path.stem == "index":
        return "Overview"
    return path.stem.replace("-", " ").replace("_", " ").title()


def split_frontmatter(text: str, source: str) -> tuple[dict[str, Any], str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, text
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() != "---":
            continue
        raw_frontmatter = "\n".join(lines[1:index])
        try:
            data = yaml.safe_load(raw_frontmatter) if raw_frontmatter.strip() else {}
        except yaml.YAMLError as exc:
            raise SystemExit(f"{source}: invalid docs frontmatter: {exc}") from exc
        if data is not None and not isinstance(data, dict):
            raise SystemExit(f"{source}: docs frontmatter must be a mapping")
        return data or {}, "\n".join(lines[index + 1 :])
    return {}, text


def markdown_title(path: pathlib.Path, source_path: str) -> str:
    text = path.read_text(encoding="utf-8")
    frontmatter, body = split_frontmatter(text, source_path)
    frontmatter_title = scalar(frontmatter.get("title"))
    if frontmatter_title:
        return frontmatter_title
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip().strip("#").strip()
    return title_from_filename(path)


def doc_entry(repo_root: pathlib.Path, path: pathlib.Path, doc_path: str) -> ProviderDoc:
    source_path = path.relative_to(repo_root).as_posix()
    title = markdown_title(path, source_path)
    if not title:
        raise SystemExit(f"{source_path}: provider doc title must not be empty")
    return ProviderDoc(
        path=doc_path,
        title=title,
        source_path=source_path,
        raw_url=repository_raw_url(source_path),
        edit_url=repository_blob_url(source_path),
    )


def provider_doc_path(path: pathlib.Path) -> str:
    if path.stem == "index":
        return "/"
    slug = path.stem
    if not DOC_SLUG_PATTERN.fullmatch(slug):
        raise SystemExit(
            f"{path.as_posix()}: provider docs filenames must be lowercase kebab-case"
        )
    return f"/{slug}/"


def discover_provider_docs(repo_root: pathlib.Path, package_dir: str) -> list[ProviderDoc]:
    package_root = repo_root / package_dir
    docs_dir = package_root / "docs"
    readme_path = package_root / "README.md"
    if not docs_dir.is_dir():
        if readme_path.is_file():
            return [doc_entry(repo_root, readme_path, "/")]
        return []

    nested_docs = [
        path
        for path in docs_dir.rglob("*")
        if path.is_file() and path.suffix in DOC_EXTENSIONS and path.parent != docs_dir
    ]
    if nested_docs:
        first_nested = nested_docs[0].relative_to(repo_root).as_posix()
        raise SystemExit(f"{first_nested}: provider docs must be top-level files in docs/")

    index_path = next(
        (docs_dir / filename for filename in ("index.mdx", "index.md") if (docs_dir / filename).is_file()),
        None,
    )
    if index_path is None:
        raise SystemExit(f"{package_dir}/docs: docs directory must include index.mdx or index.md")

    doc_files = [
        path
        for path in sorted(docs_dir.iterdir())
        if path.is_file() and path.suffix in DOC_EXTENSIONS and path.name not in {index_path.name}
    ]
    ordered_files = [index_path, *doc_files]
    docs: list[ProviderDoc] = []
    seen_paths: set[str] = set()
    for path in ordered_files:
        doc_path = provider_doc_path(path)
        if doc_path in seen_paths:
            raise SystemExit(f"{path.relative_to(repo_root).as_posix()}: duplicate provider docs path {doc_path}")
        seen_paths.add(doc_path)
        docs.append(doc_entry(repo_root, path, doc_path))
    return docs


def catalog_provider(
    repo_root: pathlib.Path,
    source: str,
    manifest: Manifest | None,
    package: dict[str, Any] | None,
) -> dict[str, Any]:
    versions_map = (package or {}).get("versions") or {}
    ordered_versions = sorted(versions_map, key=semver_sort_key, reverse=True)
    latest_installable = latest_installable_version(versions_map)
    versions = []
    for version in ordered_versions:
        entry = versions_map[version]
        version_entry: dict[str, Any] = {
            "version": version,
            "metadata": scalar(entry.get("metadata")),
            "kind": normalize_kind(entry.get("kind")),
            "runtime": scalar(entry.get("runtime")),
            "platforms": normalize_platforms(entry.get("platforms") or []),
        }
        if bool(entry.get("yanked")):
            version_entry["yanked"] = True
        versions.append(version_entry)
    package_dir = manifest.package_dir if manifest else source.removeprefix(SOURCE_PREFIX)
    latest_entry_version = latest_installable or (ordered_versions[0] if ordered_versions else None)
    latest_entry = versions_map.get(latest_entry_version) if latest_entry_version else {}
    kind = manifest.kind if manifest else normalize_kind((latest_entry or {}).get("kind"))
    display_name = (
        manifest.display_name
        if manifest
        else scalar((package or {}).get("displayName")) or pathlib.PurePosixPath(package_dir).name
    )
    description = manifest.description if manifest else scalar((package or {}).get("description"))
    readme_path = f"{package_dir}/README.md"
    manifest_path = f"{package_dir}/manifest.yaml"
    package_dir_exists = (repo_root / package_dir).is_dir()
    icon_url = None
    if manifest and manifest.icon_file:
        icon_path = pathlib.PurePosixPath(package_dir) / manifest.icon_file
        if (repo_root / icon_path).is_file():
            icon_url = repository_raw_url(icon_path.as_posix())
    docs = [
        {
            "path": doc.path,
            "title": doc.title,
            "sourcePath": doc.source_path,
            "rawUrl": doc.raw_url,
            "editUrl": doc.edit_url,
        }
        for doc in discover_provider_docs(repo_root, package_dir)
    ]
    return {
        "package": source,
        "packagePath": package_dir,
        "name": pathlib.PurePosixPath(package_dir).name,
        "kind": kind,
        "configTarget": config_target(kind),
        "displayName": display_name,
        "description": description,
        "manifestVersion": manifest.version if manifest else None,
        "latestInstallableVersion": latest_installable,
        "versions": versions,
        "registryPath": provider_registry_path(kind, package_dir),
        "sourceUrl": repository_tree_url(package_dir) if package_dir_exists else None,
        "readmeUrl": repository_blob_url(readme_path)
        if (repo_root / readme_path).is_file()
        else None,
        "manifestUrl": repository_blob_url(manifest_path)
        if (repo_root / manifest_path).is_file()
        else None,
        "iconUrl": icon_url,
        "docs": docs,
    }


def render_catalog(
    repo_root: pathlib.Path,
    manifests: dict[str, Manifest],
    packages: dict[str, dict[str, Any]],
) -> str:
    sources = set(manifests) | set(packages)
    providers = [
        catalog_provider(repo_root, source, manifests.get(source), packages.get(source))
        for source in sources
    ]
    providers.sort(key=lambda provider: (provider["kind"], provider["name"], provider["package"]))
    catalog = {
        "schema": CATALOG_SCHEMA_NAME,
        "schemaVersion": CATALOG_SCHEMA_VERSION,
        "repository": REPOSITORY,
        "indexUrl": f"https://raw.githubusercontent.com/{REPOSITORY}/main/provider-index.yaml",
        "providers": providers,
    }
    return json.dumps(catalog, indent=2, sort_keys=True) + "\n"


def compare_or_write(path: pathlib.Path, rendered: str, check: bool) -> bool:
    if check:
        if not path.is_file():
            print(f"{path} does not exist; run generate_provider_index.py", file=sys.stderr)
            return False
        current = path.read_text(encoding="utf-8")
        if current != rendered:
            print(f"{path} is out of date; run generate_provider_index.py", file=sys.stderr)
            return False
        return True
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")
    print(f"wrote {path}")
    return True


def require_all_or_none(args: argparse.Namespace) -> None:
    release_args = [
        args.release_metadata,
        args.release_manifest,
        args.release_tag,
        args.package_dir,
    ]
    if any(release_args) and not all(release_args):
        raise SystemExit(
            "--release-metadata, --release-manifest, --release-tag, and --package-dir must be used together"
        )
    if args.check and args.refresh_releases:
        raise SystemExit("--check and --refresh-releases cannot be used together")


def main() -> int:
    args = parse_args()
    require_all_or_none(args)
    repo_root = pathlib.Path(args.repo_root).resolve()
    output = pathlib.Path(args.output)
    if not output.is_absolute():
        output = repo_root / output
    catalog_output = pathlib.Path(args.catalog_output)
    if not catalog_output.is_absolute():
        catalog_output = repo_root / catalog_output

    previous = read_existing_index(output)
    packages = (
        refresh_github_releases(repo_root, previous)
        if args.refresh_releases
        else {
            source: {
                "displayName": package["displayName"],
                "description": package.get("description", ""),
                "versions": dict(package.get("versions") or {}),
            }
            for source, package in previous.items()
        }
    )
    manifests = discover_current_manifests(repo_root)
    if args.release_metadata:
        upsert_single_release(
            packages,
            previous,
            pathlib.Path(args.release_metadata),
            pathlib.Path(args.release_manifest),
            args.package_dir,
            args.release_tag,
        )
    apply_current_manifest_metadata(packages, manifests)

    ok = compare_or_write(output, render_index(packages), args.check)
    ok = compare_or_write(catalog_output, render_catalog(repo_root, manifests, packages), args.check) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
