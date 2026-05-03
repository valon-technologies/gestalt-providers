#!/usr/bin/env python3

import argparse
import json
import pathlib
import re
import subprocess
import sys


SCHEMA_NAME = "gestaltd-provider-index"
SCHEMA_VERSION = 1
REPOSITORY = "valon-technologies/gestalt-providers"
SOURCE_PREFIX = f"github.com/{REPOSITORY}/"
PACKAGE_ROOTS = (
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
EXECUTABLE_PLATFORMS = (
    "darwin/amd64",
    "darwin/arm64",
    "linux/amd64",
    "linux/arm64",
    "linux/arm",
)
TYPESCRIPT_PLATFORMS = (
    "darwin/amd64",
    "darwin/arm64",
    "linux/amd64",
    "linux/arm64",
)
LINUX_PLATFORMS = (
    "linux/amd64",
    "linux/arm64",
    "linux/arm",
)
TAG_PATTERN = re.compile(r"^(?P<dir>.+)/v(?P<version>\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate provider-index.yaml.")
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument("--output", default="provider-index.yaml", help="Output path")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if the output file is not up to date",
    )
    parser.add_argument(
        "--refresh-releases",
        action="store_true",
        help="Refresh historical versions from GitHub release metadata",
    )
    return parser.parse_args()


def normalize_scalar(value: str) -> str:
    trimmed = value.strip()
    if len(trimmed) >= 2 and trimmed[0] == trimmed[-1] and trimmed[0] in {"'", '"'}:
        return trimmed[1:-1]
    return trimmed


def read_manifest(path: pathlib.Path) -> dict[str, str]:
    fields: dict[str, str] = {}
    wanted = {"source", "version", "kind", "displayName", "description"}
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            if raw_line[:1].isspace() or ":" not in raw_line:
                continue
            key, value = raw_line.split(":", 1)
            if key in wanted:
                fields[key] = normalize_scalar(value)
    missing = [name for name in ("source", "version", "kind") if not fields.get(name)]
    if missing:
        raise SystemExit(f"{path}: missing {', '.join(missing)}")
    return fields


def read_existing_index(path: pathlib.Path) -> dict[str, dict[str, object]]:
    if not path.is_file():
        return {}
    packages: dict[str, dict[str, object]] = {}
    current_package = ""
    current_version = ""
    in_platforms = False
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if line.startswith("  github.com/"):
                current_package = line.strip().rstrip(":")
                current_version = ""
                in_platforms = False
                packages.setdefault(
                    current_package,
                    {"displayName": "", "description": "", "versions": {}},
                )
                continue
            if not current_package:
                continue
            package = packages[current_package]
            if line.startswith("    displayName:"):
                package["displayName"] = parse_yaml_string(line.split(":", 1)[1])
            elif line.startswith("    description:"):
                package["description"] = parse_yaml_string(line.split(":", 1)[1])
            elif line.startswith("      ") and not line.startswith("        ") and line.endswith(":"):
                current_version = parse_yaml_string(line.strip()[:-1])
                versions = package["versions"]
                assert isinstance(versions, dict)
                versions.setdefault(
                    current_version,
                    {"metadata": "", "kind": "", "runtime": "", "platforms": []},
                )
                in_platforms = False
            elif current_version and line.startswith("        metadata:"):
                version = version_entry(package, current_version)
                version["metadata"] = parse_yaml_string(line.split(":", 1)[1])
            elif current_version and line.startswith("        kind:"):
                version = version_entry(package, current_version)
                version["kind"] = parse_yaml_string(line.split(":", 1)[1])
            elif current_version and line.startswith("        runtime:"):
                version = version_entry(package, current_version)
                version["runtime"] = parse_yaml_string(line.split(":", 1)[1])
            elif current_version and line.startswith("        platforms:"):
                version = version_entry(package, current_version)
                version["platforms"] = []
                in_platforms = True
            elif current_version and in_platforms and line.startswith("          - "):
                version = version_entry(package, current_version)
                platforms = version["platforms"]
                assert isinstance(platforms, list)
                platforms.append(parse_yaml_string(line.split("-", 1)[1]))
            else:
                in_platforms = False
    return packages


def parse_yaml_string(value: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        return ""
    try:
        parsed = json.loads(trimmed)
    except json.JSONDecodeError:
        return normalize_scalar(trimmed)
    return str(parsed)


def version_entry(package: dict[str, object], version: str) -> dict[str, object]:
    versions = package["versions"]
    assert isinstance(versions, dict)
    entry = versions[version]
    assert isinstance(entry, dict)
    return entry


def provider_language(package_dir: pathlib.Path) -> str:
    if any(path.suffix == ".go" for path in package_dir.glob("*.go")):
        return "go"
    if (package_dir / "Cargo.toml").is_file():
        return "rust"
    if (package_dir / "pyproject.toml").is_file():
        return "python"
    package_json = package_dir / "package.json"
    if package_json.is_file() and package_dir.parts[0] != "ui":
        try:
            data = json.loads(package_json.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            data = {}
        if (data.get("gestalt") or {}).get("provider"):
            return "typescript"
    return "generic"


def release_runtime(kind: str, language: str) -> str:
    if kind == "ui":
        return "ui"
    if kind == "plugin" and language == "generic":
        return "declarative"
    return "executable"


def release_platforms(package_dir: pathlib.Path, language: str, runtime: str) -> tuple[str, ...]:
    if runtime in {"declarative", "ui"}:
        return ("generic",)
    if language == "rust":
        return LINUX_PLATFORMS
    if language == "typescript":
        if package_dir.as_posix() == "agent/cursor":
            return ("linux/amd64", "linux/arm64")
        return TYPESCRIPT_PLATFORMS
    if language == "python" and package_dir.as_posix() == "agent/claude":
        return tuple(platform for platform in EXECUTABLE_PLATFORMS if platform != "linux/arm")
    return EXECUTABLE_PLATFORMS


def release_metadata_url(package_dir: pathlib.Path, version: str) -> str:
    tag = f"{package_dir.as_posix()}/v{version}"
    return f"https://github.com/{REPOSITORY}/releases/download/{tag}/provider-release.yaml"


def upsert_version(
    packages: dict[str, dict[str, object]],
    source: str,
    display_name: str,
    description: str,
    version: str,
    metadata: str,
    kind: str,
    runtime: str,
    platforms: tuple[str, ...] | list[str],
    overwrite: bool,
) -> None:
    package = packages.setdefault(
        source,
        {"displayName": display_name, "description": description, "versions": {}},
    )
    if display_name:
        package["displayName"] = display_name
    if description:
        package["description"] = description
    versions = package["versions"]
    assert isinstance(versions, dict)
    if overwrite or version not in versions:
        versions[version] = {
            "metadata": metadata,
            "kind": kind,
            "runtime": runtime,
            "platforms": list(platforms),
        }


def discover_packages(repo_root: pathlib.Path, packages: dict[str, dict[str, object]]) -> None:
    for root_name in PACKAGE_ROOTS:
        root = repo_root / root_name
        if not root.is_dir():
            continue
        for manifest_path in sorted(root.glob("*/manifest.yaml")):
            package_dir = manifest_path.parent.relative_to(repo_root)
            manifest = read_manifest(manifest_path)
            source = manifest["source"]
            expected_source = SOURCE_PREFIX + package_dir.as_posix()
            if source != expected_source:
                raise SystemExit(
                    f"{manifest_path}: source {source!r} does not match {expected_source!r}"
                )
            language = provider_language(package_dir)
            kind = manifest["kind"]
            runtime = release_runtime(kind, language)
            upsert_version(
                packages,
                source=source,
                display_name=manifest.get("displayName", package_dir.name),
                description=manifest.get("description", ""),
                version=manifest["version"],
                metadata=release_metadata_url(package_dir, manifest["version"]),
                kind=kind,
                runtime=runtime,
                platforms=release_platforms(package_dir, language, runtime),
                overwrite=False,
            )


def refresh_github_releases(repo_root: pathlib.Path, packages: dict[str, dict[str, object]]) -> None:
    manifests = {
        manifest.parent.relative_to(repo_root).as_posix(): read_manifest(manifest)
        for root_name in PACKAGE_ROOTS
        for manifest in sorted((repo_root / root_name).glob("*/manifest.yaml"))
    }
    result = subprocess.run(
        [
            "gh",
            "release",
            "list",
            "--repo",
            REPOSITORY,
            "--limit",
            "1000",
            "--json",
            "tagName,isDraft",
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    releases = json.loads(result.stdout)
    for release in releases:
        if release.get("isDraft"):
            continue
        tag = str(release.get("tagName") or "")
        match = TAG_PATTERN.match(tag)
        if not match:
            continue
        package_dir = match.group("dir")
        manifest = manifests.get(package_dir)
        metadata_url = f"https://github.com/{REPOSITORY}/releases/download/{tag}/provider-release.yaml"
        metadata = fetch_release_metadata(tag)
        if metadata is None:
            print(f"warning: skipping {tag}; provider-release.yaml not found", file=sys.stderr)
            continue
        source = str(metadata.get("package") or "")
        if not source:
            if manifest is None:
                print(f"warning: skipping {tag}; provider-release.yaml missing package", file=sys.stderr)
                continue
            source = manifest["source"]
        if not source.startswith(SOURCE_PREFIX):
            raise SystemExit(f"{metadata_url}: package {source!r} does not start with {SOURCE_PREFIX!r}")
        if manifest is not None and source != manifest["source"]:
            raise SystemExit(f"{metadata_url}: package {source!r} does not match manifest")
        kind = str(metadata.get("kind") or (manifest or {}).get("kind") or "")
        if not kind:
            print(f"warning: skipping {tag}; provider-release.yaml missing kind", file=sys.stderr)
            continue
        runtime = str(metadata.get("runtime") or "")
        if not runtime:
            if manifest is None:
                print(f"warning: skipping {tag}; provider-release.yaml missing runtime", file=sys.stderr)
                continue
            runtime = release_runtime(kind, provider_language(repo_root / package_dir))
        display_name = pathlib.PurePosixPath(package_dir).name
        description = ""
        if manifest is not None:
            display_name = manifest.get("displayName", display_name)
            description = manifest.get("description", "")
        upsert_version(
            packages,
            source=source,
            display_name=display_name,
            description=description,
            version=metadata.get("version") or match.group("version"),
            metadata=metadata_url,
            kind=kind,
            runtime=runtime,
            platforms=metadata.get("platforms") or ("generic",),
            overwrite=True,
        )


def fetch_release_metadata(tag: str) -> dict[str, object] | None:
    result = subprocess.run(
        [
            "gh",
            "release",
            "download",
            tag,
            "--repo",
            REPOSITORY,
            "--pattern",
            "provider-release.yaml",
            "--output",
            "-",
        ],
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        return None
    data = result.stdout
    fields: dict[str, object] = {"platforms": []}
    in_artifacts = False
    for raw_line in data.splitlines():
        line = raw_line.rstrip("\n")
        if line.startswith("package:"):
            fields["package"] = normalize_scalar(line.split(":", 1)[1])
        elif line.startswith("kind:"):
            fields["kind"] = normalize_scalar(line.split(":", 1)[1])
        elif line.startswith("version:"):
            fields["version"] = normalize_scalar(line.split(":", 1)[1])
        elif line.startswith("runtime:"):
            fields["runtime"] = normalize_scalar(line.split(":", 1)[1])
        elif line == "artifacts:":
            in_artifacts = True
        elif in_artifacts and line.startswith("  ") and line.endswith(":") and not line.startswith("    "):
            platforms = fields["platforms"]
            assert isinstance(platforms, list)
            platforms.append(normalize_scalar(line.strip()[:-1]))
        elif line and not line.startswith(" "):
            in_artifacts = False
    return fields


def yaml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def semver_sort_key(version: str) -> tuple[tuple[int, int, int], int, tuple[object, ...]]:
    core, _, prerelease = version.partition("-")
    nums = tuple(int(part) for part in core.split("."))
    pre_key: tuple[object, ...] = ()
    if prerelease:
        pre_key = tuple(int(part) if part.isdigit() else part for part in prerelease.split("."))
    return (nums, 0 if prerelease else 1, pre_key)


def render_index(packages: dict[str, dict[str, object]]) -> str:
    lines = [
        f"schema: {SCHEMA_NAME}",
        f"schemaVersion: {SCHEMA_VERSION}",
        "packages:",
    ]
    for source in sorted(packages):
        package = packages[source]
        lines.append(f"  {source}:")
        lines.append(f"    displayName: {yaml_string(str(package['displayName']))}")
        if package["description"]:
            lines.append(f"    description: {yaml_string(str(package['description']))}")
        lines.append("    versions:")
        versions = package["versions"]
        assert isinstance(versions, dict)
        ordered_versions = sorted(versions, key=semver_sort_key, reverse=True)
        for version in ordered_versions:
            entry = versions[version]
            assert isinstance(entry, dict)
            lines.append(f"      {yaml_string(version)}:")
            lines.append(f"        metadata: {yaml_string(str(entry['metadata']))}")
            lines.append(f"        kind: {yaml_string(str(entry['kind']))}")
            lines.append(f"        runtime: {yaml_string(str(entry['runtime']))}")
            lines.append("        platforms:")
            for platform in entry["platforms"]:
                lines.append(f"          - {yaml_string(str(platform))}")
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    repo_root = pathlib.Path(args.repo_root).resolve()
    output = pathlib.Path(args.output)
    if not output.is_absolute():
        output = repo_root / output
    packages = read_existing_index(output)
    if args.refresh_releases:
        refresh_github_releases(repo_root, packages)
    discover_packages(repo_root, packages)
    rendered = render_index(packages)
    if args.check:
        if not output.is_file():
            print(f"{output} does not exist; run generate_provider_index.py", file=sys.stderr)
            return 1
        current = output.read_text(encoding="utf-8")
        if current != rendered:
            print(f"{output} is out of date; run generate_provider_index.py", file=sys.stderr)
            return 1
        return 0
    output.write_text(rendered, encoding="utf-8")
    print(f"wrote {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
