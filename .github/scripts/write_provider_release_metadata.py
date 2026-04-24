#!/usr/bin/env python3

import argparse
import hashlib
import pathlib
import re
import sys


SCHEMA_NAME = "gestaltd-provider-release"
SCHEMA_VERSION = 1
GENERIC_TARGET = "generic"
RUNTIME_EXECUTABLE = "executable"
RUNTIME_DECLARATIVE = "declarative"
RUNTIME_UI = "ui"

ARCHIVE_PATTERN = re.compile(
    r"^(?P<prefix>.+)_v(?P<version>[^_]+?)(?:_(?P<platform>.+))?\.tar\.gz$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate provider-release.yaml from packaged release archives."
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--manifest", help="Path to manifest.yaml")
    source_group.add_argument(
        "--template-metadata",
        help="Path to an existing provider-release.yaml used as the metadata template",
    )
    parser.add_argument("--output-dir", required=True, help="Directory containing release archives")
    parser.add_argument("--version", required=True, help="Release version for the packaged archives")
    parser.add_argument(
        "--metadata-path",
        help="Output path for provider-release.yaml (defaults to OUTPUT_DIR/provider-release.yaml)",
    )
    return parser.parse_args()


def read_manifest_fields(path: pathlib.Path) -> tuple[str, str]:
    source = ""
    kind = ""
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if line.startswith("source:"):
                source = normalize_scalar(line.split(":", 1)[1])
            elif line.startswith("kind:"):
                kind = normalize_scalar(line.split(":", 1)[1])
            if source and kind:
                break
    if not source:
        raise SystemExit(f"missing top-level source in {path}")
    if not kind:
        raise SystemExit(f"missing top-level kind in {path}")
    return source, kind


def read_template_metadata(path: pathlib.Path) -> tuple[dict[str, str], list[str]]:
    fields: dict[str, str] = {}
    artifact_paths: list[str] = []
    current_target = ""
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if line.startswith("schema:"):
                fields["schema"] = normalize_scalar(line.split(":", 1)[1])
            elif line.startswith("schemaVersion:"):
                fields["schemaVersion"] = normalize_scalar(line.split(":", 1)[1])
            elif line.startswith("package:"):
                fields["package"] = normalize_scalar(line.split(":", 1)[1])
            elif line.startswith("kind:"):
                fields["kind"] = normalize_scalar(line.split(":", 1)[1])
            elif line.startswith("version:"):
                fields["version"] = normalize_scalar(line.split(":", 1)[1])
            elif line.startswith("runtime:"):
                fields["runtime"] = normalize_scalar(line.split(":", 1)[1])
            elif line.startswith("  ") and line.endswith(":") and not line.startswith("    "):
                current_target = normalize_scalar(line.strip()[:-1])
            elif current_target and line.startswith("    path:"):
                artifact_paths.append(normalize_scalar(line.split(":", 1)[1]))
    required = ("schema", "schemaVersion", "package", "kind", "version", "runtime")
    missing = [name for name in required if not fields.get(name)]
    if missing:
        raise SystemExit(f"missing {', '.join(missing)} in {path}")
    return fields, artifact_paths


def normalize_scalar(value: str) -> str:
    trimmed = value.strip()
    if len(trimmed) >= 2 and trimmed[0] == trimmed[-1] and trimmed[0] in {"'", '"'}:
        return trimmed[1:-1]
    return trimmed


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def archive_target(path: pathlib.Path, version: str) -> tuple[str, bool]:
    match = ARCHIVE_PATTERN.match(path.name)
    if not match:
        raise SystemExit(f"unexpected archive name {path.name}")
    archive_version = match.group("version")
    if archive_version != version:
        raise SystemExit(
            f"archive {path.name} version {archive_version} does not match expected {version}"
        )
    platform_suffix = match.group("platform")
    if not platform_suffix:
        return GENERIC_TARGET, False
    parts = platform_suffix.split("_")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise SystemExit(f"archive {path.name} has invalid platform suffix {platform_suffix!r}")
    target = f"{parts[0]}/{parts[1]}"
    is_musl = any(part == "musl" for part in parts[2:])
    return target, is_musl


def merge_artifacts(output_dir: pathlib.Path, version: str) -> dict[str, dict[str, str]]:
    artifacts: dict[str, dict[str, str]] = {}
    for archive in sorted(output_dir.glob("*.tar.gz")):
        target, is_musl = archive_target(archive, version)
        candidate = {
            "path": archive.name,
            "sha256": sha256_file(archive),
            "is_musl": is_musl,
        }
        existing = artifacts.get(target)
        if existing is None:
            artifacts[target] = candidate
            continue
        if candidate["path"] == existing["path"]:
            continue
        if existing["is_musl"] and not candidate["is_musl"]:
            artifacts[target] = candidate
            continue
        if candidate["is_musl"] and not existing["is_musl"]:
            continue
        raise SystemExit(
            f"multiple archives map to {target}: {existing['path']} and {candidate['path']}"
        )
    if not artifacts:
        raise SystemExit(f"no .tar.gz archives found in {output_dir}")
    return artifacts


def release_runtime(kind: str, artifacts: dict[str, dict[str, str]]) -> str:
    if kind == "ui":
        return RUNTIME_UI
    if kind == "plugin":
        if any(target != GENERIC_TARGET for target in artifacts):
            return RUNTIME_EXECUTABLE
        return RUNTIME_DECLARATIVE
    return RUNTIME_EXECUTABLE


def metadata_lines(schema: str, schema_version: str, source: str, kind: str, version: str, runtime: str, artifacts: dict[str, dict[str, str]]) -> list[str]:
    lines = [
        f"schema: {schema}",
        f"schemaVersion: {schema_version}",
        f"package: {source}",
        f"kind: {kind}",
        f"version: {version}",
        f"runtime: {runtime}",
        "artifacts:",
    ]
    ordered_targets = sorted(t for t in artifacts if t != GENERIC_TARGET)
    if GENERIC_TARGET in artifacts:
        ordered_targets.append(GENERIC_TARGET)
    for target in ordered_targets:
        artifact = artifacts[target]
        lines.append(f"  {target}:")
        lines.append(f"    path: {artifact['path']}")
        lines.append(f"    sha256: {artifact['sha256']}")
    return lines


def main() -> int:
    args = parse_args()
    output_dir = pathlib.Path(args.output_dir)
    metadata_path = pathlib.Path(args.metadata_path) if args.metadata_path else output_dir / "provider-release.yaml"
    artifacts = merge_artifacts(output_dir, args.version)

    if args.template_metadata:
        template_fields, artifact_paths = read_template_metadata(pathlib.Path(args.template_metadata))
        if template_fields["version"] != args.version:
            raise SystemExit(
                f"template metadata version {template_fields['version']} does not match expected {args.version}"
            )
        for artifact_path in artifact_paths:
            if not (output_dir / artifact_path).is_file():
                raise SystemExit(
                    f"template metadata expects missing archive {artifact_path} in {output_dir}"
                )
        schema = template_fields["schema"]
        schema_version = template_fields["schemaVersion"]
        source = template_fields["package"]
        kind = template_fields["kind"]
        runtime = template_fields["runtime"]
    else:
        manifest_path = pathlib.Path(args.manifest)
        source, kind = read_manifest_fields(manifest_path)
        schema = SCHEMA_NAME
        schema_version = str(SCHEMA_VERSION)
        runtime = release_runtime(kind, artifacts)

    lines = metadata_lines(schema, schema_version, source, kind, args.version, runtime, artifacts)
    metadata_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"created {metadata_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
