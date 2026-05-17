#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import tarfile
import tempfile


FULL_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish commit-addressed provider snapshot archives to GCS."
    )
    parser.add_argument("--provider-dir", required=True, help="Provider directory containing manifest.yaml")
    parser.add_argument("--provider-ref", required=True, help="Full gestalt-providers commit SHA")
    parser.add_argument("--gestalt-ref", required=True, help="Full Gestalt commit SHA used to package")
    parser.add_argument("--repository", required=True, help="GitHub repository in owner/name form")
    parser.add_argument("--dist-dir", required=True, help="Directory containing .tar.gz archives and provider-release.yaml")
    parser.add_argument("--gcs-root", required=True, help="GCS root, for example gs://bucket/prefix")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print uploads without writing to GCS")
    parser.add_argument(
        "--merge-existing",
        action="store_true",
        help="Merge generated artifacts with an existing provider-release.yaml instead of failing on existing differing archives",
    )
    return parser.parse_args()


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_provider_dir(raw: str) -> str:
    path = pathlib.PurePosixPath(raw.replace(os.sep, "/"))
    if path.is_absolute() or ".." in path.parts or str(path) in {"", "."}:
        raise SystemExit(f"invalid provider dir {raw!r}")
    return path.as_posix()


def snapshot_version(provider_ref: str) -> str:
    if not FULL_SHA_RE.fullmatch(provider_ref):
        raise SystemExit("--provider-ref must be a 40-character commit SHA")
    return f"0.0.0-snapshot.g{provider_ref.lower()}"


def validate_ref(name: str, value: str) -> str:
    if not FULL_SHA_RE.fullmatch(value):
        raise SystemExit(f"{name} must be a 40-character commit SHA")
    return value.lower()


def normalize_scalar(value: str) -> str:
    trimmed = value.strip()
    if len(trimmed) >= 2 and trimmed[0] == trimmed[-1] and trimmed[0] in {"'", '"'}:
        return trimmed[1:-1]
    return trimmed


def metadata_version(path: pathlib.Path) -> str:
    version = ""
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            if raw.startswith("version:"):
                version = normalize_scalar(raw.split(":", 1)[1])
                break
    if not version:
        raise SystemExit(f"{path} is missing version")
    return version


def metadata_fields(path: pathlib.Path) -> dict[str, str]:
    fields: dict[str, str] = {}
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            if raw.startswith("schema:"):
                fields["schema"] = normalize_scalar(raw.split(":", 1)[1])
            elif raw.startswith("schemaVersion:"):
                fields["schemaVersion"] = normalize_scalar(raw.split(":", 1)[1])
            elif raw.startswith("package:"):
                fields["package"] = normalize_scalar(raw.split(":", 1)[1])
            elif raw.startswith("kind:"):
                fields["kind"] = normalize_scalar(raw.split(":", 1)[1])
            elif raw.startswith("version:"):
                fields["version"] = normalize_scalar(raw.split(":", 1)[1])
            elif raw.startswith("runtime:"):
                fields["runtime"] = normalize_scalar(raw.split(":", 1)[1])
    required = ("schema", "schemaVersion", "package", "kind", "version", "runtime")
    missing = [name for name in required if not fields.get(name)]
    if missing:
        raise SystemExit(f"{path} is missing {', '.join(missing)}")
    return fields


def metadata_artifacts(path: pathlib.Path) -> dict[str, dict[str, str]]:
    artifacts: dict[str, dict[str, str]] = {}
    current_target = ""
    in_artifacts = False
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.rstrip("\n")
            if not in_artifacts:
                if line == "artifacts:":
                    in_artifacts = True
                continue
            if line and not line.startswith(" "):
                break
            if line.startswith("  ") and line.endswith(":") and not line.startswith("    "):
                current_target = normalize_scalar(line.strip()[:-1])
                if current_target in artifacts:
                    raise SystemExit(f"{path} has duplicate artifact target {current_target!r}")
                artifacts[current_target] = {}
            elif current_target and line.startswith("    path:"):
                artifacts[current_target]["path"] = normalize_scalar(line.split(":", 1)[1])
            elif current_target and line.startswith("    sha256:"):
                artifacts[current_target]["sha256"] = normalize_scalar(line.split(":", 1)[1]).lower()
    if not artifacts:
        raise SystemExit(f"{path} is missing artifacts")
    for target, artifact in artifacts.items():
        missing = [name for name in ("path", "sha256") if not artifact.get(name)]
        if missing:
            raise SystemExit(f"{path} artifact {target!r} is missing {', '.join(missing)}")
    return artifacts


def metadata_lines(fields: dict[str, str], artifacts: dict[str, dict[str, str]]) -> list[str]:
    lines = [
        f"schema: {fields['schema']}",
        f"schemaVersion: {fields['schemaVersion']}",
        f"package: {fields['package']}",
        f"kind: {fields['kind']}",
        f"version: {fields['version']}",
        f"runtime: {fields['runtime']}",
        "artifacts:",
    ]
    ordered_targets = sorted(target for target in artifacts if target != "generic")
    if "generic" in artifacts:
        ordered_targets.append("generic")
    for target in ordered_targets:
        artifact = artifacts[target]
        lines.append(f"  {target}:")
        lines.append(f"    path: {artifact['path']}")
        lines.append(f"    sha256: {artifact['sha256']}")
    return lines


def write_metadata(path: pathlib.Path, fields: dict[str, str], artifacts: dict[str, dict[str, str]]) -> None:
    path.write_text("\n".join(metadata_lines(fields, artifacts)) + "\n", encoding="utf-8")


def archive_manifest_versions(path: pathlib.Path) -> list[str]:
    versions: list[str] = []
    with tarfile.open(path, "r:gz") as archive:
        for member in archive.getmembers():
            name = pathlib.PurePosixPath(member.name)
            if name.name not in {"manifest.yaml", "manifest.json"}:
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            data = extracted.read().decode("utf-8", errors="replace")
            for raw in data.splitlines():
                if raw.startswith("version:"):
                    versions.append(raw.split(":", 1)[1].strip().strip("'\""))
                    break
    if not versions:
        raise SystemExit(f"{path} does not contain a packaged manifest")
    return versions


def validate_metadata_artifacts(metadata: pathlib.Path, archives: list[pathlib.Path]) -> None:
    archive_digests = {archive.name: sha256_file(archive) for archive in archives}
    referenced: set[str] = set()
    for target, artifact in metadata_artifacts(metadata).items():
        artifact_path = pathlib.PurePosixPath(artifact["path"].replace(os.sep, "/"))
        if artifact_path.is_absolute() or ".." in artifact_path.parts or len(artifact_path.parts) != 1:
            raise SystemExit(
                f"{metadata} artifact {target!r} path {artifact['path']!r} must be a local archive filename"
            )
        filename = artifact_path.as_posix()
        actual_digest = archive_digests.get(filename)
        if actual_digest is None:
            raise SystemExit(f"{metadata} artifact {target!r} references missing archive {filename}")
        if artifact["sha256"] != actual_digest:
            raise SystemExit(
                f"{metadata} artifact {target!r} checksum {artifact['sha256']} does not match {actual_digest}"
            )
        referenced.add(filename)
    extras = sorted(set(archive_digests) - referenced)
    if extras:
        raise SystemExit(f"{metadata} does not reference archives: {', '.join(extras)}")


def validate_dist(dist_dir: pathlib.Path, want_version: str) -> tuple[pathlib.Path, list[pathlib.Path]]:
    metadata = dist_dir / "provider-release.yaml"
    if not metadata.is_file():
        raise SystemExit(f"{metadata} not found")
    got_metadata_version = metadata_version(metadata)
    if got_metadata_version != want_version:
        raise SystemExit(
            f"{metadata} version {got_metadata_version!r} does not match {want_version!r}"
        )
    archives = sorted(dist_dir.glob("*.tar.gz"))
    if not archives:
        raise SystemExit(f"no .tar.gz archives found in {dist_dir}")
    for archive in archives:
        for got in archive_manifest_versions(archive):
            if got != want_version:
                raise SystemExit(f"{archive} manifest version {got!r} does not match {want_version!r}")
    validate_metadata_artifacts(metadata, archives)
    return metadata, archives


def gcs_destination(root: str, repository: str, provider_ref: str, provider_dir: str, filename: str) -> str:
    root = root.rstrip("/")
    if not root.startswith("gs://"):
        raise SystemExit("--gcs-root must start with gs://")
    owner_repo = repository.strip("/")
    if owner_repo.count("/") != 1:
        raise SystemExit("--repository must be owner/name")
    return f"{root}/github.com/{owner_repo}/{provider_ref}/{provider_dir}/{filename}"


def run(args: list[str]) -> str:
    completed = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if completed.returncode != 0:
        raise RuntimeError(
            f"{' '.join(args)} failed with {completed.returncode}\n{completed.stdout}{completed.stderr}"
        )
    return completed.stdout


def object_metadata(dest: str) -> dict[str, object] | None:
    try:
        return json.loads(run(["gcloud", "storage", "objects", "describe", dest, "--format=json"]))
    except RuntimeError:
        return None


def object_exists(dest: str) -> bool:
    return object_metadata(dest) is not None


def object_bytes(dest: str) -> bytes:
    with tempfile.TemporaryDirectory() as tmp:
        local = pathlib.Path(tmp) / pathlib.PurePosixPath(dest).name
        run(["gcloud", "storage", "cp", dest, str(local)])
        return local.read_bytes()


def upload_new(local: pathlib.Path, dest: str, provider_ref: str, gestalt_ref: str) -> None:
    digest = sha256_file(local)
    metadata = f"provider-ref={provider_ref},gestalt-ref={gestalt_ref},sha256={digest}"
    run([
        "gcloud",
        "storage",
        "cp",
        "--if-generation-match=0",
        f"--custom-metadata={metadata}",
        str(local),
        dest,
    ])


def upload_replace(local: pathlib.Path, dest: str, provider_ref: str, gestalt_ref: str) -> None:
    digest = sha256_file(local)
    metadata = f"provider-ref={provider_ref},gestalt-ref={gestalt_ref},sha256={digest}"
    run([
        "gcloud",
        "storage",
        "cp",
        f"--custom-metadata={metadata}",
        str(local),
        dest,
    ])


def upload_archive(local: pathlib.Path, dest: str, provider_ref: str, gestalt_ref: str, dry_run: bool, merge_existing: bool) -> str:
    digest = sha256_file(local)
    if dry_run:
        print(f"dry-run upload {local} -> {dest} sha256={digest}")
        return digest
    if object_exists(dest):
        remote = object_bytes(dest)
        if remote == local.read_bytes():
            print(f"exists byte-identical {dest}")
            return digest
        if merge_existing:
            remote_digest = sha256_bytes(remote)
            print(f"exists with different bytes; retaining remote {dest} sha256={remote_digest}")
            return remote_digest
        raise SystemExit(f"{dest} already exists with different bytes")
    upload_new(local, dest, provider_ref, gestalt_ref)
    print(f"uploaded {dest}")
    return digest


def upload_metadata(local: pathlib.Path, dest: str, provider_ref: str, gestalt_ref: str, dry_run: bool, merge_existing: bool) -> None:
    digest = sha256_file(local)
    if dry_run:
        print(f"dry-run upload {local} -> {dest} sha256={digest}")
        return
    if object_exists(dest):
        if object_bytes(dest) == local.read_bytes():
            print(f"exists byte-identical {dest}")
            return
        if not merge_existing:
            raise SystemExit(f"{dest} already exists with different bytes")
        upload_replace(local, dest, provider_ref, gestalt_ref)
        print(f"updated {dest}")
        return
    upload_new(local, dest, provider_ref, gestalt_ref)
    print(f"uploaded {dest}")


def download_metadata(dest: str) -> pathlib.Path | None:
    if not object_exists(dest):
        return None
    handle = tempfile.NamedTemporaryFile("wb", delete=False)
    try:
        handle.write(object_bytes(dest))
        return pathlib.Path(handle.name)
    finally:
        handle.close()


def merge_metadata(existing_metadata: pathlib.Path | None, generated_metadata: pathlib.Path) -> dict[str, dict[str, str]]:
    generated_fields = metadata_fields(generated_metadata)
    merged: dict[str, dict[str, str]] = {}
    if existing_metadata is not None:
        existing_fields = metadata_fields(existing_metadata)
        for key in ("schema", "schemaVersion", "package", "kind", "version", "runtime"):
            if existing_fields[key] != generated_fields[key]:
                raise SystemExit(
                    f"existing metadata {key} {existing_fields[key]!r} does not match generated {generated_fields[key]!r}"
                )
        merged.update(metadata_artifacts(existing_metadata))
    merged.update(metadata_artifacts(generated_metadata))
    return merged


def main() -> int:
    args = parse_args()
    provider_ref = validate_ref("--provider-ref", args.provider_ref)
    gestalt_ref = validate_ref("--gestalt-ref", args.gestalt_ref)
    provider_dir = normalize_provider_dir(args.provider_dir)
    want_version = snapshot_version(provider_ref)
    dist_dir = pathlib.Path(args.dist_dir)
    metadata, archives = validate_dist(dist_dir, want_version)
    metadata_dest = gcs_destination(args.gcs_root, args.repository, provider_ref, provider_dir, "provider-release.yaml")
    existing_metadata = download_metadata(metadata_dest) if args.merge_existing and not args.dry_run else None
    fields = metadata_fields(metadata)
    generated_artifacts = metadata_artifacts(metadata)
    merged_artifacts = merge_metadata(existing_metadata, metadata) if args.merge_existing else generated_artifacts
    artifact_targets_by_path = {
        artifact["path"]: target for target, artifact in generated_artifacts.items()
    }
    for archive in archives:
        target = artifact_targets_by_path.get(archive.name)
        if target is None:
            raise SystemExit(f"{metadata} does not reference archive {archive.name}")
        generated_artifacts[target]["sha256"] = upload_archive(
            archive,
            gcs_destination(args.gcs_root, args.repository, provider_ref, provider_dir, archive.name),
            provider_ref,
            gestalt_ref,
            args.dry_run,
            args.merge_existing,
        )
        merged_artifacts[target] = generated_artifacts[target]
    if args.merge_existing:
        write_metadata(metadata, fields, merged_artifacts)

    # Upload archives first; metadata is the discoverable object and must appear last.
    upload_metadata(
        metadata,
        metadata_dest,
        provider_ref,
        gestalt_ref,
        args.dry_run,
        args.merge_existing,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
