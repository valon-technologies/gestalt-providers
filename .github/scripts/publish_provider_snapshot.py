#!/usr/bin/env python3

import argparse
import hashlib
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
    return parser.parse_args()


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def object_exists(dest: str) -> bool:
    try:
        run(["gcloud", "storage", "objects", "describe", dest, "--format=json"])
        return True
    except RuntimeError:
        return False


def object_bytes(dest: str) -> bytes:
    with tempfile.TemporaryDirectory() as tmp:
        local = pathlib.Path(tmp) / pathlib.PurePosixPath(dest).name
        run(["gcloud", "storage", "cp", dest, str(local)])
        return local.read_bytes()


def upload_write_once(local: pathlib.Path, dest: str, provider_ref: str, gestalt_ref: str, dry_run: bool) -> None:
    digest = sha256_file(local)
    if dry_run:
        print(f"dry-run upload {local} -> {dest} sha256={digest}")
        return
    if object_exists(dest):
        if object_bytes(dest) == local.read_bytes():
            print(f"exists byte-identical {dest}")
            return
        raise SystemExit(f"{dest} already exists with different bytes")
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
    print(f"uploaded {dest}")


def main() -> int:
    args = parse_args()
    provider_ref = validate_ref("--provider-ref", args.provider_ref)
    gestalt_ref = validate_ref("--gestalt-ref", args.gestalt_ref)
    provider_dir = normalize_provider_dir(args.provider_dir)
    want_version = snapshot_version(provider_ref)
    dist_dir = pathlib.Path(args.dist_dir)
    metadata, archives = validate_dist(dist_dir, want_version)

    # Upload archives first; metadata is the discoverable object and must appear last.
    for archive in archives:
        upload_write_once(
            archive,
            gcs_destination(args.gcs_root, args.repository, provider_ref, provider_dir, archive.name),
            provider_ref,
            gestalt_ref,
            args.dry_run,
        )
    upload_write_once(
        metadata,
        gcs_destination(args.gcs_root, args.repository, provider_ref, provider_dir, "provider-release.yaml"),
        provider_ref,
        gestalt_ref,
        args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
