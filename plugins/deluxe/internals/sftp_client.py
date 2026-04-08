from __future__ import annotations

import io
import posixpath
from dataclasses import dataclass

import gestalt
import paramiko


@dataclass(slots=True)
class DeluxeSFTPClient:
    host: str
    port: int
    username: str
    host_key: str
    private_key: str
    timeout_seconds: float = 30.0

    _transport: paramiko.Transport | None = None
    _sftp: paramiko.SFTPClient | None = None

    @classmethod
    def from_request(cls, req: gestalt.Request) -> DeluxeSFTPClient:
        return cls(
            host=req.connection_param("host").strip(),
            port=int(req.connection_param("port") or "10022"),
            username=req.connection_param("username").strip(),
            host_key=req.connection_param("host_key").strip(),
            private_key=req.token,
        )

    def __enter__(self) -> DeluxeSFTPClient:
        self._connect()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self.close()

    def list_files(self, folder_path: str) -> list[str]:
        sftp = self._require_sftp()
        return [posixpath.join(folder_path, name) for name in sftp.listdir(folder_path)]

    def read_file(self, remote_path: str) -> bytes:
        sftp = self._require_sftp()
        with sftp.open(remote_path, "rb") as remote_file:
            return remote_file.read()

    def write_file(self, remote_path: str, data: bytes) -> None:
        sftp = self._require_sftp()
        with sftp.open(remote_path, "wb") as remote_file:
            remote_file.write(data)

    def close(self) -> None:
        if self._sftp is not None:
            self._sftp.close()
            self._sftp = None
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    def _connect(self) -> None:
        if self._transport is not None and self._sftp is not None:
            return

        pkey = paramiko.RSAKey.from_private_key(io.StringIO(self.private_key))
        transport = paramiko.Transport((self.host, self.port))
        transport.banner_timeout = self.timeout_seconds
        transport.auth_timeout = self.timeout_seconds
        transport.start_client(timeout=self.timeout_seconds)

        expected_key = _parse_host_key(self.host, self.host_key)
        remote_key = transport.get_remote_server_key()
        if expected_key != remote_key:
            transport.close()
            raise ValueError("Deluxe SFTP host key did not match the configured host key")

        transport.auth_publickey(self.username, pkey)
        self._transport = transport
        self._sftp = paramiko.SFTPClient.from_transport(transport)

    def _require_sftp(self) -> paramiko.SFTPClient:
        if self._sftp is None:
            raise RuntimeError("SFTP client is not connected")
        return self._sftp


def _parse_host_key(host: str, host_key: str) -> paramiko.PKey:
    candidate = host_key.strip()
    if not candidate:
        raise ValueError("host_key is required")

    parts = candidate.split()
    if len(parts) == 2:
        candidate = f"{host} {candidate}"

    entry = paramiko.hostkeys.HostKeyEntry.from_line(candidate)
    if entry is None or entry.key is None:
        raise ValueError("host_key must be a valid SSH public key")
    return entry.key
