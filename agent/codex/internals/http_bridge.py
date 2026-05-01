from __future__ import annotations

import asyncio
import contextlib
import secrets
import socket
import threading
import time
from typing import Any

import uvicorn
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.applications import Starlette
from starlette.routing import Mount

from .gestalt_mcp_bridge import BridgeContext, create_server


class BridgeHTTPServer:
    def __init__(self, context: BridgeContext) -> None:
        self._context = context
        self._thread: threading.Thread | None = None
        self._server: uvicorn.Server | None = None
        self._socket: socket.socket | None = None
        self._port = 0
        self._path_token = secrets.token_urlsafe(24)

    @property
    def url(self) -> str:
        if self._port <= 0:
            raise RuntimeError("bridge HTTP server has not started")
        return f"http://127.0.0.1:{self._port}/{self._path_token}/mcp"

    def start(self) -> None:
        if self._thread is not None:
            return
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        sock.listen(128)
        self._socket = sock
        self._port = int(sock.getsockname()[1])
        app = _app(self._context, path_token=self._path_token)
        config = uvicorn.Config(app=app, log_level="warning", access_log=False, lifespan="on")
        server = uvicorn.Server(config=config)
        self._server = server

        def run() -> None:
            asyncio.run(server.serve(sockets=[sock]))

        self._thread = threading.Thread(target=run, name=f"codex-gestalt-mcp-{self._port}", daemon=True)
        self._thread.start()
        deadline = time.time() + 5
        while time.time() < deadline:
            if server.started:
                return
            if not self._thread.is_alive():
                break
            time.sleep(0.01)
        self.stop()
        raise RuntimeError("timed out starting Gestalt MCP HTTP bridge")

    def stop(self) -> None:
        server = self._server
        if server is not None:
            server.should_exit = True
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5)
        sock = self._socket
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.close()
        self._server = None
        self._thread = None
        self._socket = None


def _app(context: BridgeContext, *, path_token: str) -> Starlette:
    session_manager = StreamableHTTPSessionManager(create_server(context), stateless=True)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> Any:
        del app
        async with session_manager.run():
            yield

    return Starlette(routes=[Mount(f"/{path_token}/mcp", app=session_manager.handle_request)], lifespan=lifespan)
