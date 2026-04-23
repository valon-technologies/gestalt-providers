import os
from typing import Any

import gestalt
import grpc

from gestalt.gen.v1 import agent_pb2 as _agent_pb2

from internals import SimpleAgentConfig, SimpleAgentOrchestrator, SimpleRunStore

agent_pb2: Any = _agent_pb2


class SimpleAgentRuntimeProvider(
    gestalt.AgentProvider, gestalt.MetadataProvider, gestalt.WarningsProvider, gestalt.Closer
):
    def __init__(self) -> None:
        self._name = "simple"
        self._warnings: list[str] = ["provider has not been configured"]
        self._config: SimpleAgentConfig | None = None
        self._store: SimpleRunStore | None = None
        self._orchestrator: SimpleAgentOrchestrator | None = None

    def configure(self, name: str, config: dict[str, Any]) -> None:
        self._name = name.strip() or "simple"
        self._set_runtime(SimpleAgentConfig.from_dict(name=self._name, raw_config=config))

    def metadata(self) -> gestalt.ProviderMetadata:
        return gestalt.ProviderMetadata(
            kind=gestalt.ProviderKind.AGENT,
            name=self._name,
            display_name="Simple Agent",
            description="Simple multi-model agent provider for Gestalt with tool calling over LiteLLM.",
            version="0.0.1-alpha.4",
        )

    def warnings(self) -> list[str]:
        return list(self._warnings)

    def close(self) -> None:
        if self._store is not None:
            self._store.close()

    def StartRun(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, _ = self._require_runtime(context)
        return orchestrator.start_run(request, context)

    def GetRun(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, store = self._require_runtime(context)
        run = store.get_run(request.run_id)
        if run is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent run {request.run_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return orchestrator.run_to_proto(run)

    def ListRuns(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, store = self._require_runtime(context)
        return agent_pb2.ListAgentProviderRunsResponse(
            runs=[orchestrator.run_to_proto(run) for run in store.list_runs()]
        )

    def CancelRun(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, store = self._require_runtime(context)
        run = store.request_cancel(request.run_id, request.reason)
        if run is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent run {request.run_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return orchestrator.run_to_proto(run)

    def _require_runtime(self, context: grpc.ServicerContext) -> tuple[SimpleAgentOrchestrator, SimpleRunStore]:
        if self._orchestrator is None or self._store is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "agent provider has not been configured")
        return self._orchestrator, self._store

    def _set_runtime(self, config: SimpleAgentConfig) -> None:
        if self._store is not None:
            self._store.close()
        self._config = config
        self._apply_backend_env(config)
        self._store = SimpleRunStore(run_store=config.run_store, idempotency_store=config.idempotency_store)
        self._store.initialize()
        self._orchestrator = SimpleAgentOrchestrator(config=config, store=self._store)
        self._warnings = self._build_warnings(config)

    def _build_warnings(self, config: SimpleAgentConfig) -> list[str]:
        warnings: list[str] = []
        if not config.default_model:
            warnings.append("set config.defaultModel or pass request.model for every run")
        return warnings

    def _apply_backend_env(self, config: SimpleAgentConfig) -> None:
        if config.anthropic_api_key:
            os.environ["ANTHROPIC_API_KEY"] = config.anthropic_api_key
        if config.openai_api_key:
            os.environ["OPENAI_API_KEY"] = config.openai_api_key


provider = SimpleAgentRuntimeProvider()
