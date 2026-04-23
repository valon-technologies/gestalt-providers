from typing import Any

import gestalt
import grpc

from gestalt.gen.v1 import agent_pb2 as _agent_pb2

from internals import SimpleAgentConfig, SimpleAgentOrchestrator, SimpleRunStore

agent_pb2: Any = _agent_pb2


class SimpleAgentRuntimeProvider(
    gestalt.AgentProvider,
    gestalt.MetadataProvider,
    gestalt.WarningsProvider,
    gestalt.Closer,
):
    def __init__(self) -> None:
        self._name = "simple"
        self._warnings: list[str] = []
        self._set_runtime(SimpleAgentConfig.from_dict(name=self._name, raw_config={}))

    def configure(self, name: str, config: dict[str, Any]) -> None:
        self._name = name.strip() or "simple"
        self._set_runtime(SimpleAgentConfig.from_dict(name=self._name, raw_config=config))

    def metadata(self) -> gestalt.ProviderMetadata:
        return gestalt.ProviderMetadata(
            kind=gestalt.ProviderKind.AGENT,
            name=self._name,
            display_name="Simple Agent",
            description="Simple multi-model agent provider for Gestalt with tool calling over LiteLLM.",
            version="0.0.1-alpha.1",
        )

    def warnings(self) -> list[str]:
        return list(self._warnings)

    def close(self) -> None:
        self._store.close()

    def StartRun(self, request: Any, context: grpc.ServicerContext) -> Any:
        return self._orchestrator.start_run(request, context)

    def GetRun(self, request: Any, context: grpc.ServicerContext) -> Any:
        run = self._store.get_run(request.run_id)
        if run is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent run {request.run_id!r} was not found")
        return self._orchestrator.run_to_proto(run)

    def ListRuns(self, request: Any, context: grpc.ServicerContext) -> Any:
        return agent_pb2.ListAgentProviderRunsResponse(
            runs=[self._orchestrator.run_to_proto(run) for run in self._store.list_runs()]
        )

    def CancelRun(self, request: Any, context: grpc.ServicerContext) -> Any:
        run = self._store.request_cancel(request.run_id, request.reason)
        if run is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent run {request.run_id!r} was not found")
        return self._orchestrator.run_to_proto(run)

    def _set_runtime(self, config: SimpleAgentConfig) -> None:
        if hasattr(self, "_store"):
            self._store.close()
        self._config = config
        self._store = SimpleRunStore(run_store=config.run_store, idempotency_store=config.idempotency_store)
        self._store.initialize()
        self._orchestrator = SimpleAgentOrchestrator(config=config, store=self._store)
        self._warnings = self._build_warnings(config)

    def _build_warnings(self, config: SimpleAgentConfig) -> list[str]:
        warnings: list[str] = []
        if not config.default_model:
            warnings.append("set config.defaultModel or pass request.model for every run")
        return warnings


def main() -> None:
    SimpleAgentRuntimeProvider().serve()


if __name__ == "__main__":
    main()
