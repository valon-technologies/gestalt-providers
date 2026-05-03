import { mkdtemp, rm } from "node:fs/promises";
import { existsSync, readFileSync } from "node:fs";
import { createServer, type Http2Server } from "node:http2";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { Code, ConnectError, createClient } from "@connectrpc/connect";
import {
  connectNodeAdapter,
  createGrpcTransport,
} from "@connectrpc/connect-node";
import { Client as McpClient } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { afterEach, describe, expect, test } from "bun:test";
import {
  AgentExecutionStatus,
  AgentToolSourceMode,
  createAgentProviderService,
  type ExecuteAgentToolRequest,
  type ListedAgentTool,
} from "@valon-technologies/gestalt";
import {
  agentContractSchemas,
  agentContractServices,
  createAgentContractMessage,
} from "@valon-technologies/gestalt/test/agent-contract";

import {
  AGENT_HOST_RELAY_TOKEN_HEADER,
  ENV_AGENT_HOST_SOCKET,
  ENV_AGENT_HOST_SOCKET_TOKEN,
} from "../src/agent_host.ts";
import {
  DEFAULT_TIMEOUT_SECONDS,
  type CursorAgentConfig,
} from "../src/config.ts";
import { createCursorPlatformOptions } from "../src/cursor_platform.ts";
import { startMcpBridge } from "../src/mcp_bridge.ts";
import {
  createCursorAgentProvider,
  type CursorAgentProvider,
} from "../src/provider.ts";
import { CursorSDKRunner, type CursorAgentFactory } from "../src/runner.ts";
import type { ToolEntry } from "../src/tools.ts";

const {
  AgentHost: AgentHostService,
  AgentProvider: VendoredAgentProviderService,
} = agentContractServices as Record<string, any>;
const {
  CancelAgentProviderTurnRequestSchema,
  CreateAgentProviderSessionRequestSchema,
  CreateAgentProviderTurnRequestSchema,
  ExecuteAgentToolResponseSchema,
  GetAgentProviderCapabilitiesRequestSchema,
  GetAgentProviderSessionRequestSchema,
  GetAgentProviderTurnRequestSchema,
  ListAgentProviderTurnEventsRequestSchema,
  ListAgentToolsResponseSchema,
  ListedAgentToolSchema,
} = agentContractSchemas as Record<string, any>;

const activeHosts: FakeAgentHost[] = [];

function create<T = any>(schema: any, input: Record<string, unknown>): T {
  return createAgentContractMessage<T>(schema, input);
}

afterEach(async () => {
  for (const host of activeHosts.splice(0)) {
    await host.close();
  }
  delete process.env[ENV_AGENT_HOST_SOCKET];
  delete process.env[ENV_AGENT_HOST_SOCKET_TOKEN];
});

describe("Cursor agent provider contract", () => {
  test("package metadata declares an agent provider target", () => {
    const pkg = JSON.parse(
      readFileSync(resolve(import.meta.dir, "../package.json"), "utf8"),
    ) as {
      gestalt?: { provider?: { kind?: string; target?: string } };
    };
    expect(pkg.gestalt?.provider?.kind).toBe("agent");
    expect(pkg.gestalt?.provider?.target).toBe("./src/provider.ts#provider");
  });

  test("real Cursor SDK creates an agent with the packaged in-memory platform stores", async () => {
    const cursor = await import("@cursor/sdk");
    const agent = await cursor.Agent.create({
      name: "Gestalt smoke",
      model: { id: "composer-2" },
      local: { cwd: process.cwd(), settingSources: [] },
      agents: {},
      platform: createCursorPlatformOptions(process.cwd()),
    });
    expect(agent.agentId).toBeString();
    await agent[Symbol.asyncDispose]();
  });

  test("capabilities survive the f9 AgentProvider service adapter", async () => {
    const provider = await configuredProvider();

    const socketDir = await mkdtemp(join(tmpdir(), "cursor-provider-runtime-"));
    const socketPath = join(socketDir, "provider.sock");
    const server = createServer(
      connectNodeAdapter({
        grpc: true,
        grpcWeb: false,
        connect: false,
        routes(router) {
          router.service(
            VendoredAgentProviderService,
            createAgentProviderService(provider),
          );
        },
      }),
    );
    await listenUnix(server, socketPath);
    try {
      const client = createClient(
        VendoredAgentProviderService,
        createGrpcTransport({
          baseUrl: "http://localhost",
          nodeOptions: { path: socketPath },
        }),
      ) as any;
      const capabilities = await client.getCapabilities(
        create(GetAgentProviderCapabilitiesRequestSchema, {}),
      );
      expect(capabilities.streamingText).toBe(false);
      expect(capabilities.toolCalls).toBe(true);
      expect(capabilities.boundedListHydration).toBe(true);
      expect(capabilities.structuredOutput).toBe(false);
      expect(capabilities.interactions).toBe(false);
      expect(capabilities.resumableTurns).toBe(false);
      expect(capabilities.supportedToolSources).toEqual([
        AgentToolSourceMode.MCP_CATALOG,
      ]);
    } finally {
      await closeHttp2(server);
      await rm(socketDir, { recursive: true, force: true });
    }
  });

  test("runs a turn through Cursor SDK options, MCP tools, and AgentHost ExecuteTool", async () => {
    const host = await FakeAgentHost.start({
      pages: [
        {
          tools: [
            tool({
              id: "tool-weather",
              mcpName: "weather",
              title: "Weather",
              inputSchema: JSON.stringify({
                type: "object",
                properties: { city: { type: "string" } },
              }),
            }),
          ],
        },
      ],
      executeBody: '{"forecast":"sunny"}',
    });
    activeHosts.push(host);
    process.env[ENV_AGENT_HOST_SOCKET] = host.socketPath;
    process.env[ENV_AGENT_HOST_SOCKET_TOKEN] = "relay-token";

    const cursor = new FakeCursorAgentFactory(async (options, prompt) => {
      expect(options.model).toEqual({ id: "composer-2" });
      const gestaltServer = options.mcpServers?.gestalt;
      expect(gestaltServer?.type).toBe("http");
      if (!gestaltServer || !("url" in gestaltServer)) {
        throw new Error("expected HTTP Gestalt MCP server");
      }
      expect(options.local?.cwd).toBe(process.cwd());
      expect(options.local?.settingSources).toEqual([]);
      expect("sandboxOptions" in (options.local ?? {})).toBe(false);
      expect(options.platform?.workspaceRef).toBe(process.cwd());
      expect(typeof options.platform?.stateRoot).toBe("string");
      expect(options.agents).toEqual({});
      expect(prompt).toContain('"role":"system"');
      expect(prompt).toContain("Be concise.");

      const result = await callFirstMcpTool(gestaltServer.url, {
        Authorization: gestaltServer.headers?.Authorization ?? "",
      });
      return [
        {
          type: "tool_call",
          agent_id: "fake-agent",
          run_id: "fake-run",
          call_id: "cursor-call-1",
          name: "weather",
          status: "completed",
          args: { city: "Oakland" },
          result,
        },
        {
          type: "assistant",
          agent_id: "fake-agent",
          run_id: "fake-run",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Forecast: sunny" }],
          },
        },
      ];
    });
    const provider = await configuredProvider({
      config: { systemPrompt: "Be concise." },
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: cursor }),
    });
    const session = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        sessionId: "session-1",
        idempotencyKey: "session-key",
      }),
    );

    await provider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "turn-1",
        sessionId: "session-1",
        idempotencyKey: "turn-key",
        messages: [{ role: "user", text: "weather?" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant-1",
        toolRefs: [{ plugin: "weather-plugin", operation: "forecast" }],
      }),
    );
    const turn = await waitForTurn(
      provider,
      "turn-1",
      AgentExecutionStatus.SUCCEEDED,
    );
    expect(turn.outputText).toBe("Forecast: sunny");
    expect(host.relayTokens).toContain("relay-token");
    expect(host.listRequests).toHaveLength(1);
    expect(host.executeRequests).toHaveLength(1);
    expect(host.executeRequests[0]?.toolCallId).toBe("sdk-1");
    expect(host.executeRequests[0]?.idempotencyKey).toBe(
      "agent/cursor-sdk:turn-1:sdk-1:weather",
    );
    expect(host.executeRequests[0]?.arguments).toEqual({ city: "Oakland" });
    expect(existsSync(cursor.stateRoots[0] ?? "")).toBe(false);
  });

  test("configured sandbox flag is forwarded and absent when unset", async () => {
    const cursor = new FakeCursorAgentFactory(async (options) => {
      expect(options.local?.sandboxOptions).toEqual({ enabled: true });
      return [
        {
          type: "assistant",
          agent_id: "a",
          run_id: "r",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "ok" }],
          },
        },
      ];
    });
    const host = await FakeAgentHost.start({
      pages: [{ tools: [tool({ id: "t", mcpName: "t" })] }],
    });
    activeHosts.push(host);
    process.env[ENV_AGENT_HOST_SOCKET] = host.socketPath;
    const provider = await configuredProvider({
      config: { sandboxEnabled: true },
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: cursor }),
    });
    await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, { sessionId: "s" }),
    );
    await provider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "t",
        sessionId: "s",
        messages: [{ role: "user", text: "hi" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant",
        toolRefs: [{ plugin: "p", operation: "o" }],
      }),
    );
    await waitForTurn(provider, "t", AgentExecutionStatus.SUCCEEDED);
  });

  test("invalid reconfiguration preserves the active runtime state", async () => {
    const provider = await configuredProvider();
    await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        sessionId: "keep-session",
      }),
    );

    await expect(
      provider.configureProvider("broken-cursor", {
        defaultModel: "composer-2",
        workingDirectory: join(tmpdir(), "missing-cursor-workdir"),
      }),
    ).rejects.toThrow("workingDirectory");

    const session = await provider.getSession(
      create(GetAgentProviderSessionRequestSchema, {
        sessionId: "keep-session",
      }),
    );
    expect(session.id).toBe("keep-session");
    expect((await provider.warnings())[0]).toContain("CURSOR_API_KEY");
  });

  test("rejects unsupported session and turn inputs", async () => {
    const provider = await configuredProvider();
    await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, { sessionId: "s" }),
    );
    const base = {
      turnId: "turn",
      sessionId: "s",
      messages: [{ role: "user", text: "hi" }],
      toolSource: AgentToolSourceMode.MCP_CATALOG,
      runGrant: "grant",
      toolRefs: [{ plugin: "p", operation: "o" }],
    };
    const invalidCases: Array<[string, Record<string, unknown>, string]> = [
      [
        "wrong source",
        { toolSource: 999 as AgentToolSourceMode },
        "requires toolSource",
      ],
      ["missing grant", { runGrant: "" }, "run_grant is required"],
      ["missing refs", { toolRefs: [] }, "tool_refs are required"],
      [
        "wildcard ref",
        { toolRefs: [{ plugin: "p", operation: "*" }] },
        "wildcard",
      ],
      [
        "missing operation",
        { toolRefs: [{ plugin: "p" }] },
        "operation is required",
      ],
      [
        "missing plugin system",
        { toolRefs: [{ operation: "o" }] },
        "exactly one",
      ],
      [
        "both plugin system",
        { toolRefs: [{ plugin: "p", system: "workflow", operation: "o" }] },
        "exactly one",
      ],
      [
        "bad system",
        { toolRefs: [{ system: "not-workflow", operation: "o" }] },
        "not supported",
      ],
      [
        "resolved tools",
        { tools: [{ id: "resolved" }] },
        "resolved tools are not supported",
      ],
      [
        "response schema",
        { responseSchema: { type: "object" } },
        "response_schema",
      ],
      [
        "model options",
        { modelOptions: { unsupported: true } },
        "model_options",
      ],
    ];
    for (const [name, patch, message] of invalidCases) {
      await expect(
        provider.createTurn(
          create(CreateAgentProviderTurnRequestSchema, { ...base, ...patch }),
        ),
        name,
      ).rejects.toThrow(message);
    }
  });

  test("maps Cursor failures, timeouts, and cancellations onto terminal turns", async () => {
    const host = await FakeAgentHost.start({
      pages: [{ tools: [tool({ id: "t", mcpName: "tool" })] }],
    });
    activeHosts.push(host);
    process.env[ENV_AGENT_HOST_SOCKET] = host.socketPath;

    const failingCursor = new FakeCursorAgentFactory(async () => ({
      messages: [],
      waitStatus: "error",
    }));
    const failureProvider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: failingCursor }),
    });
    await failureProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, { sessionId: "failure" }),
    );
    await failureProvider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "failure-turn",
        sessionId: "failure",
        messages: [{ role: "user", text: "fail" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant",
        toolRefs: [{ plugin: "p", operation: "o" }],
      }),
    );
    const failed = await waitForTurn(
      failureProvider,
      "failure-turn",
      AgentExecutionStatus.FAILED,
    );
    expect(failed.statusMessage).toContain("status error");

    const timeoutCursor = new FakeCursorAgentFactory(async () => ({
      messages: [
        {
          type: "assistant",
          agent_id: "a",
          run_id: "r",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "late" }],
          },
        },
      ],
      streamDelayMs: 100,
    }));
    const timeoutProvider = await configuredProvider({
      config: { timeoutSeconds: 0.01 },
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: timeoutCursor }),
    });
    await timeoutProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, { sessionId: "timeout" }),
    );
    await timeoutProvider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "timeout-turn",
        sessionId: "timeout",
        messages: [{ role: "user", text: "timeout" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant",
        toolRefs: [{ plugin: "p", operation: "o" }],
      }),
    );
    const timedOut = await waitForTurn(
      timeoutProvider,
      "timeout-turn",
      AgentExecutionStatus.CANCELED,
    );
    expect(timedOut.statusMessage).toContain("timed out");

    const preRunCursor = new FakeCursorAgentFactory(async () => {
      throw new Error(
        "Cursor agent should not be created after pre-run cancellation",
      );
    });
    const preRunProvider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: preRunCursor }),
    });
    await preRunProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        sessionId: "pre-run-cancel",
      }),
    );
    await preRunProvider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "pre-run-turn",
        sessionId: "pre-run-cancel",
        messages: [{ role: "user", text: "cancel" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant",
        toolRefs: [{ plugin: "p", operation: "o" }],
      }),
    );
    await preRunProvider.cancelTurn(
      create(CancelAgentProviderTurnRequestSchema, {
        turnId: "pre-run-turn",
        reason: "client canceled",
      }),
    );
    const preRunCanceled = await waitForTurn(
      preRunProvider,
      "pre-run-turn",
      AgentExecutionStatus.CANCELED,
    );
    expect(preRunCanceled.statusMessage).toBe("client canceled");
    expect(preRunCursor.options).toHaveLength(0);

    let sendStarted = false;
    let resolveSend: ((response: FakeCursorResponse) => void) | undefined;
    const pendingSendCursor = new FakeCursorAgentFactory(
      async () =>
        await new Promise<FakeCursorResponse>((resolveSendPromise) => {
          sendStarted = true;
          resolveSend = resolveSendPromise;
        }),
    );
    const pendingSendProvider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: pendingSendCursor }),
    });
    await pendingSendProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        sessionId: "send-cancel",
      }),
    );
    await pendingSendProvider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "send-turn",
        sessionId: "send-cancel",
        messages: [{ role: "user", text: "cancel while sending" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant",
        toolRefs: [{ plugin: "p", operation: "o" }],
      }),
    );
    await waitUntil(() => sendStarted);
    await pendingSendProvider.cancelTurn(
      create(CancelAgentProviderTurnRequestSchema, {
        turnId: "send-turn",
        reason: "client canceled",
      }),
    );
    resolveSend?.({
      messages: [
        {
          type: "assistant",
          agent_id: "a",
          run_id: "r",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "late" }],
          },
        },
      ],
    });
    await waitUntil(() => pendingSendCursor.runs[0]?.canceled === true);
    const pendingSendCanceled = await waitForTurn(
      pendingSendProvider,
      "send-turn",
      AgentExecutionStatus.CANCELED,
    );
    expect(pendingSendCanceled.statusMessage).toBe("client canceled");

    const liveCursor = new FakeCursorAgentFactory(async () => ({
      messages: [
        {
          type: "assistant",
          agent_id: "a",
          run_id: "r",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "late" }],
          },
        },
      ],
      streamDelayMs: 100,
    }));
    const liveProvider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: liveCursor }),
    });
    await liveProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        sessionId: "live-cancel",
      }),
    );
    await liveProvider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "live-turn",
        sessionId: "live-cancel",
        messages: [{ role: "user", text: "cancel" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant",
        toolRefs: [{ plugin: "p", operation: "o" }],
      }),
    );
    await waitUntil(() => liveCursor.runs.length === 1);
    await liveProvider.cancelTurn(
      create(CancelAgentProviderTurnRequestSchema, {
        turnId: "live-turn",
        reason: "client canceled",
      }),
    );
    await waitUntil(() => liveCursor.runs[0]?.canceled === true);
    const liveCanceled = await waitForTurn(
      liveProvider,
      "live-turn",
      AgentExecutionStatus.CANCELED,
    );
    expect(liveCanceled.statusMessage).toBe("client canceled");
  });

  test("close waits for active turn cancellation and cleanup", async () => {
    const host = await FakeAgentHost.start({
      pages: [{ tools: [tool({ id: "t", mcpName: "tool" })] }],
    });
    activeHosts.push(host);
    process.env[ENV_AGENT_HOST_SOCKET] = host.socketPath;
    const cursor = new FakeCursorAgentFactory(async () => ({
      messages: [
        {
          type: "assistant",
          agent_id: "a",
          run_id: "r",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "late" }],
          },
        },
      ],
      streamDelayMs: 25,
      cancelRejects: true,
    }));
    const provider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: cursor }),
    });
    await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, { sessionId: "close" }),
    );
    await provider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "close-turn",
        sessionId: "close",
        messages: [{ role: "user", text: "close" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant",
        toolRefs: [{ plugin: "p", operation: "o" }],
      }),
    );
    await waitUntil(() => cursor.runs.length === 1);
    const stateRoot = cursor.stateRoots[0] ?? "";
    expect(existsSync(stateRoot)).toBe(true);

    await provider.closeProvider();

    expect(cursor.runs[0]?.canceled).toBe(true);
    expect(existsSync(stateRoot)).toBe(false);
  });

  test("handles ListTools paging errors without invoking tools", async () => {
    const cases: Array<
      [
        string,
        Array<{ tools: ListedAgentTool[]; nextPageToken?: string }>,
        string,
      ]
    > = [
      ["empty", [{ tools: [] }], "no tools"],
      [
        "duplicate",
        [
          {
            tools: [
              tool({ id: "a", mcpName: "dup" }),
              tool({ id: "b", mcpName: "dup" }),
            ],
          },
        ],
        "duplicate",
      ],
      [
        "unsafe",
        [{ tools: [tool({ id: "a", mcpName: "bad name" })] }],
        "unsafe",
      ],
      [
        "unsafe slash",
        [{ tools: [tool({ id: "a", mcpName: "bad/name" })] }],
        "unsafe",
      ],
      [
        "unsafe colon",
        [{ tools: [tool({ id: "a", mcpName: "bad:name" })] }],
        "unsafe",
      ],
      [
        "unsafe unicode",
        [{ tools: [tool({ id: "a", mcpName: "å" })] }],
        "unsafe",
      ],
      [
        "unsafe length",
        [{ tools: [tool({ id: "a", mcpName: "a".repeat(129) })] }],
        "unsafe",
      ],
      [
        "missing id",
        [{ tools: [tool({ id: "", mcpName: "ok" })] }],
        "without an id",
      ],
      [
        "missing mcp",
        [{ tools: [tool({ id: "a", mcpName: "" })] }],
        "without an mcp_name",
      ],
      [
        "repeated token",
        [{ tools: [], nextPageToken: "again" }],
        "repeated page token",
      ],
    ];
    for (const [name, pages, message] of cases) {
      const host = await FakeAgentHost.start({ pages });
      activeHosts.push(host);
      process.env[ENV_AGENT_HOST_SOCKET] = host.socketPath;
      const provider = await configuredProvider({
        runnerFactory: (config) =>
          new CursorSDKRunner(config, {
            agentFactory: new FakeCursorAgentFactory(),
          }),
      });
      await provider.createSession(
        create(CreateAgentProviderSessionRequestSchema, { sessionId: name }),
      );
      await provider.createTurn(
        create(CreateAgentProviderTurnRequestSchema, {
          turnId: `turn-${name}`,
          sessionId: name,
          messages: [{ role: "user", text: "hi" }],
          toolSource: AgentToolSourceMode.MCP_CATALOG,
          runGrant: "grant",
          toolRefs: [{ plugin: "p", operation: "o" }],
        }),
      );
      const turn = await waitForTurn(
        provider,
        `turn-${name}`,
        AgentExecutionStatus.FAILED,
      );
      expect(turn.statusMessage).toContain(message);
      await host.close();
      activeHosts.pop();
    }
  });

  test("MCP bridge enforces auth, raw tool names, unknown tools, and error mapping", async () => {
    const calls: Array<{ callId: string; args: Record<string, unknown> }> = [];
    const bridge = await startMcpBridge({
      tools: [
        {
          toolId: "raw-id",
          mcpName: "raw_tool",
          title: "Raw Tool",
          description: "Uses raw MCP names",
          inputSchema: { type: "object", additionalProperties: true },
        },
      ],
      executeTool: async (_entry: ToolEntry, callId, args) => {
        calls.push({ callId, args });
        return { status: 418, body: "nope" };
      },
    });
    try {
      await expect(callMcpTools(bridge.url, {})).rejects.toThrow();
      const listed = await listMcpTools(bridge.url, bridge.headers);
      expect(listed.tools.map((entry) => entry.name)).toEqual(["raw_tool"]);
      const result = await callMcpTool(bridge.url, bridge.headers, "raw_tool", {
        value: 1,
      });
      expect(result.isError).toBe(true);
      expect((result as any).content[0]).toEqual({
        type: "text",
        text: "nope",
      });
      expect(calls).toEqual([{ callId: "sdk-1", args: { value: 1 } }]);
      await expect(
        callMcpTool(bridge.url, bridge.headers, "missing", {}),
      ).rejects.toThrow();
    } finally {
      await bridge.close();
    }
  });

  test("interaction stubs and turn events match the in-memory contract", async () => {
    const provider = await configuredProvider();
    await expect(
      provider.getInteraction({ interactionId: "missing" } as never),
    ).rejects.toThrow("was not found");
    expect(
      await provider.listInteractions({ turnId: "turn" } as never),
    ).toEqual([]);
    await expect(
      provider.resolveInteraction({ interactionId: "missing" } as never),
    ).rejects.toThrow("was not found");

    await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, { sessionId: "events" }),
    );
    const host = await FakeAgentHost.start({
      pages: [{ tools: [tool({ id: "t", mcpName: "t" })] }],
    });
    activeHosts.push(host);
    process.env[ENV_AGENT_HOST_SOCKET] = host.socketPath;
    const cursor = new FakeCursorAgentFactory(async () => [
      {
        type: "assistant",
        agent_id: "a",
        run_id: "r",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "done" }],
        },
      },
    ]);
    const eventProvider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: cursor }),
    });
    await eventProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, { sessionId: "events" }),
    );
    await eventProvider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "events-turn",
        sessionId: "events",
        messages: [{ role: "user", text: "hi" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant",
        toolRefs: [{ plugin: "p", operation: "o" }],
      }),
    );
    await waitForTurn(
      eventProvider,
      "events-turn",
      AgentExecutionStatus.SUCCEEDED,
    );
    const events = await eventProvider.listTurnEvents(
      create(ListAgentProviderTurnEventsRequestSchema, {
        turnId: "events-turn",
      }),
    );
    expect(events.map((event) => event.type)).toContain("turn.completed");
  });
});

async function configuredProvider(
  input: {
    config?: Record<string, unknown>;
    runnerFactory?: (config: CursorAgentConfig) => CursorSDKRunner;
  } = {},
): Promise<CursorAgentProvider> {
  const provider = createCursorAgentProvider(
    input.runnerFactory ? { runnerFactory: input.runnerFactory } : {},
  );
  await provider.configureProvider("agent-cursor", {
    defaultModel: "composer-2",
    timeoutSeconds: DEFAULT_TIMEOUT_SECONDS,
    workingDirectory: process.cwd(),
    ...(input.config ?? {}),
  });
  return provider;
}

function tool(input: {
  id: string;
  mcpName: string;
  title?: string;
  description?: string;
  inputSchema?: string;
}): ListedAgentTool {
  return create(ListedAgentToolSchema, {
    id: input.id,
    mcpName: input.mcpName,
    title: input.title ?? "",
    description: input.description ?? "",
    inputSchema:
      input.inputSchema ?? '{"type":"object","additionalProperties":true}',
  });
}

async function waitForTurn(
  provider: CursorAgentProvider,
  turnId: string,
  status: AgentExecutionStatus,
): Promise<Awaited<ReturnType<CursorAgentProvider["getTurn"]>>> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const turn = await provider.getTurn(
      create(GetAgentProviderTurnRequestSchema, { turnId }),
    );
    if (turn.status === status) {
      return turn;
    }
    await new Promise((resolveTimer) => setTimeout(resolveTimer, 10));
  }
  return await provider.getTurn(
    create(GetAgentProviderTurnRequestSchema, { turnId }),
  );
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolveTimer) => setTimeout(resolveTimer, 10));
  }
  throw new Error("condition was not met before timeout");
}

type FakeCursorResponse =
  | import("@cursor/sdk").SDKMessage[]
  | {
      messages?: import("@cursor/sdk").SDKMessage[];
      waitStatus?: import("@cursor/sdk").RunResultStatus;
      waitResult?: string;
      streamDelayMs?: number;
      cancelRejects?: boolean;
    };

class FakeCursorAgentFactory implements CursorAgentFactory {
  readonly options: import("@cursor/sdk").AgentOptions[] = [];
  readonly stateRoots: string[] = [];
  readonly runs: FakeRun[] = [];

  constructor(
    private readonly streamFactory: (
      options: import("@cursor/sdk").AgentOptions,
      prompt: string,
    ) => Promise<FakeCursorResponse> = async () => [
      {
        type: "assistant",
        agent_id: "fake-agent",
        run_id: "fake-run",
        message: { role: "assistant", content: [{ type: "text", text: "ok" }] },
      },
    ],
  ) {}

  async create(
    options: import("@cursor/sdk").AgentOptions,
  ): Promise<import("@cursor/sdk").SDKAgent> {
    this.options.push(options);
    if (typeof options.platform?.stateRoot === "string") {
      this.stateRoots.push(options.platform.stateRoot);
    }
    return new FakeSDKAgent(options, this.streamFactory, (run) => {
      this.runs.push(run);
    }) as unknown as import("@cursor/sdk").SDKAgent;
  }
}

class FakeSDKAgent {
  readonly agentId = "fake-agent";
  readonly model = undefined;
  constructor(
    private readonly options: import("@cursor/sdk").AgentOptions,
    private readonly streamFactory: (
      options: import("@cursor/sdk").AgentOptions,
      prompt: string,
    ) => Promise<FakeCursorResponse>,
    private readonly onRun: (run: FakeRun) => void,
  ) {}
  async send(prompt: string): Promise<import("@cursor/sdk").Run> {
    const response = await this.streamFactory(this.options, prompt);
    const run = Array.isArray(response)
      ? new FakeRun(response)
      : new FakeRun(response.messages ?? [], response);
    this.onRun(run);
    return run as unknown as import("@cursor/sdk").Run;
  }
  close(): void {}
  async reload(): Promise<void> {}
  async [Symbol.asyncDispose](): Promise<void> {}
  async listArtifacts(): Promise<import("@cursor/sdk").SDKArtifact[]> {
    return [];
  }
  async downloadArtifact(_path: string): Promise<Buffer> {
    return Buffer.from("");
  }
}

class FakeRun {
  readonly id = "fake-run";
  readonly agentId = "fake-agent";
  status: import("@cursor/sdk").RunStatus = "running";
  result = "";
  readonly model = undefined;
  readonly durationMs = undefined;
  readonly git = undefined;
  readonly createdAt = Date.now();
  canceled = false;

  constructor(
    private readonly messages: import("@cursor/sdk").SDKMessage[],
    private readonly options: Exclude<
      FakeCursorResponse,
      import("@cursor/sdk").SDKMessage[]
    > = {},
  ) {}

  supports(): boolean {
    return true;
  }
  unsupportedReason(): string | undefined {
    return undefined;
  }
  async *stream(): AsyncGenerator<import("@cursor/sdk").SDKMessage, void> {
    for (const message of this.messages) {
      if (this.options.streamDelayMs) {
        await new Promise((resolveTimer) =>
          setTimeout(resolveTimer, this.options.streamDelayMs),
        );
      }
      if (this.canceled) {
        this.status = "cancelled";
        return;
      }
      if (message.type === "assistant") {
        this.result += message.message.content
          .filter((block) => block.type === "text")
          .map((block) => block.text)
          .join("");
      }
      yield message;
    }
    if (!this.canceled) {
      this.status = this.options.waitStatus ?? "finished";
    }
  }
  async conversation(): Promise<never[]> {
    return [];
  }
  async wait(): Promise<import("@cursor/sdk").RunResult> {
    const status = this.canceled
      ? "cancelled"
      : (this.options.waitStatus ?? "finished");
    this.status = status;
    return {
      id: this.id,
      status,
      result: this.options.waitResult ?? this.result,
    };
  }
  async cancel(): Promise<void> {
    this.canceled = true;
    this.status = "cancelled";
    if (this.options.cancelRejects) {
      throw new Error("cancel failed");
    }
  }
  onDidChangeStatus(): () => void {
    return () => {};
  }
}

class FakeAgentHost {
  readonly listRequests: unknown[] = [];
  readonly executeRequests: ExecuteAgentToolRequest[] = [];
  readonly relayTokens: string[] = [];

  private constructor(
    readonly socketPath: string,
    private readonly server: Http2Server,
  ) {}

  static async start(input: {
    pages: Array<{ tools: ListedAgentTool[]; nextPageToken?: string }>;
    executeBody?: string;
    executeStatus?: number;
  }): Promise<FakeAgentHost> {
    const dir = await mkdtemp(join(tmpdir(), "cursor-agent-host-"));
    const socketPath = join(dir, "host.sock");
    let host: FakeAgentHost;
    const server = createServer(
      connectNodeAdapter({
        grpc: true,
        grpcWeb: false,
        connect: false,
        routes(router) {
          router.service(AgentHostService, {
            listTools(request: any, context: any) {
              host.relayTokens.push(
                context.requestHeader.get(AGENT_HOST_RELAY_TOKEN_HEADER) ?? "",
              );
              host.listRequests.push(request);
              const pageIndex = request.pageToken
                ? input.pages.findIndex(
                    (page) => page.nextPageToken === request.pageToken,
                  ) + 1
                : 0;
              const page = input.pages[pageIndex] ??
                input.pages[input.pages.length - 1] ?? { tools: [] };
              return create(ListAgentToolsResponseSchema, {
                tools: page.tools,
                nextPageToken: page.nextPageToken ?? "",
              });
            },
            executeTool(request: any) {
              host.executeRequests.push(request);
              return create(ExecuteAgentToolResponseSchema, {
                status: input.executeStatus ?? 200,
                body: input.executeBody ?? "{}",
              });
            },
          });
        },
      }),
    );
    host = new FakeAgentHost(socketPath, server);
    await listenUnix(server, socketPath);
    return host;
  }

  async close(): Promise<void> {
    const dir = resolve(this.socketPath, "..");
    await closeHttp2(this.server);
    await rm(dir, { recursive: true, force: true });
  }
}

async function callFirstMcpTool(
  url: string,
  headers: Record<string, string>,
): Promise<unknown> {
  const listed = await listMcpTools(url, headers);
  expect(listed.tools[0]?.name).toBe("weather");
  const result = await callMcpTool(url, headers, listed.tools[0]?.name ?? "", {
    city: "Oakland",
  });
  return (result as any).content[0];
}

async function listMcpTools(url: string, headers: Record<string, string>) {
  return await callMcpTools(
    url,
    headers,
    async (client) => await client.listTools(),
  );
}

async function callMcpTool(
  url: string,
  headers: Record<string, string>,
  name: string,
  args: Record<string, unknown>,
) {
  return await callMcpTools(
    url,
    headers,
    async (client) => await client.callTool({ name, arguments: args }),
  );
}

async function callMcpTools<T>(
  url: string,
  headers: Record<string, string>,
  fn: (client: McpClient) => Promise<T> = async (client) =>
    (await client.listTools()) as T,
): Promise<T> {
  const client = new McpClient({ name: "fake-cursor", version: "0.0.0" });
  const transport = new StreamableHTTPClientTransport(new URL(url), {
    requestInit: { headers },
  });
  await client.connect(transport as never);
  try {
    return await fn(client);
  } finally {
    await client.close();
  }
}

async function listenUnix(
  server: Http2Server,
  socketPath: string,
): Promise<void> {
  await new Promise<void>((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen(socketPath, () => {
      server.off("error", rejectListen);
      resolveListen();
    });
  });
}

async function closeHttp2(server: Http2Server): Promise<void> {
  await new Promise<void>((resolveClose, rejectClose) => {
    server.close((error) => {
      if (error) {
        rejectClose(error);
        return;
      }
      resolveClose();
    });
  });
}
