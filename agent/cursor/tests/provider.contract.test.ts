import { existsSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { Code, ConnectError } from "@connectrpc/connect";
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
import { schemaFromJson, type ToolEntry } from "../src/tools.ts";

const CancelAgentProviderTurnRequestSchema = "cancel-turn";
const CreateAgentProviderSessionRequestSchema = "create-session";
const CreateAgentProviderTurnRequestSchema = "create-turn";
const GetAgentProviderCapabilitiesRequestSchema = "get-capabilities";
const GetAgentProviderSessionRequestSchema = "get-session";
const GetAgentProviderTurnRequestSchema = "get-turn";
const ListAgentProviderTurnEventsRequestSchema = "list-turn-events";

const activeHosts: FakeAgentHost[] = [];

function create<T = any>(schema: string, input: Record<string, unknown>): T {
  switch (schema) {
    case CreateAgentProviderSessionRequestSchema:
      return {
        sessionId: "",
        idempotencyKey: "",
        model: "",
        clientRef: "",
        ...input,
      } as T;
    case CreateAgentProviderTurnRequestSchema:
      return {
        turnId: "",
        sessionId: "",
        idempotencyKey: "",
        model: "",
        messages: [],
        tools: [],
        executionRef: "",
        toolRefs: [],
        toolSource: AgentToolSourceMode.UNSPECIFIED,
        runGrant: "",
        ...input,
      } as T;
    case CancelAgentProviderTurnRequestSchema:
      return { turnId: "", reason: "", ...input } as T;
    case GetAgentProviderSessionRequestSchema:
      return { sessionId: "", ...input } as T;
    case GetAgentProviderTurnRequestSchema:
      return { turnId: "", ...input } as T;
    case ListAgentProviderTurnEventsRequestSchema:
      return { turnId: "", afterSeq: 0n, limit: 0, ...input } as T;
    case GetAgentProviderCapabilitiesRequestSchema:
      return input as T;
    default:
      throw new Error(`unsupported test schema ${schema}`);
  }
}

afterEach(async () => {
  for (const host of activeHosts.splice(0)) {
    await host.close();
  }
});

describe("Cursor agent provider contract", () => {
  test("projects provider-hostile listed tool schemas", () => {
    const schema = schemaFromJson(
      JSON.stringify({
        type: ["object", "null"],
        properties: { root: { type: "string" } },
        required: ["root"],
        allOf: [
          {
            properties: { fromAllOf: { type: "string" } },
            required: ["fromAllOf"],
          },
        ],
        oneOf: [
          {
            properties: { fromOneOf: { type: "string" } },
            required: ["fromOneOf"],
          },
        ],
      }),
    );

    expect(schema.type).toBe("object");
    expect(schema.allOf).toBeUndefined();
    expect(schema.oneOf).toBeUndefined();
    expect(Object.keys(schema.properties ?? {}).sort()).toEqual([
      "fromAllOf",
      "fromOneOf",
      "root",
    ]);
    expect(schema.required).toEqual(["fromAllOf", "root"]);
  });

  test("falls back when listed tool schema branches conflict", () => {
    const schema = schemaFromJson(
      JSON.stringify({
        type: "object",
        properties: { same: { type: "string" } },
        allOf: [{ properties: { same: { type: "integer" } } }],
      }),
    );

    expect(schema).toEqual({
      type: "object",
      properties: {},
      additionalProperties: true,
    });
  });

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
    const service = createAgentProviderService(provider) as any;
    const capabilities = await service.getCapabilities(
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
  });

  test("sessionStart hooks run once and prepend context to turns", async () => {
    const host = await FakeAgentHost.start({
      pages: [{ tools: [tool({ id: "tool", mcpName: "linear__issues" })] }],
    });
    activeHosts.push(host);
    let prompt = "";
    const cursor = new FakeCursorAgentFactory(async (_options, runPrompt) => {
      prompt = runPrompt;
      return [
        {
          type: "assistant",
          agent_id: "fake-agent",
          run_id: "fake-run",
          message: { role: "assistant", content: [{ type: "text", text: "ok" }] },
        },
      ];
    });
    const provider = await configuredProvider({
      runnerFactory: (config) =>
        runnerWithHost(config, cursor, host),
    });

    const created = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        sessionId: "session-start",
        idempotencyKey: "session-start-idem",
        sessionStart: {
          hooks: [
            {
              id: "load-context",
              type: "command",
              command: ["/bin/sh", "-c", "printf '%s\\n' 'cursor context'"],
              timeout: "5s",
              output: { additionalContext: true, metadata: true },
            },
          ],
        },
      }),
    );
    expect(
      (
        created.metadata as
          | Record<string, { stdout?: string } | undefined>
          | undefined
      )?.["__gestalt.lifecycle.sessionStart.results.load-context"]?.stdout,
    ).toBe("cursor context\n");

    const replay = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        sessionId: "session-start-replay",
        idempotencyKey: "session-start-idem",
        sessionStart: {
          hooks: [
            {
              id: "should-not-run",
              type: "command",
              command: ["/bin/sh", "-c", "exit 7"],
              output: { metadata: true },
            },
          ],
        },
      }),
    );
    expect(replay.id).toBe("session-start");

    await provider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "session-start-turn",
        sessionId: "session-start",
        messages: [{ role: "user", text: "hi" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant",
        toolRefs: [{ plugin: "linear", operation: "issues" }],
      }),
    );
    await waitForTurn(
      provider,
      "session-start-turn",
      AgentExecutionStatus.SUCCEEDED,
    );

    expect(prompt).toContain("Session start context");
    expect(prompt).toContain("cursor context");
  });

  test("sessionStart reserved metadata is rejected on create and update", async () => {
    const provider = await configuredProvider();
    await expect(
      provider.createSession(
        create(CreateAgentProviderSessionRequestSchema, {
          sessionId: "reserved-create",
          metadata: {
            "__gestalt.lifecycle.sessionStart.additionalContext": "spoofed",
          },
        }),
      ),
    ).rejects.toThrow(ConnectError);

    await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        sessionId: "reserved-update",
      }),
    );
    await expect(
      provider.updateSession({
        sessionId: "reserved-update",
        metadata: {
          "__gestalt.lifecycle.sessionStart.additionalContext": "spoofed",
        },
      } as never),
    ).rejects.toThrow(ConnectError);
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
        runnerWithHost(config, cursor, host),
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
    expect(host.listRequests).toHaveLength(1);
    expect(host.executeRequests).toHaveLength(1);
    expect(host.executeRequests[0]?.toolCallId).toBe("sdk-1");
    expect(host.executeRequests[0]?.idempotencyKey).toBe(
      "agent/cursor-sdk:turn-1:sdk-1:weather",
    );
    expect(host.executeRequests[0]?.arguments).toEqual({ city: "Oakland" });
    expect(existsSync(cursor.stateRoots[0] ?? "")).toBe(false);
  });

  test("prepared workspace cwd overrides configured working directory", async () => {
    const host = await FakeAgentHost.start({
      pages: [{ tools: [tool({ id: "tool", mcpName: "workspace" })] }],
    });
    activeHosts.push(host);
    const preparedCwd = join(tmpdir(), "gestalt-prepared-cursor-workspace");
    const cursor = new FakeCursorAgentFactory(async (options) => {
      expect(options.local?.cwd).toBe(preparedCwd);
      expect(options.platform?.workspaceRef).toBe(preparedCwd);
      return [
        {
          type: "assistant",
          agent_id: "fake-agent",
          run_id: "fake-run",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "workspace ok" }],
          },
        },
      ];
    });
    const provider = await configuredProvider({
      runnerFactory: (config) =>
        runnerWithHost(config, cursor, host),
    });
    await provider.createSession({
      sessionId: "session-workspace",
      idempotencyKey: "",
      model: "",
      clientRef: "",
      preparedWorkspace: { root: tmpdir(), cwd: preparedCwd },
    } as never);
    await provider.createTurn(
      create(CreateAgentProviderTurnRequestSchema, {
        turnId: "turn-workspace",
        sessionId: "session-workspace",
        messages: [{ role: "user", text: "inspect repo" }],
        toolSource: AgentToolSourceMode.MCP_CATALOG,
        runGrant: "grant",
        toolRefs: [{ plugin: "p", operation: "o" }],
      }),
    );
    await waitForTurn(
      provider,
      "turn-workspace",
      AgentExecutionStatus.SUCCEEDED,
    );
  });

  test("prepared workspace requires both root and cwd", async () => {
    const provider = await configuredProvider();
    await expect(
      provider.createSession({
        sessionId: "session-bad-workspace",
        idempotencyKey: "",
        model: "",
        clientRef: "",
        preparedWorkspace: { root: tmpdir(), cwd: "" },
      } as never),
    ).rejects.toThrow("preparedWorkspace root and cwd are required");
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
    const provider = await configuredProvider({
      config: { sandboxEnabled: true },
      runnerFactory: (config) =>
        runnerWithHost(config, cursor, host),
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

    const failingCursor = new FakeCursorAgentFactory(async () => ({
      messages: [],
      waitStatus: "error",
    }));
    const failureProvider = await configuredProvider({
      runnerFactory: (config) =>
        runnerWithHost(config, failingCursor, host),
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
        runnerWithHost(config, timeoutCursor, host),
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
        runnerWithHost(config, preRunCursor, host),
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
        runnerWithHost(config, pendingSendCursor, host),
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
        runnerWithHost(config, liveCursor, host),
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
        runnerWithHost(config, cursor, host),
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
      const provider = await configuredProvider({
        runnerFactory: (config) =>
          new CursorSDKRunner(config, {
            agentFactory: new FakeCursorAgentFactory(),
            hostFactory: () => host as any,
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
        runnerWithHost(config, cursor, host),
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
    expect((events as readonly { type?: string }[]).map((event) => event.type)).toContain("turn.completed");
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

function runnerWithHost(
  config: CursorAgentConfig,
  agentFactory: CursorAgentFactory,
  host: FakeAgentHost,
): CursorSDKRunner {
  return new CursorSDKRunner(config, {
    agentFactory,
    hostFactory: () => host as any,
  });
}

function tool(input: {
  id: string;
  mcpName: string;
  title?: string;
  description?: string;
  inputSchema?: string;
}): ListedAgentTool {
  return {
    id: input.id,
    mcpName: input.mcpName,
    title: input.title ?? "",
    description: input.description ?? "",
    inputSchema:
      input.inputSchema ?? '{"type":"object","additionalProperties":true}',
    outputSchema: "",
    tags: [],
    searchText: "",
  };
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

  private constructor(
    private readonly input: {
      pages: Array<{ tools: ListedAgentTool[]; nextPageToken?: string }>;
      executeBody?: string;
      executeStatus?: number;
    },
  ) {}

  static async start(input: {
    pages: Array<{ tools: ListedAgentTool[]; nextPageToken?: string }>;
    executeBody?: string;
    executeStatus?: number;
  }): Promise<FakeAgentHost> {
    return new FakeAgentHost(input);
  }

  listTools(request: any) {
    this.listRequests.push(request);
    const pageIndex = request.pageToken
      ? this.input.pages.findIndex(
        (page) => page.nextPageToken === request.pageToken,
      ) + 1
      : 0;
    const page = this.input.pages[pageIndex] ??
      this.input.pages[this.input.pages.length - 1] ?? { tools: [] };
    return {
      tools: page.tools,
      nextPageToken: page.nextPageToken ?? "",
    };
  }

  executeTool(request: ExecuteAgentToolRequest) {
    this.executeRequests.push(request);
    return {
      status: this.input.executeStatus ?? 200,
      body: this.input.executeBody ?? "{}",
    };
  }

  async close(): Promise<void> {
    return undefined;
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
