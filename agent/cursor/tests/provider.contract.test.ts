import { mkdtemp, rm } from "node:fs/promises";
import { existsSync, readFileSync } from "node:fs";
import { createServer, type Http2Server } from "node:http2";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import {
  Code,
  ConnectError,
  createClient,
  type HandlerContext,
} from "@connectrpc/connect";
import {
  connectNodeAdapter,
  createGrpcTransport,
} from "@connectrpc/connect-node";
import {
  create as createMessage,
  type DescMessage,
  type MessageInitShape,
  type MessageShape,
} from "@bufbuild/protobuf";
import { Client as McpClient } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type {
  CallToolResult,
  CompatibilityCallToolResult,
} from "@modelcontextprotocol/sdk/types.js";
import { afterEach, describe, expect, test } from "bun:test";
import {
  AgentExecutionStatus,
  AgentToolSourceMode,
  createAgentProviderService,
  type AgentMessage,
  type AgentTurn,
  type AgentToolRef,
  type CreateAgentProviderSessionRequest,
  type CreateAgentProviderTurnRequest,
  type ListedAgentTool,
} from "@valon-technologies/gestalt/services/agent";
import { GestaltError } from "@valon-technologies/gestalt/services/rpc_support";
import {
  ENV_HOST_SERVICE_SOCKET,
  ENV_HOST_SERVICE_TOKEN,
} from "@valon-technologies/gestalt";
import {
  Agent as VendoredAgentProviderService,
  CancelAgentProviderTurnRequestSchema,
  CreateAgentProviderSessionRequestSchema,
  GetAgentProviderCapabilitiesRequestSchema,
  GetAgentProviderSessionRequestSchema,
  GetAgentProviderTurnRequestSchema,
  ListAgentProviderSessionsRequestSchema,
  ListAgentProviderTurnEventsRequestSchema,
  ListAgentProviderTurnsRequestSchema,
  ListedAgentToolSchema,
  UpdateAgentProviderSessionRequestSchema,
} from "../node_modules/@valon-technologies/gestalt/src/internal/gen/v1/agent_pb.ts";
import {
  App as AppService,
  type AppInvokeRequest,
  OperationResultSchema,
} from "../node_modules/@valon-technologies/gestalt/src/internal/gen/v1/app_pb.ts";

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

const activeHosts: FakeAppHost[] = [];
const HOST_SERVICE_RELAY_TOKEN_HEADER = "x-gestalt-host-service-relay-token";
const OWNER_SUBJECT = subjectFixture("user:owner@example.com", "user", "Owner");
const OTHER_SUBJECT = subjectFixture("user:other@example.com", "user", "Other");
const SLACK_SUBJECT = subjectFixture("service_account:slack-bot", "service_account", "Slack Bot");
const DEFAULT_SESSION_TOOLS: NonNullable<CreateAgentProviderSessionRequest["tools"]> = {
  source: {
    case: "catalog",
    value: {
      refs: [toolRef({ app: "p", operation: "o" })],
      tools: [tool({ id: "tool-p-o", mcpName: "p__o", app: "p", operation: "o" })],
    },
  },
};

function subjectFixture(id: string, kind: string, displayName: string) {
  return {
    id,
    displayName,
    email: kind === "user" ? id.replace(/^user:/, "") : "",
  };
}

function requestContext(
  subject = OWNER_SUBJECT,
): NonNullable<CreateAgentProviderTurnRequest["context"]> {
  return {
    subject: {
      id: subject.id,
      displayName: subject.displayName,
      email: subject.email,
      scopes: [],
      permissions: [],
    },
    toolRefs: [],
    toolRefsSet: false,
  };
}

function create(
  schema: DescMessage,
  input: Record<string, unknown> | Partial<CreateAgentProviderSessionRequest>,
): any {
  let payload = input;
  if (schema.typeName === CreateAgentProviderSessionRequestSchema.typeName) {
    const rawTools = (input as { tools?: unknown }).tools;
    const tools =
      isRecord(rawTools) && "catalog" in rawTools
        ? { source: { case: "catalog", value: rawTools.catalog } }
        : isRecord(rawTools) && "none" in rawTools
          ? { source: { case: "none", value: rawTools.none } }
          : rawTools;
    payload = {
      context: requestContext(OWNER_SUBJECT),
      ...(tools !== undefined ? { tools } : {}),
      ...input,
    };
    if (tools !== undefined) {
      (payload as Record<string, unknown>).tools = tools;
    }
  } else if (
    [
      CancelAgentProviderTurnRequestSchema,
      GetAgentProviderSessionRequestSchema,
      GetAgentProviderTurnRequestSchema,
      ListAgentProviderSessionsRequestSchema,
      ListAgentProviderTurnEventsRequestSchema,
      ListAgentProviderTurnsRequestSchema,
      UpdateAgentProviderSessionRequestSchema,
    ].some((requestSchema) => requestSchema.typeName === schema.typeName)
  ) {
    payload = { context: requestContext(OWNER_SUBJECT), ...input };
  }
  const message = createMessage(schema, payload as MessageInitShape<DescMessage>);
  if (schema.typeName === CreateAgentProviderSessionRequestSchema.typeName) {
    const sessionRequest = message as unknown as CreateAgentProviderSessionRequest;
    if ((input as Partial<CreateAgentProviderSessionRequest>).tools === undefined) {
      sessionRequest.tools = DEFAULT_SESSION_TOOLS;
    }
  }
  return message;
}

function turnRequest(
  input: Omit<Partial<CreateAgentProviderTurnRequest>, "messages"> & {
    turnId: string;
    sessionId: string;
    messages: ReadonlyArray<Pick<AgentMessage, "role" | "text"> & Partial<AgentMessage>>;
  },
): CreateAgentProviderTurnRequest {
  const protoDefaults = {
    toolRefs: [],
    toolSource: AgentToolSourceMode.UNSPECIFIED,
  } as Record<string, unknown>;
  return {
    turnId: input.turnId,
    sessionId: input.sessionId,
    idempotencyKey: input.idempotencyKey ?? "",
    model: input.model ?? "",
    messages: input.messages.map((message) => ({ parts: [], ...message })),
    output: input.output ?? { kind: { case: "text", value: {} } },
    metadata: input.metadata,
    executionRef: input.executionRef ?? "",
    ...protoDefaults,
    context: Object.prototype.hasOwnProperty.call(input, "context")
      ? input.context
      : requestContext(OWNER_SUBJECT),
    modelOptions: input.modelOptions,
    timeoutSeconds: input.timeoutSeconds ?? 0,
  } as CreateAgentProviderTurnRequest;
}

function turnText(turn: AgentTurn): string {
  if (turn.output.case === "text" || turn.output.case === "structured") {
    return turn.output.value.text;
  }
  return "";
}

function toolRef(input: Partial<AgentToolRef>): AgentToolRef {
  return {
    app: "",
    operation: "",
    connection: "",
    instance: "",
    title: "",
    description: "",
    credentialMode: "",
    system: "",
    ...input,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

afterEach(async () => {
  for (const host of activeHosts.splice(0)) {
    await host.close();
  }
  delete process.env[ENV_HOST_SERVICE_SOCKET];
  delete process.env[ENV_HOST_SERVICE_TOKEN];
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
      );
      const capabilities = await client.getCapabilities(
        create(GetAgentProviderCapabilitiesRequestSchema, {}),
      );
      expect(capabilities.streamingText).toBe(false);
      expect(capabilities.toolCalls).toBe(true);
      expect(capabilities.boundedListHydration).toBe(true);
      expect(capabilities.interactions).toBe(false);
      expect(capabilities.resumableTurns).toBe(false);
      expect(capabilities.supportedToolSources).toEqual([
        AgentToolSourceMode.CATALOG,
      ]);
    } finally {
      await closeHttp2(server);
      await rm(socketDir, { recursive: true, force: true });
    }
  });

  test("sessionStart hooks run once and prepend context to turns", async () => {
    const host = await FakeAppHost.start({
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;
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
        new CursorSDKRunner(config, { agentFactory: cursor }),
    });

    const created = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
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
    expect(replay.id).toBe(created.id);

    await provider.createTurn(
      turnRequest({
        turnId: "session-start-turn",
        sessionId: requireId(created),
        messages: [{ role: "user", text: "hi" }],
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
          metadata: {
            "__gestalt.lifecycle.sessionStart.additionalContext": "spoofed",
          },
        }),
      ),
    ).rejects.toThrow(GestaltError);

    const session = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await expect(
      provider.updateSession({
        sessionId: requireId(session),
        metadata: {
          "__gestalt.lifecycle.sessionStart.additionalContext": "spoofed",
        },
      } as never),
    ).rejects.toThrow(GestaltError);
  });

  test("createSession mints session ids and dedupes idempotency keys per subject", async () => {
    const provider = await configuredProvider();

    const first = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        idempotencyKey: "dedup-key",
      }),
    );
    expect(requireId(first).length).toBeGreaterThan(0);
    const fetched = await provider.getSession(
      create(GetAgentProviderSessionRequestSchema, { sessionId: first.id }),
    );
    expect(fetched.id).toBe(first.id);

    const replay = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        idempotencyKey: "dedup-key",
      }),
    );
    expect(replay.id).toBe(first.id);

    const otherSubject = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        idempotencyKey: "dedup-key",
        context: requestContext(OTHER_SUBJECT),
      }),
    );
    expect(otherSubject.id).not.toBe(first.id);
  });

  test("racing createSession calls with the same key converge on one session", async () => {
    const provider = await configuredProvider();
    const sessions = await Promise.all(
      Array.from({ length: 8 }, () =>
        provider.createSession(
          create(CreateAgentProviderSessionRequestSchema, {
            idempotencyKey: "race-key",
          }),
        ),
      ),
    );
    const ids = new Set(sessions.map((session) => session.id));
    expect(ids.size).toBe(1);
  });

  test("owner can read and mutate a private session", async () => {
    const provider = await configuredProvider();
    const created = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );

    const session = await provider.getSession(
      create(GetAgentProviderSessionRequestSchema, {
        sessionId: requireId(created),
      }),
    );
    expect(session.id).toBe(created.id);

    const updated = await provider.updateSession(
      create(UpdateAgentProviderSessionRequestSchema, {
        sessionId: requireId(created),
        clientRef: "owner-ref",
      }),
    );
    expect(updated.clientRef).toBe("owner-ref");
  });

  test("Slack-created company sessions are readable and listed by non-owners", async () => {
    const host = await FakeAppHost.start({
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;
    const provider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, {
          agentFactory: new FakeCursorAgentFactory(),
        }),
    });
    const session = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        context: requestContext(SLACK_SUBJECT),
        metadata: slackSessionMetadata(),
      }),
    );
    await provider.createTurn(
      turnRequest({
        turnId: "access-company-turn",
        sessionId: requireId(session),
        messages: [{ role: "user", text: "hi" }],
        context: requestContext(SLACK_SUBJECT),
      }),
    );

    const otherSession = await provider.getSession(
      create(GetAgentProviderSessionRequestSchema, {
        sessionId: requireId(session),
        context: requestContext(OTHER_SUBJECT),
      }),
    );
    expect(otherSession.id).toBe(session.id);
    await expectConnectCode(
      provider.getSession({ sessionId: session.id } as never),
      Code.NotFound,
    );
    const otherSessions = await provider.listSessions(
      create(ListAgentProviderSessionsRequestSchema, {
        context: requestContext(OTHER_SUBJECT),
      }),
    );
    expect(otherSessions.map((listed) => listed.id)).toContain(session.id);

    const otherTurns = await provider.listTurns(
      create(ListAgentProviderTurnsRequestSchema, {
        sessionId: requireId(session),
        context: requestContext(OTHER_SUBJECT),
      }),
    );
    expect(otherTurns.map((turn) => turn.id)).toContain("access-company-turn");
    const exactOtherTurns = await provider.listTurns(
      create(ListAgentProviderTurnsRequestSchema, {
        turnIds: ["access-company-turn"],
        context: requestContext(OTHER_SUBJECT),
      }),
    );
    expect(exactOtherTurns.map((turn) => turn.id)).toEqual([
      "access-company-turn",
    ]);
  });

  test("private sessions are hidden from non-owners and metadata updates do not change visibility", async () => {
    const provider = await configuredProvider();
    const privateSession = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await provider.updateSession(
      create(UpdateAgentProviderSessionRequestSchema, {
        sessionId: privateSession.id,
        metadata: slackSessionMetadata(),
      }),
    );

    await expectConnectCode(
      provider.getSession(
        create(GetAgentProviderSessionRequestSchema, {
          sessionId: privateSession.id,
          context: requestContext(OTHER_SUBJECT),
        }),
      ),
      Code.NotFound,
    );
    await expectConnectCode(
      provider.getSession({ sessionId: privateSession.id } as never),
      Code.NotFound,
    );
    const otherSessions = await provider.listSessions(
      create(ListAgentProviderSessionsRequestSchema, {
        context: requestContext(OTHER_SUBJECT),
      }),
    );
    expect(otherSessions.map((session) => session.id)).not.toContain(
      privateSession.id,
    );
    const missingSubjectSessions = await provider.listSessions(
      create(ListAgentProviderSessionsRequestSchema, {
        context: undefined,
      }),
    );
    expect(missingSubjectSessions).toEqual([]);

    const incompleteSlackSession = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {
        context: requestContext(SLACK_SUBJECT),
        metadata: { slack: { team_id: "T123", channel_id: "C456" } },
      }),
    );
    await expectConnectCode(
      provider.getSession(
        create(GetAgentProviderSessionRequestSchema, {
          sessionId: incompleteSlackSession.id,
          context: requestContext(OTHER_SUBJECT),
        }),
      ),
      Code.NotFound,
    );
  });

  test("non-owner writes are denied and private turns and events stay hidden", async () => {
    const host = await FakeAppHost.start({
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;
    const provider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, {
          agentFactory: new FakeCursorAgentFactory(),
        }),
    });
    const session = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await provider.createTurn(
      turnRequest({
        turnId: "access-private-turn",
        sessionId: requireId(session),
        messages: [{ role: "user", text: "hi" }],
      }),
    );

    await expectConnectCode(
      provider.updateSession(
        create(UpdateAgentProviderSessionRequestSchema, {
          sessionId: requireId(session),
          context: requestContext(OTHER_SUBJECT),
          clientRef: "not-owner",
        }),
      ),
      Code.PermissionDenied,
    );
    await expectConnectCode(
      provider.createTurn(
        turnRequest({
          turnId: "access-private-turn-other",
          sessionId: requireId(session),
          context: requestContext(OTHER_SUBJECT),
          messages: [{ role: "user", text: "nope" }],
        }),
      ),
      Code.PermissionDenied,
    );
    await expectConnectCode(
      provider.cancelTurn(
        create(CancelAgentProviderTurnRequestSchema, {
          turnId: "access-private-turn",
          context: requestContext(OTHER_SUBJECT),
          reason: "nope",
        }),
      ),
      Code.PermissionDenied,
    );

    await expectConnectCode(
      provider.getTurn(
        create(GetAgentProviderTurnRequestSchema, {
          turnId: "access-private-turn",
          context: requestContext(OTHER_SUBJECT),
        }),
      ),
      Code.NotFound,
    );
    expect(
      await provider.listTurns(
        create(ListAgentProviderTurnsRequestSchema, {
          sessionId: requireId(session),
          context: requestContext(OTHER_SUBJECT),
        }),
      ),
    ).toEqual([]);
    expect(
      await provider.listTurns(
        create(ListAgentProviderTurnsRequestSchema, {
          turnIds: ["access-private-turn"],
          context: requestContext(OTHER_SUBJECT),
        }),
      ),
    ).toEqual([]);
    expect(
      await provider.listTurnEvents(
        create(ListAgentProviderTurnEventsRequestSchema, {
          turnId: "access-private-turn",
          context: requestContext(OTHER_SUBJECT),
        }),
      ),
    ).toEqual([]);
  });

  test("runs a turn through Cursor SDK options, MCP tools, and direct App.Invoke", async () => {
    const host = await FakeAppHost.start({
      body: '{"forecast":"sunny"}',
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;
    process.env[ENV_HOST_SERVICE_TOKEN] = "relay-token";

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
        idempotencyKey: "session-key",
        tools: {
          catalog: {
            refs: [{ app: "weather", operation: "forecast" }],
            tools: [
              tool({
                id: "tool-weather",
                mcpName: "weather",
                app: "weather",
                operation: "forecast",
                title: "Weather",
                inputSchema: JSON.stringify({
                  type: "object",
                  properties: { city: { type: "string" } },
                }),
              }),
            ],
          },
        },
      }),
    );

    await provider.createTurn(
      turnRequest({
        turnId: "turn-1",
        sessionId: requireId(session),
        idempotencyKey: "turn-key",
        messages: [{ role: "user", text: "weather?" }],
      }),
    );
    const turn = await waitForTurn(
      provider,
      "turn-1",
      AgentExecutionStatus.SUCCEEDED,
    );
    expect(turnText(turn)).toBe("Forecast: sunny");
    expect(host.relayTokens).toContain("relay-token");
    expect(host.invokeRequests).toHaveLength(1);
    expect(host.invokeRequests[0]?.app).toBe("weather");
    expect(host.invokeRequests[0]?.operation).toBe("forecast");
    expect(host.invokeRequests[0]?.context?.subject?.id).toBe(OWNER_SUBJECT.id);
    expect(host.invokeRequests[0]?.idempotencyKey).toBe(
      "agent/cursor-sdk:turn-1:sdk-1:weather",
    );
    expect(host.invokeRequests[0]?.params).toEqual({ city: "Oakland" });
    expect(existsSync(cursor.stateRoots[0] ?? "")).toBe(false);
  });

  test("structured output requests return validated values", async () => {
    const host = await FakeAppHost.start({
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;
    let prompt = "";
    const cursor = new FakeCursorAgentFactory(async (_options, runPrompt) => {
      prompt = runPrompt;
      return [
        {
          type: "assistant",
          agent_id: "fake-agent",
          run_id: "fake-run",
          message: {
            role: "assistant",
            content: [{ type: "text", text: '{"score":1,"reasoning":"correct"}' }],
          },
        },
      ];
    });
    const provider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: cursor }),
    });
    const session = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );

    await provider.createTurn(
      turnRequest({
        turnId: "structured-turn",
        sessionId: requireId(session),
        messages: [{ role: "user", text: "grade" }],
        output: {
          kind: {
            case: "structured",
            value: {
              schema: {
                type: "object",
                required: ["score", "reasoning"],
                properties: {
                  score: { type: "number" },
                  reasoning: { type: "string" },
                },
              },
            },
          },
        },
      }),
    );
    const turn = await waitForTurn(
      provider,
      "structured-turn",
      AgentExecutionStatus.SUCCEEDED,
    );

    expect(prompt).toContain("gestalt_structured_output");
    expect(turn.output.case === "structured" ? turn.output.value.value : undefined).toEqual({
      score: 1,
      reasoning: "correct",
    });
  });

  test("structured output requests fail invalid JSON", async () => {
    const host = await FakeAppHost.start({
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;
    const cursor = new FakeCursorAgentFactory(async () => [
      {
        type: "assistant",
        agent_id: "fake-agent",
        run_id: "fake-run",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "not json" }],
        },
      },
    ]);
    const provider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: cursor }),
    });
    const session = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );

    await provider.createTurn(
      turnRequest({
        turnId: "structured-invalid-turn",
        sessionId: requireId(session),
        messages: [{ role: "user", text: "grade" }],
        output: {
          kind: {
            case: "structured",
            value: {
              schema: {
                type: "object",
                required: ["score"],
                properties: { score: { type: "number" } },
              },
            },
          },
        },
      }),
    );
    const turn = await waitForTurn(
      provider,
      "structured-invalid-turn",
      AgentExecutionStatus.FAILED,
    );

    expect(turn.statusMessage).toContain("structured output");
  });

  test("prepared workspace cwd overrides configured working directory", async () => {
    const host = await FakeAppHost.start({
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;
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
        new CursorSDKRunner(config, { agentFactory: cursor }),
    });
    const session = await provider.createSession({
      idempotencyKey: "",
      model: "",
      clientRef: "",
      context: requestContext(OWNER_SUBJECT),
      preparedWorkspace: { root: tmpdir(), cwd: preparedCwd },
      tools: DEFAULT_SESSION_TOOLS,
    } as never);
    await provider.createTurn(
      turnRequest({
        turnId: "turn-workspace",
        sessionId: requireId(session),
        messages: [{ role: "user", text: "inspect repo" }],
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
        idempotencyKey: "",
        model: "",
        clientRef: "",
        context: requestContext(OWNER_SUBJECT),
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
    const host = await FakeAppHost.start({
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;
    const provider = await configuredProvider({
      config: { sandboxEnabled: true },
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: cursor }),
    });
    const session = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await provider.createTurn(
      turnRequest({
        turnId: "t",
        sessionId: requireId(session),
        messages: [{ role: "user", text: "hi" }],
      }),
    );
    await waitForTurn(provider, "t", AgentExecutionStatus.SUCCEEDED);
  });

  test("invalid reconfiguration preserves the active runtime state", async () => {
    const provider = await configuredProvider();
    const created = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );

    await expect(
      provider.configureProvider("broken-cursor", {
        defaultModel: "composer-2",
        workingDirectory: join(tmpdir(), "missing-cursor-workdir"),
      }),
    ).rejects.toThrow("workingDirectory");

    const session = await provider.getSession(
      create(GetAgentProviderSessionRequestSchema, {
        sessionId: requireId(created),
      }),
    );
    expect(session.id).toBe(created.id);
    expect((await provider.warnings())[0]).toContain("CURSOR_API_KEY");
  });

  test("rejects unsupported session and turn inputs", async () => {
    const provider = await configuredProvider();
    const invalidSessionCases: Array<[string, unknown, string]> = [
      [
        "none tools",
        { none: {} },
        "requires tools.catalog",
      ],
      [
        "missing refs",
        { catalog: { refs: [] } },
        "tools.catalog.refs are required",
      ],
      [
        "wildcard ref",
        { catalog: { refs: [{ app: "p", operation: "*" }] } },
        "wildcard",
      ],
      [
        "missing operation",
        { catalog: { refs: [{ app: "p" }] } },
        "operation is required",
      ],
      [
        "missing app system",
        { catalog: { refs: [{ operation: "o" }] } },
        "exactly one",
      ],
      [
        "both app system",
        { catalog: { refs: [{ app: "p", system: "workflow", operation: "o" }] } },
        "exactly one",
      ],
      [
        "bad system",
        { catalog: { refs: [{ system: "not-workflow", operation: "o" }] } },
        "not supported",
      ],
      [
        "bad credential mode",
        { catalog: { refs: [{ app: "p", operation: "o", credentialMode: "user" }] } },
        "credential_mode is invalid",
      ],
      [
        "bad run as",
        { catalog: { refs: [{ app: "p", operation: "o", runAs: { id: "user:delegate" } }] } },
        "run_as is not supported",
      ],
    ];
    for (const [name, tools, message] of invalidSessionCases) {
      await expect(
        provider.createSession(
          create(CreateAgentProviderSessionRequestSchema, { tools }),
        ),
        name,
      ).rejects.toThrow(message);
    }

    const session = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    const base = {
      turnId: "turn",
      sessionId: requireId(session),
      messages: [{ role: "user", text: "hi" }],
    };
    const invalidCases: Array<[string, Record<string, unknown>, string]> = [
      ["missing request context", { context: undefined }, "request context is required"],
      [
        "empty structured output schema",
        { output: { kind: { case: "structured", value: { schema: {} } } } },
        "output.structured.schema",
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
          turnRequest({ ...base, ...patch }),
        ),
        name,
      ).rejects.toThrow(message);
    }
  });

  test("maps Cursor failures, timeouts, and cancellations onto terminal turns", async () => {
    const host = await FakeAppHost.start({
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;

    const failingCursor = new FakeCursorAgentFactory(async () => ({
      messages: [],
      waitStatus: "error",
    }));
    const failureProvider = await configuredProvider({
      runnerFactory: (config) =>
        new CursorSDKRunner(config, { agentFactory: failingCursor }),
    });
    const failureSession = await failureProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await failureProvider.createTurn(
      turnRequest({
        turnId: "failure-turn",
        sessionId: requireId(failureSession),
        messages: [{ role: "user", text: "fail" }],
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
    const timeoutSession = await timeoutProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await timeoutProvider.createTurn(
      turnRequest({
        turnId: "timeout-turn",
        sessionId: requireId(timeoutSession),
        messages: [{ role: "user", text: "timeout" }],
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
    const preRunSession = await preRunProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await preRunProvider.createTurn(
      turnRequest({
        turnId: "pre-run-turn",
        sessionId: requireId(preRunSession),
        messages: [{ role: "user", text: "cancel" }],
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
    const pendingSendSession = await pendingSendProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await pendingSendProvider.createTurn(
      turnRequest({
        turnId: "send-turn",
        sessionId: requireId(pendingSendSession),
        messages: [{ role: "user", text: "cancel while sending" }],
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
    const liveSession = await liveProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await liveProvider.createTurn(
      turnRequest({
        turnId: "live-turn",
        sessionId: requireId(liveSession),
        messages: [{ role: "user", text: "cancel" }],
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
    const host = await FakeAppHost.start({
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;
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
    const session = await provider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await provider.createTurn(
      turnRequest({
        turnId: "close-turn",
        sessionId: requireId(session),
        messages: [{ role: "user", text: "close" }],
      }),
    );
    await waitUntil(() => cursor.runs.length === 1);
    const stateRoot = cursor.stateRoots[0] ?? "";
    expect(existsSync(stateRoot)).toBe(true);

    await provider.closeProvider();

    expect(cursor.runs[0]?.canceled).toBe(true);
    expect(existsSync(stateRoot)).toBe(false);
  });

  test("rejects invalid listed catalog tools at session creation", async () => {
    const provider = await configuredProvider();
    const baseRef: AgentToolRef = toolRef({ app: "p", operation: "o" });
    const cases: Array<[string, unknown, string]> = [
      [
        "empty",
        { catalog: { refs: [baseRef], tools: [] } },
        "tools.catalog.tools are required",
      ],
      [
        "duplicate",
        {
          catalog: {
            refs: [baseRef],
            tools: [
              tool({ id: "a", mcpName: "dup" }),
              tool({ id: "b", mcpName: "dup" }),
            ],
          },
        },
        "duplicate",
      ],
      [
        "unsafe",
        { catalog: { refs: [baseRef], tools: [tool({ id: "a", mcpName: "bad name" })] } },
        "unsafe",
      ],
      [
        "non-app",
        {
          catalog: {
            refs: [baseRef],
            tools: [
              create(ListedAgentToolSchema, {
                id: "workflow",
                mcpName: "workflow__start",
                ref: { system: "workflow", operation: "start" },
              }),
            ],
          },
        },
        "must target an app operation",
      ],
      [
        "mismatched-ref",
        {
          catalog: {
            refs: [baseRef],
            tools: [tool({ id: "github", mcpName: "github__pulls", app: "github", operation: "pulls" })],
          },
        },
        "not covered by tools.catalog.refs",
      ],
      [
        "mismatched-credential-mode",
        {
          catalog: {
            refs: [{ ...baseRef, credentialMode: "subject" }],
            tools: [tool({ id: "a", mcpName: "p__o", credentialMode: "none" })],
          },
        },
        "not covered by tools.catalog.refs",
      ],
      [
        "wildcard-listed-connection",
        {
          catalog: {
            refs: [baseRef],
            tools: [tool({ id: "a", mcpName: "p__o", connection: "*" })],
          },
        },
        "concrete app operation",
      ],
      [
        "bad-listed-credential-mode",
        {
          catalog: {
            refs: [baseRef],
            tools: [tool({ id: "a", mcpName: "p__o", credentialMode: "user" })],
          },
        },
        "credential_mode is invalid",
      ],
      [
        "listed-run-as",
        {
          catalog: {
            refs: [baseRef],
            tools: [tool({ id: "a", mcpName: "p__o", runAs: { id: "user:delegate" } })],
          },
        },
        "run_as is not supported",
      ],
    ];
    for (const [name, tools, message] of cases) {
      await expect(
        provider.createSession(create(CreateAgentProviderSessionRequestSchema, { tools })),
        name,
      ).rejects.toThrow(message);
    }
  });

  test("MCP bridge enforces auth, raw tool names, unknown tools, and error mapping", async () => {
    const calls: Array<{ callId: string; args: Record<string, unknown> }> = [];
    const bridge = await startMcpBridge({
      tools: [
        {
          mcpName: "raw_tool",
          title: "Raw Tool",
          description: "Uses raw MCP names",
          ref: {
            app: "raw",
            operation: "call",
            connection: "",
            instance: "",
            credentialMode: "",
          },
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
      const result = requireCallToolResult(
        await callMcpTool(bridge.url, bridge.headers, "raw_tool", {
          value: 1,
        }),
      );
      expect(result.isError).toBe(true);
      expect(result.content[0]).toEqual({
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
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    const host = await FakeAppHost.start({
    });
    activeHosts.push(host);
    process.env[ENV_HOST_SERVICE_SOCKET] = host.socketPath;
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
    const eventSession = await eventProvider.createSession(
      create(CreateAgentProviderSessionRequestSchema, {}),
    );
    await eventProvider.createTurn(
      turnRequest({
        turnId: "events-turn",
        sessionId: requireId(eventSession),
        messages: [{ role: "user", text: "hi" }],
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

function requireId(entity: { id?: string | undefined }): string {
  if (!entity.id) {
    throw new Error("expected a minted id");
  }
  return entity.id;
}

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
  app?: string;
  operation?: string;
  connection?: string;
  instance?: string;
  credentialMode?: string;
  runAs?: unknown;
}): ListedAgentTool {
  return create(ListedAgentToolSchema, {
    id: input.id,
    mcpName: input.mcpName,
    title: input.title ?? "",
    description: input.description ?? "",
    inputSchema:
      input.inputSchema ?? '{"type":"object","additionalProperties":true}',
    ref: {
      app: input.app ?? "p",
      operation: input.operation ?? "o",
      connection: input.connection ?? "",
      instance: input.instance ?? "",
      credentialMode: input.credentialMode ?? "",
      runAs: input.runAs as never,
    },
  });
}

function slackSessionMetadata(): Record<string, unknown> {
  return {
    slack: {
      team_id: "T123",
      channel_id: "C456",
      channel_type: "channel",
      root_message_ts: "1712161829.000300",
      session_ref: "slack:T123:C456:1712161829.000300",
    },
  };
}

async function expectConnectCode(
  promise: Promise<unknown>,
  code: Code,
): Promise<void> {
  try {
    await promise;
  } catch (error) {
    expect(
      error instanceof ConnectError || error instanceof GestaltError,
    ).toBe(true);
    expect((error as ConnectError | GestaltError).code).toBe(code);
    return;
  }
  throw new Error(`expected ConnectError code ${code}`);
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

class FakeAppHost {
  readonly invokeRequests: AppInvokeRequest[] = [];
  readonly relayTokens: string[] = [];

  private constructor(
    readonly socketPath: string,
    private readonly server: Http2Server,
  ) {}

  static async start(input: {
    body?: string;
    status?: number;
  }): Promise<FakeAppHost> {
    const dir = await mkdtemp(join(tmpdir(), "cursor-app-host-"));
    const socketPath = join(dir, "host.sock");
    let host: FakeAppHost;
    const server = createServer(
      connectNodeAdapter({
        grpc: true,
        grpcWeb: false,
        connect: false,
        routes(router) {
          router.service(AppService, {
            invoke(request: AppInvokeRequest, context: HandlerContext) {
              host.relayTokens.push(
                context.requestHeader.get(HOST_SERVICE_RELAY_TOKEN_HEADER) ?? "",
              );
              host.invokeRequests.push(request);
              return create(OperationResultSchema, {
                status: input.status ?? 200,
                body: new TextEncoder().encode(input.body ?? "{}"),
              });
            },
          });
        },
      }),
    );
    host = new FakeAppHost(socketPath, server);
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
  const result = requireCallToolResult(
    await callMcpTool(url, headers, listed.tools[0]?.name ?? "", {
      city: "Oakland",
    }),
  );
  return result.content[0];
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
): Promise<CompatibilityCallToolResult> {
  return await callMcpTools(
    url,
    headers,
    async (client) => await client.callTool({ name, arguments: args }),
  );
}

function requireCallToolResult(
  result: CompatibilityCallToolResult,
): CallToolResult {
  if (!("content" in result) || !Array.isArray(result.content)) {
    throw new Error("expected immediate MCP tool result");
  }
  return result as CallToolResult;
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
