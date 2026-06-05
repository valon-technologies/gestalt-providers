import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import Ajv from "ajv";
import Ajv2020 from "ajv/dist/2020.js";
import type { AgentOptions, Run, SDKAgent, SDKMessage } from "@cursor/sdk";
import type {
  AgentMessage,
  AgentTurnOutput,
  ExecuteAgentToolRequest,
} from "@valon-technologies/gestalt";

import { GestaltAgentHost } from "./agent_host.ts";
import type { CursorAgentConfig } from "./config.ts";
import { createCursorPlatformOptions } from "./cursor_platform.ts";
import { CursorExecutionCanceled, CursorExecutionError } from "./errors.ts";
import { startMcpBridge, type StartedMcpBridge } from "./mcp_bridge.ts";
import { listGestaltTools, type ToolEntry } from "./tools.ts";

export type CursorAgentFactory = {
  create(options: AgentOptions): Promise<SDKAgent>;
};

export type AgentHostFactory = () => GestaltAgentHost;

export type TurnEventSink = (
  eventType: string,
  data: Record<string, unknown>,
) => void;

type ActiveTurn = {
  canceled: boolean;
  run?: Run;
  agent?: SDKAgent;
  bridge?: StartedMcpBridge;
  stateRoot?: string;
  done?: Promise<void>;
};

export class CursorSDKRunner {
  private readonly activeTurns = new Map<string, ActiveTurn>();
  private readonly canceledTurns = new Set<string>();

  constructor(
    private readonly config: CursorAgentConfig,
    private readonly options: {
      agentFactory?: CursorAgentFactory;
      hostFactory?: AgentHostFactory;
    } = {},
  ) {}

  async runTurn(input: {
    sessionId: string;
    turnId: string;
    model: string;
    messages: readonly AgentMessage[];
    runGrant: string;
    requestContext?: unknown;
    cwd: string;
    onEvent: TurnEventSink;
    schema?: Record<string, unknown> | undefined;
  }): Promise<AgentTurnOutput> {
    const active: ActiveTurn = {
      canceled: this.canceledTurns.delete(input.turnId),
    };
    this.activeTurns.set(input.turnId, active);
    const operation = this.runTurnInner(input, active);
    active.done = operation.then(
      () => undefined,
      () => undefined,
    );
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeout = setTimeout(() => {
        void this.cancelTurn(input.turnId);
        reject(new CursorExecutionCanceled("Cursor Agent SDK turn timed out"));
      }, this.config.timeoutSeconds * 1000);
    });
    try {
      return await Promise.race([operation, timeoutPromise]);
    } finally {
      if (timeout) {
        clearTimeout(timeout);
      }
      operation.catch(() => {});
    }
  }

  async cancelTurn(turnId: string): Promise<void> {
    const active = this.activeTurns.get(turnId);
    if (!active) {
      this.canceledTurns.add(turnId);
      return;
    }
    active.canceled = true;
    if (active.run) {
      await cancelRun(active.run);
    }
  }

  async close(): Promise<void> {
    await Promise.all(
      [...this.activeTurns.entries()].map(async ([turnId, active]) => {
        await this.cancelTurn(turnId);
        await active.done;
      }),
    );
  }

  private async runTurnInner(
    input: {
      sessionId: string;
      turnId: string;
      model: string;
      messages: readonly AgentMessage[];
      runGrant: string;
      requestContext?: unknown;
      cwd: string;
      onEvent: TurnEventSink;
      schema?: Record<string, unknown> | undefined;
    },
    active: ActiveTurn,
  ): Promise<AgentTurnOutput> {
    try {
      await this.raiseIfCanceled(active);
      const host = this.createHost();
      const tools = await listGestaltTools({
        host,
        sessionId: input.sessionId,
        turnId: input.turnId,
        runGrant: input.runGrant,
        requestContext: input.requestContext,
      });
      await this.raiseIfCanceled(active);

      active.bridge = await startMcpBridge({
        tools,
        executeTool: async (entry, toolCallId, args) => {
          const request: ExecuteAgentToolRequest & { context?: unknown } = {
            sessionId: input.sessionId,
            turnId: input.turnId,
            toolCallId,
            toolId: entry.toolId,
            arguments: args,
            runGrant: input.runGrant,
            idempotencyKey: `agent/cursor-sdk:${input.turnId}:${toolCallId}:${entry.mcpName}`,
          };
          if (input.requestContext !== undefined) {
            request.context = input.requestContext;
          }
          const response = await host.executeTool(request);
          return { status: response.status, body: response.body };
        },
      });
      active.stateRoot = await mkdtemp(join(tmpdir(), "gestalt-cursor-sdk-"));
      await this.raiseIfCanceled(active);

      active.agent = await this.createAgent(
        input,
        active.bridge,
        active.stateRoot,
        input.cwd,
      );
      await this.raiseIfCanceled(active);

      let prompt = messagesToPrompt(input.messages, this.config.systemPrompt);
      if (input.schema) {
        prompt = structuredOutputPrompt(prompt, input.schema);
      }
      active.run = await active.agent.send(prompt);
      await this.raiseIfCanceled(active);

      const assistantText: string[] = [];
      for await (const event of active.run.stream()) {
        await this.raiseIfCanceled(active);
        const text = recordSDKMessage(input.onEvent, event);
        if (text) {
          assistantText.push(text);
        }
      }
      const result = await active.run.wait();
      if (result.status === "cancelled") {
        throw new CursorExecutionCanceled();
      }
      if (result.status !== "finished") {
        throw new CursorExecutionError(
          `Cursor Agent SDK run finished with status ${result.status}`,
        );
      }
      const outputText = result.result?.trim() || assistantText.join("").trim();
      if (input.schema) {
        return {
          structured: {
            text: outputText,
            value: structuredOutputFromText(outputText, input.schema),
          },
        };
      }
      return { text: outputText };
    } finally {
      await this.cleanupActiveTurn(input.turnId, active);
    }
  }

  private async createAgent(
    input: { turnId: string; model: string },
    bridge: StartedMcpBridge,
    stateRoot: string,
    cwd: string,
  ): Promise<SDKAgent> {
    const workingDirectory = cwd || this.config.workingDirectory;
    const local: NonNullable<AgentOptions["local"]> = {
      cwd: workingDirectory,
      settingSources: [],
      ...(this.config.sandboxEnabled !== undefined
        ? { sandboxOptions: { enabled: this.config.sandboxEnabled } }
        : {}),
    };
    const cursorApiKey = this.cursorApiKey();
    const options: AgentOptions = {
      name: `Gestalt ${input.turnId}`,
      model: { id: input.model },
      mcpServers: {
        gestalt: {
          type: "http",
          url: bridge.url,
          headers: bridge.headers,
        },
      },
      agents: {},
      local,
      platform: {
        ...createCursorPlatformOptions(workingDirectory),
        stateRoot,
      } as AgentOptions["platform"],
      ...(cursorApiKey ? { apiKey: cursorApiKey } : {}),
    };
    return await (await this.createAgentFactory()).create(options);
  }

  private cursorApiKey(): string | undefined {
    return this.config.cursorApiKey || process.env.CURSOR_API_KEY || undefined;
  }

  private async createAgentFactory(): Promise<CursorAgentFactory> {
    if (this.options.agentFactory) {
      return this.options.agentFactory;
    }
    const cursor = await import("@cursor/sdk");
    return cursor.Agent;
  }

  private createHost(): GestaltAgentHost {
    return this.options.hostFactory
      ? this.options.hostFactory()
      : new GestaltAgentHost();
  }

  private async raiseIfCanceled(active: ActiveTurn): Promise<void> {
    if (active.canceled) {
      await cancelRun(active.run);
      throw new CursorExecutionCanceled();
    }
  }

  private async cleanupActiveTurn(
    turnId: string,
    active: ActiveTurn,
  ): Promise<void> {
    this.activeTurns.delete(turnId);
    this.canceledTurns.delete(turnId);
    await Promise.allSettled([
      active.bridge?.close(),
      disposeAgent(active.agent),
      active.stateRoot
        ? rm(active.stateRoot, { recursive: true, force: true })
        : undefined,
    ]);
  }
}

export function validateSchema(schema: Record<string, unknown>): void {
  if (Object.keys(schema).length === 0 || schema.type !== "object") {
    throw new CursorExecutionError(
      "output.structured.schema must be a non-empty JSON schema object with type 'object'",
    );
  }
  compileSchema(schema);
}

function structuredOutputPrompt(
  prompt: string,
  schema: Record<string, unknown>,
): string {
  return `${prompt}

<gestalt_structured_output>
Return only one JSON object matching this JSON Schema. Do not wrap it in Markdown or include explanatory text.
${JSON.stringify(schema)}
</gestalt_structured_output>`;
}

function structuredOutputFromText(
  text: string,
  schema: Record<string, unknown>,
): Record<string, unknown> {
  const value = parseJsonObject(text);
  const validate = compileSchema(schema);
  if (!validate(value)) {
    const message = validate.errors?.map((error) => error.message).filter(Boolean).join("; ");
    throw new CursorExecutionError(
      `structured output did not match output schema${message ? `: ${message}` : ""}`,
    );
  }
  return value;
}

function compileSchema(schema: Record<string, unknown>) {
  const validator =
    typeof schema.$schema === "string" && !schema.$schema.includes("2020")
      ? new Ajv({ allErrors: true, strict: true })
      : new Ajv2020({ allErrors: true, strict: true });
  try {
    return validator.compile(schema);
  } catch (error) {
    throw new CursorExecutionError(`invalid output.structured.schema: ${errorMessage(error)}`);
  }
}

function parseJsonObject(text: string): Record<string, unknown> {
  try {
    const value = JSON.parse(text);
    if (isRecord(value)) {
      return value;
    }
  } catch {
    // Fall through to extraction from mixed text.
  }
  for (let start = text.indexOf("{"); start >= 0; start = text.indexOf("{", start + 1)) {
    const end = balancedJsonObjectEnd(text, start);
    if (end === undefined) {
      continue;
    }
    try {
      const value = JSON.parse(text.slice(start, end));
      if (isRecord(value)) {
        return value;
      }
    } catch {
      // Try the next object boundary.
    }
  }
  throw new CursorExecutionError("structured output did not contain a JSON object");
}

function balancedJsonObjectEnd(text: string, start: number): number | undefined {
  let depth = 0;
  let inString = false;
  let escape = false;
  for (let index = start; index < text.length; index += 1) {
    const char = text[index];
    if (inString) {
      if (escape) {
        escape = false;
      } else if (char === "\\") {
        escape = true;
      } else if (char === "\"") {
        inString = false;
      }
      continue;
    }
    if (char === "\"") {
      inString = true;
    } else if (char === "{") {
      depth += 1;
    } else if (char === "}") {
      depth -= 1;
      if (depth === 0) {
        return index + 1;
      }
    }
  }
  return undefined;
}

function recordSDKMessage(onEvent: TurnEventSink, message: SDKMessage): string {
  switch (message.type) {
    case "assistant": {
      let text = "";
      for (const block of message.message.content) {
        if (block.type === "text") {
          text += block.text;
        }
      }
      onEvent("assistant.delta", { text });
      return text;
    }
    case "tool_call":
      onEvent("tool.call", {
        callId: message.call_id,
        name: message.name,
        status: message.status,
        args: message.args,
        result: message.result,
      });
      return "";
    case "thinking":
      onEvent("thinking.delta", { text: message.text });
      return "";
    case "status":
      onEvent("run.status", {
        status: message.status,
        message: message.message,
      });
      return "";
    case "task":
      onEvent("run.task", { status: message.status, text: message.text });
      return "";
    default:
      return "";
  }
}

function messagesToPrompt(
  messages: readonly AgentMessage[],
  systemPrompt: string,
): string {
  const sections: string[] = [];
  const trimmedSystemPrompt = systemPrompt.trim();
  if (trimmedSystemPrompt) {
    sections.push(
      `<system>\n${JSON.stringify({ role: "system", text: trimmedSystemPrompt })}\n</system>`,
    );
  }
  sections.push(
    ...messages.map((message, index) => {
      const role = (message.role || "user").trim() || "user";
      const payload = {
        role,
        text: message.text ?? "",
        parts: message.parts ?? [],
        metadata: message.metadata ?? undefined,
      };
      return `<message index="${index + 1}" role="${escapeAttribute(role)}">\n${JSON.stringify(payload)}\n</message>`;
    }),
  );
  return sections.join("\n\n");
}

function escapeAttribute(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

async function disposeAgent(agent: SDKAgent | undefined): Promise<void> {
  if (!agent) {
    return;
  }
  const asyncDispose = agent[Symbol.asyncDispose];
  if (!asyncDispose) {
    agent.close();
    return;
  }
  try {
    await asyncDispose.call(agent);
  } catch {
    agent.close();
  }
}

async function cancelRun(run: Run | undefined): Promise<void> {
  if (!run) {
    return;
  }
  try {
    await run.cancel();
  } catch {
    // Cancellation is best-effort; the canceled flag still controls provider state.
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
