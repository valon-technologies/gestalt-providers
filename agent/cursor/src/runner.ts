import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { AgentOptions, Run, SDKAgent, SDKMessage } from "@cursor/sdk";
import type {
  AgentMessage,
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
    cwd: string;
    onEvent: TurnEventSink;
  }): Promise<string> {
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
      cwd: string;
      onEvent: TurnEventSink;
    },
    active: ActiveTurn,
  ): Promise<string> {
    try {
      await this.raiseIfCanceled(active);
      const host = this.createHost();
      const tools = await listGestaltTools({
        host,
        sessionId: input.sessionId,
        turnId: input.turnId,
        runGrant: input.runGrant,
      });
      await this.raiseIfCanceled(active);

      active.bridge = await startMcpBridge({
        tools,
        executeTool: async (entry, toolCallId, args) => {
          const response = await host.executeTool({
            sessionId: input.sessionId,
            turnId: input.turnId,
            toolCallId,
            toolId: entry.toolId,
            arguments: args,
            runGrant: input.runGrant,
            idempotencyKey: `agent/cursor-sdk:${input.turnId}:${toolCallId}:${entry.mcpName}`,
          } as ExecuteAgentToolRequest);
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

      const prompt = messagesToPrompt(input.messages, this.config.systemPrompt);
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
      return result.result?.trim() || assistantText.join("").trim();
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
