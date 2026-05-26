import { spawn } from "node:child_process";
import type { AgentMessage } from "@valon-technologies/gestalt";

export const SESSION_START_PREFIX = "__gestalt.lifecycle.sessionStart";
export const SESSION_START_ADDITIONAL_CONTEXT_KEY = `${SESSION_START_PREFIX}.additionalContext`;
const RESERVED_METADATA_KEYS = new Set(["cwd", "workspacePath", "worktreePath"]);

const DEFAULT_ENV_KEYS = [
  "HOME",
  "PATH",
  "SHELL",
  "TMPDIR",
  "USER",
  "LOGNAME",
  "LANG",
  "LC_ALL",
] as const;

export async function runSessionStartHooks(
  sessionStart: unknown,
  metadata: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const hooks = sessionStartHooks(sessionStart);
  if (hooks.length === 0) {
    return { ...metadata };
  }
  const merged: Record<string, unknown> = { ...metadata };
  const contextChunks: string[] = [];
  for (const hook of hooks) {
    const { result, additionalContext } = await runHook(hook);
    merged[`${SESSION_START_PREFIX}.results.${hookId(hook)}`] = result;
    if (additionalContext) {
      contextChunks.push(additionalContext);
    }
  }
  if (contextChunks.length > 0) {
    merged[SESSION_START_ADDITIONAL_CONTEXT_KEY] = contextChunks.join("\n\n");
  }
  return merged;
}

export function validateSessionStartUserMetadata(
  metadata: Record<string, unknown> | undefined,
): void {
  if (metadata === undefined) {
    return;
  }
  for (const key of Object.keys(metadata)) {
    if (key.startsWith(SESSION_START_PREFIX) || RESERVED_METADATA_KEYS.has(key)) {
      throw new Error(
        `agent session metadata key ${JSON.stringify(key)} is reserved for Gestalt lifecycle or workspace data`,
      );
    }
  }
}

export function prependSessionStartContext(
  messages: readonly AgentMessage[],
  metadata: Record<string, unknown>,
): AgentMessage[] {
  const context = String(metadata[SESSION_START_ADDITIONAL_CONTEXT_KEY] ?? "").trim();
  if (!context) {
    return [...messages];
  }
  return [
    {
      role: "system",
      text: `Session start context:\n\n${context}`,
      metadata: { source: SESSION_START_PREFIX },
    },
    ...messages,
  ];
}

function sessionStartHooks(sessionStart: unknown): Record<string, unknown>[] {
  if (!sessionStart || typeof sessionStart !== "object") {
    return [];
  }
  const hooks = (sessionStart as { hooks?: unknown }).hooks;
  return Array.isArray(hooks) ? hooks.filter(isRecord) : [];
}

async function runHook(hook: Record<string, unknown>): Promise<{
  result: Record<string, unknown>;
  additionalContext: string;
}> {
  const id = hookId(hook);
  const type = String(hook.type ?? "command").trim() || "command";
  if (type !== "command") {
    throw new Error(`sessionStart hook ${JSON.stringify(id)} type ${JSON.stringify(type)} is not supported`);
  }
  const command = Array.isArray(hook.command)
    ? hook.command.map((part) => String(part)).filter((part) => part.trim())
    : [];
  if (command.length === 0) {
    throw new Error(`sessionStart hook ${JSON.stringify(id)} command is required`);
  }
  const timeout = String(hook.timeout ?? "");
  const completed = await runCommand({
    command: command[0]!,
    args: command.slice(1),
    cwd: String(hook.cwd ?? "").trim() || undefined,
    env: hookEnv(isRecord(hook.env) ? hook.env : {}),
    timeoutMs: parseTimeoutMs(timeout),
  });
  if (completed.code !== 0) {
    const detail = completed.stderr.trim() || completed.stdout.trim() || `exit code ${completed.code}`;
    throw new Error(`sessionStart hook ${JSON.stringify(id)} failed: ${detail}`);
  }
  const output = isRecord(hook.output) ? hook.output : {};
  const stdoutPayload = jsonStdoutPayload(completed.stdout);
  const result: Record<string, unknown> = {
    status: "succeeded",
    exitCode: completed.code,
    timeout,
    timedOut: false,
  };
  if (output.metadata === true) {
    if (isRecord(stdoutPayload.metadata)) {
      result.metadata = stdoutPayload.metadata;
    }
    result.stdout = completed.stdout;
    result.stderr = completed.stderr;
  }
  const payloadContext = stdoutPayload.additionalContext;
  return {
    result,
    additionalContext:
      output.additionalContext === true
        ? payloadContext === undefined
          ? completed.stdout.trim()
          : String(payloadContext).trim()
        : "",
  };
}

function runCommand(input: {
  command: string;
  args: string[];
  cwd: string | undefined;
  env: NodeJS.ProcessEnv;
  timeoutMs: number | undefined;
}): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(input.command, input.args, {
      cwd: input.cwd,
      env: input.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    let timeout: ReturnType<typeof setTimeout> | undefined;
    if (input.timeoutMs !== undefined) {
      timeout = setTimeout(() => {
        child.kill("SIGTERM");
        reject(new Error(`sessionStart hook timed out after ${input.timeoutMs}ms`));
      }, input.timeoutMs);
    }
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (timeout) {
        clearTimeout(timeout);
      }
      resolve({ code: code ?? 1, stdout, stderr });
    });
  });
}

function hookEnv(explicit: Record<string, unknown>): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const key of DEFAULT_ENV_KEYS) {
    if (process.env[key] !== undefined) {
      env[key] = process.env[key];
    }
  }
  for (const [key, value] of Object.entries(explicit)) {
    env[key] = String(value);
  }
  return env;
}

function parseTimeoutMs(value: string): number | undefined {
  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }
  if (trimmed.endsWith("ms")) {
    return Number(trimmed.slice(0, -2));
  }
  if (trimmed.endsWith("s")) {
    return Number(trimmed.slice(0, -1)) * 1000;
  }
  if (trimmed.endsWith("m")) {
    return Number(trimmed.slice(0, -1)) * 60_000;
  }
  return Number(trimmed) * 1000;
}

function hookId(hook: Record<string, unknown>): string {
  return String(hook.id ?? "").trim();
}

function jsonStdoutPayload(stdout: string): Record<string, unknown> {
  const trimmed = stdout.trim();
  if (!trimmed) {
    return {};
  }
  try {
    const payload: unknown = JSON.parse(trimmed);
    return isRecord(payload) ? payload : {};
  } catch {
    return {};
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
