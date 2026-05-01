import { accessSync, constants, statSync } from "node:fs";
import { resolve } from "node:path";

export const DEFAULT_MODEL = "composer-2";
export const DEFAULT_TIMEOUT_SECONDS = 300;

export type CursorAgentConfig = {
  defaultModel: string;
  timeoutSeconds: number;
  workingDirectory: string;
  cursorApiKey?: string;
  sandboxEnabled?: boolean;
};

export function parseCursorAgentConfig(raw: Record<string, unknown>): CursorAgentConfig {
  const defaultModel = stringValue(raw.defaultModel, DEFAULT_MODEL, "defaultModel");
  const timeoutSeconds = numberValue(
    raw.timeoutSeconds,
    DEFAULT_TIMEOUT_SECONDS,
    "timeoutSeconds",
  );
  if (timeoutSeconds <= 0) {
    throw new Error("timeoutSeconds must be positive");
  }

  const workingDirectory = resolve(
    stringValue(raw.workingDirectory, process.cwd(), "workingDirectory"),
  );
  let info;
  try {
    info = statSync(workingDirectory);
    accessSync(workingDirectory, constants.R_OK | constants.X_OK);
  } catch (error) {
    throw new Error(`workingDirectory ${JSON.stringify(workingDirectory)} is not accessible`);
  }
  if (!info.isDirectory()) {
    throw new Error(`workingDirectory ${JSON.stringify(workingDirectory)} is not a directory`);
  }

  const config: CursorAgentConfig = {
    defaultModel,
    timeoutSeconds,
    workingDirectory,
  };
  const cursorApiKey = optionalString(raw.cursorApiKey, "cursorApiKey");
  if (cursorApiKey !== undefined) {
    config.cursorApiKey = cursorApiKey;
  }
  const sandboxEnabled = optionalBoolean(raw.sandboxEnabled, "sandboxEnabled");
  if (sandboxEnabled !== undefined) {
    config.sandboxEnabled = sandboxEnabled;
  }
  return config;
}

export function resolveModel(config: CursorAgentConfig, requested: string): string {
  const model = requested.trim() || config.defaultModel;
  if (!model) {
    throw new Error("model is required");
  }
  return model;
}

function stringValue(value: unknown, fallback: string, field: string): string {
  if (value === undefined || value === null) {
    return fallback;
  }
  if (typeof value !== "string") {
    throw new Error(`${field} must be a string`);
  }
  const trimmed = value.trim();
  return trimmed || fallback;
}

function optionalString(value: unknown, field: string): string | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new Error(`${field} must be a string`);
  }
  const trimmed = value.trim();
  return trimmed || undefined;
}

function optionalBoolean(value: unknown, field: string): boolean | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== "boolean") {
    throw new Error(`${field} must be a boolean`);
  }
  return value;
}

function numberValue(value: unknown, fallback: number, field: string): number {
  if (value === undefined || value === null) {
    return fallback;
  }
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${field} must be a number`);
  }
  return value;
}
