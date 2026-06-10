import type { ToolAnnotations } from "@modelcontextprotocol/sdk/types.js";
import type { AgentToolRef, ListedAgentTool } from "@valon-technologies/gestalt";

import { CursorExecutionError } from "./errors.ts";

export const MCP_SERVER_NAME = "gestalt";
export const MAX_LISTED_TOOLS = 1000;
export const MAX_ERROR_TEXT = 4000;

const MCP_TOOL_NAME = /^[A-Za-z0-9._-]{1,128}$/;

export type ObjectJsonSchema = {
  type: "object";
  properties?: Record<string, unknown>;
  required?: string[];
  additionalProperties?: unknown;
  [key: string]: unknown;
};

export type ToolEntry = {
  mcpName: string;
  title: string;
  description: string;
  ref: AgentToolRef;
  inputSchema: ObjectJsonSchema;
  annotations?: ToolAnnotations;
};

export async function listGestaltTools(input: {
  listedTools: readonly ListedAgentTool[];
}): Promise<ToolEntry[]> {
  const tools: ToolEntry[] = [];
  const seenNames = new Set<string>();

  for (const listed of input.listedTools) {
    if (!isAppOperationTool(listed)) {
      // The bridge can only execute app operations; other targets (e.g.
      // workflow system tools) are not exposed to the model.
      continue;
    }
    const entry = toolEntry(listed);
    if (seenNames.has(entry.mcpName)) {
      throw new CursorExecutionError(
        `tools.catalog.tools contains duplicate mcp_name ${JSON.stringify(entry.mcpName)}`,
      );
    }
    seenNames.add(entry.mcpName);
    tools.push(entry);
    if (tools.length > MAX_LISTED_TOOLS) {
      throw new CursorExecutionError(
        `tools.catalog.tools contains more than ${MAX_LISTED_TOOLS} tools`,
      );
    }
  }

  if (tools.length === 0) {
    throw new CursorExecutionError(
      "tools.catalog.tools are required",
    );
  }
  return tools;
}

export function isAppOperationTool(tool: ListedAgentTool): boolean {
  const ref = tool.ref;
  return Boolean(ref && ref.app?.trim() && ref.operation?.trim() && !ref.system?.trim());
}

export function toolEntry(tool: ListedAgentTool): ToolEntry {
  const mcpName = (tool.mcpName ?? "").trim();
  if (!MCP_TOOL_NAME.test(mcpName)) {
    throw new CursorExecutionError(
      `tools.catalog.tools returned unsafe mcp_name ${JSON.stringify(mcpName)}`,
    );
  }
  const ref = tool.ref;
  if (!ref || !ref.app?.trim() || !ref.operation?.trim() || ref.system?.trim()) {
    throw new CursorExecutionError(
      `tools.catalog.tools returned non-app tool ${JSON.stringify(mcpName)}`,
    );
  }

  const annotations = annotationsFromTool(tool);
  return {
    mcpName,
    title: (tool.title ?? "").trim(),
    description: (tool.description ?? "").trim(),
    ref,
    inputSchema: schemaFromJson(tool.inputSchema ?? ""),
    ...(annotations ? { annotations } : {}),
  };
}

export function schemaFromJson(value: string): ObjectJsonSchema {
  const trimmed = value.trim();
  if (!trimmed) {
    return { type: "object", additionalProperties: true };
  }
  let payload: unknown;
  try {
    payload = JSON.parse(trimmed);
  } catch {
    return { type: "object", additionalProperties: true };
  }
  if (!isRecord(payload)) {
    return { type: "object", additionalProperties: true };
  }
  const projected = projectObjectSchema(payload);
  if (!projected) {
    return { type: "object", properties: {}, additionalProperties: true };
  }
  return projected;
}

function projectObjectSchema(schema: Record<string, unknown>): ObjectJsonSchema | undefined {
  if (!schemaTypeSupportsObject(schema.type)) {
    return undefined;
  }
  const schemaProperties = schema.properties;
  if (schemaProperties !== undefined && !isRecord(schemaProperties)) {
    return undefined;
  }
  const properties: Record<string, unknown> = {
    ...(isRecord(schemaProperties) ? schemaProperties : {}),
  };
  const required = new Set<string>();
  for (const [key, unionRequired] of [
    ["allOf", true],
    ["oneOf", false],
    ["anyOf", false],
  ] as const) {
    if (!mergeSchemaBranches(schema[key], properties, required, unionRequired)) {
      return undefined;
    }
  }
  for (const name of schemaRequired(schema.required, properties)) {
    required.add(name);
  }
  const projected: ObjectJsonSchema = { type: "object" };
  if (typeof schema.additionalProperties === "boolean") {
    projected.additionalProperties = schema.additionalProperties;
  }
  if (Object.keys(properties).length > 0) {
    projected.properties = properties;
  }
  if (required.size > 0) {
    projected.required = [...required].sort();
  }
  return projected;
}

function schemaTypeSupportsObject(value: unknown): boolean {
  if (value === undefined) {
    return true;
  }
  if (value === "object") {
    return true;
  }
  return Array.isArray(value) && value.includes("object");
}

function mergeSchemaBranches(
  branches: unknown,
  properties: Record<string, unknown>,
  required: Set<string>,
  unionRequired: boolean,
): boolean {
  if (branches === undefined) {
    return true;
  }
  if (!Array.isArray(branches)) {
    return false;
  }
  for (const branch of branches) {
    if (!isRecord(branch)) {
      return false;
    }
    const projected = projectObjectSchema(branch);
    if (!projected) {
      return false;
    }
    for (const [name, value] of Object.entries(projected.properties ?? {})) {
      if (
        Object.hasOwn(properties, name) &&
        !jsonValueEqual(properties[name], value)
      ) {
        return false;
      }
      properties[name] = value;
    }
    if (unionRequired) {
      for (const name of schemaRequired(projected.required, properties)) {
        required.add(name);
      }
    }
  }
  return true;
}

function schemaRequired(value: unknown, properties: Record<string, unknown>): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter(
    (item): item is string =>
      typeof item === "string" && Object.hasOwn(properties, item),
  );
}

function jsonValueEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) {
    return true;
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) {
      return false;
    }
    return left.every((value, index) => jsonValueEqual(value, right[index]));
  }
  if (isRecord(left) || isRecord(right)) {
    if (!isRecord(left) || !isRecord(right)) {
      return false;
    }
    const leftKeys = Object.keys(left).sort();
    const rightKeys = Object.keys(right).sort();
    if (leftKeys.length !== rightKeys.length) {
      return false;
    }
    return leftKeys.every(
      (key, index) =>
        key === rightKeys[index] && jsonValueEqual(left[key], right[key]),
    );
  }
  return false;
}

function annotationsFromTool(
  tool: ListedAgentTool,
): ToolAnnotations | undefined {
  const annotations = tool.annotations;
  const out: ToolAnnotations = {};
  if (tool.title?.trim()) {
    out.title = tool.title.trim();
  }
  if (annotations?.readOnlyHint !== undefined) {
    out.readOnlyHint = annotations.readOnlyHint;
  }
  if (annotations?.destructiveHint !== undefined) {
    out.destructiveHint = annotations.destructiveHint;
  }
  if (annotations?.idempotentHint !== undefined) {
    out.idempotentHint = annotations.idempotentHint;
  }
  if (annotations?.openWorldHint !== undefined) {
    out.openWorldHint = annotations.openWorldHint;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
