import type { ToolAnnotations } from "@modelcontextprotocol/sdk/types.js";
import type {
  ListAgentToolsRequest,
  ListedAgentTool,
} from "@valon-technologies/gestalt";

import type { GestaltAgentHostClient } from "./agent_host.ts";
import { CursorExecutionError } from "./errors.ts";

export const MCP_SERVER_NAME = "gestalt";
export const DEFAULT_PAGE_SIZE = 100;
export const MAX_LISTED_TOOLS = 1000;
export const MAX_PAGES = 100;
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
  toolId: string;
  mcpName: string;
  title: string;
  description: string;
  inputSchema: ObjectJsonSchema;
  annotations?: ToolAnnotations;
};

export async function listGestaltTools(input: {
  host: GestaltAgentHostClient;
  sessionId: string;
  turnId: string;
  toolGrant: string;
}): Promise<ToolEntry[]> {
  let pageToken = "";
  const seenTokens = new Set<string>();
  const tools: ToolEntry[] = [];
  const seenNames = new Set<string>();

  for (let pages = 1; ; pages += 1) {
    if (pages > MAX_PAGES) {
      throw new CursorExecutionError(`ListTools exceeded ${MAX_PAGES} pages`);
    }
    if (seenTokens.has(pageToken)) {
      throw new CursorExecutionError(`ListTools repeated page token ${JSON.stringify(pageToken)}`);
    }
    seenTokens.add(pageToken);

    const response = await input.host.listTools({
      sessionId: input.sessionId,
      turnId: input.turnId,
      pageSize: DEFAULT_PAGE_SIZE,
      pageToken,
      toolGrant: input.toolGrant,
    } as ListAgentToolsRequest);
    for (const listed of response.tools) {
      const entry = toolEntry(listed);
      if (seenNames.has(entry.mcpName)) {
        throw new CursorExecutionError(
          `ListTools returned duplicate mcp_name ${JSON.stringify(entry.mcpName)}`,
        );
      }
      seenNames.add(entry.mcpName);
      tools.push(entry);
      if (tools.length > MAX_LISTED_TOOLS) {
        throw new CursorExecutionError(`ListTools returned more than ${MAX_LISTED_TOOLS} tools`);
      }
    }
    pageToken = (response.nextPageToken ?? "").trim();
    if (!pageToken) {
      break;
    }
  }

  if (tools.length === 0) {
    throw new CursorExecutionError("ListTools returned no tools for the requested grant");
  }
  return tools;
}

export function toolEntry(tool: ListedAgentTool): ToolEntry {
  const toolId = (tool.id ?? "").trim();
  const mcpName = (tool.mcpName ?? "").trim();
  if (!toolId) {
    throw new CursorExecutionError("ListTools returned a tool without an id");
  }
  if (!mcpName) {
    throw new CursorExecutionError("ListTools returned a tool without an mcp_name");
  }
  if (!MCP_TOOL_NAME.test(mcpName)) {
    throw new CursorExecutionError(`ListTools returned unsafe mcp_name ${JSON.stringify(mcpName)}`);
  }

  const annotations = annotationsFromTool(tool);
  return {
    toolId,
    mcpName,
    title: (tool.title ?? "").trim(),
    description: (tool.description ?? "").trim(),
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
  if (payload.type !== "object") {
    return { type: "object", properties: {}, additionalProperties: true };
  }
  return payload as ObjectJsonSchema;
}

function annotationsFromTool(tool: ListedAgentTool): ToolAnnotations | undefined {
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
