import { randomBytes } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  type CallToolResult,
  type Tool,
} from "@modelcontextprotocol/sdk/types.js";

import { CursorExecutionError } from "./errors.ts";
import { MCP_SERVER_NAME, type ToolEntry } from "./tools.ts";

export type StartedMcpBridge = {
  url: string;
  headers: Record<string, string>;
  close: () => Promise<void>;
};

export async function startMcpBridge(input: {
  tools: ToolEntry[];
  executeTool: (
    entry: ToolEntry,
    toolCallId: string,
    args: Record<string, unknown>,
  ) => Promise<{ status: number; body: string | Uint8Array }>;
}): Promise<StartedMcpBridge> {
  const token = randomBytes(24).toString("base64url");
  const toolsByName = new Map(input.tools.map((tool) => [tool.mcpName, tool]));
  let callSequence = 0;

  const httpServer = createServer((req, res) => {
    void handleMcpRequest(req, res, token, () => {
      const mcpServer = createMcpServer(input.tools, async (toolName, args) => {
        const entry = toolsByName.get(toolName);
        if (!entry) {
          throw new CursorExecutionError(`unknown MCP tool ${JSON.stringify(toolName)}`);
        }
        callSequence += 1;
        return await input.executeTool(entry, `sdk-${callSequence}`, args);
      });
      return mcpServer;
    });
  });

  await new Promise<void>((resolveListen, rejectListen) => {
    httpServer.once("error", rejectListen);
    httpServer.listen(0, "127.0.0.1", () => {
      httpServer.off("error", rejectListen);
      resolveListen();
    });
  });

  let closed = false;
  const address = httpServer.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${address.port}/mcp`,
    headers: {
      Authorization: `Bearer ${token}`,
    },
    close: async () => {
      if (closed) {
        return;
      }
      closed = true;
      await new Promise<void>((resolveClose, rejectClose) => {
        httpServer.close((error) => {
          if (error) {
            rejectClose(error);
            return;
          }
          resolveClose();
        });
      });
    },
  };
}

function createMcpServer(
  tools: ToolEntry[],
  executeTool: (
    toolName: string,
    args: Record<string, unknown>,
  ) => Promise<{ status: number; body: string | Uint8Array }>,
): Server {
  const server = new Server(
    { name: MCP_SERVER_NAME, version: "0.0.1-alpha.1" },
    { capabilities: { tools: {} } },
  );

  server.setRequestHandler(ListToolsRequestSchema, () => ({
    tools: tools.map((tool): Tool => {
      const entry: Tool = {
        name: tool.mcpName,
        description: tool.description || tool.title || tool.mcpName,
        inputSchema: tool.inputSchema as Tool["inputSchema"],
      };
      if (tool.title) {
        entry.title = tool.title;
      }
      if (tool.annotations) {
        entry.annotations = tool.annotations;
      }
      return entry;
    }),
  }));

  server.setRequestHandler(CallToolRequestSchema, async (request): Promise<CallToolResult> => {
    const response = await executeTool(request.params.name, request.params.arguments ?? {});
    const body = operationBodyText(response.body) || "{}";
    return {
      content: [{ type: "text", text: body }],
      isError: response.status >= 400,
    };
  });

  return server;
}

function operationBodyText(body: string | Uint8Array | null | undefined): string {
  if (body == null) {
    return "";
  }
  if (typeof body === "string") {
    return body;
  }
  return new TextDecoder().decode(body);
}

async function handleMcpRequest(
  req: IncomingMessage,
  res: ServerResponse,
  token: string,
  createServerForRequest: () => Server,
): Promise<void> {
  if ((req.url ?? "").split("?", 1)[0] !== "/mcp") {
    res.writeHead(404).end("not found");
    return;
  }
  if (req.headers.authorization !== `Bearer ${token}`) {
    res.writeHead(401).end("unauthorized");
    return;
  }

  const mcpServer = createServerForRequest();
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: undefined,
  } as unknown as ConstructorParameters<typeof StreamableHTTPServerTransport>[0]);
  res.once("finish", () => {
    void transport.close().finally(() => {
      void mcpServer.close();
    });
  });

  try {
    await mcpServer.connect(transport as never);
    await transport.handleRequest(req, res);
  } catch (error) {
    if (!res.headersSent) {
      res.writeHead(500).end(error instanceof Error ? error.message : String(error));
    } else {
      res.end();
    }
  }
}
