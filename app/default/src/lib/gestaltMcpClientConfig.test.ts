import { describe, expect, test } from "vitest";
import {
  gestaltMcpBearerValue,
  gestaltMcpClientConfigJson,
} from "./gestaltMcpClientConfig";

describe("gestalt MCP client config", () => {
  test("emits URL, Authorization bearer header, and mcpServers key", () => {
    const json = gestaltMcpClientConfigJson({
      url: "https://example.test/mcp",
      token: "gst_api_secret",
    });
    expect(gestaltMcpBearerValue("gst_api_secret")).toBe(
      "Bearer gst_api_secret",
    );
    expect(json).toContain('"mcpServers"');
    expect(json).toContain('"url": "https://example.test/mcp"');
    expect(json).toContain(
      '"Authorization": "Bearer gst_api_secret"',
    );
  });
});
