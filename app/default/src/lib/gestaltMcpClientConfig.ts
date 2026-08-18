/**
 * Canonical Gestalt MCP client config (URL + Authorization header).
 * Setup recipes and docs share this shape so Other, Cursor paste, and
 * MCP setup docs cannot drift.
 */

export function gestaltMcpBearerValue(token: string): string {
  return `Bearer ${token}`;
}

export function gestaltMcpClientConfigJson(input: {
  url: string;
  token: string;
}): string {
  return JSON.stringify(
    {
      mcpServers: {
        gestalt: {
          url: input.url,
          headers: {
            Authorization: gestaltMcpBearerValue(input.token),
          },
        },
      },
    },
    null,
    2,
  );
}
