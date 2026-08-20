/**
 * Canonical Gestalt MCP client config (URL + Authorization header).
 * Setup recipes and docs share this shape so Other, Cursor paste, and
 * MCP setup docs cannot drift.
 */

export function gestaltMcpBearerValue(token: string): string {
  return `Bearer ${token}`;
}

/** Cursor one-click MCP install deeplink (Settings and docs share this). */
export function cursorMcpInstallHref(mcpUrl: string, apiToken: string): string {
  const config = {
    url: mcpUrl,
    headers: {
      Authorization: gestaltMcpBearerValue(apiToken),
    },
  };
  const json = JSON.stringify(config);
  const base64 = btoa(json);
  return `cursor://anysphere.cursor-deeplink/mcp/install?name=${encodeURIComponent("gestalt")}&config=${encodeURIComponent(base64)}`;
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
