import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { ASSISTANT_HOSTS } from "@/lib/assistantHosts";
import { HOST_INSTALL_RECIPES } from "./assistant-install";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "assistant-install.tsx"),
  "utf8",
);

describe("assistant install guidance", () => {
  test("token needed callout is standing help, not a live region", () => {
    expect(SOURCE).toContain('data-testid="build-install-token-needed"');
    expect(SOURCE).toContain("SETUP_TYPESET_CHROME_CLASS");
    expect(SOURCE).not.toContain("setup-overlap-callout");
    expect(SOURCE).not.toContain("live={false}");
    expect(SOURCE).toContain('from "@/components/ui/alert"');
    expect(SOURCE).not.toContain("@/components/Callout");
  });

  test("install recipes require a session-minted token", () => {
    expect(SOURCE).not.toContain("gst_api_YOUR_TOKEN");
    expect(SOURCE).toContain("HOST_INSTALL_RECIPES[agent]");
    expect(SOURCE).toContain("!hasMcpCredential");
  });

  test("picker lists sibling hosts from the catalog", () => {
    expect(SOURCE).toContain("ASSISTANT_HOSTS_IN_PICKER.map");
    expect(SOURCE).toContain("ASSISTANT_HOST_PICKER_GRID_CLASS");
    expect(SOURCE).not.toContain("ASSISTANT_HOST_GROUPS");
    expect(SOURCE).not.toContain("assistantHostsInGroup");
    expect(SOURCE).toContain("ASSISTANT_HOST_ICON[host.iconKey]");
    expect(SOURCE).toContain('"size-12 shrink-0"');
    expect(SOURCE).not.toContain("ClaudeCodeIcon");
    expect(SOURCE).toContain("CursorAgentInstallRecipe");
    expect(SOURCE).toContain("SETUP_TYPESET_CHROME_CLASS");
    expect(SOURCE).not.toContain("list-decimal");
  });

  test("every assistant host has an install recipe", () => {
    expect(Object.keys(HOST_INSTALL_RECIPES).sort()).toEqual(
      ASSISTANT_HOSTS.map((host) => host.id).sort(),
    );
  });

  test("Claude Code Authorization header uses gestaltMcpBearerValue", () => {
    expect(SOURCE).toContain(
      '--header "Authorization: ${gestaltMcpBearerValue(apiToken)}"',
    );
    expect(SOURCE).not.toContain(
      '--header "Authorization: Bearer ${apiToken}"',
    );
  });

  test("Other install shows MCP URL, Authorization, and client config", () => {
    expect(SOURCE).toContain('data-testid="build-install-other-recipe"');
    expect(SOURCE).toContain("gestaltMcpClientConfigJson");
    expect(SOURCE).toContain("gestaltMcpBearerValue");
    expect(SOURCE).toContain("function OtherInstallRecipe({ mcpUrl, apiToken }");
    expect(SOURCE).toContain("secrets={[apiToken]}");
    expect(SOURCE).toContain('revealLabel: "Show token"');
  });

  test("Cursor install is the one-click button; Cursor Agent keeps the paste recipe", () => {
    expect(SOURCE).toContain('data-testid="build-install-cursor-recipe"');
    expect(SOURCE).toContain('data-testid="build-add-to-cursor"');
    expect(SOURCE).toContain("<CursorIcon />");
    expect(SOURCE).toContain("cursorMcpInstallHref");
    expect(SOURCE).not.toContain("build-install-cursor-method");
    expect(SOURCE).not.toContain("Paste the config yourself");
    expect(SOURCE).toContain('data-testid="build-install-cursor-agent-recipe"');
    expect(SOURCE).toContain("CursorMcpConfigBlock");
    expect(SOURCE).toContain("RecipeEmphasis");
    expect(SOURCE).not.toContain("CHATGPT_INSTALL_AUTH_NOTE");
  });
});
