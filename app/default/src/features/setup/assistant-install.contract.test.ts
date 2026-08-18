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
    expect(SOURCE).toContain(
      '<Callout variant="info" data-testid="build-install-token-needed">',
    );
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
    expect(SOURCE).toContain("ClaudeCodeIcon");
    expect(SOURCE).toContain('<ClaudeCodeIcon className="size-12 shrink-0" />');
    expect(SOURCE).not.toContain("<ClaudeCodeIcon className={iconClass} />");
    expect(SOURCE).toContain('<ChatGptIcon className="size-12 shrink-0" />');
    expect(SOURCE).not.toContain("<ChatGptIcon className={iconClass} />");
    expect(SOURCE).toContain("CursorAgentInstallRecipe");
  });

  test("every assistant host has an install recipe", () => {
    expect(Object.keys(HOST_INSTALL_RECIPES).sort()).toEqual(
      ASSISTANT_HOSTS.map((host) => host.id).sort(),
    );
  });

  test("Other install shows MCP URL, Authorization, and client config", () => {
    expect(SOURCE).toContain('data-testid="build-install-other-recipe"');
    expect(SOURCE).toContain("gestaltMcpClientConfigJson");
    expect(SOURCE).toContain("gestaltMcpBearerValue");
    expect(SOURCE).toContain("function OtherInstallRecipe({ mcpUrl, apiToken }");
  });

  test("Cursor install is the one-click button; Cursor Agent keeps the paste recipe", () => {
    expect(SOURCE).toContain('data-testid="build-install-cursor-recipe"');
    expect(SOURCE).toContain('data-testid="build-add-to-cursor"');
    expect(SOURCE).not.toContain("build-install-cursor-method");
    expect(SOURCE).not.toContain("Paste the config yourself");
    expect(SOURCE).toContain('data-testid="build-install-cursor-agent-recipe"');
    expect(SOURCE).toContain("CursorMcpConfigBlock");
  });
});
