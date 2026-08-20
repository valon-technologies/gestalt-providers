import { describe, expect, it } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  ASSISTANT_DESTINATION_SWITCHER_LABEL,
  assistantDestinationIds,
  assistantDestinationLegacyIds,
  assistantDestinationMedia,
  assistantDestinationTabIds,
  assistantDestinationTabs,
  defaultAssistantDestinationId,
} from "./assistant-destinations";
import {
  ASSISTANT_DOCS_LANDING_HASH_ALIASES,
  ASSISTANT_HOSTS_OFFERED,
  assistantDestinationHash,
  assistantDocsLandingHash,
  assistantHostById,
} from "@/lib/assistantHosts";
import { resolveHashTabId } from "./docs-option-switcher";

const HERE = dirname(fileURLToPath(import.meta.url));

describe("assistant destination switcher", () => {
  it("lists offered named products, with ChatGPT and Codex as siblings", () => {
    const offeredNamed = ASSISTANT_HOSTS_OFFERED.filter(
      (host) => host.id !== "other",
    );
    expect(assistantDestinationTabs.map((tab) => tab.hostId)).toEqual(
      offeredNamed.map((host) => host.id),
    );
    expect(assistantDestinationTabs.map((tab) => tab.label)).toEqual(
      offeredNamed.map((host) => host.label),
    );
    expect(assistantDestinationTabs.map((tab) => tab.id)).toEqual(
      offeredNamed.map((host) => assistantDestinationHash(host.id)),
    );
    expect(assistantDestinationTabs.map((tab) => tab.hostId)).toEqual([
      "claude-code",
      "chatgpt",
      "codex",
      "cursor",
      "cursor-agent",
    ]);
    expect(assistantDestinationTabs.map((tab) => tab.label)).toEqual([
      "Claude Code",
      "ChatGPT",
      "Codex",
      "Cursor",
      "Cursor Agent",
    ]);
    expect(assistantDestinationTabIds).toEqual([
      "dest-claude-code",
      "dest-chatgpt",
      "dest-codex",
      "dest-cursor",
      "dest-cursor-agent",
    ]);
    expect(assistantDestinationLegacyIds).toEqual(["dest-claude"]);
    expect(assistantDestinationIds).toEqual([
      "dest-claude-code",
      "dest-chatgpt",
      "dest-codex",
      "dest-cursor",
      "dest-cursor-agent",
      "dest-claude",
    ]);
    expect(defaultAssistantDestinationId).toBe("dest-claude-code");
    expect(ASSISTANT_DESTINATION_SWITCHER_LABEL).toBe("Choose your assistant");
    for (const tab of assistantDestinationTabs) {
      const host = assistantHostById(tab.hostId);
      expect(host?.offered).toBe(true);
      expect(tab.hostId).not.toBe("other");
    }
    expect(
      ASSISTANT_HOSTS_OFFERED.filter((host) => host.id !== "other").map(
        (host) => host.id,
      ),
    ).toEqual(assistantDestinationTabs.map((tab) => tab.hostId));
  });

  it("keeps destination hashes distinct from config-file recipes", () => {
    for (const id of assistantDestinationIds) {
      expect(id.startsWith("dest-")).toBe(true);
      expect(id.startsWith("mcp-")).toBe(false);
    }
  });

  it("lands offered hosts on dest tabs and keeps leftover Claude hashes", () => {
    expect(assistantDocsLandingHash(assistantHostById("claude"))).toBe(
      "dest-claude",
    );
    expect(assistantDocsLandingHash(assistantHostById("chatgpt"))).toBe(
      "dest-chatgpt",
    );
    expect(assistantDocsLandingHash(assistantHostById("claude-code"))).toBe(
      "dest-claude-code",
    );
    expect(assistantDocsLandingHash(assistantHostById("codex"))).toBe(
      "dest-codex",
    );
    expect(assistantDocsLandingHash(assistantHostById("cursor"))).toBe(
      "dest-cursor",
    );
    expect(assistantDocsLandingHash(assistantHostById("cursor-agent"))).toBe(
      "dest-cursor-agent",
    );
    expect(ASSISTANT_DOCS_LANDING_HASH_ALIASES).toEqual({
      "mcp-claude": "dest-claude",
      "mcp-chatgpt": "dest-chatgpt",
      "mcp-claude-code": "dest-claude-code",
      "mcp-codex": "dest-codex",
      "mcp-cursor": "dest-cursor",
    });
    expect(
      resolveHashTabId(
        "mcp-chatgpt",
        assistantDestinationIds,
        defaultAssistantDestinationId,
        ASSISTANT_DOCS_LANDING_HASH_ALIASES,
      ),
    ).toBe("dest-chatgpt");
    expect(
      resolveHashTabId(
        "mcp-cursor",
        assistantDestinationIds,
        defaultAssistantDestinationId,
        ASSISTANT_DOCS_LANDING_HASH_ALIASES,
      ),
    ).toBe("dest-cursor");
    expect(
      resolveHashTabId(
        "mcp-codex",
        assistantDestinationIds,
        defaultAssistantDestinationId,
        ASSISTANT_DOCS_LANDING_HASH_ALIASES,
      ),
    ).toBe("dest-codex");
    expect(
      resolveHashTabId(
        "",
        assistantDestinationIds,
        defaultAssistantDestinationId,
        ASSISTANT_DOCS_LANDING_HASH_ALIASES,
      ),
    ).toBe(defaultAssistantDestinationId);
    expect(
      resolveHashTabId(
        "dest-claude",
        assistantDestinationIds,
        defaultAssistantDestinationId,
        ASSISTANT_DOCS_LANDING_HASH_ALIASES,
      ),
    ).toBe("dest-claude");
    expect(
      resolveHashTabId(
        "mcp-other",
        assistantDestinationIds,
        defaultAssistantDestinationId,
        ASSISTANT_DOCS_LANDING_HASH_ALIASES,
      ),
    ).toBe(defaultAssistantDestinationId);
  });

  it("ships leftover Claude video and the ChatGPT install demo", () => {
    expect(assistantDestinationMedia["dest-claude"].video).toBe(
      "/docs/add-token-claude.mp4",
    );
    expect(assistantHostById("chatgpt")?.installDemo).toEqual({
      src: "/setup/chatgpt-install.mp4",
      poster: "/setup/chatgpt-install.jpg",
    });
    const publicRoot = join(HERE, "../../public");
    expect(existsSync(join(publicRoot, "docs/add-token-claude.mp4"))).toBe(
      true,
    );
    expect(existsSync(join(publicRoot, "docs/add-token-claude.jpg"))).toBe(
      true,
    );
    expect(existsSync(join(publicRoot, "setup/chatgpt-install.mp4"))).toBe(
      true,
    );
    expect(existsSync(join(publicRoot, "setup/chatgpt-install.jpg"))).toBe(
      true,
    );
  });

  it("wires dest recipes to Setup's products, with Cursor as Add in Cursor", () => {
    const source = readFileSync(
      join(HERE, "AssistantDestinationSwitcher.tsx"),
      "utf8",
    );
    expect(source).toContain("ASSISTANT_HOST_ICON");
    expect(source).toContain("claude mcp add --transport http");
    expect(source).toContain("codex mcp add gestalt");
    expect(source).toContain("CHATGPT_INSTALL_NAME_TYPE");
    expect(source).toContain("RecipeEmphasis");
    expect(source).toContain("cursorMcpInstallHref");
    expect(source).toContain("Add in Cursor");
    expect(source).toContain("CURSOR_AGENT_INSTALL_PREAMBLE");
    expect(source).toContain("Request headers");
    expect(source).toContain("Create a token here");
    expect(source).not.toContain("CHATGPT_INSTALL_AUTH_NOTE");
    expect(source).not.toContain("Developer mode");
    expect(source).not.toContain("ChatGPT Codex");
    expect(source).not.toContain("data-docs-token-paste-target");
    expect(source).not.toMatch(/[\u2013\u2014]/);
    const cursorDest = source.slice(
      source.indexOf("function CursorDestination"),
      source.indexOf("function CursorAgentDestination"),
    );
    expect(cursorDest).toContain('data-testid="docs-add-to-cursor"');
    expect(cursorDest).toContain("Add in Cursor");
    expect(cursorDest).toContain("DestinationTokenStep");
    expect(cursorDest).not.toContain(".cursor/mcp.json");
  });
});
