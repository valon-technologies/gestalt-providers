import { describe, expect, test } from "vitest";
import {
  ASSISTANT_HOSTS,
  ASSISTANT_HOSTS_IN_PICKER,
  MCP_CLIENT_TABS,
  assistantHostById,
  isBuildInstallAgentId,
  normalizeStoredInstallAgentId,
} from "./assistantHosts";

describe("assistant host catalog", () => {
  test("keeps unique ids and test ids", () => {
    const ids = ASSISTANT_HOSTS.map((host) => host.id);
    const testIds = ASSISTANT_HOSTS.map((host) => host.testId);
    expect(new Set(ids).size).toBe(ids.length);
    expect(new Set(testIds).size).toBe(testIds.length);
  });

  test("every host has picker icon and console skin metadata", () => {
    for (const host of ASSISTANT_HOSTS) {
      expect(host.iconKey).toBeTruthy();
      expect(host.consoleSkin).toBeTruthy();
      expect(typeof host.offered).toBe("boolean");
    }
  });

  test("picker and MCP docs omit Claude desktop and ChatGPT", () => {
    expect(ASSISTANT_HOSTS_IN_PICKER.map((host) => host.id)).toEqual([
      "claude-code",
      "codex",
      "cursor",
      "cursor-agent",
      "other",
    ]);
    expect(assistantHostById("claude")?.offered).toBe(false);
    expect(assistantHostById("chatgpt")?.offered).toBe(false);
  });

  test("lists sibling hosts in picker order", () => {
    expect(ASSISTANT_HOSTS.map((host) => host.id)).toEqual([
      "claude",
      "claude-code",
      "chatgpt",
      "codex",
      "cursor",
      "cursor-agent",
      "other",
    ]);
    expect(ASSISTANT_HOSTS.map((host) => host.label)).toEqual([
      "Claude",
      "Claude Code",
      "ChatGPT",
      "Codex",
      "Cursor",
      "Cursor Agent",
      "Others",
    ]);
    expect(assistantHostById("claude")?.iconKey).toBe("claude");
    expect(assistantHostById("claude-code")?.iconKey).toBe("claude-code");
    expect(assistantHostById("cursor-agent")?.iconKey).toBe("cursor");
    expect(assistantHostById("cursor-agent")?.docsHash).toBe("mcp-cursor");
    expect(assistantHostById("other")?.installDescription).toContain(
      "MCP settings",
    );
  });

  test("exposes one docs tab per offered MCP hash", () => {
    expect(MCP_CLIENT_TABS.map((tab) => tab.id)).toEqual([
      "mcp-claude-code",
      "mcp-codex",
      "mcp-cursor",
      "mcp-other",
    ]);
    expect(new Set(MCP_CLIENT_TABS.map((tab) => tab.id)).size).toBe(
      MCP_CLIENT_TABS.length,
    );
  });
});

describe("normalizeStoredInstallAgentId", () => {
  test("maps legacy claude onto Claude Code", () => {
    expect(normalizeStoredInstallAgentId("claude", "legacy")).toBe(
      "claude-code",
    );
    expect(normalizeStoredInstallAgentId("cursor", "legacy")).toBe("cursor");
  });

  test("keeps current claude as Claude chat", () => {
    expect(normalizeStoredInstallAgentId("claude", "current")).toBe("claude");
    expect(isBuildInstallAgentId("chatgpt")).toBe(true);
    expect(isBuildInstallAgentId("claude-code")).toBe(true);
    expect(isBuildInstallAgentId("cursor-agent")).toBe(true);
    expect(normalizeStoredInstallAgentId("not-a-host", "current")).toBe("");
  });
});
