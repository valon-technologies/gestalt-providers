import { describe, expect, test } from "vitest";
import {
  ASSISTANT_HOSTS,
  MCP_CLIENT_TABS,
  assistantHostById,
  isBuildInstallAgentId,
  normalizeStoredInstallAgentId,
} from "./assistantHosts";

describe("assistant host catalog", () => {
  test("keeps unique ids, docs hashes, and test ids", () => {
    const ids = ASSISTANT_HOSTS.map((host) => host.id);
    const hashes = ASSISTANT_HOSTS.map((host) => host.docsHash);
    const testIds = ASSISTANT_HOSTS.map((host) => host.testId);
    expect(new Set(ids).size).toBe(ids.length);
    expect(new Set(hashes).size).toBe(hashes.length);
    expect(new Set(testIds).size).toBe(testIds.length);
  });

  test("lists Claude and ChatGPT as chat hosts, separate from Claude Code", () => {
    expect(assistantHostById("claude")?.label).toBe("Claude");
    expect(assistantHostById("claude")?.group).toBe("chat");
    expect(assistantHostById("chatgpt")?.label).toBe("ChatGPT");
    expect(assistantHostById("chatgpt")?.group).toBe("chat");
    expect(assistantHostById("claude-code")?.label).toBe("Claude Code");
    expect(assistantHostById("claude-code")?.group).toBe("coding");
  });

  test("exposes one docs tab per host", () => {
    expect(MCP_CLIENT_TABS.map((tab) => tab.id)).toEqual(
      ASSISTANT_HOSTS.map((host) => host.docsHash),
    );
    expect(MCP_CLIENT_TABS[0]?.id).toBe("mcp-claude");
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
    expect(normalizeStoredInstallAgentId("not-a-host", "current")).toBe("");
  });
});
