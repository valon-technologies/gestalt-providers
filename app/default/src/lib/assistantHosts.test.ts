import { describe, expect, test } from "vitest";
import {
  ASSISTANT_DOCS_LANDING_HASH_ALIASES,
  ASSISTANT_HOSTS,
  ASSISTANT_HOSTS_IN_PICKER,
  assistantDestinationHash,
  assistantHostById,
  assistantDocsLandingHash,
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

  test("picker and MCP docs omit Claude desktop", () => {
    expect(ASSISTANT_HOSTS_IN_PICKER.map((host) => host.id)).toEqual([
      "claude-code",
      "chatgpt",
      "codex",
      "cursor",
      "cursor-agent",
      "other",
    ]);
    expect(assistantHostById("claude")?.offered).toBe(false);
    expect(assistantHostById("chatgpt")?.offered).toBe(true);
    expect(assistantHostById("chatgpt")?.installDemo).toEqual({
      src: "/setup/chatgpt-install.mp4",
      poster: "/setup/chatgpt-install.jpg",
    });
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
    expect(assistantDocsLandingHash(assistantHostById("claude"))).toBe(
      "dest-claude",
    );
    expect(assistantDocsLandingHash(assistantHostById("chatgpt"))).toBe(
      "dest-chatgpt",
    );
    expect(assistantDocsLandingHash(assistantHostById("cursor"))).toBe(
      "dest-cursor",
    );
    expect(assistantDocsLandingHash(assistantHostById("cursor-agent"))).toBe(
      "dest-cursor-agent",
    );
    expect(assistantDocsLandingHash(assistantHostById("codex"))).toBe(
      "dest-codex",
    );
    expect(assistantDocsLandingHash(assistantHostById("claude-code"))).toBe(
      "dest-claude-code",
    );
    expect(assistantDocsLandingHash(undefined)).toBe("mcp-other");
    expect(assistantHostById("other")?.installDescription).toContain(
      "MCP settings",
    );
  });

  test("derives dest hashes and leftover mcp aliases from the host catalog", () => {
    expect(assistantDestinationHash("chatgpt")).toBe("dest-chatgpt");
    expect(assistantDestinationHash("cursor-agent")).toBe("dest-cursor-agent");
    expect(ASSISTANT_DOCS_LANDING_HASH_ALIASES).toEqual({
      "mcp-claude": "dest-claude",
      "mcp-chatgpt": "dest-chatgpt",
      "mcp-claude-code": "dest-claude-code",
      "mcp-codex": "dest-codex",
      "mcp-cursor": "dest-cursor",
    });
    expect(ASSISTANT_DOCS_LANDING_HASH_ALIASES["mcp-cursor"]).toBe(
      "dest-cursor",
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
