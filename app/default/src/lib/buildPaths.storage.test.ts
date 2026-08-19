// @vitest-environment happy-dom
import { afterEach, describe, expect, test } from "vitest";
import {
  BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY,
  BUILD_CREATE_NEW_TOKEN_ID,
  BUILD_INSTALL_AGENT_STORAGE_KEY,
  BUILD_SELECTED_TOKEN_ID_STORAGE_KEY,
  MCP_INSTALLED_AGENTS_STORAGE_KEY,
  MCP_INSTALLED_STORAGE_KEY,
  readMcpInstalledAgents,
  readStoredApiTokenGrantId,
  writeMcpInstalledAgents,
  writeStoredApiTokenGrantId,
} from "./buildPaths";

describe("readStoredApiTokenGrantId", () => {
  afterEach(() => {
    sessionStorage.clear();
  });

  test("copies a leftover selected grant id into the grant key", () => {
    sessionStorage.setItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY, "tok_legacy");
    expect(readStoredApiTokenGrantId()).toBe("tok_legacy");
    expect(sessionStorage.getItem(BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY)).toBe(
      "tok_legacy",
    );
    expect(sessionStorage.getItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY)).toBeNull();
  });

  test("prefers the grant key over a leftover selected id", () => {
    sessionStorage.setItem(BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY, "tok_grant");
    sessionStorage.setItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY, "tok_old");
    expect(readStoredApiTokenGrantId()).toBe("tok_grant");
    expect(sessionStorage.getItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY)).toBeNull();
  });

  test("ignores leftover radio sentinels", () => {
    sessionStorage.setItem(
      BUILD_SELECTED_TOKEN_ID_STORAGE_KEY,
      BUILD_CREATE_NEW_TOKEN_ID,
    );
    expect(readStoredApiTokenGrantId()).toBe("");
  });

  test("writeStoredApiTokenGrantId drops the leftover selected key", () => {
    sessionStorage.setItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY, "tok_old");
    writeStoredApiTokenGrantId("tok_new");
    expect(sessionStorage.getItem(BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY)).toBe(
      "tok_new",
    );
    expect(sessionStorage.getItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY)).toBeNull();
  });
});

describe("readMcpInstalledAgents", () => {
  afterEach(() => {
    sessionStorage.clear();
  });

  test("reads the per-assistant list", () => {
    sessionStorage.setItem(
      MCP_INSTALLED_AGENTS_STORAGE_KEY,
      JSON.stringify(["cursor", "codex"]),
    );
    expect(readMcpInstalledAgents()).toEqual(["cursor", "codex"]);
  });

  test("migrates the legacy boolean flag using the stored assistant", () => {
    sessionStorage.setItem(MCP_INSTALLED_STORAGE_KEY, "1");
    sessionStorage.setItem(BUILD_INSTALL_AGENT_STORAGE_KEY, "cursor");
    expect(readMcpInstalledAgents()).toEqual(["cursor"]);
  });

  test("legacy flag without a stored assistant is empty", () => {
    sessionStorage.setItem(MCP_INSTALLED_STORAGE_KEY, "1");
    expect(readMcpInstalledAgents()).toEqual([]);
  });

  test("prefers the per-assistant list over the legacy flag", () => {
    sessionStorage.setItem(
      MCP_INSTALLED_AGENTS_STORAGE_KEY,
      JSON.stringify(["codex"]),
    );
    sessionStorage.setItem(MCP_INSTALLED_STORAGE_KEY, "1");
    sessionStorage.setItem(BUILD_INSTALL_AGENT_STORAGE_KEY, "cursor");
    expect(readMcpInstalledAgents()).toEqual(["codex"]);
  });

  test("writeMcpInstalledAgents drops the legacy flag", () => {
    sessionStorage.setItem(MCP_INSTALLED_STORAGE_KEY, "1");
    writeMcpInstalledAgents(["cursor"]);
    expect(sessionStorage.getItem(MCP_INSTALLED_STORAGE_KEY)).toBeNull();
    expect(
      JSON.parse(sessionStorage.getItem(MCP_INSTALLED_AGENTS_STORAGE_KEY)!),
    ).toEqual(["cursor"]);
  });
});
