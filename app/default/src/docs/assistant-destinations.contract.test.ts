import { describe, expect, it } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  ASSISTANT_DESTINATION_SWITCHER_LABEL,
  assistantDestinationIds,
  assistantDestinationMedia,
  assistantDestinationTabs,
  defaultAssistantDestinationId,
} from "./assistant-destinations";

const HERE = dirname(fileURLToPath(import.meta.url));

describe("assistant destination switcher", () => {
  it("leads with Claude and ChatGPT as saved-settings destinations", () => {
    expect(assistantDestinationTabs.map((tab) => tab.label)).toEqual([
      "Claude",
      "ChatGPT",
      "Cursor",
    ]);
    expect(assistantDestinationIds).toEqual([
      "dest-claude",
      "dest-chatgpt",
      "dest-cursor",
    ]);
    expect(defaultAssistantDestinationId).toBe("dest-claude");
    expect(ASSISTANT_DESTINATION_SWITCHER_LABEL).toBe("Choose your assistant");
  });

  it("keeps destination hashes distinct from config-file recipes", () => {
    for (const id of assistantDestinationIds) {
      expect(id.startsWith("dest-")).toBe(true);
      expect(id.startsWith("mcp-")).toBe(false);
    }
  });

  it("ships a walkthrough video for Claude and ChatGPT", () => {
    expect(assistantDestinationMedia["dest-claude"].video).toBe(
      "/docs/add-token-claude.mp4",
    );
    expect(assistantDestinationMedia["dest-chatgpt"].video).toBe(
      "/docs/add-token-chatgpt.mp4",
    );
    const publicDocs = join(HERE, "../../public/docs");
    expect(
      existsSync(join(publicDocs, "add-token-claude.mp4")),
    ).toBe(true);
    expect(
      existsSync(join(publicDocs, "add-token-chatgpt.mp4")),
    ).toBe(true);
    expect(
      existsSync(join(publicDocs, "add-token-claude.jpg")),
    ).toBe(true);
    expect(
      existsSync(join(publicDocs, "add-token-chatgpt.jpg")),
    ).toBe(true);
  });

  it("tells Claude to use Request headers and ChatGPT to use a Token field", () => {
    const source = readFileSync(
      join(HERE, "AssistantDestinationSwitcher.tsx"),
      "utf8",
    );
    expect(source).toContain("Request headers");
    expect(source).toContain("authorization");
    expect(source).toContain("Developer mode");
    expect(source).toContain("Token or API key");
    expect(source).toContain("Create a token here");
    expect(source).toContain("Then go here");
    expect(source).toContain("Place the token here");
    expect(source).not.toMatch(/[\u2013\u2014]/);
  });
});
