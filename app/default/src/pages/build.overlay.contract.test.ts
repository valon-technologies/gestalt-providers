import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "build.tsx"),
  "utf8",
);

/**
 * Setup Connect/Try tiles must wait for the connection overlay, same as /apps.
 * Catalog schema alone must not invent Connect / Connected chrome.
 */
describe("Setup overlay contract", () => {
  test("Connect and Try cards pass overlayKnown into connectionStatusKnown", () => {
    expect(SOURCE).toContain("connectionOverlayKnown");
    expect(SOURCE).toContain("CONNECTION_STATUS_UNAVAILABLE");
    expect(SOURCE).toContain("refetchDirectory");
    expect(SOURCE).toContain("refetchOverlay");
    expect(SOURCE).toContain("connectionStatusKnown={overlayKnown}");
    const cardOpens = SOURCE.match(/<IntegrationCard\b[\s\S]*?\/>/g) ?? [];
    expect(cardOpens.length).toBeGreaterThan(0);
    for (const card of cardOpens) {
      expect(card).toContain("connectionStatusKnown={overlayKnown}");
    }
  });
});
