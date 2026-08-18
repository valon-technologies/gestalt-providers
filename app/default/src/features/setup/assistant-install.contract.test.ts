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
  test("persistent token and overlap callouts are not live regions", () => {
    expect(SOURCE).toContain(
      '<Callout variant="info" data-testid="build-install-token-needed">',
    );
    expect(SOURCE).toContain(
      '<Callout variant="info" data-testid="setup-overlap-callout">',
    );
    expect(SOURCE).not.toContain("live={false}");
    expect(SOURCE).toContain('from "@/components/ui/alert"');
    expect(SOURCE).not.toContain("@/components/Callout");
  });

  test("install recipes require a session-minted token", () => {
    expect(SOURCE).not.toContain("gst_api_YOUR_TOKEN");
    expect(SOURCE).toContain("HOST_INSTALL_RECIPES[agent]");
    expect(SOURCE).toContain("!hasMcpCredential");
  });

  test("every assistant host has an install recipe", () => {
    expect(Object.keys(HOST_INSTALL_RECIPES).sort()).toEqual(
      ASSISTANT_HOSTS.map((host) => host.id).sort(),
    );
  });
});
