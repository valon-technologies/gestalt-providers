import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "useIntegrationConnection.ts"),
  "utf8",
);

describe("useIntegrationConnection OAuth", () => {
  test("opens a sign-in window before requesting the OAuth URL", () => {
    expect(SOURCE).toContain("const popup = openOAuthPopup()");
    expect(SOURCE).toMatch(
      /const popup = openOAuthPopup\(\);\s*setLoading\(true\)/,
    );
    expect(SOURCE).toContain("navigateOAuthPopup(popup, url)");
    expect(SOURCE).toContain("watchOAuthPopup(popup");
  });

  test("toasts OAuth success only after the catalog shows the app connected", () => {
    expect(SOURCE).toContain("refetchIntegrationConnected");
    expect(SOURCE).toContain("void finishOAuthPopup()");
    expect(SOURCE).toContain(
      "toast.success(`${label} connected successfully.`)",
    );
  });

  test("remembers the connect page before a same-tab fallback or account picker", () => {
    expect(SOURCE).toContain("rememberConnectionReturnPath(returnPath)");
    expect(SOURCE).toContain("window.location.href = url");
    expect(SOURCE).toMatch(
      /rememberConnectionReturnPath\(returnPath\);\s*onFlowComplete\?/,
    );
  });
});
