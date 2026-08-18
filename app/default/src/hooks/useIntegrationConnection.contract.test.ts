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
    const finishFn = SOURCE.slice(SOURCE.indexOf("async function finishOAuthPopup"));
    const finishBody = finishFn.slice(
      0,
      finishFn.indexOf("async function handleStartOAuth"),
    );
    expect(SOURCE).toContain("refetchIntegrationConnected");
    expect(SOURCE).toContain("void finishOAuthPopup()");
    expect(finishBody).toContain("oauthConnectedToastMessage(connected, label)");
    expect(finishBody).toContain("toast.success(toastMessage)");
    expect(finishBody).not.toContain("toast.success(appIsConnectedCopy");
    expect(SOURCE).not.toContain("connected successfully");
  });

  test("remembers the connect page before a same-tab fallback or account picker", () => {
    expect(SOURCE).toContain("sanitizeAuthReturnPath(returnPath)");
    expect(SOURCE).toContain("window.location.href = url");
    expect(SOURCE).toMatch(
      /rememberConnectionReturnPath\(oauthReturnPath\);\s*onFlowComplete\?/,
    );
  });
});
