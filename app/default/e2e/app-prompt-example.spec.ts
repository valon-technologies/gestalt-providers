import type { Integration } from "../src/lib/api";
import { expect, mockIntegrations, test } from "./fixtures";

const APP = "example-app";
const DISPLAY_NAME = "Example App";
const CONFIGURED_PROMPT = "Summarize my open work and identify the next action.";
const GENERIC_MCP_PROMPT = "What can you help me with in this workspace?";

function integration(overrides: Partial<Integration> = {}): Integration {
  return {
    name: APP,
    displayName: DISPLAY_NAME,
    description: "A neutral example integration.",
    status: "ready",
    credentialState: "connected",
    healthState: "healthy",
    ...overrides,
  };
}

test.describe("App prompt example", () => {
  test("renders a configured prompt without MCP metadata", async ({
    authenticatedPage: page,
  }) => {
    await mockIntegrations(page, [
      integration({
        prompts: [{ id: "open-work", text: CONFIGURED_PROMPT }],
      }),
    ]);

    await page.goto(`/apps/${APP}`);

    const stage = page.getByTestId("app-prompt-example");
    await expect(stage).toContainText(
      `@${DISPLAY_NAME} ${CONFIGURED_PROMPT}`,
    );
    await expect(stage).toHaveCSS("background-image", /radial-gradient/);
  });

  test("renders the generic fallback for an MCP app", async ({
    authenticatedPage: page,
  }) => {
    await mockIntegrations(page, [
      integration({
        connections: [{ name: "mcp", mcpPassthrough: true }],
      }),
    ]);

    await page.goto(`/apps/${APP}`);

    await expect(page.getByTestId("app-prompt-example")).toContainText(
      `@${DISPLAY_NAME} ${GENERIC_MCP_PROMPT}`,
    );
  });

  test("hides the fallback for an app without an MCP surface", async ({
    authenticatedPage: page,
  }) => {
    await mockIntegrations(page, [integration()]);

    await page.goto(`/apps/${APP}`);

    await expect(page.getByTestId("app-prompt-example")).toHaveCount(0);
  });

  test("copies the complete configured prompt", async ({
    authenticatedPage: page,
  }) => {
    await mockIntegrations(page, [
      integration({
        prompts: [{ id: "open-work", text: CONFIGURED_PROMPT }],
      }),
    ]);
    await page.goto(`/apps/${APP}`);
    await page.context().grantPermissions(["clipboard-read", "clipboard-write"], {
      origin: new URL(page.url()).origin,
    });

    await page.getByRole("button", { name: "Copy example prompt" }).click();

    await expect(page.getByRole("button", { name: "Copied prompt" })).toBeVisible();
    await expect(page.getByRole("status")).toHaveText(
      "Prompt copied. Paste it into your AI client.",
    );
    await expect
      .poll(() => page.evaluate(() => navigator.clipboard.readText()))
      .toBe(`@${DISPLAY_NAME} ${CONFIGURED_PROMPT}`);
  });

  test("shows a visible retry state when copying fails", async ({
    authenticatedPage: page,
  }) => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, "clipboard", {
        configurable: true,
        value: {
          writeText: () => Promise.reject(new Error("clipboard unavailable")),
        },
      });
    });
    await mockIntegrations(page, [
      integration({
        prompts: [{ id: "open-work", text: CONFIGURED_PROMPT }],
      }),
    ]);
    await page.goto(`/apps/${APP}`);

    await page.getByRole("button", { name: "Copy example prompt" }).click();

    await expect(
      page.getByRole("button", { name: "Retry copying example prompt" }),
    ).toBeVisible();
    await expect(page.getByRole("status")).toHaveText(
      "Couldn’t copy the prompt. Try again.",
    );
  });
});
