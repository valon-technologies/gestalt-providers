import {
  test,
  expect,
  mockIntegrations,
  mockTokens,
  mockWorkflowRuns,
} from "./fixtures";

const hasBackend =
  !!process.env.PLAYWRIGHT_BASE_URL || !!process.env.GESTALT_BASE_URL;

test.describe("Theme", () => {
  test.skip(
    hasBackend,
    "Theme mock tests use mocked routes and do not apply when running against a real server",
  );

  test("runtime stylesheet overrides semantic defaults in light and dark mode", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await page.addInitScript(() => {
      localStorage.setItem("theme", "light");
    });
    await page.route("**/theme.css", (route) =>
      route.fulfill({
        contentType: "text/css",
        body: `
          :root {
            --background: rgb(1, 2, 3);
            --foreground: rgb(4, 5, 6);
            --card: rgb(7, 8, 9);
            --card-foreground: rgb(10, 11, 12);
            --border: rgb(13, 14, 15);
            --muted-foreground: rgb(16, 17, 18);
            --primary: rgb(19, 20, 21);
            --primary-foreground: rgb(22, 23, 24);
          }
          .dark {
            --background: rgb(25, 26, 27);
            --foreground: rgb(28, 29, 30);
            --card: rgb(31, 32, 33);
            --card-foreground: rgb(34, 35, 36);
            --border: rgb(37, 38, 39);
            --muted-foreground: rgb(40, 41, 42);
            --primary: rgb(43, 44, 45);
            --primary-foreground: rgb(46, 47, 48);
          }
        `,
      }),
    );
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowRuns(page, []);

    await page.goto("/authorization");

    const body = page.locator("body");
    const heading = page.getByRole("heading", {
      name: "Authorization",
      exact: true,
    });
    const bodyCopy = page.getByText(
      /Create personal API tokens for local tooling/,
    );
    const card = page.locator("#tokens");
    const primaryButton = page.getByRole("button", { name: "Create Token" });

    async function expectSemanticColors(colors: {
      background: string;
      foreground: string;
      card: string;
      cardForeground: string;
      border: string;
      mutedForeground: string;
      primary: string;
      primaryForeground: string;
    }) {
      await expect(body).toHaveCSS("background-color", colors.background);
      await expect(body).toHaveCSS("color", colors.foreground);
      await expect(heading).toHaveCSS("color", colors.foreground);
      await expect(bodyCopy).toHaveCSS("color", colors.mutedForeground);
      await expect(card).toHaveCSS("background-color", colors.card);
      await expect(card).toHaveCSS("color", colors.cardForeground);
      await expect(card).toHaveCSS("border-color", colors.border);
      await expect(primaryButton).toHaveCSS("background-color", colors.primary);
      await expect(primaryButton).toHaveCSS("color", colors.primaryForeground);
    }

    await expectSemanticColors({
      background: "rgb(1, 2, 3)",
      foreground: "rgb(4, 5, 6)",
      card: "rgb(7, 8, 9)",
      cardForeground: "rgb(10, 11, 12)",
      border: "rgb(13, 14, 15)",
      mutedForeground: "rgb(16, 17, 18)",
      primary: "rgb(19, 20, 21)",
      primaryForeground: "rgb(22, 23, 24)",
    });

    await page.getByRole("button", { name: "Toggle theme" }).click();
    await expectSemanticColors({
      background: "rgb(25, 26, 27)",
      foreground: "rgb(28, 29, 30)",
      card: "rgb(31, 32, 33)",
      cardForeground: "rgb(34, 35, 36)",
      border: "rgb(37, 38, 39)",
      mutedForeground: "rgb(40, 41, 42)",
      primary: "rgb(43, 44, 45)",
      primaryForeground: "rgb(46, 47, 48)",
    });
  });

  test("toggle enables dark mode and persists the selection", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await page.addInitScript(() => {
      localStorage.setItem("theme", "light");
    });
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowRuns(page, []);

    await page.goto("/");

    const toggle = page.getByRole("button", { name: "Toggle theme" });
    await expect(toggle).toHaveAttribute("title", "Light mode");
    await toggle.click();

    await expect
      .poll(async () => page.evaluate(() => localStorage.getItem("theme")))
      .toBe("dark");
    await expect
      .poll(async () =>
        page.evaluate(() =>
          document.documentElement.classList.contains("dark"),
        ),
      )
      .toBe(true);
  });
});
