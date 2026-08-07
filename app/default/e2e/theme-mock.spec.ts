import {
  test,
  expect,
  mockIntegrations,
  mockTokens,
} from "./fixtures";
import {
  accountMenuThemeControl,
  openAccountMenuThemeSection,
} from "./account-menu";

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

    await page.goto("/settings/tokens/new");

    const body = page.locator("body");
    const heading = page.getByRole("heading", { name: "Create token", exact: true });
    const bodyCopy = page.getByText(
      /Name the token, choose an expiration/,
    );
    const primaryButton = page.getByRole("button", { name: "Create token" });

    await expect(heading).toBeVisible();
    await expect(primaryButton).toBeVisible();

    async function expectSemanticColors(colors: {
      background: string;
      foreground: string;
      mutedForeground: string;
      primary: string;
      primaryForeground: string;
    }) {
      await expect(body).toHaveCSS("background-color", colors.background);
      await expect(body).toHaveCSS("color", colors.foreground);
      await expect(heading).toHaveCSS("color", colors.foreground);
      await expect(bodyCopy).toHaveCSS("color", colors.mutedForeground);
      await expect(primaryButton).toHaveCSS("background-color", colors.primary);
      await expect(primaryButton).toHaveCSS("color", colors.primaryForeground);
    }

    await expectSemanticColors({
      background: "rgb(1, 2, 3)",
      foreground: "rgb(4, 5, 6)",
      mutedForeground: "rgb(16, 17, 18)",
      primary: "rgb(19, 20, 21)",
      primaryForeground: "rgb(22, 23, 24)",
    });

    await openAccountMenuThemeSection(page);
    await accountMenuThemeControl(page).getByRole("radio", { name: "Dark" }).click();
    await page.keyboard.press("Escape");
    await expect(accountMenuThemeControl(page)).toHaveCount(0);
    await expectSemanticColors({
      background: "rgb(25, 26, 27)",
      foreground: "rgb(28, 29, 30)",
      mutedForeground: "rgb(40, 41, 42)",
      primary: "rgb(43, 44, 45)",
      primaryForeground: "rgb(46, 47, 48)",
    });
  });

  test("settings tokens subhead never uses the muted surface fill as text color", async ({
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
            --background: rgb(255, 255, 255);
            --foreground: rgb(35, 24, 16);
            --card: rgb(250, 248, 245);
            --card-foreground: rgb(35, 24, 16);
            --border: rgb(230, 225, 218);
            --muted: rgb(241, 238, 233);
            --primary: rgb(122, 79, 16);
            --primary-foreground: rgb(255, 255, 255);
          }
        `,
      }),
    );
    await mockIntegrations(page, []);
    await mockTokens(page, []);

    await page.goto("/settings/tokens");

    const bodyCopy = page.getByText(
      /Personal tokens for scripts, local tooling/,
    );
    await expect(bodyCopy).toHaveClass(/text-muted-foreground/);
    await expect(bodyCopy).not.toHaveCSS("color", "rgb(241, 238, 233)");
  });

  test("theme switcher enables dark mode and persists the selection", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await page.addInitScript(() => {
      localStorage.setItem("theme", "light");
    });
    await mockIntegrations(page, []);
    await mockTokens(page, []);

    await page.goto("/apps");

    await openAccountMenuThemeSection(page);
    const theme = accountMenuThemeControl(page);
    await expect(theme.getByRole("radio", { name: "Light" })).toHaveAttribute(
      "aria-checked",
      "true",
    );
    await theme.getByRole("radio", { name: "Dark" }).click();

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
