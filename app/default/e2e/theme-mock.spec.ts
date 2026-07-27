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

  test("runtime stylesheet endpoint is CSS, not the SPA fallback", async ({ page }) => {
    const response = await page.request.get("/theme.css");

    expect(response.status()).toBe(200);
    expect(response.headers()["content-type"]).toContain("text/css");
    expect(await response.text()).toBe("");
  });

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
          :root { --background: rgb(1, 2, 3); }
          .dark { --background: rgb(4, 5, 6); }
        `,
      }),
    );
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowRuns(page, []);

    await page.goto("/");

    await expect
      .poll(() => page.evaluate(() => getComputedStyle(document.body).backgroundColor))
      .toBe("rgb(1, 2, 3)");

    await page.getByRole("button", { name: "Toggle theme" }).click();
    await expect
      .poll(() => page.evaluate(() => getComputedStyle(document.body).backgroundColor))
      .toBe("rgb(4, 5, 6)");
  });

  test("mounted bundle resolves runtime theme assets and router links relatively", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let fontRequested = false;
    await page.addInitScript(() => {
      localStorage.setItem("theme", "light");
    });
    await page.route("**/portal/theme.css", (route) =>
      route.fulfill({
        contentType: "text/css",
        body: `
          @font-face {
            font-family: TenantTest;
            src: url("theme/fonts/tenant-test.woff2") format("woff2");
          }
          :root {
            --background: rgb(7, 8, 9);
            --ui-font-sans: TenantTest, sans-serif;
          }
        `,
      }),
    );
    await page.route("**/portal/theme/fonts/tenant-test.woff2", (route) => {
      fontRequested = true;
      return route.fulfill({ contentType: "font/woff2", body: "" });
    });
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowRuns(page, []);

    // The real server does not inject a <base> tag. Start at a canonical
    // mounted client route to verify the relative build output still loads.
    await page.goto("/portal/apps");

    await expect
      .poll(() => page.evaluate(() => getComputedStyle(document.body).backgroundColor))
      .toBe("rgb(7, 8, 9)");
    await expect.poll(() => fontRequested).toBe(true);
    const appsLink = page.getByRole("link", { name: "Apps", exact: true });
    await expect(appsLink).toHaveAttribute(
      "href",
      "/portal/apps",
    );

    await page.getByRole("link", { name: "Dashboard", exact: true }).click();
    await expect(page).toHaveURL(/\/portal\/$/);
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
