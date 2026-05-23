import { test, expect } from "@playwright/test";
import { authenticate } from "./helpers";

test.describe("Integrations", () => {
  test.beforeEach(async ({ page }) => {
    await authenticate(page);
  });

  test("shows integrations catalog", async ({ page }) => {
    await page.goto("/integrations");
    await expect(
      page.getByRole("heading", { name: "Plugins" }),
    ).toBeVisible();
    await expect(
      page.getByRole("combobox", { name: "Search plugins" }),
    ).toBeVisible();
  });
});
