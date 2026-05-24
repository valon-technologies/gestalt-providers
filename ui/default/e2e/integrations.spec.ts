import { test, expect } from "@playwright/test";
import { authenticate } from "./helpers";

test.describe("Integrations", () => {
  test.beforeEach(async ({ page }) => {
    await authenticate(page);
  });

  test("shows apps catalog", async ({ page }) => {
    await page.goto("/apps");
    await expect(
      page.getByRole("heading", { name: "Apps" }),
    ).toBeVisible();
    await expect(
      page.getByRole("combobox", { name: "Search apps" }),
    ).toBeVisible();
  });
});
