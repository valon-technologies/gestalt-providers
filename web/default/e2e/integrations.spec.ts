import { test, expect } from "@playwright/test";
import { authenticate } from "./helpers";

test.describe("Integrations", () => {
  test.beforeEach(async ({ page }) => {
    await authenticate(page);
  });

  test("shows empty state when no integrations configured", async ({
    page,
  }) => {
    await page.goto("/integrations");
    await expect(
      page.getByRole("heading", { name: "Integrations" }),
    ).toBeVisible();
    await expect(page.getByText(/no integrations/i)).toBeVisible();
  });
});
