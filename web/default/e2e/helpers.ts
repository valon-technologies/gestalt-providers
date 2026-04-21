import type { Page } from "@playwright/test";

export async function authenticate(page: Page) {
  await page.addInitScript(() => {
    localStorage.setItem("user_email", "test@gestalt.dev");
  });
  await page.goto("/");
  await page.waitForURL((url) => url.pathname !== "/login", { timeout: 10000 });
}
