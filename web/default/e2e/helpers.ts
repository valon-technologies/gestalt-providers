import type { Page } from "@playwright/test";

export async function authenticate(page: Page) {
  await page.goto("/login");
  await page.waitForURL((url) => url.pathname !== "/login", { timeout: 10000 });
}
