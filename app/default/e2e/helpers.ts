import type { Page } from "@playwright/test";
import { mockAuthSession } from "./fixtures";

export async function authenticate(page: Page) {
  await page.addInitScript(() => {
    localStorage.setItem(
      "gestalt.auth.session",
      JSON.stringify({
        subjectId: "user:test@gestalt.dev",
        email: "test@gestalt.dev",
      }),
    );
  });
  await mockAuthSession(page);
  await page.goto("/");
  await page.getByRole("heading", { name: "Dashboard" }).waitFor({ timeout: 10000 });
}
