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
  await page.goto("/apps");
  await page.getByRole("heading", { name: "Apps" }).waitFor({ timeout: 10000 });
}
