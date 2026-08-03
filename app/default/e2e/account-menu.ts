import type { Page } from "@playwright/test";
import { expect } from "@playwright/test";
import { ACCOUNT_MENU_THEME_SECTION_LABEL } from "../src/components/AccountMenu";

/** Open the signed-in account flyout (theme + utilities live here). */
export async function openAccountMenu(page: Page): Promise<void> {
  await page.getByRole("button", { name: "Open user menu" }).click();
}

/** Theme radiogroup inside the open account menu (named by the Theme section). */
export function accountMenuThemeControl(page: Page) {
  return page.getByRole("radiogroup", { name: ACCOUNT_MENU_THEME_SECTION_LABEL });
}

/** Open the account menu and wait until the theme control is available. */
export async function openAccountMenuThemeSection(page: Page): Promise<void> {
  await openAccountMenu(page);
  await expect(accountMenuThemeControl(page)).toBeVisible();
}
