/** Driver for record-scap.sh — opens theme switcher and toggles preview modes. */
export default async function run({ page }) {
  await page.getByRole("button", { name: /open theme switcher/i }).click();
  await page.waitForTimeout(800);
  await page.getByRole("menuitemradio", { name: /default theme/i }).click();
  await page.waitForTimeout(1200);
  await page.getByRole("button", { name: /open theme switcher/i }).click();
  await page.waitForTimeout(400);
  await page.getByRole("menuitemradio", { name: /tenant theme/i }).click();
  await page.waitForTimeout(1200);
}
