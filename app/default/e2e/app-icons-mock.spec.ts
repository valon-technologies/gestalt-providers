import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { test, expect, mockIntegrations } from "./fixtures";
import type { Integration } from "../src/lib/api";

/**
 * Guards the SVG sanitizer against silently blanking shipped brand marks.
 *
 * A dropped attribute does not throw — it renders an empty or clipped icon, so
 * only a test that puts every real `app/*\/assets/icon.svg` through the actual
 * render path catches it. Google Drive shipped invisible because `maskUnits` and
 * `mask-type` were not allowlisted.
 */

const APPS_DIR = join(import.meta.dirname, "..", "..", "..", "app");

type ShippedIcon = { app: string; svg: string };

function loadShippedIcons(): ShippedIcon[] {
  const icons: ShippedIcon[] = [];
  for (const app of readdirSync(APPS_DIR, { withFileTypes: true })) {
    if (!app.isDirectory()) continue;
    let manifest: string;
    try {
      manifest = readFileSync(join(APPS_DIR, app.name, "manifest.yaml"), "utf8");
    } catch {
      continue;
    }
    const iconFile = /^iconFile:\s*(\S+)\s*$/m.exec(manifest)?.[1];
    if (!iconFile) continue;
    icons.push({
      app: app.name,
      svg: readFileSync(join(APPS_DIR, app.name, iconFile), "utf8"),
    });
  }
  return icons.sort((a, b) => a.app.localeCompare(b.app));
}

const SHIPPED_ICONS = loadShippedIcons();

// A bare `<mask>` with no maskUnits/mask-type is exactly the Google Drive shape:
// userSpaceOnUse geometry plus alpha masking, both expressed the way design
// tools emit them.
const MASKED_ICON = `<svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg" fill="none">
  <mask id="m" width="80" height="80" x="10" y="10" maskUnits="userSpaceOnUse" style="mask-type:alpha">
    <path fill="#b43333" d="M10 10h80v80H10z"/>
  </mask>
  <g mask="url(#m)"><rect width="100" height="100" fill="#0ebc5f"/></g>
</svg>`;

function iconIn(page: import("@playwright/test").Page, app: string) {
  return page.locator(`[data-testid="integration-card-${app}"] svg`).first();
}

test.describe("app registry icons", () => {
  test("every shipped brand mark renders", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    expect(
      SHIPPED_ICONS.length,
      "expected to discover shipped icons on disk",
    ).toBeGreaterThan(20);

    const integrations: Integration[] = SHIPPED_ICONS.map(({ app, svg }) => ({
      name: app,
      displayName: app,
      iconSvg: svg,
    }));
    await mockIntegrations(page, integrations);
    await page.goto("/apps");

    for (const { app, svg } of SHIPPED_ICONS) {
      const icon = iconIn(page, app);
      await expect(icon, `${app}: icon should render`).toBeAttached();

      // Painted content must survive sanitization, not just the <svg> shell.
      // Some marks are favicon-generator wrappers around a single base64
      // <image> rather than vector geometry, so that counts too.
      const rendered = await icon.evaluate(
        (node) =>
          node.querySelectorAll(
            "path, rect, circle, ellipse, polygon, polyline, image, use",
          ).length,
      );
      expect(rendered, `${app}: no shapes survived the sanitizer`).toBeGreaterThan(0);

      // And it must occupy space — a collapsed mask region yields a zero box.
      const box = await icon.boundingBox();
      expect(box?.width ?? 0, `${app}: icon collapsed to zero width`).toBeGreaterThan(0);
      expect(box?.height ?? 0, `${app}: icon collapsed to zero height`).toBeGreaterThan(0);

      // Masks only work if their coordinate space came through with them.
      if (/<mask\b/.test(svg)) {
        const masks = await icon.evaluate((node) =>
          Array.from(node.querySelectorAll("mask")).map((mask) => ({
            maskUnits: mask.getAttribute("maskUnits"),
            maskType:
              mask.getAttribute("mask-type") ??
              (mask as SVGElement).style?.maskType ??
              null,
          })),
        );
        expect(masks.length, `${app}: <mask> was dropped entirely`).toBeGreaterThan(0);
        for (const mask of masks) {
          if (/maskUnits=/.test(svg)) {
            expect(
              mask.maskUnits,
              `${app}: maskUnits dropped — geometry reinterpreted as objectBoundingBox`,
            ).toBe("userSpaceOnUse");
          }
          if (/mask-type\s*:\s*alpha/.test(svg)) {
            expect(
              mask.maskType,
              `${app}: mask-type dropped — falls back to luminance masking`,
            ).toBe("alpha");
          }
        }
      }
    }
  });

  test("carries maskUnits and mask-type through sanitization", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      { name: "masked", displayName: "Masked", iconSvg: MASKED_ICON },
    ]);
    await page.goto("/apps");

    const mask = page.locator(
      '[data-testid="integration-card-masked"] svg mask',
    );
    await expect(mask).toHaveAttribute("maskUnits", "userSpaceOnUse");
    await expect(mask).toHaveAttribute("mask-type", "alpha");
    // Geometry stays in user space, so the original numbers are preserved.
    await expect(mask).toHaveAttribute("x", "10");
    await expect(mask).toHaveAttribute("width", "80");
  });

  // Drawn from the real icon-less apps in the live registry, plus the shapes
  // that decide each branch of the rule.
  const MONOGRAM_CASES: {
    name: string;
    displayName?: string;
    expected: string;
  }[] = [
    // Two or more words → first letter of the first two.
    { name: "dealHub", displayName: "Deal Hub", expected: "DH" },
    { name: "entityDiff", displayName: "Entity Diff", expected: "ED" },
    { name: "tokenPile", displayName: "Token Pile", expected: "TP" },
    { name: "helloWorld", displayName: "Hello World", expected: "HW" },
    {
      name: "agent-trace-viewer",
      displayName: "Agent Trace Viewer",
      expected: "AT",
    },
    // A leading acronym contributes only its first letter.
    { name: "ciWorkqueue", displayName: "CI Workqueue", expected: "CW" },
    { name: "sdtPipeline", displayName: "SDT Pipeline", expected: "SP" },
    {
      name: "itAccountOnboarding",
      displayName: "IT Account Onboarding",
      expected: "IA",
    },
    { name: "vmStyleGuide", displayName: "VM Style Guide", expected: "VS" },
    // Hyphens and dots are word separators, and the result is uppercased.
    {
      name: "frontPorch",
      displayName: "front-porch Internal GraphQL",
      expected: "FP",
    },
    { name: "valon-sats", displayName: "Valon SATs", expected: "VS" },
    // A display name that is itself a short acronym is kept whole.
    { name: "llm", displayName: "LLM", expected: "LLM" },
    // Single word → first two letters.
    { name: "jarvis", displayName: "Jarvis", expected: "JA" },
    { name: "delta", displayName: "Delta", expected: "DE" },
    { name: "valkey", displayName: "Valkey", expected: "VA" },
    { name: "glinks", displayName: "GLinks", expected: "GL" },
    // No display name → fall back to the id, splitting camelCase so this does
    // not degrade to "DA".
    { name: "dataSchemaExplorer", expected: "DS" },
    { name: "oncall", expected: "ON" },
  ];

  test("derives a display-font monogram for apps with no brand mark", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(
      page,
      MONOGRAM_CASES.map(({ name, displayName }) => ({
        name,
        ...(displayName ? { displayName } : {}),
      })),
    );
    await page.goto("/apps");

    for (const { name, expected } of MONOGRAM_CASES) {
      const monogram = page.locator(
        `[data-testid="integration-card-${name}"] [data-testid="app-monogram"]`,
      );
      await expect(monogram, `${name}: monogram should render`).toHaveText(
        expected,
      );
    }

    // The monogram uses the display face, not the body face.
    const first = page.locator('[data-testid="app-monogram"]').first();
    const [monogramFont, bodyFont] = await Promise.all([
      first.evaluate((node) => getComputedStyle(node).fontFamily),
      page.evaluate(() => getComputedStyle(document.body).fontFamily),
    ]);
    expect(monogramFont).not.toBe(bodyFont);
    expect(
      monogramFont,
      "monogram should resolve to the theme's display family",
    ).toBe(
      await page.evaluate(() =>
        getComputedStyle(document.documentElement)
          .getPropertyValue("--ui-font-display")
          .trim(),
      ),
    );
  });

  test("prefers a brand mark over a monogram", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      { name: "branded", displayName: "Branded App", iconSvg: MASKED_ICON },
    ]);
    await page.goto("/apps");

    const card = page.locator('[data-testid="integration-card-branded"]');
    await expect(card.locator("svg mask")).toBeAttached();
    await expect(card.locator('[data-testid="app-monogram"]')).toHaveCount(0);
  });

  test("falls back to the generic glyph when no letters can be derived", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [{ name: "---", displayName: "!!!" }]);
    await page.goto("/apps");

    const card = page.locator('[data-testid="integration-card----"]');
    await expect(card.locator('[data-testid="app-monogram"]')).toHaveCount(0);
    await expect(card.locator("svg")).toBeAttached();
  });

  test("still strips style and unsafe elements", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      {
        name: "hostile",
        displayName: "Hostile",
        iconSvg: `<svg viewBox="0 0 10 10" xmlns="http://www.w3.org/2000/svg">
          <script>window.__pwned = true</script>
          <foreignObject><div>nope</div></foreignObject>
          <path d="M0 0h10v10H0z" style="mask-type:alpha;fill:red" onclick="window.__pwned = true"/>
        </svg>`,
      },
    ]);
    await page.goto("/apps");

    const svg = page.locator('[data-testid="integration-card-hostile"] svg');
    await expect(svg).toBeAttached();
    // mask-type is lifted out of style; nothing else from style is, and the
    // style attribute itself never lands.
    const path = svg.locator("path");
    await expect(path).toHaveAttribute("mask-type", "alpha");
    expect(await path.evaluate((node) => node.getAttribute("style"))).toBeNull();
    expect(await path.evaluate((node) => node.getAttribute("onclick"))).toBeNull();
    expect(await svg.evaluate((node) => node.querySelectorAll("script").length)).toBe(0);
    expect(
      await svg.evaluate((node) => node.querySelectorAll("foreignObject").length),
    ).toBe(0);
    expect(await page.evaluate(() => (window as never as { __pwned?: boolean }).__pwned)).toBeUndefined();
  });
});
