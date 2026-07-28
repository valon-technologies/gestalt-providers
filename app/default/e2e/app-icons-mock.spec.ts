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
  // Some marks nest an <svg> inside the root one, so pin the outermost.
  return page
    .locator(`[data-testid="integration-card-${app}"] [data-testid="app-mark"] svg`)
    .first();
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
      '[data-testid="integration-card-masked"] [data-testid="app-mark"] svg mask',
    );
    await expect(mask).toHaveAttribute("maskUnits", "userSpaceOnUse");
    await expect(mask).toHaveAttribute("mask-type", "alpha");
    // Geometry stays in user space, so the original numbers are preserved.
    await expect(mask).toHaveAttribute("x", "10");
    await expect(mask).toHaveAttribute("width", "80");
  });

  // Neutral fixtures that exercise each branch of the derivation rule.
  const MONOGRAM_CASES: {
    name: string;
    displayName?: string;
    expected: string;
  }[] = [
    // Two or more words → first letter of the first two.
    { name: "acmeHub", displayName: "Acme Hub", expected: "AH" },
    { name: "recordDiff", displayName: "Record Diff", expected: "RD" },
    { name: "tokenPile", displayName: "Token Pile", expected: "TP" },
    { name: "helloWorld", displayName: "Hello World", expected: "HW" },
    {
      name: "agent-trace-viewer",
      displayName: "Agent Trace Viewer",
      expected: "AT",
    },
    // A leading acronym contributes only its first letter.
    { name: "ciQueue", displayName: "CI Queue", expected: "CQ" },
    { name: "sdtPipeline", displayName: "SDT Pipeline", expected: "SP" },
    {
      name: "itAccountOnboarding",
      displayName: "IT Account Onboarding",
      expected: "IA",
    },
    { name: "vmStyleGuide", displayName: "VM Style Guide", expected: "VS" },
    // Hyphens and dots are word separators, and the result is uppercased.
    {
      name: "fieldPortal",
      displayName: "field-portal REST API",
      expected: "FP",
    },
    { name: "example-sats", displayName: "Example SATs", expected: "ES" },
    // A display name that is itself a short acronym is kept whole.
    { name: "llm", displayName: "LLM", expected: "LLM" },
    // Single word → first two letters.
    { name: "example", displayName: "Example", expected: "EX" },
    { name: "delta", displayName: "Delta", expected: "DE" },
    { name: "valkey", displayName: "Valkey", expected: "VA" },
    { name: "glinks", displayName: "GLinks", expected: "GL" },
    // No display name → fall back to the id, splitting camelCase so this does
    // not degrade to "DA".
    { name: "dataRecordExplorer", expected: "DR" },
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

  const SOLID_ICON = `<svg viewBox="0 0 192 192" xmlns="http://www.w3.org/2000/svg"><rect width="192" height="192" fill="#4838A8"/><path fill="#fff" d="M40 150h112L96 40Z"/></svg>`;
  const GLYPH_ICON = `<svg viewBox="-5.143 -5.143 34.286 34.286" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M0 0h24v24H0z"/></svg>`;
  // A deliberately faint plate is a tint, not a background.
  const TINTED_ICON = `<svg viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg"><rect width="32" height="32" rx="6" fill="currentColor" opacity="0.12"/><path fill="currentColor" d="M8 14h16v4H8z"/></svg>`;
  // A full-size raster may be a solid square or a transparent glyph, so it must
  // not be treated as full-bleed.
  const RASTER_ICON = `<svg viewBox="0 0 128 128" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"><image x="1" y="0" width="126" height="128" xlink:href="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFAAH/q842iQAAAABJRU5ErkJggg=="/></svg>`;

  test("bleeds marks that paint their own background, insets the rest", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      { name: "solid", displayName: "Solid", iconSvg: SOLID_ICON },
      { name: "glyph", displayName: "Glyph", iconSvg: GLYPH_ICON },
      { name: "tinted", displayName: "Tinted", iconSvg: TINTED_ICON },
      { name: "raster", displayName: "Raster", iconSvg: RASTER_ICON },
    ]);
    await page.goto("/apps");

    const frameOf = (app: string) =>
      page.locator(`[data-testid="integration-card-${app}"] [data-full-bleed]`);

    // Only the opaque full-viewBox rect counts.
    await expect(frameOf("solid")).toHaveCount(1);
    await expect(frameOf("glyph")).toHaveCount(0);
    await expect(frameOf("tinted")).toHaveCount(0);
    await expect(frameOf("raster")).toHaveCount(0);

    // A bled mark fills its tile; an inset one leaves the border visible.
    const svgWidth = async (app: string) => {
      const box = await page
        .locator(
          `[data-testid="integration-card-${app}"] [data-testid="app-mark"] svg`,
        )
        .first()
        .boundingBox();
      return box?.width ?? 0;
    };
    const frameWidth = async (app: string) => {
      const box = await page
        .locator(`[data-testid="integration-card-${app}"] [data-testid="app-mark"]`)
        .boundingBox();
      return box?.width ?? 0;
    };
    // Every mark stays clear of the tile's edge — a full-bleed one because its
    // glyph would otherwise collide with the rounded corners, a glyph because it
    // sits inside a visible border.
    expect(await svgWidth("solid")).toBeLessThan(await frameWidth("solid"));
    expect(await svgWidth("glyph")).toBeLessThan(await frameWidth("glyph"));

    // Inset marks are normalised on *ink*, not on element size: a mark that
    // bakes 15% padding into its viewBox is laid out larger than one cropped
    // flush to its artwork, precisely so the visible glyphs match.
    const GLYPH_PADDING = 0.15;
    expect(await svgWidth("tinted")).toBeLessThan(await svgWidth("glyph"));
    expect((await svgWidth("glyph")) * (1 - 2 * GLYPH_PADDING)).toBeCloseTo(
      await svgWidth("tinted"),
      0,
    );
  });

  // The background is the first covering opaque rect that actually paints. A rect
  // of the same size inside a clipPath defines a shape and must not be mistaken
  // for it — several real marks are built exactly this way.
  const CLIPPED_SOLID_ICON = `<svg viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="none">
    <g clip-path="url(#c)">
      <rect width="16" height="16" fill="#7A005D"/>
      <path fill="#fff" d="M4 5h8v6H4z"/>
    </g>
    <defs><clipPath id="c"><rect width="16" height="16" fill="white"/></clipPath></defs>
  </svg>`;
  // A background we cannot restate as a CSS colour, so the mark must keep
  // filling the tile rather than exposing it.
  const GRADIENT_SOLID_ICON = `<svg viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg">
    <defs><linearGradient id="g"><stop offset="0" stop-color="#000"/><stop offset="1" stop-color="#fff"/></linearGradient></defs>
    <rect width="16" height="16" fill="url(#g)"/>
    <path fill="#fff" d="M4 5h8v6H4z"/>
  </svg>`;

  test("hands a full-bleed mark's background to the tile", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      { name: "solid", displayName: "Solid", iconSvg: SOLID_ICON },
      { name: "clipped", displayName: "Clipped", iconSvg: CLIPPED_SOLID_ICON },
      { name: "gradient", displayName: "Gradient", iconSvg: GRADIENT_SOLID_ICON },
    ]);
    await page.goto("/apps");

    const markOf = (app: string) =>
      page.locator(
        `[data-testid="integration-card-${app}"] [data-testid="app-mark"]`,
      );
    const measure = (app: string) =>
      markOf(app).evaluate((node) => ({
        background: getComputedStyle(node).backgroundColor,
        markWidth: node.getBoundingClientRect().width,
        svgWidth: node.querySelector("svg")!.getBoundingClientRect().width,
      }));

    // The tile wears the colour, so the fill reaches its rounded corners while
    // the artwork sits inside a safe area.
    const solid = await measure("solid");
    expect(solid.background).toBe("rgb(72, 56, 168)");
    expect(solid.svgWidth).toBeLessThan(solid.markWidth);

    // A clipPath rect of the same size is a definition, not the background.
    const clipped = await measure("clipped");
    expect(clipped.background).toBe("rgb(122, 0, 93)");

    // A gradient cannot be handed over, so the mark keeps covering the tile
    // rather than letting it show through.
    await expect(markOf("gradient")).toHaveAttribute("data-full-bleed", "true");
    const gradient = await measure("gradient");
    expect(gradient.svgWidth).toBeCloseTo(gradient.markWidth, 0);
  });

  test("steps down a three-character monogram", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      { name: "acmeHub", displayName: "Acme Hub" },
      { name: "llm", displayName: "LLM" },
    ]);
    await page.goto("/apps");

    const monogram = (app: string) =>
      page.locator(
        `[data-testid="integration-card-${app}"] [data-testid="app-monogram"]`,
      );
    const [two, three] = await Promise.all([
      monogram("acmeHub").evaluate((n) => ({
        size: Number.parseFloat(getComputedStyle(n).fontSize),
        width: n.getBoundingClientRect().width,
      })),
      monogram("llm").evaluate((n) => ({
        size: Number.parseFloat(getComputedStyle(n).fontSize),
        width: n.getBoundingClientRect().width,
        tile: n.parentElement!.getBoundingClientRect().width,
      })),
    ]);

    expect(three.size).toBeLessThan(two.size);
    // And the point of it: three characters still clear the tile's edges.
    expect(three.width).toBeLessThan(three.tile * 0.9);
  });

  test("centres the monogram on its cap height", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      { name: "acmeHub", displayName: "Acme Hub" },
      { name: "llm", displayName: "LLM" },
    ]);
    await page.goto("/apps");

    for (const app of ["acmeHub", "llm"]) {
      const measured = await page
        .locator(
          `[data-testid="integration-card-${app}"] [data-testid="app-monogram"]`,
        )
        .evaluate((node) => {
          const style = getComputedStyle(node);
          const tile = node.parentElement!.getBoundingClientRect();
          const box = node.getBoundingClientRect();
          return {
            trim: style.getPropertyValue("text-box-trim"),
            edge: style.getPropertyValue("text-box-edge"),
            lineHeight: style.lineHeight,
            fontSize: style.fontSize,
            above: box.y - tile.y,
            below: tile.y + tile.height - (box.y + box.height),
          };
        });

      // `cn()` merges Tailwind classes, and a font-size utility displaces a
      // preceding `leading-*` while an unrecognised `text-…` name is dropped as
      // a conflict. Both happened here and neither throws, so assert the
      // computed result rather than the class list.
      expect(measured.trim, `${app}: box not trimmed`).toBe("trim-both");
      expect(measured.edge, `${app}: not trimmed to cap height`).toBe(
        "cap alphabetic",
      );
      expect(measured.lineHeight, `${app}: leading-none was dropped`).toBe(
        measured.fontSize,
      );

      // Trimmed to the caps, flex centring leaves equal space above and below.
      expect(
        Math.abs(measured.above - measured.below),
        `${app}: monogram is not vertically centred`,
      ).toBeLessThan(0.75);
      // And the trimmed box really is cap height, not the full em box.
      expect(measured.below).toBeGreaterThan(0);
    }
  });

  test("prefers a brand mark over a monogram", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      { name: "branded", displayName: "Branded App", iconSvg: MASKED_ICON },
    ]);
    await page.goto("/apps");

    const card = page.locator(
      '[data-testid="integration-card-branded"] [data-testid="app-mark"]',
    );
    await expect(card.locator("svg mask")).toBeAttached();
    await expect(card.locator('[data-testid="app-monogram"]')).toHaveCount(0);
  });

  test("falls back to the generic glyph when no letters can be derived", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [{ name: "---", displayName: "!!!" }]);
    await page.goto("/apps");

    const card = page.locator(
      '[data-testid="integration-card----"] [data-testid="app-mark"]',
    );
    await expect(card.locator('[data-testid="app-monogram"]')).toHaveCount(0);
    await expect(card.locator("svg")).toBeAttached();
  });

  test("falls back when iconSvg parses but nothing paintable survives", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      {
        name: "empty-shell",
        displayName: "Empty Shell",
        iconSvg: `<svg viewBox="0 0 10 10" xmlns="http://www.w3.org/2000/svg">
          <script>window.__pwned = true</script>
        </svg>`,
      },
    ]);
    await page.goto("/apps");

    const mark = page.locator(
      '[data-testid="integration-card-empty-shell"] [data-testid="app-mark"]',
    );
    await expect(mark.locator('[data-testid="app-monogram"]')).toHaveText("ES");
    await expect(mark.locator("svg")).toHaveCount(0);
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

    const svg = page
      .locator('[data-testid="integration-card-hostile"] [data-testid="app-mark"] svg')
      .first();
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
