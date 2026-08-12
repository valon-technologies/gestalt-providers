/**
 * Platform product identity for the home shell chrome.
 *
 * Deployment-owned (serve-time `/brand.json` + optional index injection).
 * Default when unconfigured: Gestalt (the open-source framework name).
 * Do not hardcode tenant names here — see THEMING.md / theme-boundary.md.
 */

import { appPath } from "@/lib/mount";

export const PLATFORM_BRAND_SCRIPT_ID = "gestalt-platform-brand";
/** Absolute (app-base) path so nested SPA routes do not fetch `/apps/brand.json`. */
export const PLATFORM_BRAND_JSON_PATH = appPath("/brand.json");
export const DEFAULT_PLATFORM_BRAND_NAME = "Gestalt";

export type PlatformBrand = {
  /** Product display name in chrome + document.title suffix. */
  name: string;
  /**
   * Site-root absolute mark URL (e.g. `/theme/mark.svg`), when configured.
   * Relative values from older servers are normalized against the app base.
   */
  markSrc?: string;
};

function normalizeMarkSrc(markSrc: string): string | undefined {
  const trimmed = markSrc.trim();
  if (!trimmed) return undefined;
  if (/^(https?:|data:|blob:)/i.test(trimmed)) return trimmed;
  if (trimmed.startsWith("/")) return trimmed;
  return appPath(`/${trimmed}`);
}

type PlatformBrandWindow = Window & {
  __GESTALT_PLATFORM_BRAND__?: PlatformBrand | null;
};

function readInjectedBrand(): PlatformBrand | null {
  if (typeof document === "undefined") return null;
  const el = document.getElementById(PLATFORM_BRAND_SCRIPT_ID);
  if (!el) return null;
  const raw = el.textContent?.trim();
  if (!raw || raw === "{}") return null;
  try {
    return normalizeBrand(JSON.parse(raw) as Partial<PlatformBrand>);
  } catch {
    return null;
  }
}

function readWindowBrand(): PlatformBrand | null {
  if (typeof window === "undefined") return null;
  const value = (window as PlatformBrandWindow).__GESTALT_PLATFORM_BRAND__;
  if (!value) return null;
  return normalizeBrand(value);
}

export function normalizeBrand(
  value: Partial<PlatformBrand> | null | undefined,
): PlatformBrand | null {
  if (!value) return null;
  const name = typeof value.name === "string" ? value.name.trim() : "";
  const markSrc =
    typeof value.markSrc === "string" ? normalizeMarkSrc(value.markSrc) : undefined;
  if (!name && !markSrc) return null;
  return {
    name: name || DEFAULT_PLATFORM_BRAND_NAME,
    ...(markSrc ? { markSrc } : {}),
  };
}

export function defaultPlatformBrand(): PlatformBrand {
  return { name: DEFAULT_PLATFORM_BRAND_NAME };
}

/**
 * Synchronous brand for first paint. Prefers gestaltd index injection /
 * bootstrap script, then falls back to the Gestalt default.
 */
export function getPlatformBrand(): PlatformBrand {
  return readWindowBrand() ?? readInjectedBrand() ?? defaultPlatformBrand();
}

const BRAND_ICON_SELECTOR =
  'link[rel="icon"], link[rel="shortcut icon"], link[rel="apple-touch-icon"]';

type BrandIconLink = {
  rel: string;
  setAttribute(name: string, value: string): void;
  removeAttribute(name: string): void;
};

type BrandIconDocument = {
  querySelectorAll(selectors: string): ArrayLike<BrandIconLink>;
};

/**
 * Point tab / touch icons at the tenant mark. Raster bundle favicons would
 * otherwise win over an SVG mark in some browsers.
 */
export function applyPlatformBrandIcons(
  markSrc: string | undefined,
  doc: BrandIconDocument | undefined = typeof document === "undefined"
    ? undefined
    : (document as unknown as BrandIconDocument),
): void {
  if (!doc || !markSrc) return;
  const links = Array.from(doc.querySelectorAll(BRAND_ICON_SELECTOR));
  for (const link of links) {
    link.setAttribute("href", markSrc);
    if (link.rel === "apple-touch-icon") continue;
    link.setAttribute("type", "image/svg+xml");
    link.removeAttribute("sizes");
  }
}

export function applyPlatformBrand(brand: PlatformBrand): PlatformBrand {
  const normalized = normalizeBrand(brand) ?? defaultPlatformBrand();
  if (typeof window !== "undefined") {
    (window as PlatformBrandWindow).__GESTALT_PLATFORM_BRAND__ = normalized;
  }
  if (typeof document !== "undefined" && document.title === DEFAULT_PLATFORM_BRAND_NAME) {
    document.title = normalized.name;
  }
  applyPlatformBrandIcons(normalized.markSrc);
  return normalized;
}

export async function fetchPlatformBrand(
  fetchImpl: typeof fetch = fetch,
): Promise<PlatformBrand> {
  try {
    const res = await fetchImpl(PLATFORM_BRAND_JSON_PATH, {
      headers: { Accept: "application/json" },
      cache: "no-cache",
    });
    if (!res.ok) return getPlatformBrand();
    const raw = (await res.json()) as Partial<PlatformBrand>;
    const normalized = normalizeBrand(raw);
    if (!normalized) return getPlatformBrand();
    return applyPlatformBrand(normalized);
  } catch {
    return getPlatformBrand();
  }
}
