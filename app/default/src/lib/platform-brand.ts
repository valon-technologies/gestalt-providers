/**
 * Platform product identity for the home shell chrome.
 *
 * Deployment-owned (serve-time `/brand.json` + optional index injection).
 * Default when unconfigured: Gestalt (the open-source framework name).
 * Do not hardcode tenant names here — see THEMING.md / theme-boundary.md.
 */

export const PLATFORM_BRAND_SCRIPT_ID = "gestalt-platform-brand";
export const PLATFORM_BRAND_JSON_PATH = "brand.json";
export const DEFAULT_PLATFORM_BRAND_NAME = "Gestalt";

export type PlatformBrand = {
  /** Product display name in chrome + document.title suffix. */
  name: string;
  /** Mount-relative mark URL (e.g. `theme/mark.svg`), when configured. */
  markSrc?: string;
};

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
    typeof value.markSrc === "string" ? value.markSrc.trim() : "";
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

export function applyPlatformBrand(brand: PlatformBrand): PlatformBrand {
  const normalized = normalizeBrand(brand) ?? defaultPlatformBrand();
  if (typeof window !== "undefined") {
    (window as PlatformBrandWindow).__GESTALT_PLATFORM_BRAND__ = normalized;
  }
  if (typeof document !== "undefined" && document.title === DEFAULT_PLATFORM_BRAND_NAME) {
    document.title = normalized.name;
  }
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
