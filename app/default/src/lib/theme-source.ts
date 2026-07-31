export const THEME_SOURCE_STORAGE_KEY = "gestalt-theme-source";
export const TENANT_THEME_LINK_SELECTOR = 'link[data-theme-source="tenant"]';

export type ThemeSource = "default" | "tenant";

export function readThemeSource(): ThemeSource {
  if (typeof window === "undefined") return "tenant";
  return localStorage.getItem(THEME_SOURCE_STORAGE_KEY) === "default"
    ? "default"
    : "tenant";
}

export function applyThemeSource(source: ThemeSource) {
  const stylesheet = document.querySelector<HTMLLinkElement>(
    TENANT_THEME_LINK_SELECTOR,
  );
  if (!stylesheet) return;

  if (source === "default") {
    stylesheet.disabled = true;
    stylesheet.setAttribute("disabled", "");
  } else {
    stylesheet.disabled = false;
    stylesheet.removeAttribute("disabled");
  }
}

export const DEFAULT_TENANT_THEME_LABEL = "Tenant";
