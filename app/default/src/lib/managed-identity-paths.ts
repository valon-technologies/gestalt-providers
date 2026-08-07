export const SETTINGS_TOKENS_PATH = "/settings/tokens";
export const SETTINGS_TOKENS_NEW_PATH = "/settings/tokens/new";
export const SETTINGS_IDENTITIES_PATH = "/settings/identities";

/** Legacy `/identities?id=…` query — TanStack Router exposes search as a parsed object. */
export function legacyIdentityIdFromSearch(search: unknown): string | null {
  if (typeof search === "string") {
    const normalized = search.startsWith("?") ? search : `?${search}`;
    return new URLSearchParams(normalized).get("id");
  }
  if (search && typeof search === "object") {
    const id = (search as { id?: unknown }).id;
    return typeof id === "string" && id.length > 0 ? id : null;
  }
  return null;
}

export function legacyIdentityIdFromLocation(location: {
  search: unknown;
  searchStr?: string;
}): string | null {
  if (typeof location.searchStr === "string" && location.searchStr.length > 0) {
    const fromStr = new URLSearchParams(location.searchStr).get("id");
    if (fromStr) return fromStr;
  }
  return legacyIdentityIdFromSearch(location.search);
}

export function canonicalManagedIdentityID(value: string): string {
  const trimmed = value.trim();
  if (!trimmed || trimmed.includes(":")) return trimmed;
  return `service_account:${trimmed}`;
}

export function managedIdentityLocalId(subjectId: string): string {
  const canonical = canonicalManagedIdentityID(subjectId);
  const prefix = "service_account:";
  return canonical.startsWith(prefix)
    ? canonical.slice(prefix.length)
    : canonical;
}

export function settingsIdentityDetailPath(subjectId: string): string {
  return `${SETTINGS_IDENTITIES_PATH}/${encodeURIComponent(managedIdentityLocalId(subjectId))}`;
}

export function subjectIdFromLocalParam(localId: string): string {
  return canonicalManagedIdentityID(decodeURIComponent(localId));
}
