export const SETTINGS_TOKENS_PATH = "/settings/tokens";
export const SETTINGS_IDENTITIES_PATH = "/settings/identities";

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
