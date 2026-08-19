import type { APIToken, APITokenScope } from "@/lib/api";
import {
  SETTINGS_TOKENS_EXPIRES_NEVER_LABEL,
  SETTINGS_TOKENS_SCOPES_ALL_LABEL,
  SETTINGS_TOKENS_UNNAMED_LABEL,
} from "@/features/settings/tokens-copy";
import { formatEventWhen } from "@/lib/date";

export type TokenScopeEntry = {
  key: string;
  label: string;
  scope: string;
};

/** Default Settings inventory order: newest created first. */
export const TOKEN_INVENTORY_DEFAULT_SORT = {
  id: "createdAt",
  desc: true,
} as const;

/** Stored name after trim, or null when the grant has no display name. */
export function tokenStoredName(token: Pick<APIToken, "name">): string | null {
  const name = token.name?.trim();
  return name ? name : null;
}

/** Visible Name-column text (stored name, or the unnamed label). */
export function tokenDisplayName(token: Pick<APIToken, "name">): string {
  return tokenStoredName(token) ?? SETTINGS_TOKENS_UNNAMED_LABEL;
}

export function tokenCreatedAtMs(token: APIToken): number {
  const ms = Date.parse(token.createdAt);
  return Number.isFinite(ms) ? ms : 0;
}

/** Finder-style stamp via Registry `formatEventWhen` ("Today at 5:05 PM"). */
function tokenTimestampLabel(iso: string): string {
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? formatEventWhen(iso) : "";
}

export function tokenCreatedLabel(token: APIToken): string {
  return tokenTimestampLabel(token.createdAt);
}

/** Never-expiring tokens sort after every dated expiry. */
export function tokenExpiresAtMs(token: APIToken): number {
  if (!token.expiresAt) {
    return Number.POSITIVE_INFINITY;
  }
  const ms = Date.parse(token.expiresAt);
  return Number.isFinite(ms) ? ms : Number.POSITIVE_INFINITY;
}

export function tokenExpiresLabel(token: APIToken): string {
  if (!token.expiresAt) {
    return SETTINGS_TOKENS_EXPIRES_NEVER_LABEL;
  }
  return tokenTimestampLabel(token.expiresAt) || SETTINGS_TOKENS_EXPIRES_NEVER_LABEL;
}

function scopeLabel(scope: string, resources?: string[]): string {
  if (!resources?.length) return scope;
  return `${scope} (${resources.join(", ")})`;
}

export function tokenScopeEntries(
  token: Pick<APIToken, "scopes" | "scopeDetails">,
): TokenScopeEntry[] {
  if (token.scopeDetails?.length) {
    return token.scopeDetails.map((entry: APITokenScope) => ({
      key: `${entry.scope}:${(entry.resources ?? []).join(",")}`,
      label: scopeLabel(entry.scope, entry.resources),
      scope: entry.scope,
    }));
  }
  return (token.scopes ?? []).map((scope) => ({
    key: scope,
    label: scope,
    scope,
  }));
}

export function tokenScopesSortKey(token: APIToken): string {
  const labels = tokenScopeEntries(token).map((entry) => entry.label);
  return labels.length > 0 ? labels.join(" ") : SETTINGS_TOKENS_SCOPES_ALL_LABEL;
}

/** Show this many scopes before a count when the list is long. */
const TOKEN_SCOPE_COLLAPSED_PREVIEW = 3;

/**
 * Collapse only when the row would otherwise wrap a long manual-permission list.
 * Short lists stay fully visible.
 */
const TOKEN_SCOPE_COLLAPSE_AFTER = 4;

export function splitCollapsedTokenScopes(entries: TokenScopeEntry[]): {
  preview: TokenScopeEntry[];
  rest: TokenScopeEntry[];
} {
  if (entries.length <= TOKEN_SCOPE_COLLAPSE_AFTER) {
    return { preview: entries, rest: [] };
  }
  return {
    preview: entries.slice(0, TOKEN_SCOPE_COLLAPSED_PREVIEW),
    rest: entries.slice(TOKEN_SCOPE_COLLAPSED_PREVIEW),
  };
}
