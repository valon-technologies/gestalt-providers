import type { APIToken } from "./api";

/** Wire shape for GET /api/v2/identity/grants/{grant_id}. */
export interface IdentityGrantScopeWire {
  scope: string;
  resource?: string[];
}

export interface ListIdentityGrantsWire {
  grantIds?: string[];
}

export interface IdentityGrantWire {
  scopes?: IdentityGrantScopeWire[];
  createdAt?: number;
  expiresAt?: number;
}

export const PERSONAL_IDENTITY_GRANTS_PATH = "/api/v2/identity/grants";

type FetchAPI = <T>(path: string, options?: RequestInit) => Promise<T>;

export function unixSecondsToISO(seconds?: number): string | undefined {
  if (!seconds || seconds <= 0) {
    return undefined;
  }
  return new Date(seconds * 1000).toISOString();
}

export function grantScopesToTokenScopes(
  scopes?: IdentityGrantScopeWire[],
): string[] | undefined {
  if (!scopes?.length) {
    return undefined;
  }
  const parts = scopes
    .map((scope) => scope.scope.trim())
    .filter((scope) => scope.length > 0);
  return parts.length > 0 ? parts : undefined;
}

export function identityGrantToAPIToken(
  grantId: string,
  grant: IdentityGrantWire,
): APIToken {
  return {
    id: grantId,
    name: grantId,
    scopes: grantScopesToTokenScopes(grant.scopes),
    createdAt:
      unixSecondsToISO(grant.createdAt) ?? new Date(0).toISOString(),
    expiresAt: unixSecondsToISO(grant.expiresAt),
  };
}

/** Maps console APIToken fixtures to identity grant wire responses for e2e. */
export function apiTokenToIdentityGrantWire(token: APIToken): IdentityGrantWire {
  return {
    scopes: (token.scopes ?? []).map((scope) => ({ scope, resource: [] })),
    createdAt: Math.floor(new Date(token.createdAt).getTime() / 1000),
    expiresAt: token.expiresAt
      ? Math.floor(new Date(token.expiresAt).getTime() / 1000)
      : 0,
  };
}

export function parseIdentityGrantIdFromUrl(url: string): string {
  return decodeURIComponent(
    url.split(`${PERSONAL_IDENTITY_GRANTS_PATH}/`)[1]?.split("?")[0] ?? "",
  );
}

export async function listPersonalAPITokens(
  fetchAPI: FetchAPI,
): Promise<APIToken[]> {
  const list = await fetchAPI<ListIdentityGrantsWire>(
    PERSONAL_IDENTITY_GRANTS_PATH,
  );
  const grantIds = list.grantIds ?? [];
  if (grantIds.length === 0) {
    return [];
  }

  return Promise.all(
    grantIds.map(async (grantId) => {
      const grant = await fetchAPI<IdentityGrantWire>(
        `${PERSONAL_IDENTITY_GRANTS_PATH}/${encodeURIComponent(grantId)}`,
      );
      return identityGrantToAPIToken(grantId, grant);
    }),
  );
}

export async function revokePersonalAPIToken(
  fetchAPI: FetchAPI,
  grantId: string,
): Promise<void> {
  await fetchAPI(
    `${PERSONAL_IDENTITY_GRANTS_PATH}/${encodeURIComponent(grantId)}`,
    { method: "DELETE" },
  );
}
