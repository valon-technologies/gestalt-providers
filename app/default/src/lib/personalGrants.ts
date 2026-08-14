import type { APIToken, APITokenScope } from "./api";

/** Wire shape for GET /api/v2/identity/grants/{grant_id}. */
export interface IdentityGrantScopeWire {
  scope: string;
  resource?: string[];
}

export interface ListIdentityGrantsWire {
  grantIds?: string[];
}

export interface IdentityGrantWire {
  /** Optional display name when the grant API returns one. */
  name?: string;
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

export function grantScopesToTokenScopeDetails(
  scopes?: IdentityGrantScopeWire[],
): APITokenScope[] | undefined {
  if (!scopes?.length) {
    return undefined;
  }
  const details = scopes
    .map((entry) => {
      const scope = entry.scope.trim();
      if (!scope) return null;
      const resources = (entry.resource ?? [])
        .map((resource) => resource.trim())
        .filter((resource) => resource.length > 0);
      return {
        scope,
        ...(resources.length > 0 ? { resources } : {}),
      } satisfies APITokenScope;
    })
    .filter((entry): entry is APITokenScope => entry !== null);
  return details.length > 0 ? details : undefined;
}

export function grantScopesToTokenScopes(
  scopes?: IdentityGrantScopeWire[],
): string[] | undefined {
  const details = grantScopesToTokenScopeDetails(scopes);
  if (!details?.length) {
    return undefined;
  }
  return details.map((entry) => {
    if (!entry.resources?.length) return entry.scope;
    return `${entry.scope}:${entry.resources.join(",")}`;
  });
}

export function identityGrantToAPIToken(
  grantId: string,
  grant: IdentityGrantWire,
): APIToken {
  const scopeDetails = grantScopesToTokenScopeDetails(grant.scopes);
  const name = grant.name?.trim();
  return {
    id: grantId,
    ...(name && name !== grantId ? { name } : {}),
    scopes: grantScopesToTokenScopes(grant.scopes),
    scopeDetails,
    createdAt:
      unixSecondsToISO(grant.createdAt) ?? new Date(0).toISOString(),
    expiresAt: unixSecondsToISO(grant.expiresAt),
  };
}

/** Maps console APIToken fixtures to identity grant wire responses for e2e. */
export function apiTokenToIdentityGrantWire(token: APIToken): IdentityGrantWire {
  const fromDetails = token.scopeDetails?.map((entry) => ({
    scope: entry.scope,
    resource: entry.resources ?? [],
  }));
  const name = token.name?.trim();
  return {
    ...(name ? { name } : {}),
    scopes:
      fromDetails ??
      (token.scopes ?? []).map((scope) => ({ scope, resource: [] })),
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
