import { clearSession } from "./auth";
import { HTTP_UNAUTHORIZED } from "./constants";
import { serverLoginURL } from "./authReturn";

// The browser SDK provides the AppClient for app operation invocation. The
// provider SDK's ./client export supplies IdentityClient for personal
// API-token grants; its REST transport defaults to credentials:"omit", so
// we inject a fetch wrapper that restores cookie-session credentials.
import {
  createGestaltClient,
  rest,
  unauthenticated,
  type RestGestaltClient,
} from "@valon-technologies/gestalt/client";

export interface ConnectionParamDef {
  required?: boolean;
  description?: string;
  default?: string;
}

export interface CredentialFieldDef {
  name: string;
  label?: string;
  description?: string;
}

export interface InstanceInfo {
  name: string;
  connection?: string;
}

export type AuthType = "oauth" | "manual";
export type IntegrationStatus =
  | "ready"
  | "degraded"
  | "needs_user_connection"
  | "needs_instance_selection"
  | "needs_admin_configuration"
  | "unavailable"
  | "unknown";
export type CredentialState =
  | "not_required"
  | "connected"
  | "configured"
  | "missing"
  | "invalid"
  | "unknown";
export type HealthState =
  | "healthy"
  | "unhealthy"
  | "not_checked"
  | "not_applicable"
  | "unknown";
export type IntegrationAction =
  | "connect"
  | "disconnect"
  | "add_instance"
  | "select_instance"
  | "reconnect"
  | "admin_configure";
export type ConnectionMode = "none" | "subject";
export type CredentialMode = "none" | "subject";
export type OwnerKind =
  | "none"
  | "current_user"
  | "service_account"
  | "unknown";

export interface ConnectionDefInfo {
  name: string;
  displayName?: string;
  authTypes?: AuthType[];
  connectionParams?: Record<string, ConnectionParamDef>;
  credentialFields?: CredentialFieldDef[];
  status?: IntegrationStatus;
  credentialState?: CredentialState;
  healthState?: HealthState;
  actions?: IntegrationAction[];
  mode?: ConnectionMode;
  credentialMode?: CredentialMode;
  ownerKind?: OwnerKind;
  instances?: InstanceInfo[];
  mcpPassthrough?: boolean;
}

export interface Integration {
  name: string;
  displayName?: string;
  description?: string;
  iconSvg?: string;
  mountedPath?: string;
  connections?: ConnectionDefInfo[];
  status?: IntegrationStatus;
  credentialState?: CredentialState;
  healthState?: HealthState;
  actions?: IntegrationAction[];
}

export interface AccessPermission {
  plugin: string;
  operations?: string[];
  actions?: string[];
}

export interface APIToken {
  id: string;
  name?: string;
  scopes?: string[];
  permissions?: AccessPermission[];
  createdAt: string;
  expiresAt?: string;
}

export interface CreateTokenResponse {
  id: string;
  name?: string;
  token: string;
  permissions?: AccessPermission[];
  expiresAt?: string;
}

export interface ManagedIdentity {
  id: string;
  subjectId: string;
  kind: "service_account";
  displayName: string;
  description?: string;
  credentialSubjectId: string;
  createdBySubjectId?: string;
  createdAt: string;
  updatedAt: string;
  deletedAt?: string;
}

export interface ManagedIdentityMember {
  subjectId: string;
  email?: string;
  role: "viewer" | "editor" | "admin";
}

export interface ManagedIdentityGrant {
  plugin: string;
  role: "viewer" | "editor" | "admin";
  source: "static" | "dynamic" | string;
  mutable: boolean;
}

export interface ConnectIntegrationResult {
  status: string;
  integration?: string;
  selectionUrl?: string;
  pendingToken?: string;
  candidates?: { id: string; name?: string }[];
}

export class APIError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "APIError";
  }
}

export function isAPIErrorStatus(error: unknown, status: number): boolean {
  return error instanceof APIError && error.status === status;
}

export const PENDING_CONNECTION_PATH = "/api/v1/auth/pending-connection";

/**
 * Resolve a path for same-origin API traffic.
 *
 * Cookie auth requires one browser origin: the SPA and `/api/*` share it.
 * Production gestaltd serves both; local/prod-dev Vite proxies `/api` to
 * `GESTALT_API_PROXY_TARGET`. Absolute URLs (e.g. OAuth selection redirects)
 * pass through unchanged. Do not read `process.env` here — that is Node-only
 * and blanks the Vite SPA.
 */
export function resolveAPIPath(path: string): string {
  if (/^[a-zA-Z][a-zA-Z\d+.-]*:/.test(path)) {
    return path;
  }
  if (!path.startsWith("/")) {
    throw new Error(`API path must be absolute (got ${JSON.stringify(path)})`);
  }
  return path;
}

export async function fetchAPI<T>(
  path: string,
  options?: RequestInit,
): Promise<T> {
  const res = await fetch(resolveAPIPath(path), {
    ...options,
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  if (res.status === HTTP_UNAUTHORIZED) {
    clearSession();
    if (!window.location.pathname.startsWith("/api/v1/auth/login")) {
      window.location.href = serverLoginURL();
    }
    throw new APIError(HTTP_UNAUTHORIZED, "Session expired");
  }

  if (!res.ok) {
    const body = await res.text();
    let message: string;
    try {
      const parsed = JSON.parse(body);
      message = parsed.error || body;
    } catch {
      message = body;
    }
    throw new APIError(res.status, message);
  }

  const contentType = res.headers.get("content-type") || "";
  if (!/\bapplication\/([a-z\d.+-]*\+)?json\b/i.test(contentType)) {
    throw new APIError(
      res.status,
      `Expected JSON response from ${path}, received ${contentType || "unknown content type"}`,
    );
  }

  return res.json() as Promise<T>;
}

// gestalt/client's REST transport hardcodes credentials:"omit", which drops
// the session cookie. Wrap fetch so same-origin SDK requests carry the cookie
// and trigger the same 401 -> login redirect as fetchAPI.
async function gestaltFetch(
  input: string | URL | Request,
  init?: RequestInit,
): Promise<Response> {
  const res = await fetch(input, { ...init, credentials: "include" });
  if (res.status === HTTP_UNAUTHORIZED) {
    clearSession();
    if (!window.location.pathname.startsWith("/api/v1/auth/login")) {
      window.location.href = serverLoginURL();
    }
    throw new APIError(HTTP_UNAUTHORIZED, "Session expired");
  }
  return res;
}

let identityClientPromise: Promise<RestGestaltClient["identity"]> | undefined;

// The identity client is built once and cached. gestalt/client requires an
// absolute http(s) address; window.location.origin keeps requests same-origin
// (the SPA and /api share one origin in production and via the Vite dev proxy
// locally), matching fetchAPI's resolveAPIPath contract.
async function identityClient(): Promise<RestGestaltClient["identity"]> {
  if (!identityClientPromise) {
    identityClientPromise = createGestaltClient({
      address: window.location.origin,
      transport: rest(),
      auth: unauthenticated(),
      fetch: gestaltFetch,
    }).then((client: RestGestaltClient) => client.identity);
  }
  return identityClientPromise;
}

// Map an identity grant onto the console's APIToken shape. The grant surface
// carries no client label, so name is left unset. Grant response types are
// not re-exported by gestalt/client, so the grant shape is described
// structurally; createdAt/expiresAt are int64 epoch millis on the wire.
type IdentityClientREST = RestGestaltClient["identity"];
type IdentityGrant = Awaited<ReturnType<IdentityClientREST["getGrant"]>>;
type IdentityGrantList = Awaited<
  ReturnType<IdentityClientREST["listGrants"]>
>;

function grantToAPIToken(grantId: string, grant: IdentityGrant): APIToken {
  return {
    id: grantId,
    scopes: grant.scopes.map((entry) => entry.scope),
    createdAt: epochMillisToISO(grant.createdAt) ?? "",
    expiresAt: epochMillisToISO(grant.expiresAt),
  };
}

function epochMillisToISO(value: bigint | number): string | undefined {
  if (typeof value === "bigint") {
    if (value <= 0n) return undefined;
    return new Date(Number(value)).toISOString();
  }
  if (typeof value === "number" && value > 0) {
    return new Date(value).toISOString();
  }
  return undefined;
}

export interface AuthInfo {
  provider: string;
  displayName: string;
  loginSupported: boolean;
}

export interface AuthSession {
  subjectId: string;
  email?: string;
  displayName?: string;
}

export async function getAuthInfo(): Promise<AuthInfo> {
  return fetchAPI("/api/v1/auth/info");
}

export async function getAuthSession(): Promise<AuthSession> {
  return fetchAPI("/api/v1/auth/session");
}

export async function logout(): Promise<void> {
  await fetchAPI("/api/v1/auth/logout", { method: "POST" });
}

export async function getIntegrations(): Promise<Integration[]> {
  return fetchAPI<Integration[]>("/api/v1/apps");
}

export async function startIntegrationOAuth(
  integration: string,
  scopes?: string[],
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
): Promise<{ url: string; state: string }> {
  return fetchAPI("/api/v1/auth/start-oauth", {
    method: "POST",
    body: JSON.stringify({
      integration,
      instance,
      connection,
      scopes: scopes || [],
      connectionParams,
      returnPath,
    }),
  });
}

export async function connectManualIntegration(
  integration: string,
  credential: string | Record<string, string>,
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
): Promise<ConnectIntegrationResult> {
  const body: Record<string, unknown> = {
    integration,
    instance,
    connection,
    connectionParams,
    returnPath,
  };
  if (typeof credential === "string") {
    body.credential = credential;
  } else {
    body.credentials = credential;
  }
  return fetchAPI("/api/v1/auth/connect-manual", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function disconnectIntegration(
  name: string,
  instance?: string,
  connection?: string,
): Promise<void> {
  const query = new URLSearchParams();
  if (instance) query.set("_instance", instance);
  if (connection) query.set("_connection", connection);
  const params = query.toString();
  await fetchAPI(
    `/api/v1/apps/${encodeURIComponent(name)}${params ? `?${params}` : ""}`,
    {
      method: "DELETE",
    },
  );
}

export async function getTokens(): Promise<APIToken[]> {
  const identity = await identityClient();
  const { grantIds }: IdentityGrantList = await identity.listGrants({});
  const grants = await Promise.all(
    grantIds.map((grantId) => identity.getGrant({ grantId })),
  );
  return grantIds.map((grantId, index) =>
    grantToAPIToken(grantId, grants[index]!),
  );
}

export async function createToken(
  name: string,
  scopes: string,
  expiresIn?: number,
): Promise<CreateTokenResponse> {
  // Token creation stays on the v1 gateway: the v2 identity token endpoint
  // is an RFC 8693 token-exchange that requires a subject_token the browser
  // does not hold under cookie-session auth. The host injects the session
  // token server-side on this v1 route.
  const body: Record<string, unknown> = { name, scopes };
  if (expiresIn !== undefined) {
    body.expiresIn = expiresIn;
  }
  return fetchAPI("/api/v1/tokens", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function revokeToken(id: string): Promise<void> {
  await (await identityClient()).revokeGrant({ grantId: id });
}

const MANAGED_SUBJECTS_PATH = "/api/v1/authorization/subjects";

function managedSubjectPath(id: string): string {
  return `${MANAGED_SUBJECTS_PATH}/${encodeURIComponent(id)}`;
}

function unwrapManagedIdentityGrant(
  response: ManagedIdentityGrant | { grant?: ManagedIdentityGrant },
): ManagedIdentityGrant {
  if ("grant" in response && response.grant) {
    return response.grant;
  }
  return response as ManagedIdentityGrant;
}

export async function getManagedIdentities(): Promise<ManagedIdentity[]> {
  return fetchAPI(MANAGED_SUBJECTS_PATH);
}

export async function createManagedIdentity(
  id: string,
  displayName: string,
  description?: string,
): Promise<ManagedIdentity> {
  return fetchAPI(MANAGED_SUBJECTS_PATH, {
    method: "POST",
    body: JSON.stringify({ id, displayName, description }),
  });
}

export async function getManagedIdentity(id: string): Promise<ManagedIdentity> {
  return fetchAPI(managedSubjectPath(id));
}

export async function getManagedIdentityIntegrations(
  id: string,
): Promise<Integration[]> {
  return fetchAPI<Integration[]>(`${managedSubjectPath(id)}/apps`);
}

export async function startManagedIdentityIntegrationOAuth(
  id: string,
  integration: string,
  scopes?: string[],
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
): Promise<{ url: string; state: string }> {
  return fetchAPI(`${managedSubjectPath(id)}/auth/start-oauth`, {
    method: "POST",
    body: JSON.stringify({
      integration,
      instance,
      connection,
      scopes: scopes || [],
      connectionParams,
      returnPath,
    }),
  });
}

export async function connectManagedIdentityManualIntegration(
  id: string,
  integration: string,
  credential: string | Record<string, string>,
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
): Promise<ConnectIntegrationResult> {
  const body: Record<string, unknown> = {
    integration,
    instance,
    connection,
    connectionParams,
    returnPath,
  };
  if (typeof credential === "string") {
    body.credential = credential;
  } else {
    body.credentials = credential;
  }
  return fetchAPI(`${managedSubjectPath(id)}/auth/connect-manual`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function disconnectManagedIdentityIntegration(
  id: string,
  name: string,
  instance?: string,
  connection?: string,
): Promise<void> {
  const query = new URLSearchParams();
  if (instance) query.set("_instance", instance);
  if (connection) query.set("_connection", connection);
  const params = query.toString();
  await fetchAPI(
    `${managedSubjectPath(id)}/apps/${encodeURIComponent(name)}${params ? `?${params}` : ""}`,
    {
      method: "DELETE",
    },
  );
}

export async function updateManagedIdentity(
  id: string,
  displayName: string,
): Promise<ManagedIdentity> {
  return fetchAPI(managedSubjectPath(id), {
    method: "PATCH",
    body: JSON.stringify({ displayName }),
  });
}

export async function deleteManagedIdentity(id: string): Promise<void> {
  await fetchAPI(managedSubjectPath(id), {
    method: "DELETE",
  });
}

export async function getManagedIdentityMembers(
  id: string,
): Promise<ManagedIdentityMember[]> {
  return fetchAPI(`${managedSubjectPath(id)}/members`);
}

export async function putManagedIdentityMember(
  id: string,
  email: string,
  role: ManagedIdentityMember["role"],
): Promise<ManagedIdentityMember> {
  return fetchAPI(`${managedSubjectPath(id)}/members`, {
    method: "PUT",
    body: JSON.stringify({ email, role }),
  });
}

export async function deleteManagedIdentityMember(
  id: string,
  memberSubjectID: string,
): Promise<void> {
  await fetchAPI(
    `${managedSubjectPath(id)}/members/${encodeURIComponent(memberSubjectID)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityGrants(
  id: string,
): Promise<ManagedIdentityGrant[]> {
  return fetchAPI(`${managedSubjectPath(id)}/grants`);
}

export async function putManagedIdentityGrant(
  id: string,
  plugin: string,
  role: ManagedIdentityGrant["role"],
): Promise<ManagedIdentityGrant> {
  const response = await fetchAPI<
    ManagedIdentityGrant | { grant?: ManagedIdentityGrant }
  >(`${managedSubjectPath(id)}/grants/${encodeURIComponent(plugin)}`, {
    method: "PUT",
    body: JSON.stringify({ role }),
  });
  return unwrapManagedIdentityGrant(response);
}

export async function deleteManagedIdentityGrant(
  id: string,
  plugin: string,
): Promise<void> {
  await fetchAPI(
    `${managedSubjectPath(id)}/grants/${encodeURIComponent(plugin)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityTokens(
  id: string,
): Promise<APIToken[]> {
  return fetchAPI(`${managedSubjectPath(id)}/tokens`);
}

export async function createManagedIdentityToken(
  id: string,
  name: string,
  permissions?: AccessPermission[],
): Promise<CreateTokenResponse> {
  const body: { name: string; permissions?: AccessPermission[] } = { name };
  if (permissions !== undefined) {
    body.permissions = permissions;
  }
  return fetchAPI(`${managedSubjectPath(id)}/tokens`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function revokeManagedIdentityToken(
  id: string,
  tokenId: string,
): Promise<void> {
  await fetchAPI(
    `${managedSubjectPath(id)}/tokens/${encodeURIComponent(tokenId)}`,
    { method: "DELETE" },
  );
}
