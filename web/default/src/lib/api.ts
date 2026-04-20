import { clearSession } from "./auth";
import { HTTP_UNAUTHORIZED, LOGIN_PATH } from "./constants";

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

export interface ConnectionDefInfo {
  name: string;
  displayName?: string;
  authTypes: ("oauth" | "manual")[];
  credentialFields?: CredentialFieldDef[];
}

export interface Integration {
  name: string;
  displayName?: string;
  description?: string;
  iconSvg?: string;
  mountedPath?: string;
  connected?: boolean;
  instances?: InstanceInfo[];
  authTypes?: ("oauth" | "manual")[];
  connectionParams?: Record<string, ConnectionParamDef>;
  connections?: ConnectionDefInfo[];
  credentialFields?: CredentialFieldDef[];
}

export interface IntegrationOperation {
  id: string;
  title?: string;
  description?: string;
  readOnly?: boolean;
  visible?: boolean;
  tags?: string[];
}

export interface AccessPermission {
  plugin: string;
  operations?: string[];
}

export interface APIToken {
  id: string;
  name: string;
  scopes?: string;
  permissions?: AccessPermission[];
  createdAt: string;
  expiresAt?: string;
}

export interface CreateTokenResponse {
  id: string;
  name: string;
  token: string;
  permissions?: AccessPermission[];
  expiresAt?: string;
}

export interface ManagedIdentity {
  id: string;
  displayName: string;
  role: "viewer" | "editor" | "admin";
  createdAt: string;
  updatedAt: string;
}

export interface ManagedIdentityMember {
  userId?: string;
  email: string;
  role: "viewer" | "editor" | "admin";
  createdAt: string;
  updatedAt: string;
}

export interface ManagedIdentityGrant {
  plugin: string;
  operations?: string[];
  createdAt: string;
  updatedAt: string;
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

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";
export const PENDING_CONNECTION_PATH = "/api/v1/auth/pending-connection";

export function resolveAPIPath(path: string): string {
  if (/^[a-zA-Z][a-zA-Z\d+.-]*:/.test(path)) {
    return path;
  }
  return `${API_BASE}${path}`;
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
    if (window.location.pathname !== LOGIN_PATH) {
      window.location.href = LOGIN_PATH;
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

export interface AuthInfo {
  provider: string;
  displayName: string;
  loginSupported: boolean;
}

export async function getAuthInfo(): Promise<AuthInfo> {
  return fetchAPI("/api/v1/auth/info");
}

export async function startLogin(state: string): Promise<{ url: string }> {
  return fetchAPI("/api/v1/auth/login", {
    method: "POST",
    body: JSON.stringify({ state }),
  });
}

export async function loginCallback(
  code: string,
  state?: string,
): Promise<{ email: string; displayName: string }> {
  const params = new URLSearchParams({ code });
  if (state) params.set("state", state);
  return fetchAPI(`/api/v1/auth/login/callback?${params}`);
}

export async function logout(): Promise<void> {
  await fetchAPI("/api/v1/auth/logout", { method: "POST" });
}

export async function getIntegrations(): Promise<Integration[]> {
  return fetchAPI<Integration[]>("/api/v1/integrations");
}

export async function getIntegrationOperations(
  integration: string,
): Promise<IntegrationOperation[]> {
  return fetchAPI<IntegrationOperation[]>(
    `/api/v1/integrations/${encodeURIComponent(integration)}/operations`,
  );
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
  if (instance) query.set("instance", instance);
  if (connection) query.set("connection", connection);
  const params = query.toString();
  await fetchAPI(
    `/api/v1/integrations/${encodeURIComponent(name)}${params ? `?${params}` : ""}`,
    {
      method: "DELETE",
    },
  );
}

export async function getTokens(): Promise<APIToken[]> {
  return fetchAPI("/api/v1/tokens");
}

export async function createToken(name: string): Promise<CreateTokenResponse> {
  return fetchAPI("/api/v1/tokens", {
    method: "POST",
    body: JSON.stringify({ name }),
  });
}

export async function revokeToken(id: string): Promise<void> {
  await fetchAPI(`/api/v1/tokens/${id}`, { method: "DELETE" });
}

export async function getManagedIdentities(): Promise<ManagedIdentity[]> {
  return fetchAPI("/api/v1/identities");
}

export async function createManagedIdentity(displayName: string): Promise<ManagedIdentity> {
  return fetchAPI("/api/v1/identities", {
    method: "POST",
    body: JSON.stringify({ displayName }),
  });
}

export async function getManagedIdentity(id: string): Promise<ManagedIdentity> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}`);
}

export async function updateManagedIdentity(id: string, displayName: string): Promise<ManagedIdentity> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}`, {
    method: "PATCH",
    body: JSON.stringify({ displayName }),
  });
}

export async function deleteManagedIdentity(id: string): Promise<void> {
  await fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
}

export async function getManagedIdentityMembers(id: string): Promise<ManagedIdentityMember[]> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/members`);
}

export async function putManagedIdentityMember(
  id: string,
  email: string,
  role: ManagedIdentityMember["role"],
): Promise<ManagedIdentityMember> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/members`, {
    method: "PUT",
    body: JSON.stringify({ email, role }),
  });
}

export async function deleteManagedIdentityMember(id: string, email: string): Promise<void> {
  await fetchAPI(
    `/api/v1/identities/${encodeURIComponent(id)}/members/${encodeURIComponent(email)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityGrants(id: string): Promise<ManagedIdentityGrant[]> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/grants`);
}

export async function putManagedIdentityGrant(
  id: string,
  plugin: string,
  operations?: string[],
): Promise<ManagedIdentityGrant> {
  return fetchAPI(
    `/api/v1/identities/${encodeURIComponent(id)}/grants/${encodeURIComponent(plugin)}`,
    {
      method: "PUT",
      body: JSON.stringify({ operations }),
    },
  );
}

export async function deleteManagedIdentityGrant(id: string, plugin: string): Promise<void> {
  await fetchAPI(
    `/api/v1/identities/${encodeURIComponent(id)}/grants/${encodeURIComponent(plugin)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityTokens(id: string): Promise<APIToken[]> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/tokens`);
}

export async function createManagedIdentityToken(
  id: string,
  name: string,
  permissions: AccessPermission[],
): Promise<CreateTokenResponse> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/tokens`, {
    method: "POST",
    body: JSON.stringify({ name, permissions }),
  });
}

export async function revokeManagedIdentityToken(id: string, tokenId: string): Promise<void> {
  await fetchAPI(
    `/api/v1/identities/${encodeURIComponent(id)}/tokens/${encodeURIComponent(tokenId)}`,
    { method: "DELETE" },
  );
}

export async function getManagedIdentityIntegrations(id: string): Promise<Integration[]> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/integrations`);
}

export async function startManagedIdentityOAuth(
  id: string,
  integration: string,
  scopes?: string[],
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
): Promise<{ url: string; state: string }> {
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/auth/start-oauth`, {
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

export async function connectManagedIdentityManual(
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
  return fetchAPI(`/api/v1/identities/${encodeURIComponent(id)}/auth/connect-manual`, {
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
  if (instance) query.set("instance", instance);
  if (connection) query.set("connection", connection);
  const params = query.toString();
  await fetchAPI(
    `/api/v1/identities/${encodeURIComponent(id)}/integrations/${encodeURIComponent(name)}${params ? `?${params}` : ""}`,
    { method: "DELETE" },
  );
}
