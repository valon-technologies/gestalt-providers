import type {
  AuthType,
  ConnectionDefInfo,
  ConnectionMode,
  ConnectionParamDef,
  CredentialFieldDef,
  CredentialMode,
  CredentialState,
  HealthState,
  InstanceInfo,
  Integration,
  IntegrationAction,
  IntegrationStatus,
  OwnerKind,
} from "./api";
import {
  APP_NOT_CONNECTED_LABEL,
  APP_CONNECTED_LABEL,
  IDENTITY_CONNECTION_REQUIRED_LABEL,
  IDENTITY_CONNECTED_LABEL,
  NEEDS_SIGN_IN_LABEL,
} from "./accountCopy";

export type ConnectionContext = "current_user" | "managed_subject";
export type StatusTone = "success" | "warning" | "danger" | "neutral";

export type NormalizedConnection = {
  key: string;
  connection?: string;
  label: string;
  authTypes: AuthType[];
  connectionParams?: Record<string, ConnectionParamDef>;
  credentialFields?: CredentialFieldDef[];
  instances: InstanceInfo[];
  status: IntegrationStatus;
  credentialState: CredentialState;
  healthState: HealthState;
  actions: IntegrationAction[];
  actionSource: "server" | "inferred";
  mode: ConnectionMode;
  credentialMode: CredentialMode;
  ownerKind: OwnerKind;
  connected: boolean;
  connectable: boolean;
  disconnectable: boolean;
  canConnect: boolean;
  canDisconnect: boolean;
  canAddInstance: boolean;
  canReconnect: boolean;
  canSelectInstance: boolean;
  canAdminConfigure: boolean;
  isNoAuth: boolean;
  isSubjectOwned: boolean;
  isManagedSubjectOwned: boolean;
  isMCPPassthrough: boolean;
  summaryLabel: string;
  statusLabel: string;
  credentialLabel: string;
  healthLabel?: string;
  ownerLabel: string;
  detailLines: string[];
  usefulStatusDetail: boolean;
};

export type NormalizedIntegrationStatus = {
  status: IntegrationStatus;
  credentialState: CredentialState;
  healthState: HealthState;
  actions: IntegrationAction[];
  connections: NormalizedConnection[];
  summaryLabel: string;
  tone: StatusTone;
  connected: boolean;
  hasActionableConnections: boolean;
  hasUsefulStatusDetail: boolean;
};

/** Rollup and instance chrome when a saved login no longer works.
 *  Presentation predicate only: action enablement is `canReconnect`. */
export const NEEDS_RECONNECT_LABEL = NEEDS_SIGN_IN_LABEL;

export function connectionNeedsReconnect(
  connection: NormalizedConnection,
): boolean {
  return (
    connection.canReconnect ||
    connection.credentialState === "invalid" ||
    connection.healthState === "unhealthy"
  );
}

export function integrationNeedsReconnect(
  status: NormalizedIntegrationStatus,
): boolean {
  return (
    status.credentialState === "invalid" ||
    status.healthState === "unhealthy" ||
    status.connections.some(connectionNeedsReconnect)
  );
}

const STATUSES: IntegrationStatus[] = [
  "ready",
  "degraded",
  "needs_user_connection",
  "needs_instance_selection",
  "needs_admin_configuration",
  "unavailable",
  "unknown",
];

const CREDENTIAL_STATES: CredentialState[] = [
  "not_required",
  "connected",
  "configured",
  "missing",
  "invalid",
  "unknown",
];

const HEALTH_STATES: HealthState[] = [
  "healthy",
  "unhealthy",
  "not_checked",
  "not_applicable",
  "unknown",
];

const ACTIONS: IntegrationAction[] = [
  "connect",
  "disconnect",
  "add_instance",
  "select_instance",
  "reconnect",
  "admin_configure",
];

const MODES: ConnectionMode[] = ["none", "subject"];
const CREDENTIAL_MODES: CredentialMode[] = ["none", "subject"];
const OWNER_KINDS: OwnerKind[] = [
  "none",
  "current_user",
  "service_account",
  "unknown",
];

type RawConnection = {
  name?: string;
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
  preferredInstance?: string;
  connected?: boolean;
  mcpPassthrough?: boolean;
};

export function normalizeIntegrationStatus(
  integration: Integration,
  context: ConnectionContext = "current_user",
): NormalizedIntegrationStatus {
  const connections = buildRawConnections(integration).map((connection) =>
    normalizeConnection(integration, connection, context),
  );
  const actions = validActions(integration.actions);
  // Connection rows own whether this workspace can act. Alternative auth
  // methods are OR: an unused API key must not override a linked OAuth row.
  // Top-level API summaries that AND those methods are ignored when rows exist.
  const derivedStatus =
    aggregateStatus(connections) ??
    inferIntegrationStatus(integration, connections);
  const derivedCredentialState = aggregateCredentialState(connections);
  const derivedHealthState = aggregateHealthState(connections);
  const status =
    connections.length > 0
      ? derivedStatus
      : (validStatus(integration.status) ?? derivedStatus);
  const credentialState =
    connections.length > 0
      ? derivedCredentialState
      : (validCredentialState(integration.credentialState) ??
        derivedCredentialState);
  const healthState =
    connections.length > 0
      ? derivedHealthState
      : (validHealthState(integration.healthState) ?? derivedHealthState);
  const connected =
    typeof integration.connected === "boolean"
      ? integration.connected
      : connections.some((connection) => connection.connected);
  const hasActionableConnections = connections.some(
    (connection) =>
      connection.canConnect ||
      connection.canDisconnect ||
      connection.canAddInstance ||
      connection.canReconnect ||
      connection.canSelectInstance,
  );
  const hasUsefulStatusDetail =
    connections.length > 1 ||
    connections.some((connection) => connection.usefulStatusDetail);

  return {
    status,
    credentialState,
    healthState,
    actions,
    connections,
    summaryLabel: integrationSummaryLabel(
      status,
      credentialState,
      healthState,
      context,
    ),
    tone: statusTone(status, credentialState, healthState),
    connected,
    hasActionableConnections,
    hasUsefulStatusDetail,
  };
}

export function shouldShowIntegrationSettings(
  normalized: NormalizedIntegrationStatus,
  readOnly = false,
): boolean {
  if (readOnly) {
    return (
      normalized.hasUsefulStatusDetail ||
      normalized.connections.some((connection) => connection.instances.length > 0)
    );
  }

  return normalized.hasActionableConnections || normalized.hasUsefulStatusDetail;
}

/**
 * Whether the app workspace should expose a Connection (credentials) surface.
 *
 * Distinct from `connected`: `not_required` / mount-only apps count as ready for
 * launch, but they have nothing to connect, reconnect, or disconnect. Using
 * `connected` (or `connections.length`) for nav visibility falsely sells a
 * credential step for those apps.
 */
export function hasCredentialSurface(
  normalized: NormalizedIntegrationStatus,
): boolean {
  if (normalized.credentialState === "not_required") {
    return normalized.connections.some(
      (connection) =>
        connection.isMCPPassthrough ||
        (!connection.isNoAuth &&
          (connection.canConnect ||
            connection.canDisconnect ||
            connection.canReconnect ||
            connection.canAddInstance ||
            connection.canSelectInstance ||
            connection.instances.length > 0 ||
            connection.usefulStatusDetail)),
    );
  }

  return (
    normalized.status === "needs_user_connection" ||
    normalized.credentialState === "missing" ||
    normalized.credentialState === "invalid" ||
    normalized.hasActionableConnections ||
    normalized.connections.length > 0 ||
    normalized.hasUsefulStatusDetail
  );
}

export function statusTone(
  status: IntegrationStatus,
  credentialState: CredentialState,
  healthState: HealthState,
): StatusTone {
  if (
    status === "unavailable" ||
    credentialState === "invalid" ||
    healthState === "unhealthy"
  ) {
    return "danger";
  }
  if (
    status === "degraded" ||
    status === "needs_user_connection" ||
    status === "needs_instance_selection" ||
    status === "needs_admin_configuration" ||
    credentialState === "missing"
  ) {
    return "warning";
  }
  if (
    status === "ready" ||
    credentialState === "connected" ||
    credentialState === "configured" ||
    credentialState === "not_required"
  ) {
    return "success";
  }
  return "neutral";
}

function buildRawConnections(integration: Integration): RawConnection[] {
  if (integration.connections?.length) {
    return integration.connections.map((connection) => ({
      ...connection,
      instances: connectionInstances(integration, connection),
    }));
  }
  return [];
}

function connectionInstances(
  _integration: Integration,
  connection: ConnectionDefInfo,
): InstanceInfo[] {
  const nested = connection.instances ?? [];
  const seen = new Set<string>();
  const out: InstanceInfo[] = [];
  for (const instance of nested) {
    const key = `${instance.connection || connection.name}:${instance.name}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      ...instance,
      connection: instance.connection || connection.name,
    });
  }
  return out;
}

function normalizeConnection(
  integration: Integration,
  raw: RawConnection,
  context: ConnectionContext,
): NormalizedConnection {
  const authTypes = normalizeAuthTypes(raw.authTypes);
  const mode = resolveMode(raw, authTypes);
  const credentialMode = resolveCredentialMode(raw, mode, authTypes);
  const ownerKind = resolveOwnerKind(raw, credentialMode, context);
  const hasExplicitOwnerMode =
    !!validCredentialMode(raw.credentialMode) ||
    !!validMode(raw.mode) ||
    !!validOwnerKind(raw.ownerKind);
  const isNoAuth =
    validCredentialMode(raw.credentialMode) === "none" ||
      validMode(raw.mode) === "none" ||
      validCredentialState(raw.credentialState) === "not_required" ||
      (authTypes.length === 0 && !hasExplicitOwnerMode);
  const isManagedSubjectOwned =
    ownerKind === "service_account" || context === "managed_subject";
  const isSubjectOwned =
    !isNoAuth &&
    (credentialMode === "subject" ||
      mode === "subject" ||
      ownerKind === "current_user" ||
      ownerKind === "service_account" ||
      authTypes.length > 0);
  const credentialState =
    validCredentialState(raw.credentialState) ??
    inferConnectionCredentialState(authTypes, isNoAuth);
  const healthState = validHealthState(raw.healthState) ?? "unknown";
  const status =
    validStatus(raw.status) ??
    inferConnectionStatus(
      authTypes,
      credentialState,
      healthState,
      isNoAuth,
    );
  const actions = validActions(raw.actions);
  const inferredActions = ensureSelectInstanceAction(
    ensureReconnectAction(
      actions.length
        ? actions
        : inferConnectionActions(raw, authTypes, status, isNoAuth),
      authTypes,
      credentialState,
      healthState,
      raw.instances?.length ?? 0,
    ),
    authTypes,
    raw.instances?.length ?? 0,
  );
  const disconnectable =
    inferredActions.includes("disconnect");
  const connected =
    typeof raw.connected === "boolean"
      ? raw.connected
      : productConnectedFromStatus(status, credentialState);
  const connectable =
    inferredActions.some((action) =>
      action === "connect" ||
      action === "reconnect" ||
      action === "add_instance" ||
      action === "select_instance",
    );
  const label = raw.displayName || raw.name || integration.displayName || integration.name;
  const summaryLabel = connectionSummaryLabel(
    status,
    credentialState,
    healthState,
    isNoAuth,
    context,
  );
  const statusLabel = statusDisplayLabel(status, context);
  const credentialLabel = credentialDisplayLabel(
    credentialState,
    isNoAuth,
    isManagedSubjectOwned,
  );
  const healthLabel = healthDisplayLabel(healthState);
  const ownerLabel = ownerDisplayLabel(
    isNoAuth,
    isManagedSubjectOwned,
  );
  const isMCPPassthrough = raw.mcpPassthrough === true;
  const shouldShowCredentialDetail =
    credentialState === "missing" ||
    credentialState === "invalid" ||
    credentialState === "unknown";
  const detailLines = compact([
    isMCPPassthrough ? "Uses a shared login" : undefined,
    shouldShowCredentialDetail ? credentialLabel : undefined,
    shouldShowCredentialDetail && !isNoAuth ? ownerLabel : undefined,
    healthLabel,
  ]);
  const usefulStatusDetail =
    status === "needs_admin_configuration" ||
    status === "needs_instance_selection" ||
    status === "degraded" ||
    status === "unavailable" ||
    credentialState === "missing" ||
    credentialState === "invalid" ||
    validHealthState(raw.healthState) === "unhealthy" ||
    inferredActions.includes("admin_configure") ||
    raw.mcpPassthrough === true ||
    (raw.instances?.length ?? 0) > 0 ||
    isSubjectOwned;

  return {
    key: raw.name || integration.name,
    connection: raw.name,
    label,
    authTypes,
    connectionParams: raw.connectionParams,
    credentialFields: raw.credentialFields,
    instances: normalizeConnectionInstances(
      raw.instances,
      raw.preferredInstance,
    ),
    status,
    credentialState,
    healthState,
    actions: inferredActions,
    actionSource: actions.length ? "server" : "inferred",
    mode,
    credentialMode,
    ownerKind,
    connected,
    connectable,
    disconnectable,
    canConnect: inferredActions.includes("connect") && connectable,
    canDisconnect: inferredActions.includes("disconnect") && disconnectable,
    canAddInstance: inferredActions.includes("add_instance") && connectable,
    canReconnect: inferredActions.includes("reconnect") && connectable,
    canSelectInstance: inferredActions.includes("select_instance") && connectable,
    canAdminConfigure: inferredActions.includes("admin_configure"),
    isNoAuth,
    isSubjectOwned,
    isManagedSubjectOwned,
    isMCPPassthrough,
    summaryLabel,
    statusLabel,
    credentialLabel,
    healthLabel,
    ownerLabel,
    detailLines,
    usefulStatusDetail,
  };
}

function normalizeAuthTypes(authTypes?: AuthType[]): AuthType[] {
  const normalized: AuthType[] = [];
  if (authTypes?.includes("oauth")) normalized.push("oauth");
  if (authTypes?.includes("manual")) normalized.push("manual");
  return normalized;
}

/** Prefer per-instance `preferred`; fall back to connection-level preferredInstance. */
function normalizeConnectionInstances(
  instances: InstanceInfo[] | undefined,
  preferredInstance?: string,
): InstanceInfo[] {
  const list = instances ?? [];
  const preferredName = preferredInstance?.trim();
  if (!preferredName) return list;
  return list.map((instance) => ({
    ...instance,
    preferred:
      instance.preferred === true || instance.name === preferredName,
  }));
}

function validStatus(value: unknown): IntegrationStatus | undefined {
  return STATUSES.includes(value as IntegrationStatus)
    ? (value as IntegrationStatus)
    : undefined;
}

function validCredentialState(value: unknown): CredentialState | undefined {
  return CREDENTIAL_STATES.includes(value as CredentialState)
    ? (value as CredentialState)
    : undefined;
}

function validHealthState(value: unknown): HealthState | undefined {
  return HEALTH_STATES.includes(value as HealthState)
    ? (value as HealthState)
    : undefined;
}

function validActions(actions?: IntegrationAction[]): IntegrationAction[] {
  if (!Array.isArray(actions)) return [];
  return actions.filter((action): action is IntegrationAction =>
    ACTIONS.includes(action),
  );
}

function validMode(value: unknown): ConnectionMode | undefined {
  return MODES.includes(value as ConnectionMode)
    ? (value as ConnectionMode)
    : undefined;
}

function validCredentialMode(value: unknown): CredentialMode | undefined {
  return CREDENTIAL_MODES.includes(value as CredentialMode)
    ? (value as CredentialMode)
    : undefined;
}

function validOwnerKind(value: unknown): OwnerKind | undefined {
  return OWNER_KINDS.includes(value as OwnerKind)
    ? (value as OwnerKind)
    : undefined;
}

function resolveMode(raw: RawConnection, authTypes: AuthType[]): ConnectionMode {
  const explicit = validMode(raw.mode);
  if (explicit) return explicit;
  if (authTypes.length === 0) return "none";
  return "subject";
}

function resolveCredentialMode(
  raw: RawConnection,
  mode: ConnectionMode,
  authTypes: AuthType[],
): CredentialMode {
  const explicit = validCredentialMode(raw.credentialMode);
  if (explicit) return explicit;
  if (mode === "none" || authTypes.length === 0) return "none";
  return "subject";
}

function resolveOwnerKind(
  raw: RawConnection,
  credentialMode: CredentialMode,
  context: ConnectionContext,
): OwnerKind {
  const explicit = validOwnerKind(raw.ownerKind);
  if (explicit) return explicit;
  if (credentialMode === "none") return "none";
  return context === "managed_subject" ? "service_account" : "current_user";
}

function productConnectedFromStatus(
  status: IntegrationStatus,
  credentialState: CredentialState,
): boolean {
  // Chosen-account invariant: accounts without a selection are not connected.
  if (status === "needs_instance_selection") return false;
  if (status === "needs_user_connection") return false;
  // No-auth / mode-none is ready to use, not a subject identity.
  if (credentialState === "not_required") return false;
  return (
    status === "ready" ||
    status === "degraded" ||
    credentialState === "connected" ||
    credentialState === "configured"
  );
}

function inferConnectionCredentialState(
  authTypes: AuthType[],
  isNoAuth: boolean,
): CredentialState {
  if (isNoAuth) return "not_required";
  if (authTypes.length > 0) return "missing";
  return "unknown";
}

function inferConnectionStatus(
  authTypes: AuthType[],
  credentialState: CredentialState,
  healthState: HealthState,
  isNoAuth: boolean,
): IntegrationStatus {
  if (healthState === "unhealthy") return "degraded";
  if (credentialState === "invalid") {
    return "needs_user_connection";
  }
  if (credentialState === "missing") {
    return "needs_user_connection";
  }
  if (credentialState === "connected") return "ready";
  if (credentialState === "configured" || credentialState === "not_required") {
    return "ready";
  }
  if (isNoAuth) return "ready";
  if (authTypes.length > 0) return "needs_user_connection";
  return "unknown";
}

function inferConnectionActions(
  raw: RawConnection,
  authTypes: AuthType[],
  status: IntegrationStatus,
  isNoAuth: boolean,
): IntegrationAction[] {
  if (isNoAuth) return [];
  const actions: IntegrationAction[] = [];
  const hasAuth = authTypes.length > 0;

  const instanceCount = raw.instances?.length ?? 0;
  if (status === "needs_instance_selection" && hasAuth) {
    actions.push("select_instance");
  } else if (instanceCount > 0 && hasAuth) {
    actions.push("add_instance");
    // Preferred-account switching stays available after the first selection —
    // not only while status is needs_instance_selection.
    if (instanceCount > 1) {
      actions.push("select_instance");
    }
  } else if (hasAuth) {
    actions.push("connect");
  }

  if ((raw.instances?.length ?? 0) > 0) {
    actions.push("disconnect");
  }
  return actions;
}

function ensureReconnectAction(
  actions: IntegrationAction[],
  authTypes: AuthType[],
  credentialState: CredentialState,
  healthState: HealthState,
  instanceCount: number,
): IntegrationAction[] {
  if (authTypes.length === 0 || instanceCount < 1) return actions;
  const loginRejected =
    credentialState === "invalid" || healthState === "unhealthy";
  if (!loginRejected || actions.includes("reconnect")) return actions;
  return ["reconnect", ...actions.filter((action) => action !== "connect")];
}

/**
 * Switching the preferred account is a product action, not an optional
 * server hint. If more than one account is connected, expose select_instance
 * even when the payload listed other actions and omitted it.
 */
function ensureSelectInstanceAction(
  actions: IntegrationAction[],
  authTypes: AuthType[],
  instanceCount: number,
): IntegrationAction[] {
  if (authTypes.length === 0 || instanceCount < 2) return actions;
  if (actions.includes("select_instance")) return actions;
  return [...actions, "select_instance"];
}

/**
 * Alternative auth methods (OAuth vs API key vs PAT) are OR, not AND.
 * Once any connection is product-connected, unused methods stay as add-account
 * options. They must not roll the app back to "Not connected".
 */
function connectionsForAppRollup(
  connections: NormalizedConnection[],
): NormalizedConnection[] {
  const acting = connections.filter((connection) => connection.connected);
  return acting.length > 0 ? acting : connections;
}

function aggregateStatus(
  connections: NormalizedConnection[],
): IntegrationStatus | undefined {
  if (connections.length === 0) return undefined;
  const canAct = connections.some((connection) => connection.connected);
  const statuses = connectionsForAppRollup(connections).map(
    (connection) => connection.status,
  );
  const severity = (
    canAct
      ? [
          "unavailable",
          "needs_admin_configuration",
          "needs_instance_selection",
          "degraded",
          "unknown",
        ]
      : [
          "unavailable",
          "needs_admin_configuration",
          "needs_user_connection",
          "needs_instance_selection",
          "degraded",
          "unknown",
        ]
  ) satisfies IntegrationStatus[];
  for (const status of severity) {
    if (statuses.includes(status)) return status;
  }
  return "ready";
}

function aggregateCredentialState(
  connections: NormalizedConnection[],
): CredentialState {
  const canAct = connections.some((connection) => connection.connected);
  const states = connectionsForAppRollup(connections).map(
    (connection) => connection.credentialState,
  );
  const order = (
    canAct
      ? ["invalid", "connected", "configured", "unknown"]
      : ["invalid", "missing", "unknown", "connected", "configured"]
  ) satisfies CredentialState[];
  for (const state of order) {
    if (states.includes(state)) return state;
  }
  return "not_required";
}

function aggregateHealthState(connections: NormalizedConnection[]): HealthState {
  const states = connectionsForAppRollup(connections).map(
    (connection) => connection.healthState,
  );
  for (const state of [
    "unhealthy",
    "not_checked",
    "unknown",
    "healthy",
  ] satisfies HealthState[]) {
    if (states.includes(state)) return state;
  }
  return "not_applicable";
}

function inferIntegrationStatus(
  _integration: Integration,
  connections: NormalizedConnection[],
): IntegrationStatus {
  if (connections.some((connection) => connection.connectable)) {
    return "needs_user_connection";
  }
  return connections.some((connection) => connection.isNoAuth) ? "ready" : "unknown";
}

/**
 * Chrome / summary status — answers “what next?” for operators.
 *
 * Invariant: never promote credential *absence* (`not_required`) as a status
 * message. Catalog and Overview both consume this; readiness without a
 * credential step is simply “Ready”. Actionable auth states keep their labels.
 */
function integrationSummaryLabel(
  status: IntegrationStatus,
  credentialState: CredentialState,
  healthState: HealthState,
  context: ConnectionContext,
): string {
  if (credentialState === "invalid" || healthState === "unhealthy") {
    return NEEDS_RECONNECT_LABEL;
  }
  if (credentialState === "not_required" && status === "ready") {
    return "Ready";
  }
  if (credentialState === "configured" && status === "ready") {
    return "Deployment configured";
  }
  if (credentialState === "connected" && status === "ready") {
    return context === "managed_subject" ? IDENTITY_CONNECTED_LABEL : APP_CONNECTED_LABEL;
  }
  return statusDisplayLabel(status, context);
}

function connectionSummaryLabel(
  status: IntegrationStatus,
  credentialState: CredentialState,
  healthState: HealthState,
  isNoAuth: boolean,
  context: ConnectionContext,
): string {
  // No-auth rows are ready without a credential step — same “Ready” as
  // integration chrome. Do not surface “no credentials required”.
  if (isNoAuth && credentialState === "not_required") {
    return statusDisplayLabel(status, context);
  }
  if (credentialState === "invalid" || healthState === "unhealthy") {
    return NEEDS_RECONNECT_LABEL;
  }
  if (credentialState === "connected" && status === "ready") {
    return context === "managed_subject" ? IDENTITY_CONNECTED_LABEL : APP_CONNECTED_LABEL;
  }
  return statusDisplayLabel(status, context);
}

function statusDisplayLabel(
  status: IntegrationStatus,
  context: ConnectionContext,
): string {
  switch (status) {
    case "ready":
      return "Ready";
    case "degraded":
      return "Needs fix";
    case "needs_user_connection":
      return context === "managed_subject"
        ? IDENTITY_CONNECTION_REQUIRED_LABEL
        : APP_NOT_CONNECTED_LABEL;
    case "needs_instance_selection":
      return "Choose an account";
    case "needs_admin_configuration":
      return "Ask an admin to finish configuration";
    case "unavailable":
      return "Unavailable";
    case "unknown":
      return "Status unknown";
  }
}

/**
 * Technical credential-detail line for Connection panels.
 * Only shown when `shouldShowCredentialDetail` (missing/invalid/unknown).
 * `not_required` / no-auth never become chrome — empty string keeps absence silent.
 */
function credentialDisplayLabel(
  state: CredentialState,
  isNoAuth: boolean,
  isManagedSubjectOwned: boolean,
): string {
  if (isNoAuth || state === "not_required") return "";
  switch (state) {
    case "connected":
      return isManagedSubjectOwned
        ? "Identity credentials linked"
        : "User credentials linked";
    case "configured":
      return isManagedSubjectOwned
        ? "Identity credentials configured"
        : "User credentials configured";
    case "missing":
      return isManagedSubjectOwned
        ? "Identity credentials missing"
        : "User credentials missing";
    case "invalid":
      return isManagedSubjectOwned
        ? "Identity credentials invalid"
        : "User credentials invalid";
    case "unknown":
      return isManagedSubjectOwned
        ? "Identity credential status unknown"
        : "Credential status unknown";
  }
}

function healthDisplayLabel(state: HealthState): string | undefined {
  switch (state) {
    case "healthy":
      return "Health healthy";
    case "unhealthy":
      return "Health unhealthy";
    case "not_checked":
      return undefined;
    case "not_applicable":
      return undefined;
    case "unknown":
      return undefined;
  }
}

function ownerDisplayLabel(
  isNoAuth: boolean,
  isManagedSubjectOwned: boolean,
): string {
  if (isNoAuth) return "No credential owner";
  return isManagedSubjectOwned ? "Managed identity owned" : "User owned";
}

function compact(values: (string | undefined)[]): string[] {
  return values.filter((value): value is string => !!value);
}
