import type {
  AuthType,
  ConnectionDefInfo,
  ConnectionMode,
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

export type ConnectionContext = "current_user" | "managed_identity";
export type StatusTone = "success" | "warning" | "danger" | "neutral";

export type NormalizedConnection = {
  key: string;
  connection?: string;
  label: string;
  authTypes: AuthType[];
  credentialFields?: CredentialFieldDef[];
  instances: InstanceInfo[];
  status: IntegrationStatus;
  credentialState: CredentialState;
  healthState: HealthState;
  actions: IntegrationAction[];
  actionSource: "server" | "legacy";
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
  isPlatformManaged: boolean;
  isNoAuth: boolean;
  isSubjectOwned: boolean;
  isManagedIdentityOwned: boolean;
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

const MODES: ConnectionMode[] = ["none", "user", "platform"];
const CREDENTIAL_MODES: CredentialMode[] = ["none", "subject", "platform"];
const OWNER_KINDS: OwnerKind[] = [
  "none",
  "current_user",
  "managed_identity",
  "service_account",
  "platform",
  "unknown",
];

type RawConnection = {
  name?: string;
  displayName?: string;
  authTypes?: AuthType[];
  credentialFields?: CredentialFieldDef[];
  status?: IntegrationStatus;
  credentialState?: CredentialState;
  healthState?: HealthState;
  actions?: IntegrationAction[];
  mode?: ConnectionMode;
  credentialMode?: CredentialMode;
  ownerKind?: OwnerKind;
  disconnectable?: boolean;
  connected?: boolean;
  connectable?: boolean;
  instances?: InstanceInfo[];
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
  const status =
    validStatus(integration.status) ??
    aggregateStatus(connections) ??
    inferIntegrationStatus(integration, connections);
  const credentialState =
    validCredentialState(integration.credentialState) ??
    aggregateCredentialState(connections);
  const healthState =
    validHealthState(integration.healthState) ??
    aggregateHealthState(connections);
  const connected =
    credentialState === "connected" ||
    credentialState === "configured" ||
    credentialState === "not_required" ||
    connections.some((connection) => connection.connected);
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
    summaryLabel: integrationSummaryLabel(status, credentialState, context),
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

  return [
    {
      name: undefined,
      displayName: integration.displayName || integration.name,
      authTypes: integration.authTypes,
      credentialFields: integration.credentialFields,
      status: integration.status,
      credentialState: integration.credentialState,
      healthState: integration.healthState,
      actions: integration.actions,
      connected: integration.connected,
      instances: integration.instances ?? [],
    },
  ];
}

function connectionInstances(
  integration: Integration,
  connection: ConnectionDefInfo,
): InstanceInfo[] {
  const nested = connection.instances ?? [];
  const topLevel =
    integration.instances?.filter((instance) => instance.connection === connection.name) ??
    [];
  const seen = new Set<string>();
  const out: InstanceInfo[] = [];
  for (const instance of [...nested, ...topLevel]) {
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
  const isPlatformManaged =
    credentialMode === "platform" ||
    mode === "platform" ||
    ownerKind === "platform";
  const hasExplicitOwnerMode =
    !!validCredentialMode(raw.credentialMode) ||
    !!validMode(raw.mode) ||
    !!validOwnerKind(raw.ownerKind);
  const isNoAuth =
    !isPlatformManaged &&
    (validCredentialMode(raw.credentialMode) === "none" ||
      validMode(raw.mode) === "none" ||
      validCredentialState(raw.credentialState) === "not_required" ||
      (authTypes.length === 0 && !hasExplicitOwnerMode));
  const isManagedIdentityOwned =
    ownerKind === "managed_identity" || context === "managed_identity";
  const isSubjectOwned =
    !isPlatformManaged &&
    !isNoAuth &&
    (credentialMode === "subject" ||
      mode === "user" ||
      ownerKind === "current_user" ||
      ownerKind === "managed_identity" ||
      ownerKind === "service_account" ||
      authTypes.length > 0);
  const credentialState =
    validCredentialState(raw.credentialState) ??
    inferConnectionCredentialState(raw, authTypes, isPlatformManaged, isNoAuth);
  const healthState = validHealthState(raw.healthState) ?? "unknown";
  const status =
    validStatus(raw.status) ??
    inferConnectionStatus(
      raw,
      authTypes,
      credentialState,
      healthState,
      isPlatformManaged,
      isNoAuth,
    );
  const actions = validActions(raw.actions);
  const inferredActions = actions.length
    ? actions
    : inferConnectionActions(raw, authTypes, status, isPlatformManaged, isNoAuth);
  const disconnectable =
    raw.disconnectable ?? inferredActions.includes("disconnect");
  const connected =
    raw.connected === true ||
    credentialState === "connected" ||
    credentialState === "configured" ||
    credentialState === "not_required" ||
    status === "ready" ||
    status === "degraded";
  const connectable =
    raw.connectable === true ||
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
    isPlatformManaged,
    isNoAuth,
    context,
  );
  const statusLabel = statusDisplayLabel(status, context);
  const credentialLabel = credentialDisplayLabel(
    credentialState,
    isPlatformManaged,
    isNoAuth,
    isManagedIdentityOwned,
  );
  const healthLabel = healthDisplayLabel(healthState);
  const ownerLabel = ownerDisplayLabel(
    isPlatformManaged,
    isNoAuth,
    isManagedIdentityOwned,
  );
  const isMCPPassthrough = raw.mcpPassthrough === true;
  const shouldShowCredentialDetail =
    credentialState === "missing" ||
    credentialState === "invalid" ||
    credentialState === "unknown";
  const detailLines = compact([
    isMCPPassthrough ? "MCP passthrough" : undefined,
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
    credentialFields: raw.credentialFields?.length
      ? raw.credentialFields
      : integration.credentialFields,
    instances: raw.instances ?? [],
    status,
    credentialState,
    healthState,
    actions: inferredActions,
    actionSource: actions.length ? "server" : "legacy",
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
    isPlatformManaged,
    isNoAuth,
    isSubjectOwned,
    isManagedIdentityOwned,
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
  return "user";
}

function resolveCredentialMode(
  raw: RawConnection,
  mode: ConnectionMode,
  authTypes: AuthType[],
): CredentialMode {
  const explicit = validCredentialMode(raw.credentialMode);
  if (explicit) return explicit;
  if (mode === "platform") return "platform";
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
  if (credentialMode === "platform") return "platform";
  if (credentialMode === "none") return "none";
  return context === "managed_identity" ? "managed_identity" : "current_user";
}

function inferConnectionCredentialState(
  raw: RawConnection,
  authTypes: AuthType[],
  isPlatformManaged: boolean,
  isNoAuth: boolean,
): CredentialState {
  if (isNoAuth) return "not_required";
  if (raw.connected === true) return isPlatformManaged ? "configured" : "connected";
  if (isPlatformManaged) return "unknown";
  if (authTypes.length > 0 || raw.connectable === true) return "missing";
  return "unknown";
}

function inferConnectionStatus(
  raw: RawConnection,
  authTypes: AuthType[],
  credentialState: CredentialState,
  healthState: HealthState,
  isPlatformManaged: boolean,
  isNoAuth: boolean,
): IntegrationStatus {
  if (healthState === "unhealthy") return "degraded";
  if (credentialState === "invalid") {
    return isPlatformManaged ? "needs_admin_configuration" : "needs_user_connection";
  }
  if (credentialState === "missing") {
    return isPlatformManaged ? "needs_admin_configuration" : "needs_user_connection";
  }
  if (raw.connected === true || credentialState === "connected") return "ready";
  if (credentialState === "configured" || credentialState === "not_required") {
    return "ready";
  }
  if (isNoAuth) return "ready";
  if (authTypes.length > 0 || raw.connectable === true) return "needs_user_connection";
  return "unknown";
}

function inferConnectionActions(
  raw: RawConnection,
  authTypes: AuthType[],
  status: IntegrationStatus,
  isPlatformManaged: boolean,
  isNoAuth: boolean,
): IntegrationAction[] {
  if (isPlatformManaged || isNoAuth) return [];
  const actions: IntegrationAction[] = [];
  const hasAuth = authTypes.length > 0 || raw.connectable === true;

  if (status === "needs_instance_selection" && hasAuth) {
    actions.push("select_instance");
  } else if (raw.connected === true && hasAuth) {
    actions.push("add_instance");
  } else if (hasAuth) {
    actions.push("connect");
  }

  if (raw.connected === true && (raw.instances?.length ?? 0) > 0) {
    actions.push("disconnect");
  }
  if (raw.disconnectable === false) {
    return actions.filter((action) => action !== "disconnect");
  }
  if (raw.disconnectable === true && !actions.includes("disconnect")) {
    actions.push("disconnect");
  }
  return actions;
}

function aggregateStatus(
  connections: NormalizedConnection[],
): IntegrationStatus | undefined {
  if (connections.length === 0) return undefined;
  const statuses = connections.map((connection) => connection.status);
  for (const status of [
    "unavailable",
    "needs_admin_configuration",
    "needs_user_connection",
    "needs_instance_selection",
    "degraded",
    "unknown",
  ] satisfies IntegrationStatus[]) {
    if (statuses.includes(status)) return status;
  }
  return "ready";
}

function aggregateCredentialState(
  connections: NormalizedConnection[],
): CredentialState {
  const states = connections.map((connection) => connection.credentialState);
  for (const state of [
    "invalid",
    "missing",
    "unknown",
    "connected",
    "configured",
  ] satisfies CredentialState[]) {
    if (states.includes(state)) return state;
  }
  return "not_required";
}

function aggregateHealthState(connections: NormalizedConnection[]): HealthState {
  const states = connections.map((connection) => connection.healthState);
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
  integration: Integration,
  connections: NormalizedConnection[],
): IntegrationStatus {
  if (integration.connected === true) return "ready";
  if (connections.some((connection) => connection.connectable)) {
    return "needs_user_connection";
  }
  return connections.some((connection) => connection.isNoAuth) ? "ready" : "unknown";
}

function integrationSummaryLabel(
  status: IntegrationStatus,
  credentialState: CredentialState,
  context: ConnectionContext,
): string {
  if (credentialState === "not_required" && status === "ready") {
    return "No credentials required";
  }
  if (credentialState === "configured" && status === "ready") {
    return "Deployment configured";
  }
  if (credentialState === "connected" && status === "ready") {
    return context === "managed_identity" ? "Identity connected" : "Connected";
  }
  return statusDisplayLabel(status, context);
}

function connectionSummaryLabel(
  status: IntegrationStatus,
  credentialState: CredentialState,
  isPlatformManaged: boolean,
  isNoAuth: boolean,
  context: ConnectionContext,
): string {
  if (isNoAuth && credentialState === "not_required") {
    return "No credentials required";
  }
  if (isPlatformManaged && credentialState === "configured") {
    return "Deployment configured";
  }
  if (credentialState === "connected" && status === "ready") {
    return context === "managed_identity" ? "Identity connected" : "Connected";
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
      return "Degraded";
    case "needs_user_connection":
      return context === "managed_identity"
        ? "Identity connection required"
        : "Not connected";
    case "needs_instance_selection":
      return "Instance selection required";
    case "needs_admin_configuration":
      return "Admin configuration required";
    case "unavailable":
      return "Unavailable";
    case "unknown":
      return "Status unknown";
  }
}

function credentialDisplayLabel(
  state: CredentialState,
  isPlatformManaged: boolean,
  isNoAuth: boolean,
  isManagedIdentityOwned: boolean,
): string {
  if (isNoAuth) return "No credentials required";
  if (isPlatformManaged) {
    switch (state) {
      case "configured":
      case "connected":
        return "Deployment-managed credentials configured";
      case "missing":
      case "invalid":
        return "Deployment-managed credentials unconfigured";
      case "not_required":
        return "No credentials required";
      case "unknown":
        return "Deployment-managed credential status unknown";
    }
  }
  switch (state) {
    case "connected":
      return isManagedIdentityOwned
        ? "Identity credentials connected"
        : "User credentials connected";
    case "configured":
      return isManagedIdentityOwned
        ? "Identity credentials configured"
        : "User credentials configured";
    case "missing":
      return isManagedIdentityOwned
        ? "Identity credentials missing"
        : "User credentials missing";
    case "invalid":
      return isManagedIdentityOwned
        ? "Identity credentials invalid"
        : "User credentials invalid";
    case "not_required":
      return "No credentials required";
    case "unknown":
      return isManagedIdentityOwned
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
  isPlatformManaged: boolean,
  isNoAuth: boolean,
  isManagedIdentityOwned: boolean,
): string {
  if (isPlatformManaged) return "Deployment managed";
  if (isNoAuth) return "No credential owner";
  return isManagedIdentityOwned ? "Managed identity owned" : "User owned";
}

function compact(values: (string | undefined)[]): string[] {
  return values.filter((value): value is string => !!value);
}
