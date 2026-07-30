
import { useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent, MouseEvent, ReactNode, SyntheticEvent } from "react";
import {
  AuthType,
  ConnectionParamDef,
  CredentialFieldDef,
  Integration,
} from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import { badgeVariantFromTone } from "@/lib/catalogFilters";
import {
  normalizeIntegrationStatus,
  statusTone,
  type ConnectionContext,
  type NormalizedConnection,
  type NormalizedIntegrationStatus,
} from "@/lib/integrationStatus";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { CheckCircleIcon, CloseIcon } from "./icons";

export type ConnectionPanelView =
  | "default"
  | "disconnect"
  | "instance"
  | "token"
  | "oauth_params";
type ActionKind = "connect" | "add_instance" | "reconnect" | "select_instance";
type ConnectionTarget = {
  instance?: string;
  connection?: string;
};

type AuthAction = {
  key: string;
  kind: ActionKind;
  authType: AuthType;
  connectionKey: string;
  connection?: string;
  label: string;
  variant?: "default" | "secondary";
  requiresInstanceName: boolean;
};

type PendingAuthAction = AuthAction & {
  instance?: string;
};

export interface IntegrationConnectionPanelProps {
  integration: Integration;
  onClose?: () => void;
  onStartOAuth: (
    instance?: string,
    connection?: string,
    connectionParams?: Record<string, string>,
  ) => void;
  onSubmitToken: (
    credential: string | Record<string, string>,
    connectionParams?: Record<string, string>,
    instance?: string,
    connection?: string,
  ) => void;
  onDisconnect: (instance?: string, connection?: string) => void;
  reconnecting: boolean;
  disconnecting: boolean;
  submitting: boolean;
  error: string | null;
  readOnly?: boolean;
  connectionContext?: ConnectionContext;
  initialView?: ConnectionPanelView;
  destructiveActionLabel?: "Disconnect" | "Remove app";
  variant?: "inline" | "dialog";
  /** When false, omit the integration title block (e.g. app detail Credentials section). */
  showHeader?: boolean;
}

function shouldShowIntegrationSummary(status: NormalizedIntegrationStatus): boolean {
  if (status.connected && status.status === "ready") {
    return false;
  }
  return status.status !== "needs_user_connection";
}

function shouldShowConnectionStatusText(connection: NormalizedConnection): boolean {
  if (connection.connected && connection.status === "ready") {
    return false;
  }
  return connection.status !== "needs_user_connection";
}

function normalizeActionKinds(connection: NormalizedConnection): ActionKind[] {
  const kinds: ActionKind[] = [];
  if (connection.canConnect) kinds.push("connect");
  if (connection.canAddInstance) kinds.push("add_instance");
  if (connection.canReconnect) kinds.push("reconnect");
  return kinds;
}

function buildAuthActionLabel(
  connection: NormalizedConnection,
  kind: ActionKind,
  authType: AuthType,
  showConnectionNames: boolean,
): string {
  const dualAuth =
    connection.authTypes.includes("oauth") &&
    connection.authTypes.includes("manual");
  const name = connection.label;

  if (kind === "add_instance") {
    return showConnectionNames ? `Add ${name} connection` : "Add connection";
  }

  if (kind === "reconnect") {
    if (authType === "manual" && dualAuth) {
      return showConnectionNames
        ? `Reconnect ${name} with API token`
        : "Reconnect with API token";
    }
    return showConnectionNames ? `Reconnect ${name}` : "Reconnect";
  }

  if (kind === "select_instance") {
    return showConnectionNames ? `Select ${name} connection` : "Select connection";
  }

  if (authType === "manual") {
    if (dualAuth) {
      return showConnectionNames ? `Use API token for ${name}` : "Use API token";
    }
    return showConnectionNames ? `Connect with ${name}` : "Connect";
  }

  return showConnectionNames
    ? `Connect with ${name}`
    : dualAuth
      ? "Connect with OAuth"
      : "Connect";
}

function buildAuthActions(connections: NormalizedConnection[]): AuthAction[] {
  const actionableConnections = connections.filter(
    (connection) =>
      connection.isSubjectOwned && normalizeActionKinds(connection).length > 0,
  );
  const showConnectionNames = actionableConnections.length > 1;
  const actions: AuthAction[] = [];

  for (const connection of actionableConnections) {
    for (const kind of normalizeActionKinds(connection)) {
      for (const authType of connection.authTypes) {
        actions.push({
          key: `${connection.key}:${kind}:${authType}`,
          kind,
          authType,
          connectionKey: connection.key,
          connection: connection.connection,
          label: buildAuthActionLabel(
            connection,
            kind,
            authType,
            showConnectionNames,
          ),
          variant:
            authType === "manual" && connection.authTypes.includes("oauth")
              ? "secondary"
              : "default",
          requiresInstanceName: kind === "add_instance",
        });
      }
    }
  }

  return actions;
}

function connectionActionCopy(
  connection: NormalizedConnection,
  context: ConnectionContext,
): string | null {
  if (!connection.canAdminConfigure) {
    return null;
  }
  if (context === "managed_subject") {
    return "Ask an admin to configure credentials for this identity.";
  }
  return "Ask an admin to configure deployment-managed credentials.";
}

function disconnectCopy(displayName: string, context: ConnectionContext): string {
  if (context === "managed_subject") {
    return `This will remove this identity's connection to ${displayName}. It can be reconnected later.`;
  }
  return `This will remove your connection to ${displayName}. You can reconnect at any time.`;
}

function isPendingAction(action: AuthAction, pendingAction?: PendingAuthAction) {
  return (
    pendingAction?.kind === action.kind &&
    pendingAction?.authType === action.authType &&
    pendingAction?.connectionKey === action.connectionKey
  );
}

function firstDisconnectableTarget(
  connections: NormalizedConnection[],
): ConnectionTarget | null {
  for (const connection of connections) {
    if (!connection.canDisconnect) continue;
    if (connection.instances.length > 0) {
      const instance = connection.instances[0]!;
      return {
        instance: instance.name,
        connection: instance.connection || connection.connection,
      };
    }
    return { connection: connection.connection };
  }
  return null;
}

function hasConnectionParams(
  connectionParams: Record<string, ConnectionParamDef> | undefined,
): boolean {
  return Boolean(connectionParams && Object.keys(connectionParams).length > 0);
}

const inputClasses = `mt-1.5 w-full ${INPUT_CLASSES}`;

export default function IntegrationConnectionPanel({
  integration,
  onClose,
  onStartOAuth,
  onSubmitToken,
  onDisconnect,
  reconnecting,
  disconnecting,
  submitting,
  error,
  readOnly = false,
  connectionContext = "current_user",
  initialView = "default",
  destructiveActionLabel = "Disconnect",
  variant = "inline",
  showHeader = variant === "dialog",
}: IntegrationConnectionPanelProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const [view, setView] = useState<ConnectionPanelView>(initialView);
  const [disconnectTarget, setDisconnectTarget] = useState<ConnectionTarget>({});
  const [pendingAction, setPendingAction] = useState<PendingAuthAction | undefined>();
  const isDialog = variant === "dialog";

  const displayName = integration.displayName || integration.name;
  const headingId = `connection-panel-heading-${integration.name}`;
  const normalizedStatus = useMemo(
    () => normalizeIntegrationStatus(integration, connectionContext),
    [integration, connectionContext],
  );

  useEffect(() => {
    setView(initialView);
    if (initialView === "disconnect") {
      const target = firstDisconnectableTarget(normalizedStatus.connections);
      setDisconnectTarget(target ?? {});
    }
  }, [initialView, integration.name, normalizedStatus.connections]);

  useEffect(() => {
    if (view !== "disconnect" || disconnectTarget.instance || disconnectTarget.connection) {
      return;
    }
    const target = firstDisconnectableTarget(normalizedStatus.connections);
    if (target) setDisconnectTarget(target);
  }, [disconnectTarget.connection, disconnectTarget.instance, normalizedStatus.connections, view]);

  useEffect(() => {
    if (!isDialog) return;
    dialogRef.current?.showModal();
  }, [isDialog]);

  const authActions = buildAuthActions(normalizedStatus.connections);
  const pendingConnection = pendingAction
    ? normalizedStatus.connections.find(
        (connection) => connection.key === pendingAction.connectionKey,
      )
    : undefined;
  const pendingConnectionParams = pendingConnection?.connectionParams;

  function handleCancel(e: SyntheticEvent<HTMLDialogElement>) {
    if (disconnecting || submitting) {
      e.preventDefault();
    }
  }

  function handleBackdropClick(e: MouseEvent<HTMLDialogElement>) {
    if (e.target === e.currentTarget && !disconnecting && !submitting) {
      e.currentTarget.close();
    }
  }

  function closeDialog() {
    if (!isDialog) {
      onClose?.();
      return;
    }
    dialogRef.current?.close();
  }

  function startAuthAction(action: AuthAction) {
    setPendingAction(action);
    if (action.requiresInstanceName) {
      setView("instance");
    } else if (action.authType === "manual") {
      setView("token");
    } else {
      const connection = normalizedStatus.connections.find(
        (item) => item.key === action.connectionKey,
      );
      if (hasConnectionParams(connection?.connectionParams)) {
        setView("oauth_params");
      } else {
        onStartOAuth(undefined, action.connection);
      }
    }
  }

  function handleInstanceSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const name = (new FormData(e.currentTarget).get("instance_name") as string)?.trim();
    if (!name || !pendingAction) return;
    const action = { ...pendingAction, instance: name };
    setPendingAction(action);
    if (action.authType === "manual") {
      setView("token");
    } else if (hasConnectionParams(pendingConnection?.connectionParams)) {
      setView("oauth_params");
    } else {
      onStartOAuth(action.instance, action.connection);
    }
  }

  function handleOAuthParamsSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    if (!pendingAction || !pendingConnectionParams) return;
    const fd = new FormData(e.currentTarget);
    const collected: Record<string, string> = {};
    for (const name of Object.keys(pendingConnectionParams)) {
      const val = (fd.get(`cp_${name}`) as string)?.trim();
      if (val) collected[name] = val;
    }
    onStartOAuth(
      pendingAction.instance,
      pendingAction.connection,
      Object.keys(collected).length > 0 ? collected : undefined,
    );
  }

  function resolveCredentialFields(): CredentialFieldDef[] | undefined {
    return pendingConnection?.credentialFields;
  }

  function handleTokenSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const fd = new FormData(e.currentTarget);
    const fields = resolveCredentialFields();

    if (!fields?.length || !pendingAction) return;

    let credential: string | Record<string, string>;
    if (fields.length === 1) {
      const val = (fd.get(`cred_${fields[0].name}`) as string)?.trim();
      if (!val) return;
      credential = val;
    } else {
      const creds: Record<string, string> = {};
      for (const field of fields) {
        const val = (fd.get(`cred_${field.name}`) as string)?.trim();
        if (!val) return;
        creds[field.name] = val;
      }
      credential = creds;
    }

    let params: Record<string, string> | undefined;
    if (pendingConnectionParams) {
      const collected: Record<string, string> = {};
      for (const name of Object.keys(pendingConnectionParams)) {
        const val = (fd.get(`cp_${name}`) as string)?.trim();
        if (val) collected[name] = val;
      }
      if (Object.keys(collected).length > 0) params = collected;
    }
    onSubmitToken(
      credential,
      params,
      pendingAction.instance,
      pendingAction.connection,
    );
  }

  function renderStatusBadge(connection: NormalizedConnection) {
    if (!shouldShowConnectionStatusText(connection)) {
      return null;
    }
    const tone = statusTone(
      connection.status,
      connection.credentialState,
      connection.healthState,
    );
    return (
      <Badge size="sm" variant={badgeVariantFromTone(tone)}>
        {connection.summaryLabel}
      </Badge>
    );
  }

  function renderConnectionActions(connection: NormalizedConnection) {
    if (readOnly) {
      return null;
    }
    const actions = authActions.filter(
      (action) => action.connectionKey === connection.key,
    );
    if (actions.length === 0 && !connection.canDisconnect) {
      return null;
    }

    return (
      <div className="mt-4 flex flex-col gap-2 sm:mt-0 sm:items-end">
        {actions.map((action) => (
          <Button
            key={action.key}
            variant={action.variant}
            className="w-full sm:w-auto"
            onClick={() => startAuthAction(action)}
            disabled={reconnecting || submitting}
          >
            {reconnecting && isPendingAction(action, pendingAction)
              ? "Connecting..."
              : action.label}
          </Button>
        ))}
        {connection.canDisconnect && connection.instances.length === 0 ? (
          <Button
            type="button"
            variant="ghostDestructive"
            size="sm"
            onClick={() => {
              setDisconnectTarget({ connection: connection.connection });
              setView("disconnect");
            }}
            disabled={disconnecting}
          >
            Disconnect
          </Button>
        ) : null}
      </div>
    );
  }

  function renderConnectionRow(connection: NormalizedConnection) {
    const actionCopy = connectionActionCopy(connection, connectionContext);
    return (
      <div
        key={connection.key}
        className="rounded-md border border-border px-4 py-3"
      >
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div className="min-w-0">
            <div className="flex items-center gap-2.5">
              {connection.connected ? (
                <CheckCircleIcon className="h-4 w-4 shrink-0 text-success-foreground" />
              ) : null}
              <div className="min-w-0">
                <div className="truncate text-sm font-medium text-foreground">
                  {connection.label}
                </div>
                <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground/70">
                  {connection.detailLines.map((line) => (
                    <span key={line}>{line}</span>
                  ))}
                </div>
              </div>
            </div>

            {actionCopy ? (
              <p className="mt-3 text-xs text-muted-foreground">{actionCopy}</p>
            ) : null}

            {connection.instances.length > 0 ? (
              <div className="mt-3 space-y-2">
                {connection.instances.map((instance) => (
                  <div
                    key={`${connection.key}:${instance.name}`}
                    className="flex items-center justify-between gap-3 rounded-md bg-muted px-3 py-2"
                  >
                    <div>
                      <div className="text-sm text-foreground">{instance.name}</div>
                      {instance.connection ? (
                        <div className="text-xs text-muted-foreground/70">
                          {instance.connection}
                        </div>
                      ) : null}
                    </div>
                    {!readOnly && connection.canDisconnect ? (
                      <Button
                        type="button"
                        variant="ghostDestructive"
                        size="xs"
                        onClick={() => {
                          setDisconnectTarget({
                            instance: instance.name,
                            connection:
                              instance.connection || connection.connection,
                          });
                          setView("disconnect");
                        }}
                        disabled={disconnecting}
                      >
                        Disconnect
                      </Button>
                    ) : null}
                  </div>
                ))}
              </div>
            ) : null}
          </div>
          <div className="shrink-0 sm:text-right">
            {renderStatusBadge(connection)}
            {renderConnectionActions(connection)}
          </div>
        </div>
      </div>
    );
  }

  const disconnectHeading =
    destructiveActionLabel === "Remove app"
      ? `Remove ${displayName}?`
      : `Disconnect ${displayName}?`;

  const panel = (
    <div className={isDialog ? "p-7" : undefined}>
        {view === "disconnect" ? (
          <>
            <h2
              id={headingId}
              className="text-lg font-heading text-foreground"
            >
              {disconnectHeading}
            </h2>
            <p className="mt-3 text-sm text-muted-foreground">
              {disconnectCopy(displayName, connectionContext)}
            </p>
            {error && <p className="mt-3 text-sm text-destructive">{error}</p>}
            <div className="mt-6 flex gap-3">
              <Button
                variant="secondary"
                className="flex-1"
                onClick={() => {
                  setView("default");
                  setDisconnectTarget({});
                }}
                disabled={disconnecting}
              >
                Cancel
              </Button>
              <Button
                variant="destructive"
                className="flex-1"
                onClick={() => onDisconnect(disconnectTarget.instance, disconnectTarget.connection)}
                disabled={
                  disconnecting ||
                  (!disconnectTarget.instance && !disconnectTarget.connection)
                }
              >
                {disconnecting
                  ? destructiveActionLabel === "Remove app"
                    ? "Removing..."
                    : "Disconnecting..."
                  : destructiveActionLabel}
              </Button>
            </div>
          </>
        ) : view === "instance" ? (
          <form onSubmit={handleInstanceSubmit}>
            <h2
              id={headingId}
              className="text-lg font-heading text-foreground"
            >
              Add connection
            </h2>
            {error && <p className="mt-3 text-sm text-destructive">{error}</p>}
            <label
              htmlFor={`instance-name-${integration.name}`}
              className="mt-5 label-text block"
            >
              Connection name
            </label>
            <input
              id={`instance-name-${integration.name}`}
              name="instance_name"
              type="text"
              required
              placeholder="e.g. production, acme-workspace"
              autoFocus
              className={inputClasses}
            />
            <div className="mt-6 flex gap-3">
              <Button
                type="button"
                variant="secondary"
                className="flex-1"
                onClick={() => setView("default")}
              >
                Cancel
              </Button>
              <Button type="submit" className="flex-1">
                Continue
              </Button>
            </div>
          </form>
        ) : view === "token" ? (
          <TokenForm
            integrationName={integration.name}
            headingId={headingId}
            credentialFields={resolveCredentialFields()}
            connectionParams={pendingConnectionParams}
            error={error}
            submitting={submitting}
            onSubmit={handleTokenSubmit}
            onCancel={() =>
              setView(pendingAction?.requiresInstanceName ? "instance" : "default")
            }
          />
        ) : view === "oauth_params" ? (
          <ConnectionParamsForm
            integrationName={integration.name}
            headingId={headingId}
            connectionParams={pendingConnectionParams}
            error={error}
            submitting={reconnecting || submitting}
            onSubmit={handleOAuthParamsSubmit}
            onCancel={() =>
              setView(pendingAction?.requiresInstanceName ? "instance" : "default")
            }
          />
        ) : (
          <>
            {showHeader ? (
              <div className="flex items-start justify-between">
                <div>
                  <h2
                    id={headingId}
                    className="text-lg font-heading text-foreground"
                  >
                    {displayName}
                  </h2>
                  {shouldShowIntegrationSummary(normalizedStatus) ? (
                    <p className="mt-2 text-sm text-muted-foreground">
                      {normalizedStatus.summaryLabel}
                    </p>
                  ) : null}
                </div>
                {isDialog ? (
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-xs"
                    onClick={closeDialog}
                    aria-label="Close"
                  >
                    <CloseIcon className="h-4 w-4" />
                  </Button>
                ) : null}
              </div>
            ) : (
              <h2 id={headingId} className="sr-only">
                Credentials for {displayName}
              </h2>
            )}

            {error && <p className="mt-3 text-sm text-destructive">{error}</p>}

            <div className={showHeader ? "mt-5 space-y-3" : "space-y-3"}>
              {normalizedStatus.connections.map(renderConnectionRow)}
            </div>
          </>
        )}
      </div>
  );

  if (!isDialog) {
    return (
      <div
        aria-labelledby={headingId}
        className="text-card-foreground"
        data-testid={`integration-connection-${integration.name}`}
      >
        {panel}
      </div>
    );
  }

  return (
    <dialog
      ref={dialogRef}
      aria-labelledby={headingId}
      onCancel={handleCancel}
      onClose={() => onClose?.()}
      onClick={handleBackdropClick}
      className="m-auto w-full max-w-lg rounded-lg border border-border bg-card p-0 text-card-foreground shadow-dropdown"
    >
      {panel}
    </dialog>
  );
}

const LINK_RE = /(\[[^\]]+\]\(https?:\/\/[^)]+\))/;
const LINK_MATCH_RE = /^\[([^\]]+)\]\((https?:\/\/[^)]+)\)$/;

function renderLinkedText(text: string): ReactNode[] {
  return text.split(LINK_RE).map((seg, i) => {
    const m = seg.match(LINK_MATCH_RE);
    if (!m) return seg;
    return <a key={i} href={m[2]} target="_blank" rel="noopener noreferrer" className="text-primary hover:underline">{m[1]}</a>;
  });
}

function ConnectionParamsForm({
  integrationName,
  headingId,
  connectionParams,
  error,
  submitting,
  onSubmit,
  onCancel,
}: {
  integrationName: string;
  headingId: string;
  connectionParams: Record<string, ConnectionParamDef> | undefined;
  error: string | null;
  submitting: boolean;
  onSubmit: (e: FormEvent<HTMLFormElement>) => void;
  onCancel: () => void;
}) {
  if (!connectionParams) return null;

  return (
    <form onSubmit={onSubmit}>
      <h2 id={headingId} className="text-lg font-heading text-foreground">
        Connection details
      </h2>
      {Object.entries(connectionParams).map(([name, def]) => (
        <div key={name} className="mt-3">
          <label
            htmlFor={`cp_${name}-${integrationName}`}
            className="label-text block"
          >
            {def.description || name}
          </label>
          <input
            id={`cp_${name}-${integrationName}`}
            name={`cp_${name}`}
            type="text"
            required={def.required}
            defaultValue={def.default}
            placeholder={name}
            className={inputClasses}
          />
        </div>
      ))}
      {error ? <p className="mt-3 text-sm text-destructive">{error}</p> : null}
      <div className="mt-6 flex gap-3">
        <Button
          type="button"
          variant="secondary"
          className="flex-1"
          onClick={onCancel}
          disabled={submitting}
        >
          Cancel
        </Button>
        <Button type="submit" className="flex-1" disabled={submitting}>
          {submitting ? "Connecting..." : "Continue"}
        </Button>
      </div>
    </form>
  );
}

function TokenForm({
  integrationName,
  headingId,
  credentialFields,
  connectionParams,
  error,
  submitting,
  onSubmit,
  onCancel,
}: {
  integrationName: string;
  headingId: string;
  credentialFields: CredentialFieldDef[] | undefined;
  connectionParams: Record<string, ConnectionParamDef> | undefined;
  error: string | null;
  submitting: boolean;
  onSubmit: (e: FormEvent<HTMLFormElement>) => void;
  onCancel: () => void;
}) {
  const fields = credentialFields ?? [];
  const heading = fields.length === 1 ? (fields[0].label || fields[0].name) : "Enter credentials";

  return (
    <form onSubmit={onSubmit}>
      <h2
        id={headingId}
        className="text-lg font-heading text-foreground"
      >
        {heading}
      </h2>
      {connectionParams && Object.entries(connectionParams).map(([name, def]) => (
        <div key={name} className="mt-3">
          <label
            htmlFor={`cp_${name}-${integrationName}`}
            className="label-text block"
          >
            {def.description || name}
          </label>
          <input
            id={`cp_${name}-${integrationName}`}
            name={`cp_${name}`}
            type="text"
            required={def.required}
            defaultValue={def.default}
            placeholder={name}
            className={inputClasses}
          />
        </div>
      ))}
      {fields.map((field, idx) => (
        <div key={field.name} className="mt-4">
          <label
            htmlFor={`cred_${field.name}-${integrationName}`}
            className="label-text block"
          >
            {field.label || field.name}
          </label>
          {field.description && (
            <p className="mt-1 text-xs text-muted-foreground/70 normal-case tracking-normal">{renderLinkedText(field.description)}</p>
          )}
          <input
            id={`cred_${field.name}-${integrationName}`}
            name={`cred_${field.name}`}
            type="password"
            required
            placeholder={field.label || field.name}
            autoFocus={idx === 0}
            className={inputClasses}
          />
        </div>
      ))}
      {error && <p className="mt-3 text-sm text-destructive">{error}</p>}
      <div className="mt-6 flex gap-3">
        <Button
          type="button"
          variant="secondary"
          className="flex-1"
          onClick={onCancel}
          disabled={submitting}
        >
          Cancel
        </Button>
        <Button type="submit" className="flex-1" disabled={submitting}>
          {submitting ? "Connecting..." : "Submit"}
        </Button>
      </div>
    </form>
  );
}
