
import { useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import {
  ConnectionParamDef,
  CredentialFieldDef,
  Integration,
} from "@/lib/api";
import {
  authActionStart,
  buildAuthActions,
  hasConnectionParams,
  seedPendingAuthAction,
  type ConnectionAuthAction,
} from "@/lib/connectionAuthActions";
import { CircleAlert } from "lucide-react";
import { cn } from "@/lib/cn";
import {
  ACCOUNT_NAME_FALLBACK,
  SIGNING_IN_LABEL,
  SIGN_IN_DETAILS_HEADING,
  connectionForAppAriaLabel,
  connectAppActionLabel,
} from "@/lib/accountCopy";
import { INPUT_CLASSES } from "@/lib/constants";
import { disclosureCaretClassName } from "@/lib/disclosure-caret";
import {
  alertVariantFromTone,
  badgeVariantFromTone,
} from "@/lib/catalogFilters";
import {
  connectionNeedsReconnect,
  NEEDS_RECONNECT_LABEL,
  normalizeIntegrationStatus,
  statusTone,
  type ConnectionContext,
  type NormalizedConnection,
} from "@/lib/integrationStatus";
import {
  humanizeConnectionName,
  accountInitials,
  accountIdentityLines,
  accountRelationshipLabel,
  addAccountFormCopy,
  disconnectConfirmCopy,
  disconnectConfirmAccountLabel,
  USE_ACCOUNT_LABEL,
  DEFAULT_ACCOUNT_LABEL,
  OTHER_CONNECTION_METHODS_LABEL,
  connectionPanelAttention,
  connectionMethodTitle,
  connectionMethodPurpose,
  connectionMethodShortName,
  connectionDialogCopy,
  partitionConnectionMethods,
  shouldScopeInUseBadge,
  isInUseRelationship,
} from "@/features/app-workspace/connection-surface-copy";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Field,
  FieldContent,
  FieldDescription,
  FieldLabel,
} from "@/components/ui/field";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item";
import { CloseIcon, ChevronDownIcon } from "./icons";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  SectionHeader,
  SectionHeaderActions,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";

export type ConnectionPanelView =
  | "default"
  /** Opens the disconnect AlertDialog once; does not replace the panel body. */
  | "disconnect"
  | "instance"
  | "token"
  | "oauth_params";
type ConnectionTarget = {
  instance?: string;
  connection?: string;
};

type PendingAuthAction = ConnectionAuthAction & {
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
  ) => void | Promise<boolean | void>;
  onDisconnect: (instance?: string, connection?: string) => void | Promise<void>;
  onSelectInstance?: (
    instance: string,
    connection?: string,
  ) => void | Promise<void>;
  reconnecting: boolean;
  disconnecting: boolean;
  selectingInstance?: boolean;
  submitting: boolean;
  error: string | null;
  onClearError?: () => void;
  readOnly?: boolean;
  connectionContext?: ConnectionContext;
  initialView?: ConnectionPanelView;
  destructiveActionLabel?: "Disconnect" | "Remove app";
  variant?: "inline" | "dialog";
  /** When false, omit the integration title block (e.g. app Connection section). */
  showHeader?: boolean;
  /**
   * Hide the per-connection SectionHeader (title + actions). Use on the app
   * Connection page where PageHeader owns the title and primary actions.
   */
  omitSectionHeader?: boolean;
  /** Receives primary auth actions to place in PageHeaderActions. */
  onHeaderActionsChange?: (actions: ReactNode | null) => void;
  /** Fired when the disconnect confirm dialog is dismissed (cancel / overlay). */
  onDisconnectDialogClose?: () => void;
}

function shouldShowConnectionStatusText(connection: NormalizedConnection): boolean {
  if (connectionNeedsReconnect(connection)) {
    return true;
  }
  if (connection.connected && connection.status === "ready") {
    return false;
  }
  return connection.status !== "needs_user_connection";
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

function isPendingAction(action: ConnectionAuthAction, pendingAction?: PendingAuthAction) {
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

/** App-level remove: DELETE without `_instance` so every linked account goes. */
function removeAppDisconnectTarget(
  connections: NormalizedConnection[],
): ConnectionTarget | null {
  for (const connection of connections) {
    if (!connection.canDisconnect) continue;
    return {
      connection: connection.connection || connection.key,
    };
  }
  return null;
}

const inputClasses = `mt-1.5 w-full ${INPUT_CLASSES}`;

export default function IntegrationConnectionPanel({
  integration,
  onClose,
  onStartOAuth,
  onSubmitToken,
  onDisconnect,
  onSelectInstance,
  reconnecting,
  disconnecting,
  selectingInstance = false,
  submitting,
  error,
  onClearError,
  readOnly = false,
  connectionContext = "current_user",
  initialView = "default",
  destructiveActionLabel = "Disconnect",
  variant = "inline",
  showHeader = variant === "dialog",
  omitSectionHeader = false,
  onHeaderActionsChange,
  onDisconnectDialogClose,
}: IntegrationConnectionPanelProps) {
  const wasDisconnectingRef = useRef(false);
  const disconnectSeededRef = useRef(false);
  const [view, setView] = useState<ConnectionPanelView>(
    initialView === "disconnect" ? "default" : initialView,
  );
  const [disconnectTarget, setDisconnectTarget] = useState<ConnectionTarget>({});
  const [pendingAction, setPendingAction] = useState<PendingAuthAction | undefined>(
    () => seedPendingAuthAction(integration, connectionContext, initialView),
  );
  const [settingsOpen, setSettingsOpen] = useState(true);
  const isDialog = variant === "dialog";

  const displayName = integration.displayName || integration.name;
  const headingId = `connection-panel-heading-${integration.name}`;
  const normalizedStatus = useMemo(
    () => normalizeIntegrationStatus(integration, connectionContext),
    [integration, connectionContext],
  );

  const addAccountCopy = useMemo(() => {
    const showKey =
      Boolean(pendingAction) && normalizedStatus.connections.length > 1;
    const keyLabel = showKey
      ? humanizeConnectionName(
          normalizedStatus.connections.find(
            (c) => c.key === pendingAction?.connectionKey,
          )?.label ?? "",
        )
      : null;
    return addAccountFormCopy({
      appDisplayName: displayName,
      connectionKeyLabel:
        keyLabel && keyLabel !== ACCOUNT_NAME_FALLBACK ? keyLabel : null,
    });
  }, [normalizedStatus.connections, pendingAction]);

  useEffect(() => {
    if (initialView === "disconnect") {
      setView("default");
      if (disconnectSeededRef.current) return;
      const target =
        destructiveActionLabel === "Remove app"
          ? removeAppDisconnectTarget(normalizedStatus.connections)
          : firstDisconnectableTarget(normalizedStatus.connections);
      if (!target) return;
      setDisconnectTarget(target);
      disconnectSeededRef.current = true;
      return;
    }
    disconnectSeededRef.current = false;
    setView(initialView);
    setPendingAction(
      seedPendingAuthAction(integration, connectionContext, initialView),
    );
  }, [
    initialView,
    destructiveActionLabel,
    integration.name,
    normalizedStatus.connections,
  ]);

  useEffect(() => {
    if (wasDisconnectingRef.current && !disconnecting && !error) {
      setDisconnectTarget({});
    }
    wasDisconnectingRef.current = disconnecting;
  }, [disconnecting, error]);

  const authActions = buildAuthActions(normalizedStatus.connections, displayName);
  const { primary: primaryConnections, other: otherConnections } =
    partitionConnectionMethods(normalizedStatus.connections);
  const otherConnectionKeys = new Set(
    otherConnections.map((connection) => connection.key),
  );
  const headerAuthActions = authActions.filter(
    (action) => !otherConnectionKeys.has(action.connectionKey),
  );
  const scopeInUseBadge = shouldScopeInUseBadge(normalizedStatus.connections);
  const dialogCopy = connectionDialogCopy(normalizedStatus, displayName);
  const pendingConnection = pendingAction
    ? normalizedStatus.connections.find(
        (connection) => connection.key === pendingAction.connectionKey,
      )
    : undefined;
  const pendingConnectionParams = pendingConnection?.connectionParams;

  function startAuthAction(action: ConnectionAuthAction) {
    setPendingAction(action);
    const start = authActionStart(action, normalizedStatus.connections);
    if (start.kind === "form") {
      setView(start.view);
      return;
    }
    onStartOAuth(start.instance, start.connection);
  }

  const headerActionSignature = [
    omitSectionHeader ? "1" : "0",
    view,
    readOnly ? "1" : "0",
    reconnecting ? "1" : "0",
    submitting ? "1" : "0",
    disconnecting ? "1" : "0",
    selectingInstance ? "1" : "0",
    pendingAction?.key ?? "",
    headerAuthActions.map((action) => action.key).join(","),
    otherConnections.map((connection) => connection.key).join(","),
    normalizedStatus.connections
      .map(
        (connection) =>
          `${connection.key}:${connection.canDisconnect ? 1 : 0}:${connection.instances.length}`,
      )
      .join("|"),
  ].join(";");

  useEffect(() => {
    if (!onHeaderActionsChange) return;
    // Keep header CTAs while the add-account dialog overlays the list.
    if (
      !omitSectionHeader ||
      (view !== "default" && view !== "instance") ||
      readOnly
    ) {
      onHeaderActionsChange(null);
      return;
    }

    const actionButtons = headerAuthActions.map((action) => (
      <Button
        key={action.key}
        type="button"
        variant={action.variant}
        onClick={() => startAuthAction(action)}
        disabled={reconnecting || submitting || disconnecting || selectingInstance}
      >
        {reconnecting && isPendingAction(action, pendingAction)
          ? SIGNING_IN_LABEL
          : action.label}
      </Button>
    ));

    const disconnectWithoutAccounts = normalizedStatus.connections.flatMap(
      (connection) => {
        if (!connection.canDisconnect || connection.instances.length > 0) {
          return [];
        }
        return [
          <Button
            key={`disconnect:${connection.key}`}
            type="button"
            variant="danger"
            size="sm"
            onClick={() => {
              onClearError?.();
              setDisconnectTarget({ connection: connection.connection });
            }}
            disabled={disconnecting || selectingInstance}
          >
            Disconnect
          </Button>,
        ];
      },
    );

    const actions = [...actionButtons, ...disconnectWithoutAccounts];
    onHeaderActionsChange(actions.length > 0 ? <>{actions}</> : null);
    return () => onHeaderActionsChange(null);
  }, [headerActionSignature, onHeaderActionsChange]);

  function closeDialog() {
    if (!isDialog) {
      onClose?.();
      return;
    }
    setSettingsOpen(false);
    onClose?.();
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
    void (async () => {
      const ok = await onSubmitToken(
        credential,
        params,
        pendingAction.instance,
        pendingAction.connection,
      );
      // Hook returns false on failure; void callers (legacy) treat as success.
      if (ok === false) return;
      setPendingAction(undefined);
      setView("default");
    })();
  }

  function renderStatusBadge(connection: NormalizedConnection) {
    // Attention / recovery states use Alert — never a status Badge.
    if (connectionPanelAttention(connection)) {
      return null;
    }
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

  function renderConnectionAttention(connection: NormalizedConnection) {
    const attention = connectionPanelAttention(connection);
    if (!attention) return null;
    const tone = statusTone(
      connection.status,
      connection.credentialState,
      connection.healthState,
    );
    return (
      <Alert
        variant={alertVariantFromTone(tone)}
        data-testid={`connection-attention-${connection.key}`}
      >
        <CircleAlert aria-hidden />
        <AlertTitle>{attention.title}</AlertTitle>
        <AlertDescription>{attention.description}</AlertDescription>
      </Alert>
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
      <>
        {actions.map((action) => (
          <Button
            key={action.key}
            variant={action.variant}
            size="sm"
            onClick={() => startAuthAction(action)}
            disabled={reconnecting || submitting}
          >
            {reconnecting && isPendingAction(action, pendingAction)
              ? SIGNING_IN_LABEL
              : action.label}
          </Button>
        ))}
        {connection.canDisconnect && connection.instances.length === 0 ? (
          <Button
            type="button"
            variant="danger"
            size="sm"
            onClick={() => {
              onClearError?.();
              setDisconnectTarget({ connection: connection.connection });
            }}
            disabled={disconnecting}
          >
            Disconnect
          </Button>
        ) : null}
      </>
    );
  }

  function renderConnectionRow(
    connection: NormalizedConnection,
    opts?: { forceSectionActions?: boolean },
  ) {
    const actionCopy = connectionActionCopy(connection, connectionContext);
    const connectionTitle = connectionMethodTitle(connection);
    const titleId = `connection-section-${integration.name}-${connection.key}`;
    const purpose = connectionMethodPurpose(connection);
    const description =
      actionCopy ||
      (connection.isMCPPassthrough
        ? connection.detailLines[0] || purpose
        : purpose);
    const statusBadge = renderStatusBadge(connection);
    const attention = renderConnectionAttention(connection);
    const showSectionActions = opts?.forceSectionActions || !omitSectionHeader;
    const connectionActions = showSectionActions
      ? renderConnectionActions(connection)
      : null;
    const showSectionHeader =
      (!omitSectionHeader ||
        connection.isMCPPassthrough ||
        normalizedStatus.connections.length > 1) &&
      (Boolean(statusBadge) ||
        Boolean(connectionActions) ||
        Boolean(description) ||
        connection.connected ||
        connection.isMCPPassthrough ||
        normalizedStatus.connections.length > 1);

    return (
      <section
        key={connection.key}
        className="space-y-3"
        aria-labelledby={showSectionHeader ? titleId : undefined}
        aria-label={showSectionHeader ? undefined : connectionTitle}
        data-testid={`connection-section-${connection.key}`}
      >
        {showSectionHeader ? (
          <SectionHeader>
            <SectionHeaderContent size="xs">
              <SectionHeaderTitle as="h3" id={titleId}>
                {connectionTitle}
              </SectionHeaderTitle>
              {description ? (
                <SectionHeaderDescription>{description}</SectionHeaderDescription>
              ) : null}
            </SectionHeaderContent>
            {statusBadge || connectionActions ? (
              <SectionHeaderActions>
                {statusBadge}
                {connectionActions}
              </SectionHeaderActions>
            ) : null}
          </SectionHeader>
        ) : null}

        {attention}

        {connection.instances.length > 0 ? (
          <ItemGroup className="gap-3" data-testid="connection-account-list">
            {[...connection.instances]
              .sort((a, b) => Number(Boolean(b.preferred)) - Number(Boolean(a.preferred)))
              .map((instance) => {
              const instanceLabel = humanizeConnectionName(
                instance.name,
                DEFAULT_ACCOUNT_LABEL,
              );
              const connectionKeyLabel = instance.connection
                ? humanizeConnectionName(instance.connection)
                : null;
              const showConnectionKey =
                Boolean(connectionKeyLabel) &&
                connectionKeyLabel !== instanceLabel &&
                connectionKeyLabel !== connectionTitle;
              const accountDescription = accountRelationshipLabel({
                preferred: instance.preferred,
                needsInstanceSelection:
                  connection.status === "needs_instance_selection" ||
                  normalizedStatus.status === "needs_instance_selection",
                connectionKeyLabel: showConnectionKey ? connectionKeyLabel : null,
                soleLinkedAccount:
                  connectionNeedsReconnect(connection) &&
                  connection.instances.length === 1,
                methodScope: scopeInUseBadge
                  ? connectionMethodShortName(connection)
                  : null,
              });
              const loginRejected = connectionNeedsReconnect(connection);
              const { primary: identityPrimary, additional: identityAdditional } =
                accountIdentityLines(instance.identity);
              const canUseAccount =
                !readOnly &&
                Boolean(onSelectInstance) &&
                connection.canSelectInstance &&
                connection.instances.length > 1 &&
                !instance.preferred;
              return (
                <Item
                  key={`${connection.key}:${instance.name}`}
                  variant="outline"
                  className="items-baseline"
                  role="listitem"
                  data-testid={`connection-account-${instance.name}`}
                  data-account-name={instance.name}
                  data-preferred={instance.preferred ? "true" : undefined}
                >
                    <ItemMedia>
                      <Avatar size="lg" aria-hidden>
                        <AvatarFallback>
                          {accountInitials(instanceLabel)}
                        </AvatarFallback>
                      </Avatar>
                    </ItemMedia>
                    <ItemContent>
                      <div className="flex flex-wrap items-center gap-2">
                        <ItemTitle>{instanceLabel}</ItemTitle>
                        {isInUseRelationship(accountDescription) ? (
                          <>
                            <Badge
                              variant={loginRejected ? "outline" : "success"}
                              size="sm"
                            >
                              {accountDescription}
                            </Badge>
                            {loginRejected ? (
                              <Badge variant="warning" size="sm">
                                {NEEDS_RECONNECT_LABEL}
                              </Badge>
                            ) : null}
                          </>
                        ) : null}
                      </div>
                      {identityPrimary ? (
                        <ItemDescription data-testid="connection-account-identity-primary">
                          {identityPrimary.value}
                        </ItemDescription>
                      ) : null}
                      {identityAdditional.map((fact) => (
                        <ItemDescription
                          key={`${fact.kind}:${fact.value}`}
                          className="text-muted-foreground/80"
                          data-testid="connection-account-identity-additional"
                        >
                          {fact.value}
                        </ItemDescription>
                      ))}
                      {!isInUseRelationship(accountDescription) &&
                      accountDescription !== "Not in use" ? (
                        <ItemDescription>{accountDescription}</ItemDescription>
                      ) : null}
                    </ItemContent>
                    {!readOnly ? (
                      <ItemActions className="ml-auto flex-wrap justify-end">
                        {canUseAccount ? (
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={() => {
                              void onSelectInstance?.(
                                instance.name,
                                instance.connection || connection.connection,
                              );
                            }}
                            disabled={
                              disconnecting || selectingInstance || submitting
                            }
                          >
                            {selectingInstance
                              ? "Updating..."
                              : USE_ACCOUNT_LABEL}
                          </Button>
                        ) : null}
                        {connection.canDisconnect ? (
                          <Button
                            type="button"
                            variant="danger"
                            size="sm"
                            onClick={() => {
                              onClearError?.();
                              setDisconnectTarget({
                                instance: instance.name,
                                connection:
                                  instance.connection || connection.connection,
                              });
                            }}
                            disabled={disconnecting || selectingInstance}
                          >
                            Disconnect
                          </Button>
                        ) : null}
                      </ItemActions>
                    ) : null}
                </Item>
              );
            })}
          </ItemGroup>
        ) : null}
      </section>
    );
  }

  const disconnectInstance = disconnectTarget.instance
    ? normalizedStatus.connections
        .flatMap((connection) => connection.instances)
        .find((instance) => instance.name === disconnectTarget.instance)
    : undefined;
  const disconnectIdentityPrimary = disconnectInstance
    ? accountIdentityLines(disconnectInstance.identity).primary?.value
    : null;
  const disconnectConfirmLabel = disconnectConfirmAccountLabel({
    identityPrimary: disconnectIdentityPrimary,
    instanceName: disconnectTarget.instance,
  });
  const disconnectConfirm = disconnectConfirmCopy({
    displayName,
    accountLabel: disconnectConfirmLabel,
    context: connectionContext,
    removeApp: destructiveActionLabel === "Remove app",
  });
  const disconnectOpen = Boolean(
    disconnectTarget.instance || disconnectTarget.connection,
  );

  function closeDisconnectDialog() {
    if (disconnecting) return;
    onClearError?.();
    setDisconnectTarget({});
    onDisconnectDialogClose?.();
  }

  const disconnectDialog = (
    <AlertDialog
      open={disconnectOpen}
      onOpenChange={(open) => {
        if (!open) closeDisconnectDialog();
      }}
    >
      <AlertDialogContent data-testid="disconnect-confirm-dialog">
        <AlertDialogHeader>
          <AlertDialogTitle>{disconnectConfirm.heading}</AlertDialogTitle>
          <AlertDialogDescription>{disconnectConfirm.body}</AlertDialogDescription>
          {error ? (
            <p className="text-sm text-destructive" role="alert">
              {error}
            </p>
          ) : null}
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={disconnecting}>Cancel</AlertDialogCancel>
          <AlertDialogAction
            variant="destructive"
            disabled={
              disconnecting ||
              (!disconnectTarget.instance && !disconnectTarget.connection)
            }
            onClick={(event) => {
              event.preventDefault();
              // Remove app deletes the whole connection (no `_instance`).
              void onDisconnect(
                destructiveActionLabel === "Remove app"
                  ? undefined
                  : disconnectTarget.instance,
                disconnectTarget.connection,
              );
            }}
          >
            {disconnecting
              ? destructiveActionLabel === "Remove app"
                ? "Removing..."
                : "Disconnecting..."
              : destructiveActionLabel}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );

  function closeAddAccountDialog() {
    onClearError?.();
    setView("default");
  }

  const addAccountOpen = view === "instance";

  const addAccountDialog = (
    <Dialog
      open={addAccountOpen}
      onOpenChange={(open) => {
        if (!open) closeAddAccountDialog();
      }}
    >
      <DialogContent
        className="sm:max-w-sm"
        showCloseButton={false}
        data-testid="add-account-dialog"
      >
        <form onSubmit={handleInstanceSubmit} className="grid gap-4">
          <DialogHeader>
            <DialogTitle>{addAccountCopy.title}</DialogTitle>
            <DialogDescription>{addAccountCopy.description}</DialogDescription>
          </DialogHeader>
          {error ? (
            <p className="text-sm text-destructive" role="alert">
              {error}
            </p>
          ) : null}
          <Field>
            <FieldLabel htmlFor={`instance-name-${integration.name}`}>
              {addAccountCopy.label}
            </FieldLabel>
            <FieldContent>
              <Input
                id={`instance-name-${integration.name}`}
                name="instance_name"
                type="text"
                required
                placeholder={addAccountCopy.placeholder}
                autoFocus
                autoComplete="off"
              />
              <FieldDescription>
                {addAccountCopy.fieldDescription}
              </FieldDescription>
            </FieldContent>
          </Field>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={closeAddAccountDialog}
            >
              {addAccountCopy.cancelLabel}
            </Button>
            <Button type="submit">{addAccountCopy.continueLabel}</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );

  const panel = (
    <div className={isDialog ? "p-7" : undefined}>
        {view === "token" ? (
          <TokenForm
            integrationName={integration.name}
            headingId={headingId}
            credentialFields={resolveCredentialFields()}
            connectionParams={pendingConnectionParams}
            error={error}
            submitting={submitting}
            submitLabel={connectAppActionLabel(displayName)}
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
              <SectionHeader>
                <SectionHeaderContent size="sm">
                  <SectionHeaderTitle as="h2" id={headingId}>
                    {dialogCopy.title}
                  </SectionHeaderTitle>
                  {dialogCopy.description ? (
                    <SectionHeaderDescription className="text-sm">
                      {dialogCopy.description}
                    </SectionHeaderDescription>
                  ) : null}
                </SectionHeaderContent>
              {isDialog ? (
                <SectionHeaderActions>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon-xs"
                      onClick={closeDialog}
                      aria-label="Close"
                    >
                      <CloseIcon className="h-4 w-4" />
                    </Button>
                  </SectionHeaderActions>
                ) : null}
              </SectionHeader>
            ) : (
              <h2 id={headingId} className="sr-only">
                {connectionForAppAriaLabel(displayName)}
              </h2>
            )}

            {error && !disconnectOpen && !addAccountOpen ? (
              <Alert variant="destructive">
                <CircleAlert />
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            ) : null}

            <div className={showHeader ? "mt-5 space-y-3" : "space-y-3"}>
              {primaryConnections.map((connection) =>
                renderConnectionRow(connection),
              )}
              {otherConnections.length > 0 ? (
                <Collapsible
                  defaultOpen={false}
                  data-testid="connection-other-methods"
                >
                  <CollapsibleTrigger
                    type="button"
                    className="group w-auto max-w-none justify-start gap-inline-glyph rounded-md px-2.5 py-2 font-normal text-muted-foreground"
                  >
                    <ChevronDownIcon
                      className={cn(
                        disclosureCaretClassName,
                        "size-inline-glyph stroke-inline-glyph text-current",
                      )}
                    />
                    {OTHER_CONNECTION_METHODS_LABEL}
                  </CollapsibleTrigger>
                  <CollapsibleContent className="space-y-3 pt-3">
                    {otherConnections.map((connection) =>
                      renderConnectionRow(connection, {
                        forceSectionActions: omitSectionHeader,
                      }),
                    )}
                  </CollapsibleContent>
                </Collapsible>
              ) : null}
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
        {disconnectDialog}
        {addAccountDialog}
      </div>
    );
  }

  return (
    <>
      <Dialog
        open={settingsOpen}
        onOpenChange={(open) => {
          if (!open) {
            if (disconnecting || submitting || selectingInstance) return;
            closeDialog();
          }
        }}
      >
        <DialogContent
          className="max-w-lg gap-0 p-0"
          showCloseButton={false}
          aria-labelledby={headingId}
          data-testid={`integration-connection-${integration.name}`}
          onPointerDownOutside={(event) => {
            if (disconnecting || submitting || selectingInstance || addAccountOpen || disconnectOpen) {
              event.preventDefault();
            }
          }}
          onEscapeKeyDown={(event) => {
            if (disconnecting || submitting || selectingInstance || addAccountOpen || disconnectOpen) {
              event.preventDefault();
            }
          }}
        >
          {panel}
        </DialogContent>
      </Dialog>
      {disconnectDialog}
      {addAccountDialog}
    </>
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
        {SIGN_IN_DETAILS_HEADING}
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
      {error ? (
        <Alert variant="destructive" className="mt-3">
          <CircleAlert />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}
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
          {submitting ? SIGNING_IN_LABEL : "Continue"}
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
  submitLabel,
  onSubmit,
  onCancel,
}: {
  integrationName: string;
  headingId: string;
  credentialFields: CredentialFieldDef[] | undefined;
  connectionParams: Record<string, ConnectionParamDef> | undefined;
  error: string | null;
  submitting: boolean;
  submitLabel: string;
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
      {error ? (
        <Alert variant="destructive" className="mt-3">
          <CircleAlert />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}
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
          {submitting ? SIGNING_IN_LABEL : submitLabel}
        </Button>
      </div>
    </form>
  );
}
