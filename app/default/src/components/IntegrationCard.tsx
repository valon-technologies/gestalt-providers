import { useNavigate } from "@tanstack/react-router";
import { useState } from "react";
import type { KeyboardEvent, MouseEvent } from "react";
import {
  Integration,
  startIntegrationOAuth,
  connectManualIntegration,
  disconnectIntegration,
} from "@/lib/api";
import {
  appOpenPath,
  badgeVariantFromTone,
  canManageApp,
  catalogInstallState,
  catalogShowOpenAppButton,
  getAppSurfaces,
  primaryConnectLabel,
} from "@/lib/catalogFilters";
import { shouldShowIntegrationSettings } from "@/lib/integrationStatus";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import {
  normalizeIntegrationStatus,
  type ConnectionContext,
} from "@/lib/integrationStatus";
import { resolveMountedAppHref } from "@/lib/mount";
import { useIntegrationConnection } from "@/hooks/useIntegrationConnection";
import { cn } from "@/lib/cn";
import { Badge } from "@/components/ui/badge";
import AppListingDetail from "./AppListingDetail";
import { SearchHighlight } from "@/components/ui/search-highlight";
import IntegrationIcon from "./IntegrationIcon";
import {
  MoreHorizontalIcon,
  PlusIcon,
  TrashIcon,
} from "./icons";
import IntegrationSettingsModal from "./IntegrationSettingsModal";
import { Button } from "./ui/button";
import { SelectionCheck } from "./ui/selection-check";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "./ui/dropdown-menu";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "./ui/tooltip";

type StartOAuthFn = (
  integration: string,
  scopes?: string[],
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
) => Promise<{ url: string; state: string }>;

type ConnectManualFn = (
  integration: string,
  credential: string | Record<string, string>,
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
) => Promise<{
  status: string;
  integration?: string;
  selectionUrl?: string;
  pendingToken?: string;
}>;

type DisconnectFn = (
  integration: string,
  instance?: string,
  connection?: string,
) => Promise<void>;

export default function IntegrationCard({
  integration,
  onConnected,
  onDisconnected,
  onStatusMessage,
  startOAuth = startIntegrationOAuth,
  connectManual = connectManualIntegration,
  disconnect = disconnectIntegration,
  returnPath,
  readOnly = false,
  disableNavigation = false,
  connectionContext = "current_user",
  connectionEntry = connectionContext === "current_user" ? "app-detail" : "modal",
  highlightQuery = "",
}: {
  integration: Integration;
  onConnected?: () => void;
  onDisconnected?: () => void;
  /** Catalog/admin toast feedback after connect or disconnect. */
  onStatusMessage?: (message: string) => void;
  startOAuth?: StartOAuthFn;
  connectManual?: ConnectManualFn;
  disconnect?: DisconnectFn;
  returnPath?: string;
  readOnly?: boolean;
  disableNavigation?: boolean;
  connectionContext?: ConnectionContext;
  /** Where credential flows open — app detail page or modal dialog. */
  connectionEntry?: "app-detail" | "modal";
  /** Catalog search query — highlights matching tokens in title/description. */
  highlightQuery?: string;
}) {
  const navigate = useNavigate();
  const label = getIntegrationLabel(integration);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [settingsInitialView, setSettingsInitialView] = useState<
    "default" | "disconnect"
  >("default");
  const [destructiveActionLabel, setDestructiveActionLabel] = useState<
    "Disconnect" | "Remove app"
  >("Disconnect");

  const useAppDetailConnection = connectionEntry === "app-detail";

  const connection = useIntegrationConnection({
    integration,
    onConnected,
    onDisconnected,
    onStatusMessage,
    startOAuth,
    connectManual,
    disconnect,
    returnPath,
    onFlowComplete: () => setSettingsOpen(false),
  });

  const normalizedStatus = normalizeIntegrationStatus(
    integration,
    connectionContext,
  );
  const surfaces = getAppSurfaces(integration);
  const installState = catalogInstallState(integration, connectionContext);
  const isAppAdmin = canManageApp(integration);
  const mountedPath = appOpenPath(integration);
  const connectLabel = primaryConnectLabel(integration, connectionContext);
  const settingsAvailable =
    !useAppDetailConnection &&
    shouldShowIntegrationSettings(normalizedStatus, readOnly);
  /** Attention chip only — Ready is a check beside the options menu. */
  const statusBadgeLabel =
    installState === "needs_attention" ? normalizedStatus.summaryLabel : null;
  const statusBadgeVariant = badgeVariantFromTone(normalizedStatus.tone);
  const cardNavigationEnabled = !disableNavigation && !settingsOpen;
  /** Installed → More (Remove app). Discovery → Add when connectable. */
  const showInstalledMenu =
    useAppDetailConnection &&
    !readOnly &&
    (installState === "connected" || installState === "needs_attention");
  const showInstalledCheck = showInstalledMenu;
  const showAddButton =
    !readOnly &&
    (installState === "mount_only" ||
      installState === "not_connected" ||
      connectLabel !== null);
  const showOpenAppButton =
    !readOnly && catalogShowOpenAppButton(integration, connectionContext);

  function navigateToAppDetail(options?: {
    connection?: boolean;
    action?: "disconnect";
  }) {
    if (useAppDetailConnection) {
      const toConnection =
        options?.connection ?? installState === "needs_attention";
      void navigate({
        to: toConnection ? "/apps/$app/connection" : "/apps/$app",
        params: { app: integration.name },
        search: options?.action ? { action: options.action } : {},
      });
      return;
    }
    openConnectionModal(options?.action === "disconnect" ? "disconnect" : "default");
  }

  function openConnectionModal(view: "default" | "disconnect" = "default") {
    setSettingsInitialView(view);
    setDestructiveActionLabel(view === "disconnect" ? "Remove app" : "Disconnect");
    connection.clearError();
    setSettingsOpen(true);
  }

  function handleSettingsClose() {
    setSettingsOpen(false);
    setSettingsInitialView("default");
    setDestructiveActionLabel("Disconnect");
    connection.clearError();
  }

  function openRemoveApp() {
    if (useAppDetailConnection) {
      navigateToAppDetail({ connection: true, action: "disconnect" });
      return;
    }
    openConnectionModal("disconnect");
  }

  function navigateToMountedApp() {
    if (!mountedPath) return;
    window.location.assign(resolveMountedAppHref(mountedPath));
  }

  function activateCard() {
    navigateToAppDetail();
  }

  function handleCardClick(e: MouseEvent<HTMLDivElement>) {
    if (!cardNavigationEnabled) return;
    const target = e.target as HTMLElement | null;
    if (target?.closest("button, a, input, textarea, select, label, form")) {
      return;
    }
    activateCard();
  }

  function handleCardKeyDown(e: KeyboardEvent<HTMLDivElement>) {
    if (!cardNavigationEnabled || e.target !== e.currentTarget) return;
    if (e.key !== "Enter" && e.key !== " ") return;
    e.preventDefault();
    activateCard();
  }

  const cardAriaLabel = `View details for ${label}`;

  return (
    <div
      data-testid={
        settingsOnly
          ? `integration-settings-${integration.name}`
          : `integration-card-${integration.name}`
      }
      className={cn(
        "rounded-xl bg-neutral-hover p-4 text-foreground",
        "hover:bg-neutral-dark-hover active:bg-neutral-dark-pressed",
        "hover:has-[button:hover,[role=button]:hover,[data-no-row-click]:hover]:bg-neutral-hover",
        "active:has-[button:active,[role=button]:active,[data-no-row-click]:active]:bg-neutral-hover",
        cardNavigationEnabled &&
          "cursor-pointer focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-gold-400 focus-visible:ring-offset-2 focus-visible:ring-offset-background",
      )}
      onClick={settingsOnly ? undefined : handleCardClick}
      onKeyDown={settingsOnly ? undefined : handleCardKeyDown}
      role={!settingsOnly && cardNavigationEnabled ? "link" : undefined}
      tabIndex={!settingsOnly && cardNavigationEnabled ? 0 : undefined}
      aria-label={
        !settingsOnly && cardNavigationEnabled ? cardAriaLabel : undefined
      }
    >
      {connection.pendingSelection && (
        <form
          ref={connection.pendingSelectionFormRef}
          method="post"
          action={connection.pendingSelection.action}
          className="hidden"
        >
          <input
            type="hidden"
            name="pending_token"
            value={connection.pendingSelection.pendingToken}
          />
        </form>
      )}
      {!settingsOnly ? (
        <div className="flex items-start justify-between gap-3">
        <div className="flex min-w-0 items-start gap-4">
          <IntegrationIcon
            iconSvg={integration.iconSvg}
            name={integration.name}
            displayName={integration.displayName}
            size="xl"
          />
          <div className="min-w-0">
            <h3 className="text-base font-heading text-foreground">
              <SearchHighlight text={label} query={highlightQuery} variant="vivid" />
            </h3>
            {integration.description && (
              <p className="mt-1 line-clamp-2 text-sm text-muted-foreground">
                <SearchHighlight
                  text={integration.description}
                  query={highlightQuery}
                  variant="vivid"
                />
              </p>
            )}
            {(statusBadgeLabel || surfaces.hasUi || isAppAdmin) && (
              <div className="mt-2 flex flex-wrap items-center gap-1.5">
                {statusBadgeLabel ? (
                  <Badge
                    size="sm"
                    variant={statusBadgeVariant}
                    aria-label={statusBadgeLabel}
                  >
                    {statusBadgeLabel}
                  </Badge>
                ) : null}
                {surfaces.hasUi ? (
                  <Badge size="sm" variant="secondary">
                    App
                  </Badge>
                ) : null}
                {isAppAdmin ? (
                  <Badge size="sm" variant="info">
                    Admin
                  </Badge>
                ) : null}
              </div>
            )}
          </div>
        </div>
        <div
          data-no-row-click
          className="flex shrink-0 flex-col items-end gap-1.5 sm:flex-row sm:items-center"
          onClick={(event) => event.stopPropagation()}
          onKeyDown={(event) => event.stopPropagation()}
        >
          <TooltipProvider>
            {showInstalledCheck ? (
              <Tooltip>
                <TooltipTrigger asChild>
                  <span
                    className="flex size-control-sm items-center justify-center text-success"
                    aria-label="Installed"
                  >
                    <SelectionCheck
                      checked
                      tone="current"
                      density="default"
                    />
                  </span>
                </TooltipTrigger>
                <TooltipContent side="top">Installed</TooltipContent>
              </Tooltip>
            ) : null}

            {showOpenAppButton ? (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                data-testid={`open-app-${integration.name}`}
                onClick={navigateToMountedApp}
              >
                Open app
              </Button>
            ) : null}

            {showInstalledMenu ? (
              <DropdownMenu>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <span className="inline-flex">
                      <DropdownMenuTrigger asChild>
                        <Button
                          type="button"
                          variant="ghost"
                          size="icon-sm"
                          aria-label={`${label} options`}
                        >
                          <MoreHorizontalIcon />
                        </Button>
                      </DropdownMenuTrigger>
                    </span>
                  </TooltipTrigger>
                  <TooltipContent side="top">More</TooltipContent>
                </Tooltip>
                <DropdownMenuContent align="end" className="w-44">
                  <DropdownMenuItem
                    onClick={openRemoveApp}
                    className="text-destructive"
                  >
                    <TrashIcon />
                    Remove app
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            ) : null}

            {settingsAvailable ? (
              <DropdownMenu>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <span className="inline-flex">
                      <DropdownMenuTrigger asChild>
                        <Button
                          type="button"
                          variant="ghost"
                          size="icon-sm"
                          aria-label={`${label} options`}
                        >
                          <MoreHorizontalIcon />
                        </Button>
                      </DropdownMenuTrigger>
                    </span>
                  </TooltipTrigger>
                  <TooltipContent side="top">More</TooltipContent>
                </Tooltip>
                <DropdownMenuContent align="end" className="w-44">
                  <DropdownMenuItem onClick={() => openConnectionModal()}>
                    Connection
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    onClick={openRemoveApp}
                    className="text-destructive"
                  >
                    <TrashIcon />
                    Remove app
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            ) : null}

            {showAddButton ? (
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="inline-flex">
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon-sm"
                      aria-label={`Add ${label}`}
                      onClick={() => navigateToAppDetail({ connection: true })}
                    >
                      <PlusIcon />
                    </Button>
                  </span>
                </TooltipTrigger>
                <TooltipContent side="top">Add</TooltipContent>
              </Tooltip>
            ) : null}
          </TooltipProvider>
        </div>
      </div>
      {connection.error && !settingsOpen && (
        <p className="mt-3 text-sm text-ember-500">{connection.error}</p>
      )}
      {settingsOpen && (
        <IntegrationSettingsModal
          integration={integration}
          onClose={handleSettingsClose}
          onStartOAuth={connection.handleStartOAuth}
          onSubmitToken={connection.handleSubmitToken}
          onDisconnect={connection.handleDisconnect}
          reconnecting={connection.loading}
          disconnecting={connection.disconnecting}
          submitting={connection.submitting}
          error={connection.error}
          readOnly={readOnly}
          connectionContext={connectionContext}
          initialView={settingsInitialView}
          destructiveActionLabel={destructiveActionLabel}
          presentation="modal"
        />
      )}
    </div>
  );
}
