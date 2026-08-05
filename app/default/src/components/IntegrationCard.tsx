import { Link, useNavigate } from "@tanstack/react-router";
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
  alertVariantFromTone,
  canManageApp,
  catalogCardActivateRoute,
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
import {
  NESTED_INTERACTIVE_OPT_OUT_ATTR,
  nestedInteractiveSuppress,
} from "@/lib/nested-interactive";
import {
  isInteractiveTarget,
  rowLinkClickIntent,
} from "@/lib/row-link";
import { useIntegrationConnection } from "@/hooks/useIntegrationConnection";
import { cn } from "@/lib/cn";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { SearchHighlight } from "@/components/ui/search-highlight";
import { CircleAlert } from "lucide-react";
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
  /** Attention notice — inline Alert, not a status Badge. */
  const attentionMessage =
    installState === "needs_attention" ? normalizedStatus.summaryLabel : null;
  const attentionAlertVariant = alertVariantFromTone(normalizedStatus.tone);
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
  const activateRoute = catalogCardActivateRoute(integration);
  /** Catalog tiles: real stretch link. Modal entry: whole-card click → settings. */
  const useStretchLink = cardNavigationEnabled && useAppDetailConnection;
  const useCardClickActivate = cardNavigationEnabled && !useAppDetailConnection;

  function navigateToAppDetail(options?: {
    connection?: boolean;
    action?: "disconnect";
  }) {
    if (useAppDetailConnection) {
      const toConnection = options?.connection ?? false;
      void navigate({
        to: toConnection ? "/apps/$app/connection" : activateRoute.to,
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

  function handleCardClick(event: MouseEvent<HTMLDivElement>) {
    if (!useCardClickActivate) return;
    if (isInteractiveTarget(event.target)) return;
    navigateToAppDetail();
  }

  function handleCardKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    if (!useCardClickActivate || event.target !== event.currentTarget) return;
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    navigateToAppDetail();
  }

  function renderCardTitle() {
    const title = (
      <h3 className="text-base font-heading text-foreground">
        <SearchHighlight text={label} query={highlightQuery} variant="vivid" />
      </h3>
    );
    if (!useStretchLink) return title;
    // Stretched heading link (cards.md): ::after covers the relative card;
    // nested controls sit above via relative z-10 + data-no-row-click.
    return (
      <Link
        to={activateRoute.to}
        params={activateRoute.params}
        data-row-link=""
        className={cn(
          "font-heading text-foreground no-underline",
          "after:absolute after:inset-0 after:z-[1] after:rounded-xl after:content-['']",
          "focus-visible:outline-none focus-visible:after:outline-3",
          "focus-visible:after:outline-offset-2 focus-visible:after:outline-ring",
        )}
        onClick={(event) => {
          const intent = rowLinkClickIntent({
            button: event.button,
            metaKey: event.metaKey,
            ctrlKey: event.ctrlKey,
            shiftKey: event.shiftKey,
            altKey: event.altKey,
            targetIsInteractive: isInteractiveTarget(
              event.target,
              event.currentTarget,
            ),
          });
          if (intent === "suppress") {
            event.preventDefault();
          }
          // "navigate" / "native" — TanStack Link + browser handle the rest.
        }}
      >
        {title}
      </Link>
    );
  }

  return (
    <div
      data-testid={`integration-card-${integration.name}`}
      className={cn(
        // Solid catalog card — Neutral hover rest so Neutral dark deepen is a
        // visible L-step (tenant `--secondary` may diverge from `--neutral-hover`).
        "relative rounded-xl bg-neutral-hover p-4 text-foreground",
        "transition-[background-color] duration-hover-out ease-out-quart",
        "hover:bg-neutral-dark-hover hover:duration-hover-in active:bg-neutral-dark-pressed",
        nestedInteractiveSuppress.solidNeutralHoverStretchLink,
        (useStretchLink || useCardClickActivate) && "cursor-pointer",
        useCardClickActivate &&
          "focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background",
      )}
      onClick={useCardClickActivate ? handleCardClick : undefined}
      onKeyDown={useCardClickActivate ? handleCardKeyDown : undefined}
      role={useCardClickActivate ? "link" : undefined}
      tabIndex={useCardClickActivate ? 0 : undefined}
      aria-label={
        useCardClickActivate ? `Open ${label}` : undefined
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
      <div className="flex items-start justify-between gap-3">
        <div className="flex min-w-0 items-start gap-4">
          <IntegrationIcon
            iconSvg={integration.iconSvg}
            name={integration.name}
            displayName={integration.displayName}
            size="xl"
          />
          <div className="min-w-0">
            {renderCardTitle()}
            {integration.description && (
              <p className="mt-1 line-clamp-2 text-sm text-muted-foreground">
                <SearchHighlight
                  text={integration.description}
                  query={highlightQuery}
                  variant="vivid"
                />
              </p>
            )}
            {(surfaces.hasUi || isAppAdmin) && (
              <div className="mt-2 flex flex-wrap items-center gap-1.5">
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
          {...{ [NESTED_INTERACTIVE_OPT_OUT_ATTR]: "" }}
          className="relative z-10 flex shrink-0 flex-col items-end gap-1.5 sm:flex-row sm:items-center"
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
                    Manage connection
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
      {attentionMessage ? (
        <Alert
          variant={attentionAlertVariant}
          className="relative z-10 mt-3"
          data-testid={`integration-card-attention-${integration.name}`}
        >
          <CircleAlert aria-hidden />
          <AlertDescription>{attentionMessage}</AlertDescription>
        </Alert>
      ) : null}
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
          onSelectInstance={connection.handleSelectInstance}
          reconnecting={connection.loading}
          disconnecting={connection.disconnecting}
          selectingInstance={connection.selectingInstance}
          submitting={connection.submitting}
          error={connection.error}
          onClearError={connection.clearError}
          readOnly={readOnly}
          connectionContext={connectionContext}
          initialView={settingsInitialView}
          destructiveActionLabel={destructiveActionLabel}
          presentation="modal"
          onDisconnectDialogClose={() => {
            setSettingsInitialView("default");
            setDestructiveActionLabel("Disconnect");
          }}
        />
      )}
    </div>
  );
}
