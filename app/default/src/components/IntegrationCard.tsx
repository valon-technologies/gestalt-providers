import { useNavigate } from "@tanstack/react-router";
import { useEffect, useId, useRef, useState } from "react";
import type { KeyboardEvent, MouseEvent } from "react";
import {
  Integration,
  PENDING_CONNECTION_PATH,
  resolveAPIPath,
  startIntegrationOAuth,
  connectManualIntegration,
  disconnectIntegration,
} from "@/lib/api";
import {
  badgeVariantFromTone,
  catalogCardActivateTarget,
  connectionSetupBucket,
  getAppSurfaces,
} from "@/lib/catalogFilters";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import {
  normalizeIntegrationStatus,
  shouldShowIntegrationSettings,
  type ConnectionContext,
} from "@/lib/integrationStatus";
import { cn } from "@/lib/cn";
import { Badge } from "./Badge";
import AppListingDetail from "./AppListingDetail";
import { HighlightMatch } from "./HighlightMatch";
import IntegrationIcon from "./IntegrationIcon";
import {
  MoreHorizontalIcon,
  SlidersIcon,
  TrashIcon,
} from "./icons";
import IntegrationSettingsModal from "./IntegrationSettingsModal";
import { Switch } from "./Switch";
import { Button } from "./ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "./ui/dropdown-menu";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "./ui/tooltip";

type ConnectionTarget = {
  instance?: string;
  connection?: string;
};

type PendingSelection = {
  action: string;
  pendingToken: string;
};

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
  settingsOpen: settingsOpenProp,
  onSettingsOpenChange,
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
  /** Controlled settings modal (e.g. App Admin header Connect). */
  settingsOpen?: boolean;
  onSettingsOpenChange?: (open: boolean) => void;
  /** Catalog search query — highlights matching tokens in title/description. */
  highlightQuery?: string;
}) {
  const navigate = useNavigate();
  const label = getIntegrationLabel(integration);
  const connectedSwitchId = useId();
  const [loading, setLoading] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [settingsOpenUncontrolled, setSettingsOpenUncontrolled] =
    useState(false);
  const settingsOpen = settingsOpenProp ?? settingsOpenUncontrolled;
  function setSettingsOpen(open: boolean) {
    onSettingsOpenChange?.(open);
    if (settingsOpenProp === undefined) {
      setSettingsOpenUncontrolled(open);
    }
  }
  const [settingsInitialView, setSettingsInitialView] = useState<
    "default" | "disconnect"
  >("default");
  const [destructiveActionLabel, setDestructiveActionLabel] = useState<
    "Disconnect" | "Uninstall"
  >("Disconnect");
  const [listingOpen, setListingOpen] = useState(false);
  const [pendingOAuthTarget, setPendingOAuthTarget] = useState<ConnectionTarget>(
    {},
  );
  const [pendingSelection, setPendingSelection] =
    useState<PendingSelection | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const pendingSelectionFormRef = useRef<HTMLFormElement>(null);

  const normalizedStatus = normalizeIntegrationStatus(
    integration,
    connectionContext,
  );
  const settingsAvailable = shouldShowIntegrationSettings(
    normalizedStatus,
    readOnly,
  );
  const surfaces = getAppSurfaces(integration);
  const setupBucket = connectionSetupBucket(integration, connectionContext);
  const isConnected = setupBucket === "ready";
  const attentionLabel =
    setupBucket === "needs_attention" ? normalizedStatus.summaryLabel : null;
  const cardActivateTarget = catalogCardActivateTarget(
    integration,
    connectionContext,
  );
  const cardNavigationEnabled = !disableNavigation && !settingsOpen;

  useEffect(() => {
    if (!pendingSelection) return;
    pendingSelectionFormRef.current?.submit();
  }, [pendingSelection]);

  async function beginOAuth(
    connectionParams?: Record<string, string>,
    target: ConnectionTarget = pendingOAuthTarget,
  ) {
    setLoading(true);
    setError(null);
    try {
      const { url } = await startOAuth(
        integration.name,
        undefined,
        connectionParams,
        target.instance,
        target.connection,
        returnPath,
      );
      window.location.href = url;
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "Couldn't start sign-in. Try again.",
      );
      setLoading(false);
    }
  }

  async function handleStartOAuth(
    instance?: string,
    connection?: string,
    connectionParams?: Record<string, string>,
  ) {
    const target = { instance, connection };
    setPendingOAuthTarget(target);
    await beginOAuth(connectionParams, target);
  }

  async function handleSubmitToken(
    credential: string | Record<string, string>,
    connectionParams?: Record<string, string>,
    instance?: string,
    connection?: string,
  ) {
    setSubmitting(true);
    setError(null);
    try {
      const result = await connectManual(
        integration.name,
        credential,
        connectionParams,
        instance,
        connection,
        returnPath,
      );
      if (result.status === "selection_required") {
        if (!result.pendingToken) {
          throw new Error(
            "Connection requires selection, but the server did not return a pending token.",
          );
        }
        setSettingsOpen(false);
        setPendingSelection({
          action: resolveAPIPath(
            result.selectionUrl || PENDING_CONNECTION_PATH,
          ),
          pendingToken: result.pendingToken,
        });
      } else {
        setSettingsOpen(false);
        onStatusMessage?.(`${label} connected successfully.`);
        onConnected?.();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Couldn't connect. Try again.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDisconnect(instance?: string, connection?: string) {
    setDisconnecting(true);
    setError(null);
    try {
      await disconnect(integration.name, instance, connection);
      onStatusMessage?.(`${label} disconnected.`);
      onDisconnected?.();
      setSettingsOpen(false);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Couldn't disconnect. Try again.",
      );
    } finally {
      setDisconnecting(false);
    }
  }

  function handleSettingsClose() {
    setSettingsOpen(false);
    setSettingsInitialView("default");
    setDestructiveActionLabel("Disconnect");
    setError(null);
  }

  function openManage() {
    setSettingsInitialView("default");
    setDestructiveActionLabel("Disconnect");
    setSettingsOpen(true);
  }

  function openUninstall() {
    setSettingsInitialView("disconnect");
    setDestructiveActionLabel("Uninstall");
    setSettingsOpen(true);
  }

  function openListingDetail(e?: MouseEvent) {
    e?.stopPropagation();
    setListingOpen(true);
  }

  function handleListingConnect() {
    setListingOpen(false);
    setSettingsOpen(true);
  }

  function handleListingOpenApp() {
    setListingOpen(false);
    navigateToAdmin();
  }

  function navigateToAdmin() {
    void navigate({
      to: "/apps/$appName",
      params: { appName: integration.name },
    });
  }

  function activateCard() {
    if (cardActivateTarget === "listing") {
      setListingOpen(true);
      return;
    }
    navigateToAdmin();
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

  function handleConnectedChange(nextConnected: boolean) {
    if (readOnly || loading || disconnecting) return;
    if (nextConnected) {
      if (cardActivateTarget === "listing") {
        setListingOpen(true);
        return;
      }
      openManage();
      return;
    }
    // Disconnect / uninstall confirmation lives in settings.
    openUninstall();
  }

  const cardAriaLabel =
    cardActivateTarget === "listing"
      ? `View details for ${label}`
      : `Open ${label}`;

  return (
    <div
      data-testid={`integration-card-${integration.name}`}
      className={cn(
        // Registry Card `variant="solid"` rest = secondary ≈ --neutral-hover.
        // Interactive deepen: Neutral dark (selectable-rows.md) — named L-step
        // tokens, not a ramp jump to base-200 (hover-pressed-color.md).
        "rounded-xl bg-neutral-hover p-4 text-foreground",
        "hover:bg-neutral-dark-hover active:bg-neutral-dark-pressed",
        cardNavigationEnabled &&
          "cursor-pointer focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-gold-400 focus-visible:ring-offset-2 focus-visible:ring-offset-background",
      )}
      onClick={handleCardClick}
      onKeyDown={handleCardKeyDown}
      role={cardNavigationEnabled ? "link" : undefined}
      tabIndex={cardNavigationEnabled ? 0 : undefined}
      aria-label={cardNavigationEnabled ? cardAriaLabel : undefined}
    >
      {pendingSelection && (
        <form
          ref={pendingSelectionFormRef}
          method="post"
          action={pendingSelection.action}
          className="hidden"
        >
          <input
            type="hidden"
            name="pending_token"
            value={pendingSelection.pendingToken}
          />
        </form>
      )}
      <div className="flex items-start justify-between gap-3">
        <div className="flex min-w-0 items-start gap-4">
          <IntegrationIcon iconSvg={integration.iconSvg} size="xl" />
          <div className="min-w-0">
            <h3 className="text-base font-heading text-foreground">
              <HighlightMatch text={label} query={highlightQuery} />
            </h3>
            {integration.description && (
              <p className="mt-1 line-clamp-2 text-sm text-muted-foreground">
                <HighlightMatch
                  text={integration.description}
                  query={highlightQuery}
                />
              </p>
            )}
            {(attentionLabel || surfaces.hasUi) && (
              <div className="mt-2 flex flex-wrap items-center gap-1.5">
                {attentionLabel ? (
                  <Badge
                    size="sm"
                    variant={badgeVariantFromTone(normalizedStatus.tone)}
                    aria-label={attentionLabel}
                  >
                    {attentionLabel}
                  </Badge>
                ) : null}
                {surfaces.hasUi ? (
                  <Badge size="sm" variant="secondary">
                    App page
                  </Badge>
                ) : null}
              </div>
            )}
          </div>
        </div>
        <div
          className="flex shrink-0 items-center gap-1"
          onClick={(event) => event.stopPropagation()}
          onKeyDown={(event) => event.stopPropagation()}
        >
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="inline-flex">
                  <Switch
                    id={connectedSwitchId}
                    size="default"
                    checked={isConnected}
                    disabled={readOnly || loading || disconnecting}
                    onCheckedChange={handleConnectedChange}
                    aria-label={`${label} installed`}
                  />
                </span>
              </TooltipTrigger>
              <TooltipContent side="top">Installed</TooltipContent>
            </Tooltip>

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
                  <DropdownMenuItem onClick={openManage}>
                    <SlidersIcon />
                    Manage
                  </DropdownMenuItem>
                  {isConnected && !readOnly ? (
                    <>
                      <DropdownMenuSeparator />
                      <DropdownMenuItem
                        onClick={openUninstall}
                        className="text-destructive"
                      >
                        <TrashIcon />
                        Uninstall
                      </DropdownMenuItem>
                    </>
                  ) : null}
                </DropdownMenuContent>
              </DropdownMenu>
            ) : null}
          </TooltipProvider>
        </div>
      </div>
      {error && !settingsOpen && (
        <p className="mt-3 text-sm text-ember-500">{error}</p>
      )}
      {listingOpen && (
        <AppListingDetail
          integration={integration}
          onClose={() => setListingOpen(false)}
          onConnect={handleListingConnect}
          onOpenApp={handleListingOpenApp}
          readOnly={readOnly}
        />
      )}
      {settingsOpen && (
        <IntegrationSettingsModal
          integration={integration}
          onClose={handleSettingsClose}
          onStartOAuth={handleStartOAuth}
          onSubmitToken={handleSubmitToken}
          onDisconnect={handleDisconnect}
          reconnecting={loading}
          disconnecting={disconnecting}
          submitting={submitting}
          error={error}
          readOnly={readOnly}
          connectionContext={connectionContext}
          initialView={settingsInitialView}
          destructiveActionLabel={destructiveActionLabel}
        />
      )}
    </div>
  );
}
