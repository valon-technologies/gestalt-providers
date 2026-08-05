import { useNavigate, useSearch } from "@tanstack/react-router";
import { ExternalLink } from "lucide-react";
import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import {
  appDetailConnectionPath,
  appShowsCredentialSurface,
} from "@/lib/catalogFilters";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
import { resolveMountedAppHref } from "@/lib/mount";
import IntegrationConnectionPanel from "@/components/IntegrationConnectionPanel";
import { Button } from "@/components/ui/button";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { useIntegrationConnection } from "@/hooks/useIntegrationConnection";
import { useAppWorkspace } from "@/features/app-workspace/app-workspace-context";
import {
  CONNECTION_SURFACE_TITLE,
  connectionSurfaceCopy,
  connectionSurfaceCopyForStatus,
  connectionSurfaceMode,
} from "@/features/app-workspace/connection-surface-copy";

export default function AppWorkspaceConnectionPage() {
  const navigate = useNavigate({ from: "/apps/$app/connection" });
  const { action: actionSearch } = useSearch({ from: "/apps/$app/connection" });
  const { app, integration, reloadIntegration, showConnectionNav } =
    useAppWorkspace();
  const [connectionPanelView, setConnectionPanelView] = useState<
    "default" | "disconnect"
  >("default");
  const [removeAppConfirm, setRemoveAppConfirm] = useState(false);
  const [headerActions, setHeaderActions] = useState<ReactNode>(null);
  const handleHeaderActionsChange = useCallback((actions: ReactNode | null) => {
    setHeaderActions(actions);
  }, []);


  const connectionFlow = useIntegrationConnection({
    integration: integration ?? { name: app },
    onConnected: reloadIntegration,
    onDisconnected: reloadIntegration,
    onFlowComplete: () => {
      setConnectionPanelView("default");
      setRemoveAppConfirm(false);
    },
    returnPath: appDetailConnectionPath(integration ?? { name: app }),
  });

  useEffect(() => {
    if (actionSearch === "disconnect") {
      setConnectionPanelView("disconnect");
      setRemoveAppConfirm(true);
      void navigate({
        search: { action: undefined },
        replace: true,
      });
    }
  }, [actionSearch, navigate]);

  const status = useMemo(
    () =>
      integration
        ? normalizeIntegrationStatus(integration, "current_user")
        : null,
    [integration],
  );

  if (!integration || !status) return null;

  const hasCredentialSurface =
    showConnectionNav || appShowsCredentialSurface(integration);
  const mode = hasCredentialSurface
    ? connectionSurfaceMode(status)
    : "none";
  const mountedPath = integration.mountedPath?.trim();
  const openHref = mountedPath ? resolveMountedAppHref(mountedPath) : null;

  // Deep-linked no-surface apps (nav hidden) still get the honest empty state.
  if (!hasCredentialSurface || mode === "none") {
    const emptyCopy = connectionSurfaceCopy("none");
    return (
      <section
        className="space-y-6"
        aria-label={CONNECTION_SURFACE_TITLE}
        id="app-admin-connection"
        data-testid="app-admin-connection"
        data-credential-surface="none"
      >
        <PageHeader className="sm:items-baseline">
          <PageHeaderContent size="md">
            <PageHeaderTitle>{emptyCopy.title}</PageHeaderTitle>
            <PageHeaderDescription>{emptyCopy.description}</PageHeaderDescription>
          </PageHeaderContent>
          {openHref ? (
            <PageHeaderActions>
              <Button asChild>
                <a
                  href={openHref}
                  target="_blank"
                  rel="noopener noreferrer"
                  data-testid="open-app-connection-empty"
                >
                  Open app
                  <ExternalLink aria-hidden />
                </a>
              </Button>
            </PageHeaderActions>
          ) : null}
        </PageHeader>
      </section>
    );
  }

  const copy = connectionSurfaceCopyForStatus(status);

  return (
    <section
      className="space-y-6"
      aria-label={CONNECTION_SURFACE_TITLE}
      id="app-admin-connection"
      data-testid="app-admin-connection"
      data-credential-surface="manage"
      data-connection-mode={mode}
    >
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>{copy.title}</PageHeaderTitle>
          <PageHeaderDescription>{copy.description}</PageHeaderDescription>
        </PageHeaderContent>
        {headerActions ? (
          <PageHeaderActions>{headerActions}</PageHeaderActions>
        ) : null}
      </PageHeader>
      {copy.trustNote ? (
        <p className="text-xs text-muted-foreground-soft">{copy.trustNote}</p>
      ) : null}
      <IntegrationConnectionPanel
        integration={integration}
        onStartOAuth={connectionFlow.handleStartOAuth}
        onSubmitToken={connectionFlow.handleSubmitToken}
        onDisconnect={connectionFlow.handleDisconnect}
        onSelectInstance={connectionFlow.handleSelectInstance}
        reconnecting={connectionFlow.loading}
        disconnecting={connectionFlow.disconnecting}
        selectingInstance={connectionFlow.selectingInstance}
        submitting={connectionFlow.submitting}
        error={connectionFlow.error}
        onClearError={connectionFlow.clearError}
        initialView={connectionPanelView}
        destructiveActionLabel={removeAppConfirm ? "Remove app" : "Disconnect"}
        variant="inline"
        showHeader={false}
        omitSectionHeader
        onHeaderActionsChange={handleHeaderActionsChange}
        onDisconnectDialogClose={() => {
          setConnectionPanelView("default");
          setRemoveAppConfirm(false);
        }}
      />
      {connectionFlow.pendingSelection ? (
        <form
          ref={connectionFlow.pendingSelectionFormRef}
          method="post"
          action={connectionFlow.pendingSelection.action}
          className="hidden"
        >
          <input
            type="hidden"
            name="pending_token"
            value={connectionFlow.pendingSelection.pendingToken}
          />
        </form>
      ) : null}
    </section>
  );
}
