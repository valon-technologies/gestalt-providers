import { useNavigate, useSearch } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { appDetailConnectionPath } from "@/lib/catalogFilters";
import IntegrationConnectionPanel from "@/components/IntegrationConnectionPanel";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { useIntegrationConnection } from "@/hooks/useIntegrationConnection";
import { useAppWorkspace } from "@/features/app-workspace/app-workspace-context";

export default function AppWorkspaceConnectionPage() {
  const navigate = useNavigate({ from: "/apps/$app/connection" });
  const { action: actionSearch } = useSearch({ from: "/apps/$app/connection" });
  const { app, integration, reloadIntegration } = useAppWorkspace();
  const [connectionPanelView, setConnectionPanelView] = useState<
    "default" | "disconnect"
  >("default");
  const [removeAppConfirm, setRemoveAppConfirm] = useState(false);

  const connectionFlow = useIntegrationConnection({
    integration: integration ?? { name: app },
    onConnected: reloadIntegration,
    onDisconnected: reloadIntegration,
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

  if (!integration) return null;

  return (
    <section
      className="space-y-6"
      aria-label="Connection"
      id="app-admin-connection"
      data-testid="app-admin-connection"
    >
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>Credentials</PageHeaderTitle>
          <PageHeaderDescription>
            Connect or reconnect this app under your user. Disconnect to revoke
            access.
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>
      <p className="text-xs text-faint">
        Connecting grants this workspace permission to use the app with your
        credentials. Review the provider’s privacy policy before continuing.
      </p>
      <IntegrationConnectionPanel
        integration={integration}
        onStartOAuth={connectionFlow.handleStartOAuth}
        onSubmitToken={connectionFlow.handleSubmitToken}
        onDisconnect={connectionFlow.handleDisconnect}
        reconnecting={connectionFlow.loading}
        disconnecting={connectionFlow.disconnecting}
        submitting={connectionFlow.submitting}
        error={connectionFlow.error}
        initialView={connectionPanelView}
        destructiveActionLabel={removeAppConfirm ? "Remove app" : "Disconnect"}
        variant="inline"
        showHeader={false}
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
