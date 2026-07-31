import {
  Outlet,
  createRoute,
  createRouter,
  redirect,
} from "@tanstack/react-router";
import DocsShell from "@/docs/DocsShell";
import {
  AuthorizationDocsPage,
  ConnectDocsPage,
  GettingStartedDocsPage,
  InvokeDocsPage,
  McpDocsPage,
  TokensDocsPage,
  TroubleshootingDocsPage,
  WorkflowsDocsPage,
} from "@/docs/DocsContent";
import { useDocumentTitle } from "@/hooks/use-document-title";
import AppAdminSnapshotsPage from "@/pages/app-admin-snapshots";
import AppAdminWorkflowsPage from "@/pages/app-admin-workflows";
import AppWorkspaceLayout from "@/pages/app-workspace-layout";
import AppWorkspaceConnectionPage from "@/pages/app-workspace/connection";
import AppWorkspaceOperationsPage from "@/pages/app-workspace/operations";
import AppWorkspaceOverviewPage from "@/pages/app-workspace/overview";
import AppAdminAgentIdentitiesPage from "@/pages/app-workspace/admin/agent-identities";
import AppAdminMembersPage from "@/pages/app-workspace/admin/members";
import AppsPage from "@/pages/apps";
import BuildPage, { BuildIndexRedirect } from "@/pages/build";
import SettingsPage from "@/pages/settings";
import SettingsIdentitiesList from "@/components/SettingsIdentitiesList";
import SettingsIdentityDetail from "@/components/SettingsIdentityDetail";
import SettingsTokensSection from "@/components/SettingsTokensSection";
import { appBasepath } from "@/lib/mount";
import {
  legacyIdentityIdFromLocation,
  managedIdentityLocalId,
} from "@/lib/managed-identity-paths";
import { rootRoute } from "./routes/__root";

function DocsLayout() {
  return (
    <DocsShell>
      <Outlet />
    </DocsShell>
  );
}

function DocsGettingStartedRoute() {
  useDocumentTitle("Getting Started");
  return <GettingStartedDocsPage />;
}

function DocsConnectRoute() {
  useDocumentTitle("Connect Apps");
  return <ConnectDocsPage />;
}

function DocsInvokeRoute() {
  useDocumentTitle("Invoke Operations");
  return <InvokeDocsPage />;
}

function DocsTokensRoute() {
  useDocumentTitle("Manage API Tokens");
  return <TokensDocsPage />;
}

function DocsAuthorizationRoute() {
  useDocumentTitle("Grant Authorization");
  return <AuthorizationDocsPage />;
}

function DocsWorkflowsRoute() {
  useDocumentTitle("Manage Workflows");
  return <WorkflowsDocsPage />;
}

function DocsMcpRoute() {
  useDocumentTitle("Use With MCP");
  return <McpDocsPage />;
}

function DocsTroubleshootingRoute() {
  useDocumentTitle("Troubleshooting");
  return <TroubleshootingDocsPage />;
}

const LEGACY_APP_SECTIONS = {
  overview: "",
  connection: "/connection",
  access: "/admin/members",
  workflows: "/admin/workflows",
  operations: "/operations",
} as const;

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  beforeLoad: () => {
    throw redirect({ to: "/apps" });
  },
});

const agentsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/agents",
  beforeLoad: () => {
    throw redirect({ to: "/apps" });
  },
});

const appsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/apps",
  component: AppsPage,
});

const buildIndexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/build",
  component: BuildIndexRedirect,
});

const buildStepRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/build/$stepId",
  component: BuildPage,
});

const appWorkspaceLayoutRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/apps/$app",
  beforeLoad: ({ location, params }) => {
    const search = new URLSearchParams(location.searchStr);
    const section = search.get("section");
    if (section && section in LEGACY_APP_SECTIONS) {
      const suffix =
        LEGACY_APP_SECTIONS[section as keyof typeof LEGACY_APP_SECTIONS];
      const operation = search.get("operation");
      const action = search.get("action");
      throw redirect({
        to: suffix
          ? (`/apps/$app${suffix}` as "/apps/$app/connection")
          : "/apps/$app",
        params: { app: params.app },
        hash: operation && section === "operations" ? operation : undefined,
        search:
          section === "connection" && action === "disconnect"
            ? { action: "disconnect" as const }
            : {},
      });
    }
  },
  component: AppWorkspaceLayout,
});

const appOverviewRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/",
  component: AppWorkspaceOverviewPage,
});

const appConnectionRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/connection",
  validateSearch: (
    search: Record<string, unknown>,
  ): { action?: "disconnect" } => ({
    action: search.action === "disconnect" ? "disconnect" : undefined,
  }),
  component: AppWorkspaceConnectionPage,
});

const appOperationsRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/operations",
  component: AppWorkspaceOperationsPage,
});

const appVersionsRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/versions",
  component: AppAdminSnapshotsPage,
});

const appAdminIndexRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin",
  beforeLoad: ({ params }) => {
    throw redirect({
      to: "/apps/$app/versions",
      params: { app: params.app },
    });
  },
});

const appAdminSnapshotsRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/snapshots",
  beforeLoad: ({ params }) => {
    throw redirect({
      to: "/apps/$app/versions",
      params: { app: params.app },
    });
  },
});

const appAdminHistoryRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/history",
  beforeLoad: ({ params }) => {
    throw redirect({
      to: "/apps/$app/versions",
      params: { app: params.app },
    });
  },
});

const appAdminWorkflowsRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/workflows",
  component: AppAdminWorkflowsPage,
});

const appAdminMembersRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/members",
  component: AppAdminMembersPage,
});

const appAdminAgentIdentitiesRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/agent-identities",
  component: AppAdminAgentIdentitiesPage,
});

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings",
  component: SettingsPage,
});

const settingsIndexRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: "/",
  beforeLoad: ({ location }) => {
    if (location.hash === "#identities") {
      throw redirect({ to: "/settings/identities" });
    }
    throw redirect({
      to: "/settings/tokens",
      hash: location.hash || undefined,
    });
  },
});

const settingsTokensRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: "/tokens",
  component: SettingsTokensSection,
});

const settingsIdentitiesRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: "/identities",
  component: SettingsIdentitiesList,
});

const settingsIdentityDetailRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: "/identities/$identityLocalId",
  component: SettingsIdentityDetail,
});

const authorizationRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/authorization",
  beforeLoad: () => {
    throw redirect({ to: "/settings/tokens" });
  },
});

const identitiesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/identities",
  beforeLoad: ({ location }) => {
    const id = legacyIdentityIdFromLocation(location);
    if (id) {
      throw redirect({
        to: "/settings/identities/$identityLocalId",
        params: { identityLocalId: managedIdentityLocalId(id) },
        hash: location.hash,
      });
    }
    throw redirect({ to: "/settings/identities", hash: location.hash });
  },
});

const integrationsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/integrations",
  beforeLoad: ({ location }) => {
    throw redirect({
      to: "/apps",
      search: location.search,
      hash: location.hash,
    });
  },
});

const tokensRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/tokens",
  beforeLoad: () => {
    throw redirect({ to: "/settings/tokens" });
  },
});

const workflowsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/workflows",
  beforeLoad: () => {
    throw redirect({ to: "/apps" });
  },
});

const docsLayoutRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/docs",
  component: DocsLayout,
});

const docsIndexRoute = createRoute({
  getParentRoute: () => docsLayoutRoute,
  path: "/",
  component: DocsGettingStartedRoute,
});

const docsGettingStartedRoute = createRoute({
  getParentRoute: () => docsLayoutRoute,
  path: "/getting-started",
  component: DocsGettingStartedRoute,
});

const docsConnectRoute = createRoute({
  getParentRoute: () => docsLayoutRoute,
  path: "/connect",
  component: DocsConnectRoute,
});

const docsInvokeRoute = createRoute({
  getParentRoute: () => docsLayoutRoute,
  path: "/invoke",
  component: DocsInvokeRoute,
});

const docsTokensRoute = createRoute({
  getParentRoute: () => docsLayoutRoute,
  path: "/tokens",
  component: DocsTokensRoute,
});

const docsAuthorizationRoute = createRoute({
  getParentRoute: () => docsLayoutRoute,
  path: "/authorization",
  component: DocsAuthorizationRoute,
});

const docsWorkflowsRoute = createRoute({
  getParentRoute: () => docsLayoutRoute,
  path: "/workflows",
  component: DocsWorkflowsRoute,
});

const docsMcpRoute = createRoute({
  getParentRoute: () => docsLayoutRoute,
  path: "/mcp",
  component: DocsMcpRoute,
});

const docsTroubleshootingRoute = createRoute({
  getParentRoute: () => docsLayoutRoute,
  path: "/troubleshooting",
  component: DocsTroubleshootingRoute,
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  agentsRoute,
  appsRoute,
  appWorkspaceLayoutRoute.addChildren([
    appOverviewRoute,
    appConnectionRoute,
    appOperationsRoute,
    appVersionsRoute,
    appAdminIndexRoute,
    appAdminSnapshotsRoute,
    appAdminHistoryRoute,
    appAdminWorkflowsRoute,
    appAdminMembersRoute,
    appAdminAgentIdentitiesRoute,
  ]),
  buildIndexRoute,
  buildStepRoute,
  settingsRoute.addChildren([
    settingsIndexRoute,
    settingsTokensRoute,
    settingsIdentitiesRoute,
    settingsIdentityDetailRoute,
  ]),
  authorizationRoute,
  identitiesRoute,
  integrationsRoute,
  tokensRoute,
  workflowsRoute,
  docsLayoutRoute.addChildren([
    docsIndexRoute,
    docsGettingStartedRoute,
    docsConnectRoute,
    docsInvokeRoute,
    docsTokensRoute,
    docsAuthorizationRoute,
    docsWorkflowsRoute,
    docsMcpRoute,
    docsTroubleshootingRoute,
  ]),
]);

export const router = createRouter({ routeTree, basepath: appBasepath });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
