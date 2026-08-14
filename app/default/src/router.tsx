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
import AppAdminWorkflowRunPage from "@/pages/app-admin-workflow-run";
import AppAdminWorkflowRunStepPage from "@/pages/app-admin-workflow-run-step";
import AppAdminWorkflowDefinitionPage from "@/pages/app-admin-workflow-definition";
import AppWorkspaceLayout from "@/pages/app-workspace-layout";
import AppWorkspaceConnectionPage from "@/pages/app-workspace/connection";
import AppWorkspaceOperationsPage from "@/pages/app-workspace/operations";
import AppWorkspaceOverviewPage from "@/pages/app-workspace/overview";
import AppAdminMembersPage from "@/pages/app-workspace/admin/members";
import AppAdminMetricsPage from "@/pages/app-workspace/admin/metrics";
import AppAdminServiceAccountsPage from "@/pages/app-workspace/admin/service-accounts";
import AdminPage from "@/pages/admin";
import AdminAppPage from "@/pages/admin-app";
import AdminMetricsPage from "@/pages/admin-metrics";
import AdminPlatformAdminsPage from "@/pages/admin-platform-admins";
import AdminVersionDetailPage from "@/pages/admin-version";
import AdminVersionsPage from "@/pages/admin-versions";
import AppsPage from "@/pages/apps";
import BuildPage, { BuildIndexRedirect } from "@/pages/build";
import SettingsPage from "@/pages/settings";
import SettingsTokenCreate from "@/components/SettingsTokenCreate";
import SettingsTokensSection from "@/components/SettingsTokensSection";
import { canAccessAdminRoute, hasGrantedGestaltAdminAccess } from "@/features/admin-access/admin-access-gate";
import { AdminLayout } from "@/features/admin-access/admin-layout";
import { appBasepath } from "@/lib/mount";
import { parseWorkflowRunsSearch } from "@/features/app-workflows/workflow-runs-list-query";
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
  useDocumentTitle("Grant App Access");
  return <AuthorizationDocsPage />;
}

function DocsWorkflowsRoute() {
  useDocumentTitle("Inspect Workflows");
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

/** Unified Admin for Gestalt admins. */
const adminLayoutRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/admin",
  beforeLoad: () => {
    // Sibling Admin nav must not wait on another app-registries round trip.
    // An async beforeLoad keeps Outlet on the previous page while the URL (and
    // a location-based rail) already moved.
    if (hasGrantedGestaltAdminAccess()) return;
    return canAccessAdminRoute().then((allowed) => {
      if (!allowed) {
        throw redirect({ to: "/apps" });
      }
    });
  },
  component: AdminLayout,
});

const adminIndexRoute = createRoute({
  getParentRoute: () => adminLayoutRoute,
  path: "/",
  component: AdminPage,
});

const adminAppRoute = createRoute({
  getParentRoute: () => adminLayoutRoute,
  path: "/apps/$app",
  component: AdminAppPage,
});

const adminPlatformAdminsRoute = createRoute({
  getParentRoute: () => adminLayoutRoute,
  path: "/platform-admins",
  component: AdminPlatformAdminsPage,
});

const adminVersionsRoute = createRoute({
  getParentRoute: () => adminLayoutRoute,
  path: "/versions",
  component: AdminVersionsPage,
});

const adminVersionDetailRoute = createRoute({
  getParentRoute: () => adminLayoutRoute,
  path: "/versions/$app",
  component: AdminVersionDetailPage,
});

const adminMetricsRoute = createRoute({
  getParentRoute: () => adminLayoutRoute,
  path: "/metrics",
  component: AdminMetricsPage,
});

const adminRegistryRedirectRoute = createRoute({
  getParentRoute: () => adminLayoutRoute,
  path: "/registry",
  beforeLoad: () => {
    throw redirect({ to: "/admin/versions" });
  },
});

const adminRegistryAppRedirectRoute = createRoute({
  getParentRoute: () => adminLayoutRoute,
  path: "/registry/$app",
  beforeLoad: ({ params }) => {
    throw redirect({
      to: "/admin/versions/$app",
      params: { app: params.app },
    });
  },
});

const setupIndexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/setup",
  component: BuildIndexRedirect,
});

const setupStepRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/setup/$stepId",
  component: BuildPage,
});

const buildIndexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/build",
  beforeLoad: () => {
    throw redirect({ to: "/setup" });
  },
});

const buildStepRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/build/$stepId",
  beforeLoad: ({ params }) => {
    throw redirect({
      to: "/setup/$stepId",
      params: { stepId: params.stepId },
    });
  },
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

const appMetricsRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/metrics",
  component: AppAdminMetricsPage,
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
  validateSearch: (search: Record<string, unknown>) =>
    parseWorkflowRunsSearch(search),
  component: AppAdminWorkflowsPage,
});

const appAdminWorkflowRunRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/workflows/runs/$runId",
  component: AppAdminWorkflowRunPage,
});

const appAdminWorkflowRunStepRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/workflows/runs/$runId/jobs/$jobId/steps/$stepId",
  component: AppAdminWorkflowRunStepPage,
});

const appAdminWorkflowDefinitionsRedirectRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/workflows/definitions",
  beforeLoad: ({ params }) => {
    throw redirect({
      to: "/apps/$app/admin/workflows",
      params: { app: params.app },
      replace: true,
    });
  },
});

const appAdminWorkflowDefinitionRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/workflows/definitions/$definitionId",
  component: AppAdminWorkflowDefinitionPage,
});

const appAdminMembersRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/members",
  component: AppAdminMembersPage,
});

const appAdminServiceAccountsRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/service-accounts",
  component: AppAdminServiceAccountsPage,
});

/** Legacy `/admin/agent-identities` → Service accounts. */
const appAdminAgentIdentitiesRedirectRoute = createRoute({
  getParentRoute: () => appWorkspaceLayoutRoute,
  path: "/admin/agent-identities",
  beforeLoad: ({ params }) => {
    throw redirect({
      to: "/apps/$app/admin/service-accounts",
      params: { app: params.app },
    });
  },
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
    const hash = location.hash.replace(/^#/, "");
    if (hash === "identities") {
      throw redirect({ to: "/apps" });
    }
    if (hash === "authorization") {
      throw redirect({ to: "/settings/tokens/new" });
    }
    throw redirect({
      to: "/settings/tokens",
      hash: location.hash || undefined,
    });
  },
});

const settingsTokenCreateRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: "/tokens/new",
  component: SettingsTokenCreate,
});

const settingsTokensRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: "/tokens",
  beforeLoad: ({ location }) => {
    const hash = location.hash.replace(/^#/, "");
    if (hash === "authorization") {
      throw redirect({ to: "/settings/tokens/new" });
    }
  },
  component: SettingsTokensSection,
});

/** Legacy Settings → Identities; agent identities now live under each app's Admin. */
const settingsIdentitiesRedirectRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: "/identities",
  beforeLoad: () => {
    throw redirect({ to: "/apps" });
  },
});

const settingsIdentityDetailRedirectRoute = createRoute({
  getParentRoute: () => settingsRoute,
  path: "/identities/$identityLocalId",
  beforeLoad: () => {
    throw redirect({ to: "/apps" });
  },
});

const authorizationRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/authorization",
  beforeLoad: () => {
    throw redirect({ to: "/settings/tokens/new" });
  },
});

const identitiesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/identities",
  beforeLoad: () => {
    throw redirect({ to: "/apps" });
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
  adminLayoutRoute.addChildren([
    adminIndexRoute,
    adminAppRoute,
    adminPlatformAdminsRoute,
    adminVersionsRoute,
    adminVersionDetailRoute,
    adminMetricsRoute,
    adminRegistryRedirectRoute,
    adminRegistryAppRedirectRoute,
  ]),
  appWorkspaceLayoutRoute.addChildren([
    appOverviewRoute,
    appConnectionRoute,
    appOperationsRoute,
    appVersionsRoute,
    appMetricsRoute,
    appAdminIndexRoute,
    appAdminSnapshotsRoute,
    appAdminHistoryRoute,
    appAdminWorkflowsRoute,
    appAdminWorkflowRunRoute,
    appAdminWorkflowRunStepRoute,
    appAdminWorkflowDefinitionsRedirectRoute,
    appAdminWorkflowDefinitionRoute,
    appAdminMembersRoute,
    appAdminServiceAccountsRoute,
    appAdminAgentIdentitiesRedirectRoute,
  ]),
  setupIndexRoute,
  setupStepRoute,
  buildIndexRoute,
  buildStepRoute,
  settingsRoute.addChildren([
    settingsIndexRoute,
    settingsTokenCreateRoute,
    settingsTokensRoute,
    settingsIdentitiesRedirectRoute,
    settingsIdentityDetailRedirectRoute,
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
