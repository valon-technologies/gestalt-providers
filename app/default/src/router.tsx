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
import AppAdminPage from "@/pages/app-admin";
import AppsPage from "@/pages/apps";
import BuildPage, { BuildIndexRedirect } from "@/pages/build";
import SettingsPage from "@/pages/settings";
import SettingsIdentitiesList from "@/components/SettingsIdentitiesList";
import SettingsIdentityDetail from "@/components/SettingsIdentityDetail";
import SettingsTokensSection from "@/components/SettingsTokensSection";
import WorkflowsPage from "@/pages/workflows";
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

const appAdminRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/apps/$app/admin",
  validateSearch: (
    search: Record<string, unknown>,
  ): { section: "registry" | "workflows" } => ({
    section: search.section === "workflows" ? "workflows" : "registry",
  }),
  component: AppAdminPage,
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
  buildIndexRoute,
  buildStepRoute,
  appAdminRoute,
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
