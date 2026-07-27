
import { Link } from "@tanstack/react-router";
import { AGENT_SESSION_ROUTE } from "@/lib/agentLinks";
import {
  useAgentSessionsQuery,
  useAuthInfoQuery,
  useIntegrationsQuery,
  useTokensQuery,
  useWorkflowRunsQuery,
} from "@/lib/queries";
import { isAPIErrorStatus } from "@/lib/api";
import Nav from "@/components/Nav";
import AuthGuard from "@/components/AuthGuard";
import Container from "@/components/Container";

export default function DashboardPage() {
  const authInfoQuery = useAuthInfoQuery();
  const integrationsQuery = useIntegrationsQuery();
  const tokensQuery = useTokensQuery();
  const workflowRunsQuery = useWorkflowRunsQuery();
  const agentFeature = authInfoQuery.data?.features?.agent;
  const agentSessionsQuery = useAgentSessionsQuery(
    { view: "summary", limit: 50 },
    authInfoQuery.isSuccess && agentFeature !== false,
  );

  const integrations = integrationsQuery.isSuccess
    ? integrationsQuery.data.length
    : null;
  const tokens = tokensQuery.isSuccess ? tokensQuery.data.length : null;
  const workflowResources = workflowRunsQuery.isSuccess
    ? workflowRunsQuery.data.length
    : null;
  const agentAvailable =
    typeof agentFeature === "boolean"
      ? agentFeature
      : !(
          agentSessionsQuery.isError &&
          isAPIErrorStatus(agentSessionsQuery.error, 412)
        );
  const agentSessions =
    agentAvailable && agentSessionsQuery.isSuccess
      ? agentSessionsQuery.data.length
      : null;
  const error =
    integrationsQuery.isError
      ? errorMessage(integrationsQuery.error)
      : tokensQuery.isError
        ? errorMessage(tokensQuery.error)
        : null;

  return (
    <AuthGuard>
      <div className="min-h-screen">
        <Nav />
        <Container as="main" className="py-12">
          <div className="animate-fade-in-up">
            <span className="label-text">Overview</span>
            <h1 className="mt-2 text-2xl font-heading text-foreground">
              Dashboard
            </h1>
            <p className="mt-2 text-sm text-muted-foreground">
              Manage the client-facing app workspace from one place.
            </p>
          </div>

          {error && (
            <p className="mt-8 text-sm text-destructive">{error}</p>
          )}

          <div className="mt-10 grid grid-cols-1 gap-5 sm:grid-cols-2 xl:grid-cols-4 animate-fade-in-up [animation-delay:60ms]">
            <Link
              to="/authorization"
              className="group rounded-lg border border-border bg-card p-8 text-card-foreground transition-all duration-150 hover:border-input hover:shadow-card"
            >
              <span className="label-text">Authorization</span>
              <p className="mt-3 text-3xl font-heading font-bold text-card-foreground">
                {tokens ?? "--"}
              </p>
              <p className="mt-3 text-sm text-muted-foreground transition-colors duration-150 group-hover:text-card-foreground">
                Manage API tokens
                <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                  &rarr;
                </span>
              </p>
            </Link>
            <Link
              to="/apps"
              className="group rounded-lg border border-border bg-card p-8 text-card-foreground transition-all duration-150 hover:border-input hover:shadow-card"
            >
              <span className="label-text">Apps</span>
              <p className="mt-3 text-3xl font-heading font-bold text-card-foreground">
                {integrations ?? "--"}
              </p>
              <p className="mt-3 text-sm text-muted-foreground transition-colors duration-150 group-hover:text-card-foreground">
                Manage apps
                <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                  &rarr;
                </span>
              </p>
            </Link>
            <Link
              to="/workflows"
              className="group rounded-lg border border-border bg-card p-8 text-card-foreground transition-all duration-150 hover:border-input hover:shadow-card"
            >
              <span className="label-text">Workflows</span>
              <p className="mt-3 text-3xl font-heading font-bold text-card-foreground">
                {workflowResources ?? "--"}
              </p>
              <p className="mt-3 text-sm text-muted-foreground transition-colors duration-150 group-hover:text-card-foreground">
                Inspect workflow runs
                <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                  &rarr;
                </span>
              </p>
            </Link>
            {agentAvailable && (
              <Link
                to={AGENT_SESSION_ROUTE}
                className="group rounded-lg border border-border bg-card p-8 text-card-foreground transition-all duration-150 hover:border-input hover:shadow-card"
              >
                <span className="label-text">Agents</span>
                <p className="mt-3 text-3xl font-heading font-bold text-card-foreground">
                  {agentSessions ?? "--"}
                </p>
                <p className="mt-3 text-sm text-muted-foreground transition-colors duration-150 group-hover:text-card-foreground">
                  View agent sessions
                  <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
                    &rarr;
                  </span>
                </p>
              </Link>
            )}
          </div>
        </Container>
      </div>
    </AuthGuard>
  );
}

function errorMessage(reason: unknown): string {
  if (reason instanceof Error) {
    return reason.message;
  }
  if (typeof reason === "string") {
    return reason;
  }
  return "Failed to load";
}
