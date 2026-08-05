import { useNavigate } from "@tanstack/react-router";
import { useEffect, useMemo, useState } from "react";
import { CircleAlert, ExternalLink } from "lucide-react";
import { getAuthSession, type AuthSession } from "@/lib/api";
import {
  alertVariantFromTone,
  appShowsCredentialSurface,
  badgeVariantFromTone,
  getAppSurfaces,
  primaryConnectLabel,
} from "@/lib/catalogFilters";
import { getAppPromptExamples } from "@/lib/appPromptExamples";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import { resolveMountedAppHref } from "@/lib/mount";
import AppPromptExamplePromo from "@/components/AppPromptExamplePromo";
import IntegrationIcon from "@/components/IntegrationIcon";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { SelectionCheck } from "@/components/ui/selection-check";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { useAppWorkspace } from "@/features/app-workspace/app-workspace-context";
import {
  CONNECTION_ACCESS_BLURB,
  MANAGE_CONNECTION_LABEL,
  overviewConnectionAttention,
} from "@/features/app-workspace/connection-surface-copy";

const overviewSectionClass = "border-t border-border pt-8";

export default function AppWorkspaceOverviewPage() {
  const navigate = useNavigate();
  const { app, integration } = useAppWorkspace();
  const [session, setSession] = useState<AuthSession | null>(null);

  useEffect(() => {
    let active = true;
    getAuthSession()
      .then((value) => {
        if (active) setSession(value);
      })
      .catch(() => {
        if (active) setSession(null);
      });
    return () => {
      active = false;
    };
  }, []);

  const status = useMemo(
    () =>
      integration
        ? normalizeIntegrationStatus(integration, "current_user")
        : null,
    [integration],
  );
  const surfaces = useMemo(
    () => (integration ? getAppSurfaces(integration) : null),
    [integration],
  );

  const checklist = useMemo(() => {
    if (!status || !surfaces) return [];
    const items: Array<{ id: string; label: string; done: boolean; skip?: boolean }> = [
      {
        id: "connected",
        // Same vocabulary as the status badge — avoid "Ready" vs "No credentials required".
        label: status.summaryLabel,
        done: status.connected && status.tone === "success",
      },
      {
        id: "ui",
        label: "Has an app",
        done: Boolean(surfaces.hasUi),
        skip: !surfaces.hasUi,
      },
      {
        id: "mcp",
        label: "Works with AI",
        done: Boolean(surfaces.hasMcp),
        skip: !surfaces.hasMcp,
      },
    ];
    return items
      .filter((item) => !item.skip)
      .map(({ id, label: itemLabel, done }) => ({
        id,
        label: itemLabel,
        done,
      }));
  }, [status, surfaces]);

  if (!integration || !status || !surfaces) return null;

  const label = getIntegrationLabel(integration);
  const promptExamples = getAppPromptExamples(integration, surfaces.hasMcp);
  const connectLabel = primaryConnectLabel(integration, "current_user");
  const hasCredentialSurface = appShowsCredentialSurface(integration);
  const attention = overviewConnectionAttention(status);
  const showManageConnection =
    hasCredentialSurface && connectLabel === null && status.connected;
  const mountedPath = integration.mountedPath?.trim();
  const openHref = mountedPath ? resolveMountedAppHref(mountedPath) : null;
  const showOperations = operationCount > 0 || surfaces.hasMcp;
  const sectionAfterPromptClass =
    promptExamples.length > 0 ? "pt-8" : overviewSectionClass;

  return (
    <section aria-label="Overview" className="flex flex-col gap-8">
      <div className="flex flex-col gap-4">
        <IntegrationIcon
          iconSvg={integration.iconSvg}
          name={integration.name}
          displayName={integration.displayName}
          size="lg"
          className="shrink-0"
        />
        <div className="flex min-w-0 flex-col gap-3">
          <PageHeader className="sm:items-baseline">
            <PageHeaderContent size="md">
              <PageHeaderTitle>{label}</PageHeaderTitle>
              {integration.description ? (
                <PageHeaderDescription>{integration.description}</PageHeaderDescription>
              ) : (
                <PageHeaderDescription>
                  {openHref
                    ? "Open the app to get work done, or use Operations for agents and the CLI."
                    : "Use Operations for agents and the CLI when you need callable methods."}
                </PageHeaderDescription>
              )}
            </PageHeaderContent>
            {openHref || connectLabel || showManageConnection ? (
              <PageHeaderActions>
                {connectLabel ? (
                  <Button
                    type="button"
                    variant={openHref ? "secondary" : "default"}
                    onClick={() =>
                      void navigate({
                        to: "/apps/$app/connection",
                        params: { app },
                      })
                    }
                  >
                    {connectLabel}
                  </Button>
                ) : showManageConnection ? (
                  <Button
                    type="button"
                    variant={openHref ? "secondary" : "default"}
                    onClick={() =>
                      void navigate({
                        to: "/apps/$app/connection",
                        params: { app },
                      })
                    }
                    data-testid="manage-connection-overview"
                  >
                    {MANAGE_CONNECTION_LABEL}
                  </Button>
                ) : null}
                {openHref ? (
                  <Button asChild>
                    <a
                      href={openHref}
                      target="_blank"
                      rel="noopener noreferrer"
                      data-testid="open-app-detail"
                    >
                      Open app
                      <ExternalLink aria-hidden />
                    </a>
                  </Button>
                ) : null}
              </PageHeaderActions>
            ) : null}
          </PageHeader>
          {attention ? (
            <Alert
              variant={alertVariantFromTone(status.tone)}
              data-testid="overview-connection-attention"
            >
              <CircleAlert aria-hidden />
              <AlertTitle>{attention.title}</AlertTitle>
              <AlertDescription>
                {attention.description}{" "}
                <UiLink asChild className="text-sm">
                  <Link to="/apps/$app/connection" params={{ app }}>
                    {attention.actionLabel}
                  </Link>
                </UiLink>
              </AlertDescription>
            </Alert>
          ) : null}
          {!attention || surfaces.hasUi || surfaces.hasMcp ? (
            <div className="flex flex-wrap items-center gap-2">
              {/* Attention uses Alert above — never a status Badge for recovery copy. */}
              {!attention ? (
                <Badge
                  variant={badgeVariantFromTone(status.tone)}
                  aria-label={status.summaryLabel}
                >
                  {status.summaryLabel}
                </Badge>
              ) : null}
              {surfaces.hasUi ? (
                <Badge variant="secondary" size="sm">
                  App
                </Badge>
              ) : null}
              {surfaces.hasMcp ? (
                <Badge variant="secondary" size="sm">
                  Works with AI
                </Badge>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>

      {promptExamples.length > 0 ? (
        <div>
          <AppPromptExamplePromo displayName={label} prompts={promptExamples} />
        </div>
      ) : null}

      <div
        className={
          promptExamples.length > 0
            ? sectionAfterPromptClass
            : overviewSectionClass
        }
      >
        <h2 className="text-lg font-heading text-foreground">
          {hasCredentialSurface ? "Your access" : "Signed-in as"}
        </h2>
        <p className="mt-1 text-sm text-muted-foreground">
          {hasCredentialSurface
            ? CONNECTION_ACCESS_BLURB
            : "The account viewing this app in the workspace."}
        </p>
        <dl className="mt-4 grid gap-3 sm:grid-cols-2">
          <div>
            <dt className="text-xs font-medium text-muted-foreground">User</dt>
            <dd className="mt-1 text-sm text-foreground">
              {session?.email ||
                session?.displayName ||
                session?.subjectId ||
                "—"}
            </dd>
          </div>
          {hasCredentialSurface ? (
            <div>
              <dt className="text-xs font-medium text-muted-foreground">
                Status
              </dt>
              <dd className="mt-1 text-sm text-foreground">
                {status.summaryLabel}
              </dd>
            </div>
          ) : null}
        </dl>
      </div>

      {checklist.length > 0 ? (
        <div
          className={overviewSectionClass}
          data-testid="app-admin-checklist"
        >
          <h2 className="text-lg font-heading text-foreground">Setup</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {checklist.filter((item) => item.done).length}/{checklist.length}{" "}
            ready
          </p>
          <ul className="mt-4 space-y-2">
            {checklist.map((item) => (
              <li
                key={item.id}
                className="flex items-center gap-2 text-sm text-foreground"
              >
                <SelectionCheck checked={item.done} tone="solid" density="default" />
                {item.label}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className={overviewSectionClass}>
        <h2 className="text-lg font-heading text-foreground">Details</h2>
        <dl className="mt-4 grid gap-3 sm:grid-cols-2">
          <div>
            <dt className="text-xs font-medium text-muted-foreground">App name</dt>
            <dd className="mt-1 font-mono text-sm text-foreground">
              {integration.name}
            </dd>
          </div>
          <div>
            <dt className="text-xs font-medium text-muted-foreground">
              Available as
            </dt>
            <dd className="mt-1 flex flex-wrap gap-1.5">
              <Badge size="sm" variant="secondary">
                API
              </Badge>
              {surfaces.hasMcp ? (
                <Badge size="sm" variant="secondary">
                  Works with AI
                </Badge>
              ) : null}
              {surfaces.hasUi ? (
                <Badge size="sm" variant="secondary">
                  App
                </Badge>
              ) : null}
            </dd>
          </div>
          {openHref ? (
            <div className="sm:col-span-2">
              <dt className="text-xs font-medium text-muted-foreground">
                App URL
              </dt>
              <dd className="mt-1 font-mono text-sm text-foreground break-all">
                {openHref}
              </dd>
            </div>
          ) : null}
        </dl>
      </div>
    </section>
  );
}
