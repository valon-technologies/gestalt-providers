import { useNavigate } from "@tanstack/react-router";
import { useEffect, useMemo, useState } from "react";
import { getAuthSession, type AuthSession } from "@/lib/api";
import {
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
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { SelectionCheck } from "@/components/ui/selection-check";
import { useAppWorkspace } from "@/features/app-workspace/app-workspace-context";

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

  if (!integration) return null;

  const label = getIntegrationLabel(integration);
  const promptExample = getAppPromptExamples(app, label)[0]!;
  const status = normalizeIntegrationStatus(integration, "current_user");
  const surfaces = getAppSurfaces(integration);
  const connectLabel = primaryConnectLabel(integration, "current_user");
  const mountedPath = integration.mountedPath?.trim();

  const checklist = useMemo(() => {
    const items: Array<{ id: string; label: string; done: boolean; skip?: boolean }> = [
      {
        id: "connected",
        label: "Ready",
        done: status.connected && status.tone === "success",
      },
      {
        id: "ui",
        label: "Has an app page",
        done: Boolean(surfaces.hasUi),
        skip: !surfaces.hasUi,
      },
      {
        id: "mcp",
        label: "Works with AI clients",
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
          <h1 className="text-2xl font-heading text-foreground">{label}</h1>
          {integration.description ? (
            <p className="text-sm text-muted-foreground">
              {integration.description}
            </p>
          ) : (
            <p className="text-sm text-muted-foreground">
              Connection, workflows, and operations for{" "}
              <code className="font-mono text-xs">{integration.name}</code>.
            </p>
          )}
          <div className="flex flex-wrap items-center gap-2">
            <Badge
              variant={badgeVariantFromTone(status.tone)}
              aria-label={status.summaryLabel}
            >
              {status.summaryLabel}
            </Badge>
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
          <div className="flex flex-wrap items-center gap-2">
            {connectLabel ? (
              <Button
                type="button"
                onClick={() =>
                  void navigate({
                    to: "/apps/$app/connection",
                    params: { app },
                  })
                }
              >
                {connectLabel}
              </Button>
            ) : null}
            {mountedPath ? (
              <Button
                type="button"
                variant="secondary"
                data-testid="open-app-detail"
                onClick={() =>
                  window.location.assign(resolveMountedAppHref(mountedPath))
                }
              >
                Open app
              </Button>
            ) : null}
          </div>
        </div>
      </div>

      <AppPromptExamplePromo
        displayName={promptExample.displayName}
        body={promptExample.body}
      />

      <div className={overviewSectionClass}>
        <h2 className="text-lg font-heading text-foreground">Your access</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Connection and credentials for the signed-in user.
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
          <div>
            <dt className="text-xs font-medium text-muted-foreground">
              Connection
            </dt>
            <dd className="mt-1 text-sm text-foreground">
              {status.summaryLabel}
            </dd>
          </div>
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
            <dt className="text-xs font-medium text-muted-foreground">Status</dt>
            <dd className="mt-1 text-sm text-foreground">
              {status.summaryLabel}
            </dd>
          </div>
          <div>
            <dt className="text-xs font-medium text-muted-foreground">Surfaces</dt>
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
          {mountedPath ? (
            <div className="sm:col-span-2">
              <dt className="text-xs font-medium text-muted-foreground">
                Mounted path
              </dt>
              <dd className="mt-1 font-mono text-sm text-foreground">
                {mountedPath}
              </dd>
            </div>
          ) : null}
        </dl>
      </div>
    </section>
  );
}
