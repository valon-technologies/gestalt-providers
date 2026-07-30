import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  fleetStatePresentation,
  hasRecoveredFailedRollout,
} from "@/features/registry/fleet-state";
import {
  formatDurationSeconds,
  formatRegistryTime,
  formatRegistryTimeAgo,
  shortenSourceRef,
} from "@/features/registry/format";
import { RegistryCode } from "@/features/registry/registry-code";
import type { AppAdminRegistryResponse } from "@/features/registry/types";
import { useLiveNow } from "@/hooks/use-live-now";

function FleetMetric({
  label,
  value,
  testId,
}: {
  label: string;
  value: number;
  testId: string;
}) {
  return (
    <div>
      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        {label}
      </dt>
      <dd className="mt-1 text-xl font-semibold tabular-nums" data-testid={testId}>
        {value}
      </dd>
    </div>
  );
}

export function AppAdminFleetState({
  registry,
}: {
  registry: Pick<
    AppAdminRegistryResponse,
    "desiredVersion" | "fleetState" | "recovery" | "rollout"
  >;
}) {
  const { fleetState, recovery } = registry;
  const presentation = fleetStatePresentation(fleetState);
  const showRecovery = hasRecoveredFailedRollout(registry);
  const liveNow = useLiveNow({ enabled: Boolean(fleetState || showRecovery) });
  const evaluatedAgo = fleetState?.evaluatedAt
    ? formatRegistryTimeAgo(fleetState.evaluatedAt, liveNow)
    : "";
  const recoveredAgo = recovery?.recoveredAt
    ? formatRegistryTimeAgo(recovery.recoveredAt, liveNow)
    : "";
  const desiredVersion = fleetState?.desiredVersion || registry.desiredVersion;

  return (
    <Card data-testid="app-admin-fleet-state">
      <CardHeader className="gap-3 sm:flex-row sm:items-start sm:justify-between sm:space-y-0">
        <div className="space-y-1.5">
          <CardTitle>Current fleet</CardTitle>
          <CardDescription>{presentation.description}</CardDescription>
        </div>
        <Badge
          variant={presentation.badgeVariant}
          size="lg"
          data-testid="fleet-state-badge"
        >
          {presentation.label}
        </Badge>
      </CardHeader>

      {fleetState ? (
        <CardContent className="space-y-5">
          <dl className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5">
            <FleetMetric
              label="Live"
              value={fleetState.liveInstances}
              testId="fleet-live-instances"
            />
            <FleetMetric
              label="Minimum expected"
              value={fleetState.minimumHealthyInstances}
              testId="fleet-minimum-instances"
            />
            <FleetMetric
              label="Running desired"
              value={fleetState.runningDesiredVersion}
              testId="fleet-running-desired"
            />
            <FleetMetric
              label="Mismatched"
              value={fleetState.mismatched}
              testId="fleet-mismatched"
            />
            <FleetMetric
              label="Errors"
              value={fleetState.errors}
              testId="fleet-errors"
            />
          </dl>

          <dl className="grid gap-3 text-sm sm:grid-cols-2">
            <div>
              <dt className="text-muted-foreground">Desired version</dt>
              <dd className="mt-1" data-testid="fleet-desired-version">
                {desiredVersion ? (
                  <RegistryCode title={desiredVersion}>{desiredVersion}</RegistryCode>
                ) : (
                  "—"
                )}
              </dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Source version</dt>
              <dd className="mt-1" data-testid="fleet-source-version">
                {fleetState.sourceVersion ? (
                  <RegistryCode title={fleetState.sourceVersion}>
                    {shortenSourceRef(fleetState.sourceVersion)}
                  </RegistryCode>
                ) : (
                  "—"
                )}
              </dd>
            </div>
          </dl>

          <p className="text-xs text-muted-foreground">
            {fleetState.evaluatedAt ? (
              <time
                dateTime={fleetState.evaluatedAt}
                title={formatRegistryTime(fleetState.evaluatedAt)}
                data-testid="fleet-evaluated-at"
              >
                Evaluated {evaluatedAgo || formatRegistryTime(fleetState.evaluatedAt)}
              </time>
            ) : (
              "Evaluation time unavailable"
            )}
            {fleetState.heartbeatTtlSeconds > 0
              ? ` · Heartbeats are fresh for ${formatDurationSeconds(
                  fleetState.heartbeatTtlSeconds,
                )}`
              : null}
          </p>
        </CardContent>
      ) : null}

      {showRecovery && recovery ? (
        <div
          className="border-t border-border px-4 py-3"
          data-testid="recovered-after-failed-rollout"
        >
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="success">Recovered after failed rollout</Badge>
            <time
              className="text-sm text-muted-foreground"
              dateTime={recovery.recoveredAt}
              title={formatRegistryTime(recovery.recoveredAt)}
              data-testid="fleet-recovered-at"
            >
              {recoveredAgo || formatRegistryTime(recovery.recoveredAt)}
            </time>
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            {recovery.liveInstances} live / {recovery.minimumHealthyInstances} minimum
            {recovery.sourceVersion ? (
              <>
                {" "}
                on{" "}
                <RegistryCode title={recovery.sourceVersion}>
                  {shortenSourceRef(recovery.sourceVersion)}
                </RegistryCode>
              </>
            ) : null}
          </p>
        </div>
      ) : null}
    </Card>
  );
}
