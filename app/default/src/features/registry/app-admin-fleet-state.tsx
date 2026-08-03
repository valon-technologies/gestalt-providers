import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  formatDurationSeconds,
  formatRegistryTime,
  formatRegistryTimeShort,
  shortenSourceRef,
} from "@/features/registry/format";
import {
  groupFleetReplicasByRunningVersion,
  replicaClassLabel,
  shortInstanceId,
  type FleetReplicaVersionGroup,
} from "@/features/registry/fleet-replicas";
import { presentFleetStatus } from "@/features/registry/fleet-status-presentation";
import { RegistryCode } from "@/features/registry/registry-code";
import type { AppAdminRegistryResponse } from "@/features/registry/types";
import { cn } from "@/lib/cn";

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
      <dd
        className="mt-0.5 text-base font-semibold tabular-nums md:mt-1 md:text-xl"
        data-testid={testId}
      >
        {value}
      </dd>
    </div>
  );
}

function FleetReplicaTree({
  groups,
  bordered,
}: {
  groups: FleetReplicaVersionGroup[];
  bordered?: boolean;
}) {
  if (groups.length === 0) return null;
  return (
    <div
      className={cn(bordered && "space-y-3 border-t border-border pt-3")}
      data-testid="fleet-replica-tree"
    >
      <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        Live replicas
      </p>
      <ul className={cn("space-y-3", !bordered && "mt-3")}>
        {groups.map((group) => (
          <li key={group.version || "__none"} className="space-y-1.5">
            <div className="flex flex-wrap items-baseline gap-2 text-sm">
              <span className="text-muted-foreground">
                {group.version ? "Running" : "No running version"}
              </span>
              {group.version ? (
                <RegistryCode title={group.version}>{group.version}</RegistryCode>
              ) : null}
              <span className="tabular-nums text-muted-foreground">
                ({group.replicas.length})
              </span>
            </div>
            <ul className="space-y-1 border-l border-border pl-3">
              {group.replicas.map((replica) => (
                <li
                  key={replica.instanceId}
                  className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-sm"
                  data-testid="fleet-replica-row"
                  data-replica-class={replica.class}
                >
                  <code
                    className="font-mono text-xs"
                    title={replica.instanceId}
                  >
                    {shortInstanceId(replica.instanceId)}
                  </code>
                  <span className="text-muted-foreground">
                    {replicaClassLabel(replica.class)}
                  </span>
                  {replica.lastError ? (
                    <span
                      className="text-xs text-destructive"
                      title={replica.lastError}
                    >
                      {replica.lastError}
                    </span>
                  ) : null}
                </li>
              ))}
            </ul>
          </li>
        ))}
      </ul>
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
  const view = presentFleetStatus(registry);
  const quiet = view.density === "quiet";
  const replicaGroups = groupFleetReplicasByRunningVersion(
    registry.fleetState?.replicas,
  );
  const hasBody =
    view.metrics.length > 0 ||
    view.showEvaluated ||
    view.showIdentity ||
    replicaGroups.length > 0;
  const evaluatedClock = view.evaluatedAt
    ? formatRegistryTimeShort(view.evaluatedAt, { second: true })
    : null;
  const recoveredClock = view.recovery?.recoveredAt
    ? formatRegistryTimeShort(view.recovery.recoveredAt, { second: true })
    : null;
  const showMetricsBlock =
    view.metrics.length > 0 || view.showEvaluated || view.showIdentity;

  return (
    <Card
      data-testid="app-admin-fleet-state"
      data-fleet-density={view.density}
    >
      <CardHeader
        className={cn(
          "gap-2 sm:flex-row sm:items-start sm:justify-between sm:space-y-0",
          quiet ? "py-3 md:py-4" : "py-4 md:gap-3 md:py-6",
        )}
      >
        <div className="space-y-1 md:space-y-1.5">
          <CardTitle className={cn(quiet ? "text-sm md:text-base" : "text-base md:text-lg")}>
            Current fleet
          </CardTitle>
          <CardDescription>{view.verdict.description}</CardDescription>
        </div>
        <Badge
          variant={view.verdict.badgeVariant}
          size="lg"
          data-testid="fleet-state-badge"
        >
          {view.verdict.label}
        </Badge>
      </CardHeader>

      {hasBody ? (
        <CardContent
          className={cn("space-y-3 pt-0", !quiet && "space-y-4 md:space-y-5")}
        >
          {view.metrics.length > 0 ? (
            <dl
              className={cn(
                "grid gap-3",
                view.metrics.length <= 2
                  ? "grid-cols-2 sm:grid-cols-3"
                  : "grid-cols-2 sm:grid-cols-3 lg:grid-cols-5",
              )}
            >
              {view.metrics.map((fact) => (
                <FleetMetric
                  key={fact.id}
                  label={fact.label}
                  value={fact.value}
                  testId={fact.testId}
                />
              ))}
            </dl>
          ) : null}

          {view.showIdentity ? (
            <dl className="grid gap-3 text-sm sm:grid-cols-2">
              <div>
                <dt className="text-muted-foreground">Desired version</dt>
                <dd className="mt-1" data-testid="fleet-desired-version">
                  {view.desiredVersion ? (
                    <RegistryCode title={view.desiredVersion}>
                      {view.desiredVersion}
                    </RegistryCode>
                  ) : (
                    "—"
                  )}
                </dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Running source</dt>
                <dd className="mt-1" data-testid="fleet-source-version">
                  {view.sourceVersion ? (
                    <RegistryCode title={view.sourceVersion}>
                      {shortenSourceRef(view.sourceVersion)}
                    </RegistryCode>
                  ) : (
                    "—"
                  )}
                </dd>
              </div>
            </dl>
          ) : null}

          {view.showEvaluated ? (
            <p className="text-xs text-muted-foreground">
              {view.evaluatedAt && evaluatedClock ? (
                <time
                  dateTime={view.evaluatedAt}
                  title={formatRegistryTime(view.evaluatedAt)}
                  data-testid="fleet-evaluated-at"
                >
                  Last evaluated {evaluatedClock}
                </time>
              ) : (
                "Evaluation time unavailable"
              )}
              {view.showFreshnessWindow && view.heartbeatTtlSeconds > 0
                ? ` · Fresh report window: ${formatDurationSeconds(
                    view.heartbeatTtlSeconds,
                  )}`
                : null}
            </p>
          ) : null}

          <FleetReplicaTree
            groups={replicaGroups}
            bordered={showMetricsBlock}
          />
        </CardContent>
      ) : null}

      {view.recovery ? (
        <div
          className="border-t border-border px-4 py-3"
          data-testid="recovered-after-failed-rollout"
        >
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="success">Recovered after failed rollout</Badge>
            <time
              className="text-sm text-muted-foreground"
              dateTime={view.recovery.recoveredAt}
              title={formatRegistryTime(view.recovery.recoveredAt)}
              data-testid="fleet-recovered-at"
            >
              {recoveredClock || formatRegistryTime(view.recovery.recoveredAt)}
            </time>
          </div>
          {view.recovery.sourceVersion ? (
            <p className="mt-2 text-xs text-muted-foreground">
              Running source{" "}
              <RegistryCode title={view.recovery.sourceVersion}>
                {shortenSourceRef(view.recovery.sourceVersion)}
              </RegistryCode>
            </p>
          ) : null}
        </div>
      ) : null}
    </Card>
  );
}
