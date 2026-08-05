import { memo } from "react";
import { Badge } from "@/components/ui/badge";
import { Link } from "@/components/ui/link";
import {
  Stat,
  StatGroup,
  StatLabel,
  StatValue,
} from "@/components/ui/stat";
import {
  formatDurationSeconds,
  formatRegistryTime,
  formatRegistryTimeShort,
  shortenSourceRef,
} from "@/features/registry/format";
import {
  presentFleetStatus,
  type FleetStatusView,
} from "@/features/registry/fleet-status-presentation";
import { fleetReplicasPollKey, fleetStatePollKey } from "@/features/registry/fleet-replicas";
import { SnapshotRowLiveReplicas } from "@/features/registry/snapshot-live-replicas";
import { RegistryCode } from "@/features/registry/registry-code";
import { rolloutKey } from "@/features/registry/stable-snapshot-registry";
import type { AppAdminRegistryResponse } from "@/features/registry/types";

function DesiredVersionRef({
  version,
  href,
}: {
  version: string;
  href?: string | null;
}) {
  if (!href) {
    return (
      <RegistryCode title={version}>{version}</RegistryCode>
    );
  }
  return (
    <Link
      href={href}
      target="_blank"
      rel="noreferrer"
      underlineVariant="always"
      className="font-mono text-sm break-all"
      title={version}
      data-testid="fleet-desired-version-link"
    >
      {version}
    </Link>
  );
}

/** Platform SOURCE_VERSION — display only; no tenant/org commit URL in this bundle. */
function SourceVersionRef({ sourceVersion }: { sourceVersion: string }) {
  const short = shortenSourceRef(sourceVersion);
  return (
    <span
      className="font-mono text-sm"
      title={sourceVersion}
      data-testid="fleet-source-version-ref"
    >
      {short}
    </span>
  );
}

function FleetStripBody({
  view,
  replicas,
}: {
  view: FleetStatusView;
  replicas: NonNullable<
    AppAdminRegistryResponse["fleetState"]
  >["replicas"];
}) {
  const recoveredClock = view.recovery?.recoveredAt
    ? formatRegistryTimeShort(view.recovery.recoveredAt, { second: true })
    : null;

  return (
    <>
      <div className="space-y-2">
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
          <Badge
            variant={view.verdict.badgeVariant}
            size="sm"
            data-testid="fleet-state-badge"
          >
            {view.verdict.label}
          </Badge>
          {view.summaryLine ? (
            <p
              className="text-sm text-muted-foreground"
              data-testid="fleet-summary-line"
            >
              {view.summaryLine}
            </p>
          ) : null}
        </div>

        {view.metrics.length > 0 ? (
          <StatGroup className="w-full" data-testid="fleet-metrics">
            {view.metrics.map((fact) => (
              <Stat
                key={fact.id}
                variant="plain"
                className="w-max max-w-full shrink-0"
              >
                <StatLabel>{fact.label}</StatLabel>
                <StatValue className="tabular-nums" data-testid={fact.testId}>
                  {fact.value}
                </StatValue>
              </Stat>
            ))}
          </StatGroup>
        ) : null}

        {view.verdict.description ? (
          <p className="text-sm text-muted-foreground">
            {view.verdict.description}
          </p>
        ) : null}

        {view.showReplicaChips && replicas && replicas.length > 0 ? (
          <div data-testid="fleet-live-replicas">
            <SnapshotRowLiveReplicas
              replicas={replicas}
              className="mt-0"
              hoverScope="fleet"
              heartbeatTtlSeconds={view.heartbeatTtlSeconds}
            />
          </div>
        ) : null}

        {view.failingReplica ? (
          <p
            className="text-sm text-destructive text-pretty"
            data-testid="fleet-failing-replica"
          >
            Replica{" "}
            <span className="font-mono">{view.failingReplica.shortId}</span>
            {" — "}
            {view.failingReplica.error}
          </p>
        ) : null}

        {view.wrongVersionReplica ? (
          <p
            className="text-sm text-muted-foreground text-pretty"
            data-testid="fleet-wrong-version-replica"
          >
            Replica{" "}
            <span className="font-mono">{view.wrongVersionReplica.shortId}</span>
            {" — running "}
            <RegistryCode title={view.wrongVersionReplica.runningVersion}>
              {view.wrongVersionReplica.runningVersion}
            </RegistryCode>
            {view.wrongVersionReplica.desiredVersion ? (
              <>
                {" · desired "}
                <RegistryCode title={view.wrongVersionReplica.desiredVersion}>
                  {view.wrongVersionReplica.desiredVersion}
                </RegistryCode>
              </>
            ) : null}
          </p>
        ) : null}

        {view.pathHint ? (
          <p
            className="text-sm text-muted-foreground"
            data-testid="fleet-path-hint"
          >
            {view.pathHint}
          </p>
        ) : null}
      </div>

      {view.showDesiredVersion || view.showSourceVersion ? (
        <dl className="grid gap-3 text-sm sm:grid-cols-2">
          {view.showDesiredVersion ? (
            <div>
              <dt className="text-muted-foreground">Desired version</dt>
              <dd className="mt-1" data-testid="fleet-desired-version">
                {view.desiredVersion ? (
                  <DesiredVersionRef
                    version={view.desiredVersion}
                    href={view.desiredVersionHref}
                  />
                ) : (
                  "—"
                )}
              </dd>
            </div>
          ) : null}
          {view.showSourceVersion ? (
            <div>
              <dt className="text-muted-foreground">Runtime commit</dt>
              <dd className="mt-1" data-testid="fleet-source-version">
                {view.sourceVersion ? (
                  <SourceVersionRef sourceVersion={view.sourceVersion} />
                ) : (
                  "—"
                )}
              </dd>
              <p className="mt-1 text-xs text-muted-foreground">
                Runtime commit SHA (not the published snapshot).
              </p>
            </div>
          ) : null}
        </dl>
      ) : null}

      {view.showFreshnessWindow && view.heartbeatTtlSeconds > 0 ? (
        <p className="text-xs text-muted-foreground">
          Fresh report window: {formatDurationSeconds(view.heartbeatTtlSeconds)}
        </p>
      ) : null}

      {view.recovery ? (
        <div data-testid="recovered-after-failed-rollout">
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
              Runtime commit{" "}
              <SourceVersionRef sourceVersion={view.recovery.sourceVersion} />
            </p>
          ) : null}
        </div>
      ) : null}
    </>
  );
}

export const AppAdminFleetState = memo(function AppAdminFleetState({
  registry,
  view: viewProp,
}: {
  registry: Pick<
    AppAdminRegistryResponse,
    | "desiredVersion"
    | "fleetState"
    | "recovery"
    | "rollout"
    | "publishedVersions"
    | "pendingVersions"
    | "failedVersions"
  >;
  /** Optional precomputed view — layout uses this to gate the rollout banner. */
  view?: FleetStatusView;
}) {
  const view = viewProp ?? presentFleetStatus(registry);
  const replicas = registry.fleetState?.replicas ?? [];

  if (!registry.fleetState && !view.recovery) {
    return null;
  }

  return (
    <section
      className="space-y-3"
      data-testid="app-admin-fleet-state"
      data-fleet-density={view.density}
      data-owns-rollout-headline={view.ownsActiveRolloutHeadline ? "true" : "false"}
    >
      <FleetStripBody view={view} replicas={replicas} />
    </section>
  );
}, (prev, next) => {
  // Compare only the strip's presentation inputs — not the snapshots-table
  // poll slice (autoDeploy / selectionDisabled), which this component does
  // not take and does not render.
  if (
    fleetReplicasPollKey(prev.registry.fleetState?.replicas) !==
    fleetReplicasPollKey(next.registry.fleetState?.replicas)
  ) {
    return false;
  }
  if (
    fleetStatePollKey(prev.registry.fleetState) !==
    fleetStatePollKey(next.registry.fleetState)
  ) {
    return false;
  }
  if (
    prev.registry.recovery?.recoveredAt !== next.registry.recovery?.recoveredAt ||
    prev.registry.recovery?.sourceVersion !== next.registry.recovery?.sourceVersion
  ) {
    return false;
  }
  if (prev.registry.desiredVersion !== next.registry.desiredVersion) {
    return false;
  }
  if (
    rolloutKey(prev.registry.rollout) !== rolloutKey(next.registry.rollout)
  ) {
    return false;
  }
  // Reconcile keeps these array refs stable across heartbeat-only polls.
  return (
    prev.registry.publishedVersions === next.registry.publishedVersions &&
    prev.registry.pendingVersions === next.registry.pendingVersions &&
    prev.registry.failedVersions === next.registry.failedVersions
  );
});
