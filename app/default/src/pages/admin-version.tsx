import { type ReactNode } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { Badge } from "@/components/ui/badge";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SectionHeader,
  SectionHeaderActions,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  APP_VERSIONS_LOAD_ERROR,
  APP_VERSIONS_NAV_LABEL,
  APP_VERSIONS_NO_DATA,
  APP_VERSIONS_NOT_FOUND,
  APP_VERSIONS_NOT_INSTALLED,
  APP_VERSIONS_REPLICA_ERROR,
} from "@/features/admin-access/admin-access-copy";
import {
  adminFleetBadge,
  adminFleetCapacityLabel,
  adminFleetDiagnostic,
  adminFleetRolloutNote,
  adminHeartbeatAgeLabel,
  adminReplicaSourceLabel,
} from "@/features/admin-access/admin-versions-presentation";
import { RolloutBadge } from "@/features/registry/rollout-badge";
import { RegistryCode } from "@/features/registry/registry-code";
import { useDocumentTitle } from "@/hooks/use-document-title";
import type { AdminFleetReplica } from "@/lib/api";
import { isAPIErrorStatus } from "@/lib/api";
import { useAdminRegistryAppQuery } from "@/lib/queries";

function formatTime(value?: string | null): string {
  if (!value) return APP_VERSIONS_NO_DATA;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function SummaryField({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="min-w-0">
      <dt className="text-xs font-medium text-muted-foreground">{label}</dt>
      <dd className="mt-1 min-w-0">{children}</dd>
    </div>
  );
}

function ReplicaTable({ replicas }: { replicas: AdminFleetReplica[] }) {
  if (replicas.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">No replica observations.</p>
    );
  }
  return (
    <div className="overflow-hidden rounded-md border border-border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Instance</TableHead>
            <TableHead>Source</TableHead>
            <TableHead>Heartbeat</TableHead>
            <TableHead>Running version</TableHead>
            <TableHead>Runtime state</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {replicas.map((replica) => (
            <TableRow key={replica.instanceId}>
              <TableCell>
                <RegistryCode>{replica.instanceId}</RegistryCode>
              </TableCell>
              <TableCell>
                <div className="space-y-1">
                  <RegistryCode>{replica.sourceVersion}</RegistryCode>
                  <div className="text-xs text-muted-foreground">
                    {adminReplicaSourceLabel(replica.sourceStatus)}
                  </div>
                </div>
              </TableCell>
              <TableCell>
                {adminHeartbeatAgeLabel(replica.heartbeatAgeSeconds, replica.fresh)}
              </TableCell>
              <TableCell>
                <RegistryCode>
                  {replica.appObservation?.runningVersion || APP_VERSIONS_NO_DATA}
                </RegistryCode>
              </TableCell>
              <TableCell>
                <div className="space-y-1">
                  <span>{replica.appObservation?.state || "unknown"}</span>
                  {replica.appObservation?.lastError ? (
                    <div className="max-w-72 text-xs text-destructive">
                      {APP_VERSIONS_REPLICA_ERROR}
                    </div>
                  ) : null}
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

export default function AdminVersionDetailPage() {
  const { app } = useParams({ from: "/admin/versions/$app" });
  const appQuery = useAdminRegistryAppQuery(app, { refetchInterval: 15_000 });
  const title = appQuery.data?.app ?? app;
  useDocumentTitle(`${title} · ${APP_VERSIONS_NAV_LABEL}`);

  if (appQuery.isPending) {
    return <Skeleton className="h-64 w-full" />;
  }

  if (appQuery.isError || !appQuery.data) {
    const notFound = isAPIErrorStatus(appQuery.error, 404);
    return (
      <div className="space-y-6">
        <Breadcrumb>
          <BreadcrumbList>
            <BreadcrumbItem>
              <BreadcrumbLink asChild>
                <Link to="/admin/versions">{APP_VERSIONS_NAV_LABEL}</Link>
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbPage>{app}</BreadcrumbPage>
            </BreadcrumbItem>
          </BreadcrumbList>
        </Breadcrumb>
        <p className="text-sm text-destructive">
          {notFound ? APP_VERSIONS_NOT_FOUND : APP_VERSIONS_LOAD_ERROR}
        </p>
      </div>
    );
  }

  const detail = appQuery.data;
  const desired = detail.knownVersions.find(
    (item) => item.version === detail.desiredVersion,
  );
  const fleet = adminFleetBadge(detail.fleetState);
  const fleetRolloutNote = adminFleetRolloutNote(
    detail.fleetState,
    detail.rollout?.state,
  );

  return (
    <div className="min-w-0 space-y-8">
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem>
            <BreadcrumbLink asChild>
              <Link to="/admin/versions">{APP_VERSIONS_NAV_LABEL}</Link>
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>{detail.app}</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>

      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>{detail.app}</PageHeaderTitle>
          <PageHeaderDescription>Registry: {detail.registry}</PageHeaderDescription>
        </PageHeaderContent>
        <PageHeaderActions>
          <RolloutBadge app={detail} />
        </PageHeaderActions>
      </PageHeader>

      <section className="min-w-0 space-y-4 overflow-hidden rounded-lg border border-border bg-card p-4 text-card-foreground">
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle>Summary</SectionHeaderTitle>
            <SectionHeaderDescription>
              Fleet-known version and install details.
            </SectionHeaderDescription>
          </SectionHeaderContent>
        </SectionHeader>
        <dl className="grid min-w-0 gap-4 sm:grid-cols-2">
          <SummaryField label="Version">
            <RegistryCode className="block w-full">
              {detail.desiredVersion || APP_VERSIONS_NOT_INSTALLED}
            </RegistryCode>
          </SummaryField>
          <SummaryField label="Latest published">
            <RegistryCode className="block w-full">
              {detail.latestPublished?.version || APP_VERSIONS_NO_DATA}
            </RegistryCode>
          </SummaryField>
          <SummaryField label="Installed by">
            <span className="text-sm break-all">
              {desired?.installedBy || APP_VERSIONS_NO_DATA}
            </span>
          </SummaryField>
          <SummaryField label="Installed at">
            <span className="text-sm">{formatTime(desired?.installedAt)}</span>
          </SummaryField>
        </dl>
      </section>

      <section className="space-y-4 rounded-lg border border-border bg-card p-4 text-card-foreground">
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle>Current fleet</SectionHeaderTitle>
            <SectionHeaderDescription>
              Live runtime observations, independent from the historical rollout outcome.
            </SectionHeaderDescription>
          </SectionHeaderContent>
          <SectionHeaderActions>
            <Badge variant={fleet.badgeVariant}>{fleet.label}</Badge>
          </SectionHeaderActions>
        </SectionHeader>
        <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <SummaryField label="Capacity">
            <span className="text-sm">
              {adminFleetCapacityLabel(detail.fleetState)}
            </span>
          </SummaryField>
          <SummaryField label="Running desired">
            <span className="text-sm">
              {detail.fleetState.runningDesiredVersion}/
              {detail.fleetState.liveInstances}
            </span>
          </SummaryField>
          <SummaryField label="Current source">
            <RegistryCode className="block w-full">
              {detail.fleetState.sourceVersion || "unavailable"}
            </RegistryCode>
          </SummaryField>
          <SummaryField label="Desired version">
            <RegistryCode className="block w-full">
              {detail.fleetState.desiredVersion || "unavailable"}
            </RegistryCode>
          </SummaryField>
          <SummaryField label="Evaluation">
            <span className="text-sm">{adminFleetDiagnostic(detail.fleetState)}</span>
          </SummaryField>
        </dl>
        {fleetRolloutNote ? (
          <p className="text-sm text-muted-foreground">{fleetRolloutNote}</p>
        ) : null}
      </section>

      <section className="space-y-4 rounded-lg border border-border bg-card p-4 text-card-foreground">
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle>Rollout</SectionHeaderTitle>
          </SectionHeaderContent>
        </SectionHeader>
        {detail.rollout ? (
          <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {(
              [
                ["State", detail.rollout.state],
                [
                  "Target source version",
                  detail.rollout.targetSourceVersion || APP_VERSIONS_NO_DATA,
                ],
                ["Created", formatTime(detail.rollout.createdAt)],
                ["Enrollment ends", formatTime(detail.rollout.enrollmentEndsAt)],
                ["Deadline", formatTime(detail.rollout.deadline)],
                ["Completed", formatTime(detail.rollout.completedAt)],
                ["Failed", formatTime(detail.rollout.failedAt)],
              ] as const
            ).map(([label, value]) => (
              <div key={label}>
                <dt className="text-xs font-medium text-muted-foreground">{label}</dt>
                <dd className="mt-1 text-sm">{value}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <p className="text-sm text-muted-foreground">No rollout has started.</p>
        )}
      </section>

      <section className="space-y-5 rounded-lg border border-border bg-card p-4 text-card-foreground">
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle>Replica observations</SectionHeaderTitle>
            <SectionHeaderDescription>
              Heartbeat freshness and app state by process. Superseded sources do not count toward fleet health.
            </SectionHeaderDescription>
          </SectionHeaderContent>
        </SectionHeader>
        <div className="space-y-2">
          <h3 className="text-sm font-medium">Fresh heartbeats</h3>
          <ReplicaTable replicas={detail.freshReplicas} />
        </div>
        <div className="space-y-2">
          <h3 className="text-sm font-medium">Stale heartbeats</h3>
          <ReplicaTable replicas={detail.staleReplicas} />
        </div>
      </section>

      <section className="space-y-4 rounded-lg border border-border bg-card p-4 text-card-foreground">
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle>Replica pool</SectionHeaderTitle>
            <SectionHeaderDescription>
              Enrollment cohort totals for the current rollout (replicas that acknowledged before enrollment closed).
            </SectionHeaderDescription>
          </SectionHeaderContent>
        </SectionHeader>
        {!detail.rollout ? (
          <p className="text-sm text-muted-foreground">No rollout in progress.</p>
        ) : !detail.cohort || detail.cohort.acknowledged === 0 ? (
          <p className="text-sm text-muted-foreground">
            No replicas have joined the rollout cohort yet.
          </p>
        ) : (
          <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <SummaryField label="Acknowledged">
              <span className="text-sm">{detail.cohort.acknowledged}</span>
            </SummaryField>
            <SummaryField label="Materialized">
              <span className="text-sm">{detail.cohort.materialized}</span>
            </SummaryField>
            <SummaryField label="Reloaded">
              <span className="text-sm">{detail.cohort.restarted}</span>
            </SummaryField>
            <SummaryField label="Failed">
              <span className="text-sm">{detail.cohort.failed}</span>
            </SummaryField>
          </dl>
        )}
      </section>
    </div>
  );
}
