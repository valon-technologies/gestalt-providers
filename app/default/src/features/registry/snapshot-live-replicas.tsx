import type { ReactNode } from "react";
import { Check, X } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { Eyebrow } from "@/components/ui/eyebrow";
import { RegistryCode } from "@/features/registry/registry-code";
import {
  formatRegistryTimeAgo,
  formatRegistryTime,
} from "@/features/registry/format";
import {
  replicaClassLabel,
  replicaClassMarkTone,
  replicaStatusIndicatorKind,
  shortInstanceId,
} from "@/features/registry/fleet-replicas";
import { useExclusiveReplicaHover } from "@/features/registry/replica-hover-exclusive";
import type { AppAdminFleetReplica } from "@/features/registry/types";
import { cn } from "@/lib/cn";

function ReplicaHoverFacts({ replica }: { replica: AppAdminFleetReplica }) {
  const heartbeatAgo = formatRegistryTimeAgo(replica.heartbeatAt);
  const facts: Array<{ label: string; value: ReactNode }> = [
    {
      label: "Instance",
      value: (
        <code className="font-mono text-xs text-foreground" title={replica.instanceId}>
          {replica.instanceId}
        </code>
      ),
    },
    {
      label: "Status",
      value: replicaClassLabel(replica.class),
    },
    {
      label: "Runtime",
      value: replica.appState || "—",
    },
    {
      label: "Running version",
      value: replica.runningVersion?.trim() ? (
        <RegistryCode title={replica.runningVersion}>
          {replica.runningVersion}
        </RegistryCode>
      ) : (
        "—"
      ),
    },
  ];
  if (replica.observedDesiredVersion?.trim()) {
    facts.push({
      label: "Reported desired version",
      value: (
        <RegistryCode title={replica.observedDesiredVersion}>
          {replica.observedDesiredVersion}
        </RegistryCode>
      ),
    });
  }
  facts.push({
    label: "Heartbeat",
    value: heartbeatAgo
      ? `${heartbeatAgo} (${formatRegistryTime(replica.heartbeatAt)})`
      : formatRegistryTime(replica.heartbeatAt),
  });
  if (replica.lastError?.trim()) {
    facts.push({
      label: "Last error",
      value: (
        <span className="text-destructive text-pretty">{replica.lastError}</span>
      ),
    });
  }
  return (
    <dl className="space-y-2">
      {facts.map((fact) => (
        <div key={fact.label} className="space-y-0.5">
          <dt className="text-xs text-muted-foreground">{fact.label}</dt>
          <dd className="text-sm text-foreground break-all">{fact.value}</dd>
        </div>
      ))}
    </dl>
  );
}

/**
 * GitHub Actions–style status mark:
 * success check · skipped dash · pending spinner · failure X.
 */
function ReplicaStatusIndicator({ replicaClass }: { replicaClass: string }) {
  const kind = replicaStatusIndicatorKind(replicaClass);
  const shell =
    "relative inline-flex size-3.5 shrink-0 items-center justify-center rounded-full";

  if (kind === "success") {
    return (
      <span
        aria-hidden
        data-slot="replica-status-indicator"
        data-kind={kind}
        className={cn(shell, "bg-status-indicator-success text-white")}
      >
        <Check className="size-2.5" strokeWidth={3} absoluteStrokeWidth />
      </span>
    );
  }

  if (kind === "failure") {
    return (
      <span
        aria-hidden
        data-slot="replica-status-indicator"
        data-kind={kind}
        className={cn(shell, "bg-status-indicator-danger text-white")}
      >
        <X className="size-2.5" strokeWidth={3} absoluteStrokeWidth />
      </span>
    );
  }

  if (kind === "pending") {
    return (
      <span
        aria-hidden
        data-slot="replica-status-indicator"
        data-kind={kind}
        className={cn(shell)}
      >
        <span className="absolute inset-[3px] rounded-full bg-status-indicator-warning" />
        <span
          className={cn(
            "absolute inset-0 rounded-full border-2 border-status-indicator-warning/35",
            "border-t-status-indicator-warning motion-safe:animate-spin",
          )}
        />
      </span>
    );
  }

  // skipped — hollow circle + diagonal dash
  return (
    <span
      aria-hidden
      data-slot="replica-status-indicator"
      data-kind={kind}
      className={cn(
        shell,
        "border-[1.5px] border-muted-foreground text-muted-foreground",
      )}
    >
      <span className="block h-[1.5px] w-2 -rotate-45 rounded-full bg-current" />
    </span>
  );
}

/**
 * One badge per replica: GitHub Actions–style status mark + unique short id.
 */
function FleetReplicaBadge({
  replica,
  hoverKey,
}: {
  replica: AppAdminFleetReplica;
  hoverKey: string;
}) {
  const shortId = shortInstanceId(replica.instanceId);
  const status = replicaClassLabel(replica.class);
  const tone = replicaClassMarkTone(replica.class);
  const { open, onOpenChange } = useExclusiveReplicaHover(hoverKey);
  return (
    <HoverCard open={open} onOpenChange={onOpenChange} openDelay={0} closeDelay={100}>
      <HoverCardTrigger asChild>
        <button
          type="button"
          data-no-row-click
          data-testid="fleet-replica-indicator"
          data-replica-class={replica.class}
          data-replica-tone={tone}
          aria-label={`Replica ${shortId}: ${status}`}
          className="inline-flex focus-ring rounded-sm outline-none"
        >
          <Badge
            variant="secondary"
            size="default"
            className="pointer-events-none gap-1.5 font-normal"
          >
            <ReplicaStatusIndicator replicaClass={replica.class} />
            <span className="font-mono text-muted-foreground">{shortId}</span>
          </Badge>
        </button>
      </HoverCardTrigger>
      <HoverCardContent
        side="top"
        align="start"
        className="w-80 space-y-3 p-4"
        data-testid="fleet-replica-hover"
      >
        <div className="space-y-1">
          <Eyebrow>Live replica</Eyebrow>
          <p className="font-mono text-sm font-medium text-foreground">{shortId}</p>
          <p className="text-sm text-muted-foreground">{status}</p>
        </div>
        <ReplicaHoverFacts replica={replica} />
      </HoverCardContent>
    </HoverCard>
  );
}

export function SnapshotRowLiveReplicas({
  replicas,
  className,
  hoverScope = "row",
}: {
  replicas: AppAdminFleetReplica[];
  className?: string;
  /** Prefix so the same instance in fleet / row / orphan lists stay exclusive. */
  hoverScope?: string;
}) {
  if (replicas.length === 0) return null;
  return (
    <ul
      className={cn("mt-1.5 flex flex-wrap items-center gap-1.5", className)}
      data-testid="snapshot-row-live-replicas"
      aria-label={`${replicas.length} live replica${replicas.length === 1 ? "" : "s"}`}
    >
      {replicas.map((replica) => (
        <li key={replica.instanceId} data-testid="fleet-replica-row">
          <FleetReplicaBadge
            replica={replica}
            hoverKey={`${hoverScope}:${replica.instanceId}`}
          />
        </li>
      ))}
    </ul>
  );
}
