import {
  memo,
  useRef,
  type ReactNode,
  type RefObject,
} from "react";
import { Check, TriangleAlert, X } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { CopyableCode } from "@/components/ui/copyable-code";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  buildReplicaHoverPresentation,
  fleetReplicaPresentationEqual,
  replicaClassLabel,
  replicaClassMarkTone,
  replicaHoverDensity,
  replicaHoverFreshness,
  replicaRowSummary,
  replicaStatusIndicatorKind,
  shortInstanceId,
  sortReplicasByTriage,
  type ReplicaVersionAlignment,
} from "@/features/registry/fleet-replicas";
import { useExclusiveReplicaHover } from "@/features/registry/replica-hover-exclusive";
import type { AppAdminFleetReplica } from "@/features/registry/types";
import { useLiveNow } from "@/hooks/use-live-now";
import { ghostQuietChromeGroupActivateClassName } from "@/lib/press-feedback";
import { cn } from "@/lib/cn";

const HOVER_COPYABLE_CODE_CLASS =
  "w-fit max-w-full text-xs [&_code]:text-xs";

function HoverFact({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="space-y-0.5">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="text-sm text-foreground break-all">{children}</dd>
    </div>
  );
}

function VersionAlignmentFacts({
  alignment,
}: {
  alignment: ReplicaVersionAlignment;
}) {
  switch (alignment.kind) {
    case "aligned":
      return (
        <HoverFact label="Version">
          <div className="space-y-1">
            <p className="text-sm text-foreground">Matches desired version</p>
            <CopyableCode
              value={alignment.version}
              className={HOVER_COPYABLE_CODE_CLASS}
              tooltip={`Copy ${alignment.version}`}
            />
          </div>
        </HoverFact>
      );
    case "diverged":
      return (
        <>
          <HoverFact label="Running version">
            <CopyableCode
              value={alignment.running}
              className={HOVER_COPYABLE_CODE_CLASS}
              tooltip={`Copy ${alignment.running}`}
            />
          </HoverFact>
          <HoverFact label="Desired version">
            <CopyableCode
              value={alignment.expected}
              className={HOVER_COPYABLE_CODE_CLASS}
              tooltip={`Copy ${alignment.expected}`}
            />
          </HoverFact>
        </>
      );
    case "running_only":
      return (
        <HoverFact label="Running version">
          <CopyableCode
            value={alignment.version}
            className={HOVER_COPYABLE_CODE_CLASS}
            tooltip={`Copy ${alignment.version}`}
          />
        </HoverFact>
      );
    case "expected_only":
      return (
        <HoverFact label="Desired version">
          <CopyableCode
            value={alignment.version}
            className={HOVER_COPYABLE_CODE_CLASS}
            tooltip={`Copy ${alignment.version}`}
          />
        </HoverFact>
      );
    case "unknown":
      return null;
  }
}

/** Ticks in the portal body only — never on the HoverCard trigger island. */
function ReplicaHoverFreshnessLine({
  heartbeatAt,
  heartbeatTtlSeconds,
}: {
  heartbeatAt: string;
  heartbeatTtlSeconds?: number;
}) {
  const now = useLiveNow({ enabled: true });
  const freshness = replicaHoverFreshness(heartbeatAt, {
    now,
    heartbeatTtlSeconds,
  });
  return (
    <p
      className={cn(
        "text-xs",
        freshness.stale ? "text-destructive" : "text-muted-foreground",
      )}
      title={freshness.absolute}
      data-testid="fleet-replica-hover-freshness"
      data-stale={freshness.stale ? "true" : undefined}
    >
      {freshness.stale
        ? `Last reported ${freshness.relative} · may be stale`
        : `Reported ${freshness.relative}`}
    </p>
  );
}

function ReplicaHoverBody({
  replica,
  heartbeatTtlSeconds,
  pinned,
}: {
  replica: AppAdminFleetReplica;
  heartbeatTtlSeconds?: number;
  pinned: boolean;
}) {
  const presentation = buildReplicaHoverPresentation(replica, {
    heartbeatTtlSeconds,
  });
  const shortId = shortInstanceId(presentation.instanceId);
  const hasFacts =
    Boolean(presentation.processLabel) ||
    presentation.density === "dense" ||
    Boolean(presentation.lastError);
  return (
    <>
      <div className="space-y-2.5">
        <div className="flex flex-col gap-1.5">
          <Eyebrow size="sm" tone="secondary">Live replica</Eyebrow>
          <CopyableCode
            value={presentation.instanceId}
            className={HOVER_COPYABLE_CODE_CLASS}
            tooltip={`Copy ${presentation.instanceId}`}
          >
            {shortId}
          </CopyableCode>
        </div>
        <p className="text-sm text-muted-foreground">{presentation.statusLabel}</p>
        {presentation.statusHint ? (
          <p className="text-xs text-muted-foreground text-pretty">
            {presentation.statusHint}
          </p>
        ) : null}
        <ReplicaHoverFreshnessLine
          heartbeatAt={replica.heartbeatAt}
          heartbeatTtlSeconds={heartbeatTtlSeconds}
        />
      </div>
      {hasFacts ? (
        <dl className="space-y-2">
          {presentation.processLabel ? (
            <HoverFact label="Process">{presentation.processLabel}</HoverFact>
          ) : null}
          {presentation.density === "dense" ? (
            <VersionAlignmentFacts alignment={presentation.alignment} />
          ) : null}
          {presentation.lastError ? (
            <HoverFact label="Last error">
              <span className="text-destructive text-pretty">
                {presentation.lastError}
              </span>
            </HoverFact>
          ) : null}
        </dl>
      ) : null}
      {pinned ? (
        <p className="text-xs text-muted-foreground">Pinned — click chip to unpin</p>
      ) : null}
    </>
  );
}

/**
 * Status mark:
 * success check · steady warning (drift) · failure X · skipped dash.
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

  if (kind === "warning") {
    return (
      <span
        aria-hidden
        data-slot="replica-status-indicator"
        data-kind={kind}
        className={cn(shell, "bg-status-indicator-warning text-white")}
      >
        <TriangleAlert className="size-2.5" strokeWidth={2.5} absoluteStrokeWidth />
      </span>
    );
  }

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
 * Trigger island — presentation-stable props only. Must not re-render on
 * heartbeat clocks (those remount Radix's pointer target). Stale is a discrete
 * presentation bit computed at poll/render time, not a live ticker.
 */
const FleetReplicaTrigger = memo(function FleetReplicaTrigger({
  instanceId,
  replicaClass,
  open,
  pinned,
  stale,
  triggerRef,
  onTriggerClick,
}: {
  instanceId: string;
  replicaClass: string;
  open: boolean;
  pinned: boolean;
  stale: boolean;
  triggerRef: RefObject<HTMLButtonElement | null>;
  onTriggerClick: () => void;
}) {
  const shortId = shortInstanceId(instanceId);
  const status = replicaClassLabel(replicaClass);
  const tone = replicaClassMarkTone(replicaClass);
  const statusWithFreshness = stale ? `${status}, report may be stale` : status;
  return (
    <HoverCardTrigger asChild>
      <button
        ref={triggerRef}
        type="button"
        data-no-row-click
        data-testid="fleet-replica-indicator"
        data-replica-class={replicaClass}
        data-replica-tone={tone}
        data-state={open ? "open" : "closed"}
        data-pinned={pinned ? "true" : undefined}
        data-stale={stale ? "true" : undefined}
        aria-label={
          pinned
            ? `Replica ${shortId}: ${statusWithFreshness}. Pinned — click to unpin`
            : open
              ? `Replica ${shortId}: ${statusWithFreshness}. Click to pin details`
              : `Replica ${shortId}: ${statusWithFreshness}. Show details, click to pin`
        }
        title={
          pinned
            ? "Pinned — click to unpin"
            : open
              ? "Click to pin details"
              : "Show details — click to pin"
        }
        className="group inline-flex min-h-6 min-w-6 cursor-pointer items-center justify-center focus-ring rounded-sm outline-none"
        onClick={(event) => {
          event.preventDefault();
          event.stopPropagation();
          onTriggerClick();
        }}
      >
        <Badge
          variant="ghost"
          size="default"
          className={cn(
            "pointer-events-none font-normal text-foreground",
            ghostQuietChromeGroupActivateClassName,
            open &&
              "text-foreground after:opacity-[var(--state-overlay-hover,0.08)]",
            stale && "ring-1 ring-destructive/40",
            pinned && "ring-1 ring-foreground/25",
          )}
        >
          <ReplicaStatusIndicator replicaClass={replicaClass} />
          <span
            className={cn(
              "font-mono text-foreground",
              stale && "text-destructive",
            )}
          >
            {shortId}
          </span>
        </Badge>
      </button>
    </HoverCardTrigger>
  );
});

/**
 * One badge per replica.
 *
 * Open + pin live in the exclusive session store so both survive chip remounts.
 * Stable trigger identity (reconcile + presentation memo) removes the need for
 * deferred-close timers around Radix synthetic closes.
 */
function FleetReplicaBadge({
  replica,
  hoverKey,
  heartbeatTtlSeconds,
}: {
  replica: AppAdminFleetReplica;
  hoverKey: string;
  heartbeatTtlSeconds?: number;
}) {
  const { open, pinned, onOpenChange, onTriggerClick, dismiss } =
    useExclusiveReplicaHover(hoverKey);
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const stale = replicaHoverFreshness(replica.heartbeatAt, {
    heartbeatTtlSeconds,
  }).stale;

  return (
    <HoverCard
      open={open}
      onOpenChange={onOpenChange}
      openDelay={120}
      closeDelay={200}
    >
      <FleetReplicaTrigger
        instanceId={replica.instanceId}
        replicaClass={replica.class}
        open={open}
        pinned={pinned}
        stale={stale}
        triggerRef={triggerRef}
        onTriggerClick={onTriggerClick}
      />
      <HoverCardContent
        side="top"
        align="start"
        className="w-80 space-y-3 p-4"
        data-testid="fleet-replica-hover"
        data-density={replicaHoverDensity(replica)}
        data-pinned={pinned ? "true" : undefined}
        onEscapeKeyDown={dismiss}
      >
        <ReplicaHoverBody
          replica={replica}
          heartbeatTtlSeconds={heartbeatTtlSeconds}
          pinned={pinned}
        />
      </HoverCardContent>
    </HoverCard>
  );
}

const MemoFleetReplicaBadge = memo(
  FleetReplicaBadge,
  (prev, next) =>
    prev.hoverKey === next.hoverKey &&
    prev.heartbeatTtlSeconds === next.heartbeatTtlSeconds &&
    fleetReplicaPresentationEqual(prev.replica, next.replica) &&
    // Heartbeat may update while open (freshness line); allow that through.
    prev.replica.heartbeatAt === next.replica.heartbeatAt,
);

export function SnapshotRowLiveReplicas({
  replicas,
  className,
  hoverScope = "row",
  heartbeatTtlSeconds,
}: {
  replicas: AppAdminFleetReplica[];
  className?: string;
  /** Prefix so the same instance in fleet / row / orphan lists stay exclusive. */
  hoverScope?: string;
  /** Fleet heartbeat TTL — drives stale copy in the hover card. */
  heartbeatTtlSeconds?: number;
}) {
  if (replicas.length === 0) return null;
  const ordered = sortReplicasByTriage(replicas);
  const summary = replicaRowSummary(ordered);
  return (
    <div
      className={cn("mt-1.5 flex flex-wrap items-center gap-1.5", className)}
      data-testid="snapshot-row-live-replicas"
    >
      {summary ? (
        <span
          className="text-xs text-muted-foreground"
          data-testid="fleet-replica-row-summary"
        >
          {summary}
        </span>
      ) : null}
      <ul
        className="flex flex-wrap items-center gap-1.5"
        aria-label={`${ordered.length} live replica${ordered.length === 1 ? "" : "s"}`}
      >
        {ordered.map((replica) => (
          <li key={replica.instanceId} data-testid="fleet-replica-row">
            <MemoFleetReplicaBadge
              replica={replica}
              hoverKey={`${hoverScope}:${replica.instanceId}`}
              heartbeatTtlSeconds={heartbeatTtlSeconds}
            />
          </li>
        ))}
      </ul>
    </div>
  );
}

