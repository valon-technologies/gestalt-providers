import { Badge } from "@/components/ui/badge";
import { formatRolloutStateLabel } from "@/features/registry/format";
import type { RegistryAppSummary } from "@/features/registry/types";

type BadgeVariant = "success" | "warning" | "destructive" | "muted";

export function rolloutState(app: RegistryAppSummary): string {
  return app.rollout?.state || (app.desiredVersion ? "not started" : "not installed");
}

export function rolloutBadgeLabel(app: RegistryAppSummary): string {
  return formatRolloutStateLabel(rolloutState(app));
}

export function rolloutBadgeVariant(state: string): BadgeVariant {
  switch (state) {
    case "complete":
      return "success";
    case "failed":
      return "destructive";
    case "enrolling":
    case "restarting":
      return "warning";
    default:
      return "muted";
  }
}

export function RolloutBadge({ app }: { app: RegistryAppSummary }) {
  const state = rolloutState(app);
  const label = formatRolloutStateLabel(state);
  const variant = rolloutBadgeVariant(state);
  return (
    <Badge
      data-testid="rollout-badge"
      variant={variant === "muted" ? "muted" : variant}
    >
      {label}
    </Badge>
  );
}
