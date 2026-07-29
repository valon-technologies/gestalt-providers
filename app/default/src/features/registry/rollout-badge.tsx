import { Badge } from "@/components/ui/badge";
import { fleetRolloutBadgeLabel } from "@/features/registry/rollout-stepper";
import type { RegistryAppSummary } from "@/features/registry/types";

type BadgeVariant = "success" | "warning" | "destructive" | "muted";

export function rolloutBadgeVariant(app: RegistryAppSummary): BadgeVariant {
  const rolloutState = app.rollout?.state;
  if (rolloutState === "enrolling" || rolloutState === "restarting") {
    return "warning";
  }
  if (rolloutState === "failed") {
    return "destructive";
  }
  if (app.desiredVersion) {
    return "success";
  }
  return "muted";
}

export function RolloutBadge({ app }: { app: RegistryAppSummary }) {
  const label = fleetRolloutBadgeLabel(app);
  const variant = rolloutBadgeVariant(app);
  return (
    <Badge
      data-testid="rollout-badge"
      variant={variant === "muted" ? "muted" : variant}
    >
      {label}
    </Badge>
  );
}
