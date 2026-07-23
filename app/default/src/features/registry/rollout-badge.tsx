import { Badge } from "@/components/ui/badge";
import type { RegistryAppSummary } from "@/features/registry/types";

type BadgeVariant = "success" | "warning" | "destructive" | "muted";

export function rolloutBadgeLabel(app: RegistryAppSummary): string {
  return app.rollout?.state || (app.desiredVersion ? "not started" : "not installed");
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
  const label = rolloutBadgeLabel(app);
  const variant = rolloutBadgeVariant(label);
  return (
    <Badge
      data-testid="rollout-badge"
      variant={variant === "muted" ? "muted" : variant}
    >
      {label}
    </Badge>
  );
}
