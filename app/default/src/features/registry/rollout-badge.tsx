import type { RegistryAppSummary } from "@/features/registry/types";

type BadgeVariant = "success" | "warning" | "destructive" | "muted";

const variantClasses: Record<BadgeVariant, string> = {
  success: "bg-grove-100 text-grove-700 dark:bg-grove-700/20 dark:text-grove-100",
  warning: "bg-gold-100 text-gold-800 dark:bg-gold-900/30 dark:text-gold-100",
  destructive: "bg-ember-50 text-ember-700 dark:bg-ember-700/20 dark:text-ember-50",
  muted: "bg-alpha-10 text-muted",
};

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
  return (
    <span
      data-testid="rollout-badge"
      className={`inline-flex items-center rounded-sm px-1.5 py-0.5 text-xs font-normal ${variantClasses[rolloutBadgeVariant(label)]}`}
    >
      {label}
    </span>
  );
}
