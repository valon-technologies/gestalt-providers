import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/cn";

/** Group principal chrome — same Badge on the admin app list and access roster. */
export function AccessGroupBadge({
  label,
  className,
}: {
  label: string;
  className?: string;
}) {
  return (
    <Badge variant="secondary">
      <span className={cn("truncate", className)}>{label}</span>
    </Badge>
  );
}
