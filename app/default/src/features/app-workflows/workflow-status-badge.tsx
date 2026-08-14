import { Badge } from "@/components/ui/badge";
import { normalizeWorkflowStatus } from "@/lib/api";
import { workflowRunBadgeVariant } from "@/lib/workflowActivity";
import { capitalize } from "./workflow-format";

export function WorkflowStatusBadge({
  status,
  size = "sm",
}: {
  status?: string;
  /** Badge size ladder — default `sm` for dense lists; use `default` on page summaries. */
  size?: "sm" | "default" | "lg";
}) {
  const normalized = normalizeWorkflowStatus(status);
  return (
    <Badge size={size} variant={workflowRunBadgeVariant(normalized)}>
      {capitalize(normalized)}
    </Badge>
  );
}
