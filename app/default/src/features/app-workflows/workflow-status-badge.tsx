import { Badge } from "@/components/ui/badge";
import { normalizeWorkflowStatus } from "@/lib/api";
import { workflowRunBadgeVariant } from "@/lib/workflowActivity";
import { capitalize } from "./workflow-format";

export function WorkflowStatusBadge({ status }: { status?: string }) {
  const normalized = normalizeWorkflowStatus(status);
  return (
    <Badge size="sm" variant={workflowRunBadgeVariant(normalized)}>
      {capitalize(normalized)}
    </Badge>
  );
}
