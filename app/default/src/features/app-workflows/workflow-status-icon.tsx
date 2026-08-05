import {
  RunStatusIndicator,
  type RunStatusIndicatorSize,
} from "@/components/ui/run-status-indicator";
import { normalizeWorkflowStatus } from "@/lib/api";
import { cn } from "@/lib/cn";

export function WorkflowStatusIcon({
  status,
  className,
  title,
  size = "md",
}: {
  status?: string;
  className?: string;
  title?: string;
  size?: RunStatusIndicatorSize;
}) {
  const key = normalizeWorkflowStatus(status);
  return (
    <RunStatusIndicator
      status={key}
      size={size}
      iconOnly
      label={title || key}
      className={cn(className)}
    />
  );
}
