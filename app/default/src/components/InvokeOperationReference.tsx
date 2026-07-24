import { Link } from "@tanstack/react-router";
import { Link as UiLink } from "@/components/Link";
import { Code } from "@/components/ui/code";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { Eyebrow } from "@/components/ui/eyebrow";
import { SpinnerIcon } from "@/components/icons";
import { useIntegrationOperationsQuery } from "@/hooks/use-server-queries";
import { cn } from "@/lib/cn";

type InvokeOperationReferenceProps = {
  appId: string;
  operationId: string;
  appLabel: string;
  className?: string;
};

export function InvokeOperationReference({
  appId,
  operationId,
  appLabel,
  className,
}: InvokeOperationReferenceProps) {
  const label = `${appId}.${operationId}`;
  const opsQuery = useIntegrationOperationsQuery(appId);
  const operation = opsQuery.data?.find((item) => item.id === operationId);
  const title =
    operation?.title?.trim() && operation.title !== operationId
      ? operation.title
      : null;
  const description =
    operation?.description?.trim() ||
    "Callable operation exposed by this app for agents and the CLI.";

  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        <Link
          to="/apps/$appName"
          params={{ appName: appId }}
          search={{ section: "operations", operation: operationId }}
          data-testid="build-invoke-operation"
          className={cn("inline rounded-sm focus-ring", className)}
        >
          <Code className="cursor-pointer transition-colors hover:bg-neutral-hover">
            {label}
          </Code>
        </Link>
      </HoverCardTrigger>
      <HoverCardContent
        side="top"
        align="start"
        className="w-72 space-y-3 p-4"
        data-testid="build-invoke-operation-hover"
      >
        <div className="space-y-1">
          <Eyebrow>{appLabel}</Eyebrow>
          <p className="font-mono text-sm font-medium text-foreground">
            {operationId}
          </p>
          {title ? (
            <p className="text-sm font-medium text-foreground">{title}</p>
          ) : null}
        </div>
        {opsQuery.isPending ? (
          <p className="flex items-center gap-1.5 text-sm text-muted-foreground">
            <SpinnerIcon className="size-3.5 animate-spin" aria-hidden />
            Loading operation details…
          </p>
        ) : (
          <p className="text-sm text-muted-foreground text-pretty">
            {description}
          </p>
        )}
        {operation?.readOnly ? (
          <p className="text-xs text-faint">Read-only operation</p>
        ) : null}
        <UiLink asChild className="text-sm">
          <Link
            to="/apps/$appName"
            params={{ appName: appId }}
            search={{ section: "operations", operation: operationId }}
          >
            View in app operations
          </Link>
        </UiLink>
      </HoverCardContent>
    </HoverCard>
  );
}
