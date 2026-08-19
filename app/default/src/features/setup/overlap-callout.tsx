import { Info } from "lucide-react";
import { AlertDescription, AlertTitle, Callout } from "@/components/ui/alert";
import {
  ASSISTANT_OVERLAP_TITLE,
  assistantOverlapBody,
} from "@/lib/assistantConnectionCopy";

/** Standing help for Connect apps: Gestalt MCP is the path for company apps. */
export function SetupOverlapCallout({ agentId }: { agentId: string }) {
  return (
    <Callout variant="info" data-testid="setup-overlap-callout">
      <Info aria-hidden="true" />
      <AlertTitle>{ASSISTANT_OVERLAP_TITLE}</AlertTitle>
      <AlertDescription>{assistantOverlapBody(agentId)}</AlertDescription>
    </Callout>
  );
}
