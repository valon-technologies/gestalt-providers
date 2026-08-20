import { Info } from "lucide-react";
import { AlertDescription, AlertTitle, Callout } from "@/components/ui/alert";
import {
  ASSISTANT_OVERLAP_TITLE,
  assistantOverlapBody,
} from "@/lib/assistantConnectionCopy";
import { SETUP_TYPESET_CHROME_CLASS } from "./setup-typeset";

/** Standing help for Connect apps: Gestalt MCP is the path for company apps. */
export function SetupOverlapCallout({ agentId }: { agentId: string }) {
  return (
    <Callout
      variant="info"
      className={SETUP_TYPESET_CHROME_CLASS}
      data-testid="setup-overlap-callout"
    >
      <Info aria-hidden="true" />
      <AlertTitle>{ASSISTANT_OVERLAP_TITLE}</AlertTitle>
      <AlertDescription>{assistantOverlapBody(agentId)}</AlertDescription>
    </Callout>
  );
}
