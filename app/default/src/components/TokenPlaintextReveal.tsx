import type { ReactNode } from "react";
import { Info } from "lucide-react";

import { Callout } from "@/components/Callout";
import { AlertDescription } from "@/components/ui/alert";
import { CopyableCode } from "@/components/ui/copyable-code";

/**
 * One-time API secret: copy-only identifier, not a form field.
 * Persistent guidance under the chip is not a live region.
 */
export function TokenPlaintextReveal({
  plaintext,
  description,
  actions,
}: {
  plaintext: string;
  description: string;
  actions?: ReactNode;
}) {
  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <div role="group" aria-label="API token">
          <CopyableCode
            value={plaintext}
            className="w-fit max-w-full"
            tooltip="Copy token"
          />
        </div>
        <Callout>
          <Info aria-hidden />
          <AlertDescription>{description}</AlertDescription>
        </Callout>
      </div>
      {actions}
    </div>
  );
}
