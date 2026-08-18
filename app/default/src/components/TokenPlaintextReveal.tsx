import { useEffect, useRef, type ReactNode } from "react";
import { Info } from "lucide-react";

import { AlertDescription, Callout } from "@/components/ui/alert";
import { CopyableCode } from "@/components/ui/copyable-code";

const TOKEN_CREATED_STATUS = "Token created. Copy it now.";

/**
 * One-time API secret: copy-only identifier, not a form field.
 * Persistent guidance under the chip is not a live region.
 * Mint success is announced on the status region; focus moves to copy.
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
  const regionRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const copyButton = regionRef.current?.querySelector("button");
    copyButton?.focus({ preventScroll: true });
  }, []);

  return (
    <div ref={regionRef} className="space-y-4">
      <p className="sr-only" role="status">
        {TOKEN_CREATED_STATUS}
      </p>
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
