import type { ReactNode } from "react";
import { Info } from "lucide-react";
import { AlertDescription, AlertTitle, Callout } from "@/components/ui/alert";
import { DOCS_SETTINGS_TOKENS_HREF } from "./docs-data";
import { DocsLink } from "./DocsLink";

/**
 * Admin-audience callout owned by docs chrome, rendered when
 * `DocsNavItem.audience === "admin"` so the IA model enforces the notice.
 *
 * Persistent orientation chrome (not a transient status flash): use Callout,
 * not Alert. Keep `variant="info"` for the wash.
 */
export function DocsAudienceCallout({
  children,
}: {
  children?: ReactNode;
}) {
  return (
    <div className="not-typeset mb-[length:var(--typeset-flow,1.5em)]">
      <Callout variant="info">
        <Info aria-hidden="true" />
        <AlertTitle>For admins</AlertTitle>
        <AlertDescription>
          {children ?? (
            <>
              These commands manage who can invoke apps. Personal API tokens for
              scripts and MCP live under{" "}
              <DocsLink to={DOCS_SETTINGS_TOKENS_HREF}>
                Settings → API tokens
              </DocsLink>
              , not on this page.
            </>
          )}
        </AlertDescription>
      </Callout>
    </div>
  );
}
