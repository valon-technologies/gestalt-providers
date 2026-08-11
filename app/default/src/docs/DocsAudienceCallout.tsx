import type { ReactNode } from "react";
import { Info } from "lucide-react";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { DOCS_SETTINGS_TOKENS_HREF } from "./docs-data";
import { DocsLink } from "./DocsLink";

/**
 * Admin-audience callout owned by docs chrome — rendered when
 * `DocsNavItem.audience === "admin"` so the IA model enforces the banner.
 *
 * Persistent orientation chrome (not a transient status flash): use
 * `layout="banner"` so Alert does not assert `role="alert"` on every
 * admin-doc navigation. Keep `variant="info"` for the wash.
 */
export function DocsAudienceCallout({
  children,
}: {
  children?: ReactNode;
}) {
  return (
    <div className="not-typeset mb-[length:var(--typeset-flow,1.5em)]">
      <Alert variant="info" layout="banner">
        <Info aria-hidden="true" />
        <AlertTitle>Admin audience</AlertTitle>
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
      </Alert>
    </div>
  );
}
