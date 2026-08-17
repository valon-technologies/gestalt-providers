import type { ReactNode } from "react";
import { Info } from "lucide-react";
import {
  Alert,
  AlertDescription,
  AlertIcon,
  AlertTitle,
} from "@/components/ui/alert";
import { DOCS_SETTINGS_TOKENS_HREF } from "./docs-data";
import { DocsLink } from "./DocsLink";

/**
 * Admin-audience callout owned by docs chrome, rendered when
 * `DocsNavItem.audience === "admin"` so the IA model enforces the notice.
 *
 * Persistent orientation chrome (not a transient status flash): keep the
 * default stacked layout and set `live={false}` so Alert does not assert
 * `role="alert"` on every admin-doc navigation. Do not use `layout="banner"`
 * just to drop the live region. Keep `variant="info"` for the wash.
 */
export function DocsAudienceCallout({
  children,
}: {
  children?: ReactNode;
}) {
  return (
    <div className="not-typeset mb-[length:var(--typeset-flow,1.5em)]">
      <Alert variant="info" live={false}>
        <AlertIcon>
          <Info aria-hidden="true" />
        </AlertIcon>
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
      </Alert>
    </div>
  );
}
