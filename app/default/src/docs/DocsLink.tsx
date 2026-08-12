import type { ReactNode } from "react";
import { Link as RouterLink } from "@tanstack/react-router";
import { Link as UiLink } from "@/components/ui/link";

/**
 * Docs prose links — Registry Link treatment, with TanStack Router for in-app
 * destinations. Prefer this over raw `<a className="doc-link">` or bare Router
 * `Link` so underline/color stay on the design-system contract.
 */
export function DocsLink({
  to,
  href,
  children,
}: {
  children: ReactNode;
  /** In-app path (TanStack Router). */
  to?: string;
  /** External URL (`target=_blank`). */
  href?: string;
}) {
  if (to) {
    return (
      <UiLink asChild>
        <RouterLink to={to}>{children}</RouterLink>
      </UiLink>
    );
  }

  if (!href) {
    throw new Error("DocsLink requires either `to` or `href`");
  }

  return (
    <UiLink href={href} target="_blank" rel="noreferrer">
      {children}
    </UiLink>
  );
}
