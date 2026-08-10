import type { DocsNavItem } from "./docs-data";
import { DocsLink } from "./DocsLink";

export function DocsJourneyFooter({ item }: { item: DocsNavItem }) {
  const hasPrereqs = (item.prerequisites?.length ?? 0) > 0;
  const hasNext = item.next != null;
  if (!hasPrereqs && !hasNext) return null;

  return (
    <footer
      className="mt-10 space-y-3 border-t border-border pt-6"
      data-testid="docs-journey-footer"
    >
      {hasPrereqs ? (
        <p className="text-sm text-muted-foreground">
          Prerequisites:{" "}
          {item.prerequisites!.map((link, index) => (
            <span key={link.href}>
              {index > 0 ? ", " : null}
              <DocsLink to={link.href}>{link.label}</DocsLink>
            </span>
          ))}
        </p>
      ) : null}
      {hasNext ? (
        <p className="text-sm text-foreground">
          Next:{" "}
          <DocsLink to={item.next!.href}>{item.next!.label}</DocsLink>
        </p>
      ) : null}
    </footer>
  );
}
