/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import {
  SearchHighlightProvider,
  useSearchHighlightQueryContext,
} from "@/lib/search-highlight-context";
import {
  extractSearchSnippet,
  searchTokensMissingFromText,
  splitSearchHighlightParts,
  textContainsAllSearchTokens,
} from "@/lib/search-highlight";
import { cn } from "@/lib/cn";

export { SearchHighlightProvider };

const HIGHLIGHT_VARIANT_CLASS = {
  default: "bg-accent-highlight",
  vivid: "bg-accent-vivid",
} as const;

export function useSearchHighlightQuery(explicitQuery?: string): string {
  const fromContext = useSearchHighlightQueryContext();
  return explicitQuery ?? fromContext;
}

export function SearchHighlight({
  text,
  className,
  query: explicitQuery,
  variant = "default",
}: {
  text: string;
  className?: string;
  query?: string;
  variant?: "default" | "vivid";
}) {
  const query = useSearchHighlightQuery(explicitQuery);
  const parts = splitSearchHighlightParts(text, query);
  if (!query.trim()) return <>{text}</>;

  return (
    <>
      {parts.map((part, index) => (
        part.highlight ? (
          <mark
            key={`${index}-${part.text}`}
            className={cn(
              "rounded-xs text-inherit",
              HIGHLIGHT_VARIANT_CLASS[variant],
              className,
            )}
          >
            {part.text}
          </mark>
        ) : (
          <span key={`${index}-${part.text}`}>{part.text}</span>
        )
      ))}
    </>
  );
}

export function SearchablePrimaryCell({
  title,
  description,
  strong = true,
  query: explicitQuery,
}: {
  title: string;
  description?: string;
  strong?: boolean;
  query?: string;
}) {
  const query = useSearchHighlightQuery(explicitQuery);
  const showSnippet = Boolean(
    query.trim()
    && description?.trim()
    && !textContainsAllSearchTokens(title, query),
  );
  const snippet = showSnippet
    ? extractSearchSnippet(
        description ?? "",
        query,
        48,
        searchTokensMissingFromText(title, query),
      )
    : null;

  return (
    <div className={cn("min-w-0 truncate text-sm", strong && "font-medium")}>
      <SearchHighlight text={title} query={explicitQuery} />
      {snippet ? (
        <div className="mt-0.5 truncate text-xs font-normal text-muted-foreground">
          <SearchHighlight text={snippet} query={explicitQuery} />
        </div>
      ) : null}
    </div>
  );
}
