import type { ReactNode } from "react";
import { matchRangesForQuery } from "@/lib/integrationSearch";

/**
 * Case-insensitive highlight of query tokens inside plain text.
 * Tokens are whitespace-split; each occurrence is wrapped in <mark>.
 *
 * Search-match fill is Registry `bg-accent-highlight` (color.md — between
 * accent-wash and accent-vivid), matching g-issues `SearchHighlight`.
 * Do not use accent-wash here — that rung is for pale hover/surface tint.
 */
export function HighlightMatch({
  text,
  query,
}: {
  text: string;
  query: string;
}): ReactNode {
  const ranges = matchRangesForQuery(text, query);
  if (!text || ranges.length === 0) {
    return text;
  }

  const parts: ReactNode[] = [];
  let cursor = 0;
  ranges.forEach((range, index) => {
    if (range.start > cursor) {
      parts.push(text.slice(cursor, range.start));
    }
    parts.push(
      <mark
        key={`${range.start}-${range.end}-${index}`}
        className="rounded-sm bg-accent-highlight text-inherit"
      >
        {text.slice(range.start, range.end)}
      </mark>,
    );
    cursor = range.end;
  });
  if (cursor < text.length) {
    parts.push(text.slice(cursor));
  }
  return parts;
}
