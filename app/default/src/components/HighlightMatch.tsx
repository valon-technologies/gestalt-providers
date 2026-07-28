import type { ReactNode } from "react";
import { matchRangesForQuery } from "@/lib/integrationSearch";

/**
 * Case-insensitive highlight of query tokens inside plain text.
 * Tokens are whitespace-split; each occurrence is wrapped in <mark>.
 *
 * Search-match fill uses Registry `bg-accent-vivid` (gold-300) — one rung
 * darker than `accent-highlight` (gold-200) so matches read on card surfaces.
 * Ladder: wash → highlight → vivid (color.md). Do not use accent-wash here.
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
        className="rounded-sm bg-accent-vivid text-inherit"
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
