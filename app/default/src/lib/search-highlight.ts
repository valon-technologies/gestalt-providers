/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

// UI highlight semantics mirror list-search.ts (provider): token-AND substring, same
// normalization. Keep in sync when search matching changes.
//
// Matching walks a once-built normalized index (O(n)), not per-suffix re-normalization.

export function normalizeSearchText(value: string): string {
  return value.toLowerCase().normalize("NFKD").replace(/[\u0300-\u036f]/g, "");
}

export function searchTokensFromQuery(query: string): string[] {
  return normalizeSearchText(query.trim()).split(/\s+/).filter(Boolean);
}

export type SearchTextPart = { text: string; highlight: boolean };

type NormalizedIndex = {
  normalized: string;
  origAt: number[];
};

function buildNormalizedIndex(text: string): NormalizedIndex {
  let normalized = "";
  const origAt: number[] = [];
  for (let index = 0; index < text.length; ) {
    const codePoint = text.codePointAt(index)!;
    const width = codePoint > 0xffff ? 2 : 1;
    const piece = String.fromCodePoint(codePoint)
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "");
    for (let i = 0; i < piece.length; i += 1) {
      normalized += piece[i]!;
      origAt.push(index);
    }
    index += width;
  }
  return { normalized, origAt };
}

function origRange(
  index: NormalizedIndex,
  normStart: number,
  normEnd: number,
  text: string,
): { start: number; end: number } {
  const start = index.origAt[normStart] ?? 0;
  if (normEnd >= index.origAt.length) {
    return { start, end: text.length };
  }
  const endFromMap = index.origAt[normEnd] ?? text.length;
  if (endFromMap > start) {
    return { start, end: endFromMap };
  }
  const codePoint = text.codePointAt(start)!;
  const width = codePoint > 0xffff ? 2 : 1;
  return { start, end: start + width };
}

function findNextTokenRange(
  index: NormalizedIndex,
  tokens: string[],
  fromNorm: number,
  text: string,
): { start: number; end: number; normEnd: number } | null {
  let best: { start: number; end: number; normEnd: number; normStart: number } | null = null;
  for (const token of tokens) {
    if (!token) continue;
    const at = index.normalized.indexOf(token, fromNorm);
    if (at === -1) continue;
    const range = origRange(index, at, at + token.length, text);
    if (
      !best
      || at < best.normStart
      || (at === best.normStart && token.length > best.normEnd - best.normStart)
    ) {
      best = { ...range, normStart: at, normEnd: at + token.length };
    }
  }
  return best ? { start: best.start, end: best.end, normEnd: best.normEnd } : null;
}

export function splitSearchHighlightParts(text: string, query: string): SearchTextPart[] {
  const tokens = searchTokensFromQuery(query);
  if (!tokens.length || !text) return [{ text, highlight: false }];

  const index = buildNormalizedIndex(text);
  const ranges: Array<{ start: number; end: number }> = [];
  let cursor = 0;
  while (cursor < index.normalized.length) {
    const next = findNextTokenRange(index, tokens, cursor, text);
    if (!next) break;
    ranges.push({ start: next.start, end: next.end });
    cursor = next.normEnd;
  }

  if (!ranges.length) return [{ text, highlight: false }];

  const parts: SearchTextPart[] = [];
  let last = 0;
  for (const range of ranges) {
    if (range.end <= last) continue;
    const start = Math.max(range.start, last);
    if (start > last) {
      parts.push({ text: text.slice(last, start), highlight: false });
    }
    parts.push({ text: text.slice(start, range.end), highlight: true });
    last = range.end;
  }
  if (last < text.length) parts.push({ text: text.slice(last), highlight: false });
  return parts;
}

export function textContainsAllSearchTokens(text: string, query: string): boolean {
  const tokens = searchTokensFromQuery(query);
  if (!tokens.length) return true;
  const haystack = normalizeSearchText(text);
  return tokens.every((token) => haystack.includes(token));
}

/** Query tokens absent from `text` (e.g. title tokens already satisfied elsewhere). */
export function searchTokensMissingFromText(text: string, query: string): string[] {
  const tokens = searchTokensFromQuery(query);
  if (!tokens.length) return [];
  const haystack = normalizeSearchText(text);
  return tokens.filter((token) => !haystack.includes(token));
}

export function extractSearchSnippet(
  text: string,
  query: string,
  radius = 48,
  anchorTokens?: string[],
): string | null {
  const tokens = anchorTokens?.length ? anchorTokens : searchTokensFromQuery(query);
  if (!tokens.length || !text.trim()) return null;

  const index = buildNormalizedIndex(text);
  const next = findNextTokenRange(index, tokens, 0, text);
  if (!next) return null;

  const start = Math.max(0, next.start - radius);
  const end = Math.min(text.length, next.end + radius);
  let snippet = text.slice(start, end).replace(/\s+/g, " ").trim();
  if (start > 0) snippet = `…${snippet}`;
  if (end < text.length) snippet = `${snippet}…`;
  return snippet;
}
