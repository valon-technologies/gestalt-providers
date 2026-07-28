/**
 * Every app in the registry has a *mark* — its visual identity in the catalog.
 * A mark is one of two things, and never both:
 *
 *   brand    — a sanitized SVG the app ships (`Integration.iconSvg`)
 *   monogram — initials derived from the app's own identity, for the ~33 internal
 *              apps that have no brand mark and never will
 *
 * The generic grid glyph remains only as a true last resort, for the case where
 * we can derive neither. Keeping that as a third state rather than folding it
 * into the monogram keeps the invariant honest: a monogram always shows real
 * letters from a real name, never a placeholder.
 *
 * Derivation lives here, apart from rendering, because it is pure, order-
 * dependent, and worth testing on its own. Whether a brand mark is *renderable*
 * can only be known after sanitization, so the brand-vs-monogram choice itself
 * belongs to the renderer (see `IntegrationIcon`) — not to this module.
 */

/** Word separators used in app names and display names alike. */
const WORD_SEPARATORS = /[\s\-_./]+/;

/** Splits camelCase and PascalCase runs: `dealHub` → `deal Hub`. */
function splitCamelCase(value: string): string {
  return value
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2");
}

function words(value: string): string[] {
  return splitCamelCase(value)
    .split(WORD_SEPARATORS)
    .map((word) => word.replace(/[^\p{L}\p{N}]/gu, ""))
    .filter(Boolean);
}

function isShortAcronym(value: string): boolean {
  return /^[\p{Lu}]{2,3}$/u.test(value);
}

/**
 * Initials for an app's monogram: 1–3 characters, always uppercase.
 *
 * Rules, in order:
 *   - A display name that is itself a short acronym is kept whole, so `LLM`
 *     reads as LLM rather than a truncated LL.
 *   - Two or more words take the first letter of the first two — `Deal Hub` → DH,
 *     `front-porch REST API` → FP.
 *   - A single word takes its first two letters — `Jarvis` → JA. Two characters
 *     read as a monogram where one reads as a stray letter.
 *
 * `displayName` is preferred, falling back to `name`, whose camelCase is split
 * first so `dealHub` still yields DH rather than DE.
 *
 * Returns "" when no letters or digits can be found at all, which is the
 * caller's signal to fall back to the generic glyph.
 */
export function appInitials(displayName: string | undefined, name: string): string {
  for (const source of [displayName, name]) {
    if (!source?.trim()) continue;

    const parts = words(source);
    if (parts.length === 0) continue;

    if (parts.length === 1) {
      const [only] = parts;
      if (isShortAcronym(only)) return only;
      return only.slice(0, 2).toUpperCase();
    }

    // A leading acronym still contributes only its first letter, so
    // `CI Workqueue` → CW rather than CI.
    return (parts[0][0] + parts[1][0]).toUpperCase();
  }

  return "";
}
