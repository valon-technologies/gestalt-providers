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

/**
 * A brand mark is either *full-bleed* — it paints its own background, so it
 * should be clipped flush to the tile like an app icon — or a *glyph*, drawn on
 * transparency, which needs to sit inset within a bordered tile to read at a
 * consistent optical size next to its neighbours.
 *
 * Ideally an app would declare this. It cannot today: nothing in the manifest
 * or in `/api/v1/apps` describes an icon's style, so it is inferred from the
 * artwork. Should a declared field ever land, it should win over this and this
 * becomes the fallback.
 *
 * The signal is an *opaque* `<rect>` covering essentially the whole viewBox,
 * which is exactly how the solid-square marks are authored. Glyph marks fail it
 * because they bake 15–20% padding into their viewBox.
 *
 * Inference errs toward inset on purpose. An inset mark is never *wrong*, only
 * occasionally double-framed; a mark wrongly bled to the edge breaks the grid's
 * rhythm and drops its border while its neighbours keep theirs. Two rules were
 * tried and rejected for failing in that direction:
 *
 *   - Counting a full-size `<image>`: a favicon-wrapped raster may be a solid
 *     square or a transparent glyph, and nothing short of decoding the pixels
 *     tells them apart.
 *   - Ignoring `opacity`: several marks paint a deliberately faint
 *     `fill="currentColor" opacity="0.12"` plate, which is a tint, not a
 *     background.
 *
 * Known limitation: a background drawn as a full-size `<path>` reads as a glyph.
 * No shipped mark does that today.
 */
const FULL_BLEED_COVERAGE = 0.97;
const FULL_BLEED_ORIGIN_TOLERANCE = 0.03;

function coversViewBox(element: Element, box: readonly number[]): boolean {
  const [minX, minY, width, height] = box;
  const read = (attribute: string): number =>
    Number.parseFloat(element.getAttribute(attribute) ?? "0") || 0;

  return (
    read("width") >= width * FULL_BLEED_COVERAGE &&
    read("height") >= height * FULL_BLEED_COVERAGE &&
    Math.abs(read("x") - minX) <= width * FULL_BLEED_ORIGIN_TOLERANCE &&
    Math.abs(read("y") - minY) <= height * FULL_BLEED_ORIGIN_TOLERANCE
  );
}

const MIN_BACKGROUND_OPACITY = 0.9;

function isOpaque(element: Element): boolean {
  const fill = element.getAttribute("fill")?.trim().toLowerCase();
  if (!fill || fill === "none" || fill === "transparent") return false;

  for (const attribute of ["opacity", "fill-opacity"]) {
    const raw = element.getAttribute(attribute);
    if (raw === null) continue;
    const value = Number.parseFloat(raw);
    if (Number.isFinite(value) && value < MIN_BACKGROUND_OPACITY) return false;
  }
  return true;
}

/**
 * Share of the tile a glyph's *ink* should occupy. Chosen to match what the
 * common 15%-padded mark already rendered at, so the majority are unaffected.
 */
const TARGET_INK_FRACTION = 0.476;
/** Never inset further than this, or a sparse mark disappears. */
const MIN_INSET = 0.48;
/** Never scale a mark up past what it rendered at before normalisation. */
const MAX_INSET = 0.68;

/**
 * How much of its own viewBox a mark reserves as padding, per side.
 *
 * Marks are authored to wildly different conventions — measured across what we
 * ship, anywhere from 0% to 28%, clustering at 15% — and the padding is encoded
 * as a negative viewBox origin. A mark cropped flush to its artwork has a
 * non-negative origin and therefore no padding at all, which is why it reads
 * oversized beside its neighbours at a fixed inset.
 */
function paddingFraction(box: readonly number[]): number {
  const [minX, minY, width, height] = box;
  const horizontal = minX < 0 ? -minX / width : 0;
  const vertical = minY < 0 ? -minY / height : 0;
  return Math.min(horizontal, vertical);
}

export type BrandMarkShape = {
  /** The mark paints its own background and should be clipped flush. */
  fullBleed: boolean;
  /**
   * Fraction of the tile the `<svg>` should occupy, compensated for the padding
   * the mark bakes into its own viewBox so every glyph's ink lands at one size.
   */
  inset: number;
};

function parseViewBox(root: Element): number[] | null {
  const box = (root.getAttribute("viewBox") ?? "")
    .split(/[\s,]+/)
    .map(Number.parseFloat)
    .filter((value) => Number.isFinite(value));
  return box.length === 4 && box[2] > 0 && box[3] > 0 ? box : null;
}

export function describeBrandMark(svg: string): BrandMarkShape {
  const fallback: BrandMarkShape = { fullBleed: false, inset: MAX_INSET };

  const doc = new DOMParser().parseFromString(svg, "image/svg+xml");
  const root = doc.documentElement;
  if (root.nodeName !== "svg" || doc.querySelector("parsererror")) return fallback;

  const box = parseViewBox(root);
  if (!box) return fallback;

  const fullBleed = Array.from(root.querySelectorAll("rect")).some(
    (rect) => isOpaque(rect) && coversViewBox(rect, box),
  );
  if (fullBleed) return { fullBleed: true, inset: 1 };

  const inner = 1 - 2 * paddingFraction(box);
  const inset = inner > 0 ? TARGET_INK_FRACTION / inner : MAX_INSET;
  return {
    fullBleed: false,
    inset: Math.min(MAX_INSET, Math.max(MIN_INSET, inset)),
  };
}

/** Word separators used in app names and display names alike. */
const WORD_SEPARATORS = /[\s\-_./]+/;

/** Splits camelCase and PascalCase runs: `acmeHub` → `acme Hub`. */
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
 *   - Two or more words take the first letter of the first two — `Acme Hub` → AH,
 *     `field-portal REST API` → FP.
 *   - A single word takes its first two letters — `Example` → EX. Two characters
 *     read as a monogram where one reads as a stray letter.
 *
 * `displayName` is preferred, falling back to `name`, whose camelCase is split
 * first so `acmeHub` still yields AH rather than AC.
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
