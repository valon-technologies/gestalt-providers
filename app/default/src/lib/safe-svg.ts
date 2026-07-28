import { createElement } from "react";
import type { ReactNode } from "react";

/**
 * Allowlist sanitizer for `Integration.iconSvg` from `/api/v1/apps`.
 *
 * Brand marks arrive as raw SVG strings, so nothing here may be trusted. Only
 * the elements and attributes below survive; everything else is dropped. IDs are
 * remapped per render so several icons on one page cannot collide over `url(#…)`
 * references.
 */

const SAFE_SVG_ELEMENTS = new Set([
  "clipPath",
  "circle",
  "defs",
  "ellipse",
  "feColorMatrix",
  "feComponentTransfer",
  "feComposite",
  "feFlood",
  "feFuncA",
  "filter",
  "g",
  "image",
  "line",
  "linearGradient",
  "mask",
  "path",
  "polygon",
  "polyline",
  "radialGradient",
  "rect",
  "stop",
  "svg",
  "title",
  "use",
]);

const SAFE_SVG_ATTRIBUTES = new Set([
  "aria-label",
  "aria-labelledby",
  "clip-path",
  "clip-rule",
  // Coordinate-space declarations. Dropping any of these does not merely lose
  // styling — it silently changes how the sibling x/y/width/height numbers are
  // interpreted (userSpaceOnUse vs objectBoundingBox), which collapses the
  // region to nothing. They must travel with the geometry.
  "clipPathUnits",
  "maskContentUnits",
  "maskUnits",
  "patternContentUnits",
  "patternUnits",
  "color-interpolation-filters",
  "cx",
  "cy",
  "d",
  "fill",
  "fill-opacity",
  "fill-rule",
  "filter",
  "flood-color",
  "gradientTransform",
  "gradientUnits",
  "height",
  "href",
  "id",
  "in",
  "in2",
  "mask",
  // Selects alpha vs luminance masking. Without it a mask falls back to
  // luminance and any dark fill in the mask path erases the artwork.
  "mask-type",
  "offset",
  "opacity",
  "operator",
  "points",
  "preserveAspectRatio",
  "r",
  "result",
  "role",
  "rx",
  "ry",
  "stop-color",
  "stop-opacity",
  "stroke",
  "stroke-linecap",
  "stroke-linejoin",
  "stroke-miterlimit",
  "stroke-opacity",
  "stroke-width",
  "tableValues",
  "transform",
  "type",
  "viewBox",
  "width",
  "x",
  "x1",
  "x2",
  "xlink:href",
  "xmlns",
  "y",
  "y1",
  "y2",
]);

/**
 * Presentation attributes we accept when an icon expresses them through `style`
 * instead. `style` itself stays banned — it is the injection surface — so the
 * few properties that change *geometry* rather than appearance are lifted out
 * individually.
 */
const STYLE_PROPERTIES_AS_ATTRIBUTES = new Set(["mask-type"]);

/**
 * Attributes React has no camelCase mapping for. Camel-casing these produces a
 * literal `maskType="alpha"` in the DOM, which is not the `mask-type` attribute
 * and is silently ignored — so they must be passed through hyphenated.
 */
const VERBATIM_SVG_ATTRIBUTES = new Set(["mask-type"]);

function normalizeSVGAttrName(name: string): string {
  if (name === "class") return "className";
  if (
    name.startsWith("aria-") ||
    name.startsWith("data-") ||
    VERBATIM_SVG_ATTRIBUTES.has(name)
  ) {
    return name;
  }
  return name.replace(/[:-]([a-z])/g, (_, letter: string) =>
    letter.toUpperCase(),
  );
}

function isSafeSVGHref(value: string): boolean {
  const normalized = value.replace(/\s/g, "").toLowerCase();
  return normalized.startsWith("#") || normalized.startsWith("data:image/");
}

/**
 * Pull the allowlisted presentation properties out of a `style` attribute.
 * Illustrator and Figma both emit `style="mask-type:alpha"` rather than the
 * equivalent presentation attribute, so an icon that looks fine in a browser
 * renders blank once `style` is stripped.
 */
function styleDerivedAttributes(style: string): Record<string, string> {
  const derived: Record<string, string> = {};
  for (const declaration of style.split(";")) {
    const separator = declaration.indexOf(":");
    if (separator === -1) continue;
    const property = declaration.slice(0, separator).trim().toLowerCase();
    const value = declaration.slice(separator + 1).trim();
    if (!value || !STYLE_PROPERTIES_AS_ATTRIBUTES.has(property)) continue;
    derived[property] = value;
  }
  return derived;
}

function buildSVGIDMap(root: Element, prefix: string): Map<string, string> {
  const ids = new Map<string, string>();
  let index = 0;
  for (const element of [root, ...Array.from(root.querySelectorAll("[id]"))]) {
    const currentID = element.getAttribute("id");
    if (!currentID) continue;
    ids.set(currentID, `${prefix}-${index}`);
    index += 1;
  }
  return ids;
}

function rewriteSVGReferences(
  value: string,
  idMap: Map<string, string>,
): string {
  let rewritten = value.replace(/url\(#([^)]+)\)/g, (match, id: string) => {
    const mappedID = idMap.get(id);
    return mappedID ? `url(#${mappedID})` : match;
  });
  if (rewritten.startsWith("#")) {
    const mappedID = idMap.get(rewritten.slice(1));
    if (mappedID) {
      rewritten = `#${mappedID}`;
    }
  }
  return rewritten;
}

function renderSafeSVGNode(
  node: ChildNode,
  key: string,
  idMap: Map<string, string>,
): ReactNode | null {
  if (node.nodeType === Node.TEXT_NODE) {
    const text = node.textContent?.trim();
    return text ? text : null;
  }
  if (node.nodeType !== Node.ELEMENT_NODE) {
    return null;
  }

  const element = node as Element;
  const tagName = element.tagName;
  if (!SAFE_SVG_ELEMENTS.has(tagName)) {
    return null;
  }

  const props: Record<string, string> = { key };

  // Seed from `style` first so an explicit presentation attribute on the same
  // element wins, matching SVG's own precedence in the other direction being
  // irrelevant here: we only ever carry one of the two forward.
  const style = element.getAttribute("style");
  if (style) {
    for (const [property, value] of Object.entries(
      styleDerivedAttributes(style),
    )) {
      props[normalizeSVGAttrName(property)] = value;
    }
  }

  for (const attr of Array.from(element.attributes)) {
    if (!SAFE_SVG_ATTRIBUTES.has(attr.name)) {
      continue;
    }

    const value =
      attr.name === "id"
        ? idMap.get(attr.value) ?? attr.value
        : rewriteSVGReferences(attr.value, idMap);
    if (
      (attr.name === "href" || attr.name === "xlink:href") &&
      !isSafeSVGHref(value)
    ) {
      continue;
    }
    props[normalizeSVGAttrName(attr.name)] = value;
  }

  if (tagName === "svg") {
    props["aria-hidden"] = "true";
    props.focusable = "false";
    // Let the frame own layout size; baked-in width/height fight fill.
    delete props.width;
    delete props.height;
  }

  const children: ReactNode[] = [];
  Array.from(element.childNodes).forEach((child, index) => {
    const rendered = renderSafeSVGNode(child, `${key}-${index}`, idMap);
    if (rendered !== null) {
      children.push(rendered);
    }
  });
  return createElement(tagName, props, ...children);
}

/**
 * Parse and sanitize an icon SVG string into React elements.
 * Returns null when the string is not parseable as SVG, so callers can fall
 * back to a placeholder glyph.
 *
 * `prefix` must be unique per rendered icon — it namespaces internal IDs.
 */
export function renderSafeIcon(svg: string, prefix: string): ReactNode | null {
  const doc = new DOMParser().parseFromString(svg, "image/svg+xml");
  const root = doc.documentElement;
  if (root.nodeName !== "svg" || doc.querySelector("parsererror")) {
    return null;
  }
  return renderSafeSVGNode(root, prefix, buildSVGIDMap(root, prefix));
}
