import { createElement, useId } from "react";
import type { ReactNode } from "react";
import { DefaultIcon } from "@/components/icons";
import { cn } from "@/lib/cn";

/**
 * Canonical renderer for Integration.iconSvg from `/api/v1/apps`.
 * Owns the SVG allowlist + ID remapping so multiple icons on one page do not
 * collide. Falls back to DefaultIcon when svg is missing or unsafe.
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

function normalizeSVGAttrName(name: string): string {
  if (name === "class") return "className";
  if (name.startsWith("aria-") || name.startsWith("data-")) {
    return name;
  }
  return name.replace(/[:\-]([a-z])/g, (_, letter: string) =>
    letter.toUpperCase(),
  );
}

function isSafeSVGHref(value: string): boolean {
  const normalized = value.replace(/\s/g, "").toLowerCase();
  return normalized.startsWith("#") || normalized.startsWith("data:image/");
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

function rewriteSVGReferences(value: string, idMap: Map<string, string>): string {
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

export function renderSafeIcon(
  svg: string,
  prefix: string,
): ReactNode | null {
  const doc = new DOMParser().parseFromString(svg, "image/svg+xml");
  const root = doc.documentElement;
  if (root.nodeName !== "svg" || doc.querySelector("parsererror")) {
    return null;
  }
  return renderSafeSVGNode(root, prefix, buildSVGIDMap(root, prefix));
}

export default function IntegrationIcon({
  iconSvg,
  className,
  size = "md",
}: {
  iconSvg?: string;
  className?: string;
  size?: "sm" | "md" | "lg";
}) {
  const iconIDPrefix = `provider-icon-${useId().replace(/:/g, "")}`;
  const iconNode = iconSvg ? renderSafeIcon(iconSvg, iconIDPrefix) : null;

  return (
    <div
      className={cn(
        "flex shrink-0 items-center justify-center rounded-lg bg-base-100 text-muted dark:bg-surface-raised",
        size === "sm" && "h-8 w-8 [&>svg]:h-4 [&>svg]:w-4",
        size === "md" && "h-10 w-10 [&>svg]:h-5 [&>svg]:w-5",
        size === "lg" && "h-12 w-12 [&>svg]:h-7 [&>svg]:w-7",
        className,
      )}
    >
      {iconNode ?? <DefaultIcon />}
    </div>
  );
}
