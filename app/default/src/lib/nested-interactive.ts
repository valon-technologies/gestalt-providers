/**
 * Nested interactive ownership for navigable surfaces (table rows, cards, …).
 * Normative: `guidelines/nested-interactive.md`.
 */

export const NESTED_INTERACTIVE_OPT_OUT_ATTR = "data-no-row-click";
export const SURFACE_LINK_ANCHOR_ATTR = "data-row-link";

const NESTED_INTERACTIVE_MATCHERS = [
  "a",
  "button",
  "input",
  "select",
  "textarea",
  "[role=button]",
  "[role=checkbox]",
  "[role=combobox]",
  `[${NESTED_INTERACTIVE_OPT_OUT_ATTR}]`,
] as const;

export const NESTED_INTERACTIVE_SELECTOR = NESTED_INTERACTIVE_MATCHERS.join(",");

export const nestedInteractiveSuppress = {
  tableRow:
    "[&_tr:hover:has(a:not([data-row-link]):hover,button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[data-no-row-click]:hover)]:bg-transparent [&_tr:active:has(a:not([data-row-link]):active,button:active,input:active,select:active,textarea:active,[role=button]:active,[role=checkbox]:active,[role=combobox]:active,[data-no-row-click]:active)]:bg-transparent",
} as const;
