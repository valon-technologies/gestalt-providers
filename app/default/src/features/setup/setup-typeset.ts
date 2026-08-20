/**
 * Setup reading column uses Registry typeset (same contract as docs).
 * Chrome islands opt out with `.not-typeset`. Nested islands (not a direct
 * child of `.typeset`) need the flow token so they do not sit flush under prose.
 */

export const SETUP_TYPESET_CLASS = "typeset typeset-docs";

export const SETUP_TYPESET_CHROME_CLASS = "not-typeset";

export const SETUP_TYPESET_NESTED_CHROME_CLASS =
  "not-typeset mt-[length:var(--typeset-flow,1.5em)]";
