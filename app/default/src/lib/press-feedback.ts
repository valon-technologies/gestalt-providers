/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 *
 * On-color state-layer overlay + transparent quiet chrome (`ghost`).
 *
 * Normative: Registry `guidelines/press-feedback.md`, `hover-pressed-color.md`,
 * `buttons.md` (RES-20260617-004 / RES-20260702-002). toolshed#4081 / #4191
 *
 * Prefer these role utilities — never hand-roll `after:bg-current` opacity
 * scrims or `hover:bg-accent` on transparent quiet chrome.
 *
 * | Export | Rest | Hover | Press |
 * | `pressFeedbackScrimClassName` | (inherits fill) | on-color scrim 0.08 | on-color scrim 0.14 |
 * | `secondarySurfaceFillClassName` | `bg-foreground/[0.06]` (parent shows through) | (scrim on Button base) | (scrim) |
 * | `ghostQuietChromePaintClassName` | transparent + muted ink | foreground ink | — |
 * | `ghostQuietChromeClassName` | paint + hover/press scrim | (composed) | (composed) |
 *
 * Button puts the scrim on its **base** (every filled variant needs it) and
 * composes `secondarySurfaceFillClassName` on `variant="secondary"` and
 * `ghostQuietChromePaintClassName` on `variant="ghost"`.
 * Badge (and any surface without a control base) consumes
 * `ghostQuietChromeClassName` on its ghost variant and
 * `secondarySurfaceFillClassName` on secondary.
 */

/** Mechanical `::after` on-color overlay — hover then press. Snap, never transition. */
export const pressFeedbackScrimClassName = [
  "relative",
  "after:pointer-events-none after:absolute after:inset-0 after:rounded-[inherit]",
  "after:bg-current after:opacity-0 after:transition-none",
  "hover:after:opacity-[var(--state-overlay-hover,0.08)]",
  "active:after:opacity-[var(--state-overlay-press,0.14)]",
].join(" ");

/** Kill the scrim under disabled / aria-disabled (disabled-states.md). */
export const pressFeedbackScrimOptOutClassName =
  "disabled:after:hidden aria-disabled:after:hidden";

/**
 * Transparent quiet chrome **paint** — muted ink at rest, foreground on hover.
 * Never `--accent-hover` / `hover:bg-accent` (gold is for menus/breadcrumbs).
 */
export const ghostQuietChromePaintClassName =
  "bg-transparent text-muted-foreground hover:text-foreground";

/**
 * Soft secondary fill — translucent foreground ink so the parent surface shows
 * through (darkens cream / lightens night). Same recipe as Badge `secondary`
 * (`badges-and-tags.md`). Prefer this over `bg-secondary` on controls that sit
 * on washed parents (Alert, tinted cards) — a solid `--secondary` chip reads as
 * a second surface, not a deepen of the parent.
 */
export const secondarySurfaceFillClassName = "bg-foreground/[0.06]";

/**
 * Full transparent quiet chrome for surfaces that do not already carry the
 * press-feedback scrim on a shared base (Badge `ghost`, ad-hoc quiet chips).
 */
export const ghostQuietChromeClassName = [
  ghostQuietChromePaintClassName,
  pressFeedbackScrimClassName,
].join(" ");

/**
 * Activate Badge/Button ghost scrim from a `.group` parent when the painted
 * surface is `pointer-events-none` (e.g. nested chip inside a hit-target button).
 * Pair with `ghostQuietChromeClassName` / Badge `variant="ghost"` — do not
 * hand-roll `after:opacity` at call sites.
 */
export const ghostQuietChromeGroupActivateClassName = [
  "group-hover:text-foreground",
  "group-hover:after:opacity-[var(--state-overlay-hover,0.08)]",
  "group-active:after:opacity-[var(--state-overlay-press,0.14)]",
].join(" ");
