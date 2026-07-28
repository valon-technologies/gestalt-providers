/**
 * Gestalt console vendor of Valon Registry `flyout`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/lib/flyout.ts`).
 * Synced from toolshed origin/main — no token adaptations.
 */

/**
 * Flyout panel chrome (guidelines/flyout.md).
 * - `flyoutMenuPanelClassName` — menu/list surfaces (DropdownMenu, Select): rounded-xl, p-1.
 * - `flyoutContentPanelClassName` — arbitrary content (HoverCard, Popover tier): rounded-md, p-0.
 * Motion is shared; padding/radius follow the flyout taxonomy — do not fork per primitive.
 */
export const flyoutPanelMotionClassName =
  "data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=open]:duration-reveal data-[state=closed]:duration-dismiss data-[state=open]:ease-out-back data-[state=closed]:ease-out-expo data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2";

export const flyoutMenuPanelClassName =
  `z-50 min-w-[8rem] overflow-hidden rounded-xl border bg-popover p-1 text-popover-foreground shadow-md ${flyoutPanelMotionClassName}`;

/** Popover-tier arbitrary content: same motion as menus, p-0 so callers own inset. */
export const flyoutContentPanelClassName =
  `z-50 overflow-hidden rounded-md border bg-popover p-0 text-popover-foreground shadow-md outline-none ${flyoutPanelMotionClassName}`;
