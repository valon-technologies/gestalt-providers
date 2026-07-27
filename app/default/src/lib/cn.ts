/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import { clsx, type ClassValue } from "clsx";
import { extendTailwindMerge } from "tailwind-merge";

// Theme font-size / control tokens ride custom `text-*` / `h-*` / `size-*` names
// that stock tailwind-merge can't classify — it then treats them as text-COLOR
// and drops real color utilities (e.g. text-body-lg kills text-muted-foreground).
// Register them in the matching groups so size and color can coexist, and so
// call-site overrides (text-4xl, h-auto, …) reliably win.
const twMerge = extendTailwindMerge({
  extend: {
    classGroups: {
      "font-size": [
        {
          text: [
            "control-xs",
            "control-sm",
            "control-default",
            "control-lg",
            "body-lg",
            "heading-sm",
            "heading-md",
            "heading-lg",
            "heading-xl",
            "display-sm",
            "display-md",
            "display-lg",
            "display-xl",
          ],
        },
      ],
      h: [{ h: ["control-xs", "control-sm", "control-default", "control-lg"] }],
      size: [{ size: ["control-xs", "control-sm", "control-default", "control-lg"] }],
    },
  },
});

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
