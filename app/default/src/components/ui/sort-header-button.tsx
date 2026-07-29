/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import { ChevronDown, ChevronUp } from "lucide-react";

import { Button } from "@/components/ui/button";
import { eyebrowVariants } from "@/components/ui/eyebrow";
import { cn } from "@/lib/cn";

export type SortHeaderDirection = "asc" | "desc";
export type SortHeaderAriaSort = "ascending" | "descending" | "none";

/** Apply on `TableHead` — `aria-sort` is invalid on the inner button. */
export function sortHeaderAriaSort(
  active: boolean,
  direction: SortHeaderDirection,
): SortHeaderAriaSort {
  if (!active) return "none";
  return direction === "asc" ? "ascending" : "descending";
}

export type SortHeaderAlign = "start" | "center" | "end";

function sortHeaderButtonOffset(align: SortHeaderAlign = "start") {
  switch (align) {
    case "end":
      return "-mr-2";
    case "center":
      return "-mx-2";
    default:
      return "-ml-2";
  }
}

export function SortHeaderButton({
  label,
  active,
  direction,
  onClick,
  align = "start",
  className,
}: {
  label: string;
  active: boolean;
  direction: SortHeaderDirection;
  onClick: () => void;
  /** Mirror `TableHead` align so padding compensation matches column alignment. */
  align?: SortHeaderAlign;
  className?: string;
}) {
  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      className={cn(sortHeaderButtonOffset(align), "gap-0.5", className)}
      onClick={onClick}
    >
      <span
        className={cn(
          eyebrowVariants({ tone: "secondary" }),
          "leading-none text-inherit",
        )}
      >
        {label}
      </span>
      {active ? (
        direction === "desc" ? (
          <ChevronDown className="size-3.5" aria-hidden="true" />
        ) : (
          <ChevronUp className="size-3.5" aria-hidden="true" />
        )
      ) : null}
    </Button>
  );
}
