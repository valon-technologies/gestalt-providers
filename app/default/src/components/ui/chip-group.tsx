/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import { createContext, useContext, type ComponentProps } from "react";
import * as ToggleGroupPrimitive from "@radix-ui/react-toggle-group";
import { type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import { ChipFilterContent, chipVariants } from "@/components/ui/chip";

type ChipGroupContextValue = VariantProps<typeof chipVariants> & {
  showSelectedCheck?: boolean;
};

const ChipGroupContext = createContext<ChipGroupContextValue>({
  size: "default",
  showSelectedCheck: true,
});

/**
 * Spaced filter-chip set (M3 / shadcnblocks interactive filter chips).
 * Not a joined ToggleGroup toolbar — items keep individual pill chrome.
 */
function ChipGroup({
  className,
  size = "default",
  showSelectedCheck = true,
  children,
  ...props
}: ComponentProps<typeof ToggleGroupPrimitive.Root> &
  Pick<VariantProps<typeof chipVariants>, "size"> & {
    showSelectedCheck?: boolean;
  }) {
  return (
    <ToggleGroupPrimitive.Root
      data-slot="chip-group"
      data-size={size}
      className={cn("flex flex-wrap items-center gap-1.5", className)}
      {...props}
    >
      <ChipGroupContext.Provider value={{ size, showSelectedCheck }}>
        {children}
      </ChipGroupContext.Provider>
    </ToggleGroupPrimitive.Root>
  );
}

function ChipGroupItem({
  className,
  children,
  size,
  showSelectedCheck,
  ...props
}: ComponentProps<typeof ToggleGroupPrimitive.Item> &
  Pick<VariantProps<typeof chipVariants>, "size"> & {
    showSelectedCheck?: boolean;
  }) {
  const context = useContext(ChipGroupContext);
  // Item props override group defaults (context always supplies size/check).
  const resolvedSize = size ?? context.size ?? "default";
  const resolvedShowCheck = showSelectedCheck ?? context.showSelectedCheck ?? true;

  return (
    <ToggleGroupPrimitive.Item
      data-slot="chip"
      data-variant="filter"
      className={cn(
        "group/chip",
        chipVariants({ variant: "filter", size: resolvedSize }),
        className,
      )}
      {...props}
    >
      <ChipFilterContent
        showSelectedCheck={resolvedShowCheck}
        size={resolvedSize}
      >
        {children}
      </ChipFilterContent>
    </ToggleGroupPrimitive.Item>
  );
}

export { ChipGroup, ChipGroupItem };
