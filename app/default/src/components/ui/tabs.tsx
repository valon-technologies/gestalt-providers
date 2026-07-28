"use client";


/**
 * Gestalt console vendor of Valon Registry `tabs`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/tabs.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import { useCallback, useEffect, useRef, useState, type ComponentProps, type Ref } from "react";
import * as TabsPrimitive from "@radix-ui/react-tabs";
import { cva } from "class-variance-authority";

import { cn } from "@/lib/cn";

type IndicatorStyle = { left: string; width: string };

const EMPTY_INDICATOR_STYLE: IndicatorStyle = { left: "0px", width: "0px" };

function assignRef<T>(ref: Ref<T> | undefined, value: T | null) {
  if (typeof ref === "function") {
    ref(value);
  } else if (ref) {
    ref.current = value;
  }
}

function Tabs({ className, ...props }: ComponentProps<typeof TabsPrimitive.Root>) {
  return (
    <TabsPrimitive.Root
      data-slot="tabs"
      className={cn("group/tabs flex flex-col gap-2", className)}
      {...props}
    />
  );
}

// Line underline only — content navigation chrome.
// Mode / option switching (muted track + sliding pill) is SegmentedControl, not Tabs.
// Height comes from TabsTrigger padding (size), not a fixed list h + pb band —
// that pill-era model stacked empty space under the label when lists used h-auto.
const tabsListVariants = cva(
  "group/tabs-list text-muted-foreground relative inline-flex w-fit items-center justify-center gap-1 border-b border-border bg-transparent",
);

// Absolute bottom is the padding edge — above border-b. Shift onto the border
// plane so the accent *is* the selected segment of the divider (no gray peeking
// under the highlight).
const tabsActiveIndicatorClassName =
  "pointer-events-none absolute -bottom-px h-0.5 bg-accent-solid transition-all duration-move ease-out-quart";

type TabsListSize = "sm" | "default" | "lg";

function TabsList({
  className,
  size = "default",
  children,
  ref,
  ...props
}: ComponentProps<typeof TabsPrimitive.List> & { size?: TabsListSize }) {
  const resolvedSize = size ?? "default";

  const listRef = useRef<HTMLDivElement>(null);
  const [activeStyle, setActiveStyle] = useState<IndicatorStyle>(EMPTY_INDICATOR_STYLE);

  const setListRef = useCallback(
    (element: HTMLDivElement | null) => {
      listRef.current = element;
      assignRef(ref, element);
    },
    [ref],
  );

  useEffect(() => {
    const listElement = listRef.current;
    if (!listElement) return;

    // Indicator is abspos against the list scrollport (padding box), not the
    // scrolled content — subtract scrollLeft so overflow-x lists (CodeBlock
    // file/language strips) keep the underline under the visible active tab.
    const updateActiveFromDOM = () => {
      const activeElement = listElement.querySelector('[data-state="active"]') as HTMLElement | null;
      if (activeElement) {
        setActiveStyle({
          left: `${activeElement.offsetLeft - listElement.scrollLeft}px`,
          width: `${activeElement.offsetWidth}px`,
        });
      } else {
        setActiveStyle(EMPTY_INDICATOR_STYLE);
      }
    };

    const animationFrame = requestAnimationFrame(updateActiveFromDOM);

    const mutationObserver = new MutationObserver(updateActiveFromDOM);
    mutationObserver.observe(listElement, { attributes: true, attributeFilter: ["data-state"], subtree: true });

    const resizeObserver = new ResizeObserver(updateActiveFromDOM);
    resizeObserver.observe(listElement);

    listElement.addEventListener("scroll", updateActiveFromDOM, { passive: true });
    window.addEventListener("resize", updateActiveFromDOM);

    return () => {
      cancelAnimationFrame(animationFrame);
      mutationObserver.disconnect();
      resizeObserver.disconnect();
      listElement.removeEventListener("scroll", updateActiveFromDOM);
      window.removeEventListener("resize", updateActiveFromDOM);
    };
  }, []);

  return (
    <TabsPrimitive.List
      ref={setListRef}
      data-slot="tabs-list"
      data-size={resolvedSize}
      className={cn(tabsListVariants(), className)}
      {...props}
    >
      {activeStyle.width !== "0px" && (
        <div
          data-slot="tabs-active-indicator"
          className={tabsActiveIndicatorClassName}
          style={activeStyle}
        />
      )}

      {children}
    </TabsPrimitive.List>
  );
}

function TabsTrigger({
  className,
  value,
  ...props
}: ComponentProps<typeof TabsPrimitive.Trigger>) {
  return (
    <TabsPrimitive.Trigger
      value={value}
      data-slot="tabs-trigger"
      data-value={value}
      // `disabled:text-disabled-foreground!` is important so it wins over the
      // tab's `dark:text-*` / `data-[state=active]:text-*` color variants, which
      // otherwise tie (equal specificity) or outrank a plain `disabled:` rule and
      // leave a disabled tab showing its enabled color. Important applies ONLY
      // while disabled, so enabled hover/active/dark styling is untouched (the
      // button has no such variants, so it doesn't need this). No
      // `disabled:pointer-events-none` — that would suppress `cursor-not-allowed`.
      className={cn(
        // focus-ring-inset: Tabs often sit in overflow (scroll lists, fence shells);
        // an outward ring clips and reads as a half-rounded outline.
        "text-foreground/60 hover:text-foreground data-[state=active]:text-foreground dark:text-muted-foreground dark:hover:text-foreground relative z-10 inline-flex flex-1 cursor-pointer items-center justify-center gap-1.5 rounded-md bg-transparent font-medium whitespace-nowrap transition-[color,background-color] duration-hover-out ease-out-quart hover:duration-hover-in focus-ring-inset disabled:cursor-not-allowed disabled:text-disabled-foreground! data-[state=active]:font-medium [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4",
        // Idle Neutral on paper/transparent track (selectable-rows.md).
        "data-[state=inactive]:hover:bg-neutral-hover data-[state=inactive]:active:bg-neutral-pressed",
        // Balanced px/py — not wide-x / tight-y.
        "group-data-[size=sm]/tabs-list:px-2 group-data-[size=sm]/tabs-list:py-1 group-data-[size=sm]/tabs-list:text-xs",
        "group-data-[size=default]/tabs-list:px-2.5 group-data-[size=default]/tabs-list:py-1.5 group-data-[size=default]/tabs-list:text-sm",
        "group-data-[size=lg]/tabs-list:px-3 group-data-[size=lg]/tabs-list:py-2 group-data-[size=lg]/tabs-list:text-sm",
        className,
      )}
      {...props}
    />
  );
}

function TabsContent({ className, ...props }: ComponentProps<typeof TabsPrimitive.Content>) {
  return (
    <TabsPrimitive.Content
      data-slot="tabs-content"
      className={cn("flex-1 outline-none", className)}
      {...props}
    />
  );
}

export { Tabs, TabsList, TabsTrigger, TabsContent, tabsListVariants };
