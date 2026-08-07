/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { ChevronDownIcon, ChevronLeftIcon, ChevronRightIcon } from "lucide-react";
import {
  DayPicker,
  getDefaultClassNames,
  type DayButtonProps,
} from "react-day-picker";

import { listItemInteraction } from "@/lib/list-item-interaction";
import { cn } from "@/lib/cn";
import { Button, buttonVariants } from "@/components/ui/button";

function Calendar({
  className,
  classNames,
  showOutsideDays = true,
  captionLayout = "label",
  buttonVariant = "ghost",
  formatters,
  components,
  ...props
}: React.ComponentProps<typeof DayPicker> & {
  buttonVariant?: React.ComponentProps<typeof Button>["variant"];
}) {
  const defaultClassNames = getDefaultClassNames();

  return (
    <DayPicker
      showOutsideDays={showOutsideDays}
      className={cn(
        "group/calendar bg-background p-3 [--cell-size:--spacing(8)] [[data-slot=card-content]_&]:bg-transparent [[data-slot=popover-content]_&]:bg-transparent",
        String.raw`rtl:**:[.rdp-button\_next>svg]:rotate-180`,
        String.raw`rtl:**:[.rdp-button\_previous>svg]:rotate-180`,
        className,
      )}
      captionLayout={captionLayout}
      formatters={{
        formatMonthDropdown: (date) => date.toLocaleString("default", { month: "short" }),
        ...formatters,
      }}
      classNames={{
        root: cn("w-fit", defaultClassNames.root),
        months: cn("relative flex flex-col gap-4 md:flex-row", defaultClassNames.months),
        // Size from `--cell-size`, not `w-full`: inside a `w-auto` popover, percentage
        // widths resolve against the viewport and stretch dual-month range picks into
        // tall column highlights.
        month: cn("flex w-fit flex-col gap-4", defaultClassNames.month),
        nav: cn(
          "absolute inset-x-0 top-0 flex w-full items-center justify-between gap-1",
          defaultClassNames.nav,
        ),
        button_previous: cn(
          buttonVariants({ variant: buttonVariant }),
          "size-(--cell-size) p-0 select-none aria-disabled:opacity-50",
          defaultClassNames.button_previous,
        ),
        button_next: cn(
          buttonVariants({ variant: buttonVariant }),
          "size-(--cell-size) p-0 select-none aria-disabled:opacity-50",
          defaultClassNames.button_next,
        ),
        month_caption: cn(
          "flex h-(--cell-size) items-center justify-center gap-1 px-(--cell-size)",
          defaultClassNames.month_caption,
        ),
        dropdowns: cn(
          "flex h-(--cell-size) w-full items-center justify-center gap-1.5 text-sm font-medium",
          defaultClassNames.dropdowns,
        ),
        dropdown_root: cn(
          "relative rounded-md border border-input focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-ring",
          defaultClassNames.dropdown_root,
        ),
        dropdown: cn("absolute inset-0 bg-popover opacity-0", defaultClassNames.dropdown),
        caption_label: cn(
          "font-medium select-none",
          captionLayout === "label"
            ? "text-sm"
            : "flex h-8 items-center gap-1 rounded-md pr-1 pl-2 text-sm [&>svg]:size-3.5 [&>svg]:text-muted-foreground",
          defaultClassNames.caption_label,
        ),
        month_grid: cn("w-fit border-collapse", defaultClassNames.month_grid),
        weekdays: cn("flex w-fit items-center", defaultClassNames.weekdays),
        weekday: cn(
          "flex size-(--cell-size) items-center justify-center rounded-md text-[0.8rem] font-normal text-muted-foreground select-none",
          defaultClassNames.weekday,
        ),
        week: cn("mt-2 flex w-fit items-center", defaultClassNames.week),
        week_number_header: cn(
          "w-(--cell-size) select-none",
          defaultClassNames.week_number_header,
        ),
        week_number: cn(
          "text-[0.8rem] text-muted-foreground select-none",
          defaultClassNames.week_number,
        ),
        day: cn(
          "group/day relative flex size-(--cell-size) shrink-0 items-center justify-center p-0 text-center select-none [&:last-child[data-selected=true]_button]:rounded-r-md",
          props.showWeekNumber
            ? "[&:nth-child(2)[data-selected=true]_button]:rounded-l-md"
            : "[&:first-child[data-selected=true]_button]:rounded-l-md",
          defaultClassNames.day,
        ),
        range_start: cn("rounded-l-md bg-accent", defaultClassNames.range_start),
        range_middle: cn("rounded-none", defaultClassNames.range_middle),
        range_end: cn("rounded-r-md bg-accent", defaultClassNames.range_end),
        today: cn(
          // Today is a muted solid fill on the day button (`data-today`), not an
          // accent fill — accent soft/vivid are reserved for range / selection.
          "rounded-md data-[selected=true]:rounded-none",
          defaultClassNames.today,
        ),
        outside: cn(
          "text-muted-foreground aria-selected:text-muted-foreground",
          defaultClassNames.outside,
        ),
        disabled: cn("text-muted-foreground opacity-50", defaultClassNames.disabled),
        hidden: cn("invisible", defaultClassNames.hidden),
        ...classNames,
      }}
      components={{
        Root: ({ className, rootRef, ...rootProps }) => (
          <div data-slot="calendar" ref={rootRef} className={cn(className)} {...rootProps} />
        ),
        Chevron: ({ className, orientation, ...chevronProps }) => {
          if (orientation === "left") {
            return <ChevronLeftIcon className={cn("size-4", className)} {...chevronProps} />;
          }
          if (orientation === "right") {
            return <ChevronRightIcon className={cn("size-4", className)} {...chevronProps} />;
          }
          return <ChevronDownIcon className={cn("size-4", className)} {...chevronProps} />;
        },
        DayButton: CalendarDayButton,
        WeekNumber: ({ children, ...weekNumberProps }) => (
          <td {...weekNumberProps}>
            <div className="flex size-(--cell-size) items-center justify-center text-center">
              {children}
            </div>
          </td>
        ),
        ...components,
      }}
      {...props}
    />
  );
}

function CalendarDayButton({ className, day, modifiers, ...props }: DayButtonProps) {
  const defaultClassNames = getDefaultClassNames();
  const ref = React.useRef<HTMLButtonElement>(null);

  React.useEffect(() => {
    if (modifiers.focused) ref.current?.focus();
  }, [modifiers.focused]);

  // Selected = single pick or range endpoints (accent-vivid ladder).
  // Soft = range middle only (accent + accent-fill ladder).
  // Today = muted solid fill — neutral landmark, not accent (avoids looking selected).
  const isSelected = Boolean(
    modifiers.range_start ||
      modifiers.range_end ||
      (modifiers.selected && !modifiers.range_middle),
  );
  const isSoft = Boolean(modifiers.range_middle);

  return (
    <Button
      {...props}
      ref={ref}
      variant="ghost"
      size="icon"
      data-day={day.date.toLocaleDateString()}
      data-selected={isSelected || undefined}
      data-soft={isSoft || undefined}
      data-today={modifiers.today || undefined}
      data-range-start={modifiers.range_start || undefined}
      data-range-end={modifiers.range_end || undefined}
      data-range-middle={modifiers.range_middle || undefined}
      className={cn(
        // Fixed cell geometry — do not use `w-full`/`size-full` here: Button
        // `size="icon"` flex mins fight percentage sizing and collapse digits.
        "flex size-(--cell-size) shrink-0 flex-col gap-1 leading-none font-normal text-foreground hover:after:opacity-0 active:after:opacity-0 group-data-[focused=true]/day:relative group-data-[focused=true]/day:z-10 [&>span]:text-xs [&>span]:opacity-70",
        // Keyboard focus = Button's `focus-ring` only (focus-ring.md). Do not add
        // a second ring-* / border-ring — that is the double-ring bug.
        listItemInteraction({ pointer: "css" }),
        "data-[range-start]:rounded-l-md data-[range-end]:rounded-r-md data-[range-middle]:rounded-none",
        // Today muted landmark ONLY when idle. Do not re-assert selected/soft
        // rest fills here — that matches listItemInteraction hover/press
        // specificity and blocks deepen (Bugbot). Selected/soft attrs leave the
        // full accent ladders to listItemInteraction alone.
        "data-[today]:[&:not([data-selected]):not([data-soft])]:bg-muted",
        "data-[today]:[&:not([data-selected]):not([data-soft])]:text-foreground",
        "data-[today]:[&:not([data-selected]):not([data-soft])]:hover:bg-neutral-dark-hover",
        "data-[today]:[&:not([data-selected]):not([data-soft])]:active:bg-neutral-dark-pressed",
        defaultClassNames.day,
        className,
        // Last wins twMerge against Button `size="icon"` (h/w/min).
        "size-(--cell-size) min-h-(--cell-size) min-w-(--cell-size) shrink-0",
      )}
    />
  );
}

export { Calendar, CalendarDayButton };
