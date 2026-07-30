/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { eyebrowVariants } from "@/components/ui/eyebrow";
import { dataValuesClassName } from "@/lib/data-values";
import { nestedInteractiveSuppress } from "@/lib/nested-interactive";
import { cn } from "@/lib/cn";

// Table chrome variants name the header/frame treatment — not the Card they may
// sit in. Compose Card (or DataTable's bordered shell) at the call site.
//
// - `line` — style-guide / Applied Spacing: hairline row separators, transparent
//   header, no outer table border.
// - `surface` — marketing / embedded product tables with a muted header band and
//   hairline body separators. Pair with a Card (`overflow-hidden`, no content
//   padding) when the header fill should meet the rounded frame edge.
const tableVariants = cva("group/table relative w-full overflow-auto", {
  variants: {
    variant: {
      line: "",
      surface: "",
    },
  },
  defaultVariants: {
    variant: "line",
  },
});

const tableAlignClass = {
  start: "text-left",
  center: "text-center",
  end: "text-right",
} as const;

type TableAlign = keyof typeof tableAlignClass;

/**
 * Data-values face for table composition outside `<td>` (custom DataTable cell
 * renderers, inline spans). Prefer `TableCell numeric` when the host is a cell.
 * Alias of `dataValuesClassName` — do not re-derive `font-mono tabular-nums`.
 */
const tableNumericClassName = dataValuesClassName;

export interface TableProps
  extends React.HTMLAttributes<HTMLTableElement>,
    VariantProps<typeof tableVariants> {}

const Table = React.forwardRef<HTMLTableElement, TableProps>(
  ({ className, variant = "line", ...props }, ref) => (
    <div
      data-slot="table-container"
      data-variant={variant ?? "line"}
      className={cn(tableVariants({ variant }))}
    >
      <table
        ref={ref}
        data-slot="table"
        className={cn("w-full caption-bottom text-sm", className)}
        {...props}
      />
    </div>
  ),
);
Table.displayName = "Table";

const TableHeader = React.forwardRef<
  HTMLTableSectionElement,
  React.HTMLAttributes<HTMLTableSectionElement>
>(({ className, ...props }, ref) => (
  <thead
    ref={ref}
    data-slot="table-header"
    className={cn(
      "[&_tr]:border-b",
      // surface: continuous muted header band; keep the hairline into the body.
      "group-data-[variant=surface]/table:bg-muted",
      className,
    )}
    {...props}
  />
));
TableHeader.displayName = "TableHeader";

const TableBody = React.forwardRef<
  HTMLTableSectionElement,
  React.HTMLAttributes<HTMLTableSectionElement>
>(({ className, ...props }, ref) => (
  <tbody
    ref={ref}
    data-slot="table-body"
    className={cn(
      "[&_tr:last-child]:border-0",
      // Row hover/press only in tbody — header/footer rows have no row-level affordance.
      // Nested controls own the hit: suppress wash via nestedInteractive (guidelines/nested-interactive.md).
      // Suppress classes must follow the paint utilities so equal-specificity :has() wins.
      "[&_tr]:transition-colors [&_tr]:duration-hover-out",
      "[&_tr:hover]:bg-neutral-hover [&_tr:hover]:duration-hover-in",
      "[&_tr:active]:bg-neutral-pressed [&_tr:active]:duration-press",
      nestedInteractiveSuppress.tableRow,
      className,
    )}
    {...props}
  />
));
TableBody.displayName = "TableBody";

const TableFooter = React.forwardRef<
  HTMLTableSectionElement,
  React.HTMLAttributes<HTMLTableSectionElement>
>(({ className, ...props }, ref) => (
  <tfoot
    ref={ref}
    data-slot="table-footer"
    className={cn("border-t bg-muted/50 font-medium [&>tr]:last:border-b-0", className)}
    {...props}
  />
));
TableFooter.displayName = "TableFooter";

const TableRow = React.forwardRef<
  HTMLTableRowElement,
  React.HTMLAttributes<HTMLTableRowElement>
>(({ className, ...props }, ref) => (
  <tr
    ref={ref}
    data-slot="table-row"
    className={cn("border-b data-[state=selected]:bg-accent-subtle", className)}
    {...props}
  />
));
TableRow.displayName = "TableRow";

export type TableHeadProps = Omit<
  React.ThHTMLAttributes<HTMLTableCellElement>,
  "align"
> & {
  /** Text alignment — prefer over className `text-*`. Not the HTML align attr. */
  align?: TableAlign;
};

// Column labels are eyebrow microtype at secondary ink (60%) — dense chrome,
// not a kicker over a display value. Pick tone="secondary"; do not override
// text-* after eyebrowVariants() (guidelines/eyebrow.md).
const TableHead = React.forwardRef<HTMLTableCellElement, TableHeadProps>(
  ({ className, align, ...props }, ref) => (
    <th
      ref={ref}
      data-slot="table-head"
      data-align={align}
      className={cn(
        eyebrowVariants({ tone: "secondary" }),
        "h-10 px-3 align-middle",
        align ? tableAlignClass[align] : "text-left",
        className,
      )}
      {...props}
    />
  ),
);
TableHead.displayName = "TableHead";

export type TableCellProps = Omit<
  React.TdHTMLAttributes<HTMLTableCellElement>,
  "align"
> & {
  /**
   * Data-values face (`dataValuesClassName`) for money, counts, IDs.
   * Prefer this over className `font-mono tabular-nums`.
   */
  numeric?: boolean;
  /** Text alignment — prefer over className `text-*`. Not the HTML align attr. */
  align?: TableAlign;
};

const TableCell = React.forwardRef<HTMLTableCellElement, TableCellProps>(
  ({ className, numeric = false, align, ...props }, ref) => (
    <td
      ref={ref}
      data-slot="table-cell"
      data-numeric={numeric || undefined}
      data-align={align}
      className={cn(
        "px-3 py-3 align-baseline",
        align && tableAlignClass[align],
        numeric && dataValuesClassName,
        className,
      )}
      {...props}
    />
  ),
);
TableCell.displayName = "TableCell";

const TableCaption = React.forwardRef<
  HTMLTableCaptionElement,
  React.HTMLAttributes<HTMLTableCaptionElement>
>(({ className, ...props }, ref) => (
  <caption
    ref={ref}
    data-slot="table-caption"
    className={cn("mt-4 text-sm text-muted-foreground", className)}
    {...props}
  />
));
TableCaption.displayName = "TableCaption";

export {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableFooter,
  TableHead,
  TableHeader,
  TableRow,
  tableNumericClassName,
  tableVariants,
};
