/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 *
 * Console ships the sortable table chrome (column headers + TanStack body). Full
 * list-toolbar DataTable (search, faceted filters, cursor paging) lives in the
 * registry when a surface needs it.
 */

import * as React from "react";
import {
  flexRender,
  type Row,
  type Table as TanStackTable,
} from "@tanstack/react-table";
import { Search } from "lucide-react";

import { Input } from "@/components/ui/input";
import {
  SortHeaderButton,
  sortHeaderAriaSort,
  type SortHeaderAlign,
} from "@/components/ui/sort-header-button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { SearchHighlightQueryContext } from "@/lib/search-highlight-context";
import { cn } from "@/lib/cn";

export { sortHeaderAriaSort } from "@/components/ui/sort-header-button";

/** Primary text row in registry tables — shared baseline + line box across columns. */
export const DATA_TABLE_REGISTRY_PRIMARY_LINE_CLASS =
  "flex min-h-5 items-center text-sm leading-5";

/** Secondary subline in registry tables — timers, metadata below badges. */
export const DATA_TABLE_REGISTRY_SECONDARY_LINE_CLASS =
  "text-xs leading-4 text-muted-foreground";

/** Status icon gutter — px-3 + size-5 indicator + px-3 (12px + 20px + 12px). */
export const DATA_TABLE_REGISTRY_SEVERITY_GUTTER_CLASS = "w-11 px-3";

export function DataTableRegistryPrimaryLine({
  className,
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div className={cn(DATA_TABLE_REGISTRY_PRIMARY_LINE_CLASS, className)}>
      {children}
    </div>
  );
}

export function DataTableRegistrySecondaryLine({
  className,
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div className={cn(DATA_TABLE_REGISTRY_SECONDARY_LINE_CLASS, className)}>
      {children}
    </div>
  );
}

export function DataTableRegistryCell({
  className,
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div className={cn("flex min-w-0 flex-col gap-1", className)}>{children}</div>
  );
}

interface DataTableColumnHeaderProps<TData, TValue>
  extends React.HTMLAttributes<HTMLDivElement> {
  column: import("@tanstack/react-table").Column<TData, TValue>;
  title: string;
  align?: SortHeaderAlign;
}

export function DataTableColumnHeader<TData, TValue>({
  column,
  title,
  className,
  align: alignProp,
}: DataTableColumnHeaderProps<TData, TValue>) {
  if (!column.getCanSort()) {
    return <div className={cn(className)}>{title}</div>;
  }

  const sorted = column.getIsSorted();
  const active = sorted !== false;
  const direction = sorted === "desc" ? "desc" : "asc";
  const align =
    alignProp ??
    (column.columnDef.meta?.align === "end"
      ? "end"
      : column.columnDef.meta?.align === "center"
        ? "center"
        : "start");

  return (
    <SortHeaderButton
      label={title}
      active={active}
      direction={direction}
      align={align}
      className={className}
      onClick={() => column.toggleSorting(sorted === "asc")}
    />
  );
}

export type DataTableViewRowProps = {
  "data-testid"?: string;
  className?: string;
} & Record<string, string | undefined>;

export function DataTableSearchField({
  value,
  onChange,
  placeholder = "Search",
  className,
  testId = "data-table-search",
}: {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  className?: string;
  testId?: string;
}) {
  return (
    <div className={cn("relative w-full min-w-0 sm:w-60", className)}>
      <Search
        className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
        aria-hidden
      />
      <Input
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        aria-label={placeholder}
        className="pl-9"
        data-testid={testId}
      />
    </div>
  );
}

export function DataTableSearchShell({
  search,
  onSearchChange,
  searchPlaceholder = "Search",
  children,
}: {
  search: string;
  onSearchChange: (value: string) => void;
  searchPlaceholder?: string;
  children: React.ReactNode;
}) {
  const deferredSearchHighlightQuery = React.useDeferredValue(search);

  return (
    <SearchHighlightQueryContext.Provider value={deferredSearchHighlightQuery}>
      <div className="space-y-3">
        <div className="flex justify-end">
          <DataTableSearchField
            value={search}
            onChange={onSearchChange}
            placeholder={searchPlaceholder}
          />
        </div>
        {children}
      </div>
    </SearchHighlightQueryContext.Provider>
  );
}

const DATA_TABLE_HIDE_BELOW_CLASS = {
  md: "hidden md:table-cell",
  lg: "hidden lg:table-cell",
} as const;

function dataTableHideBelowClass(
  hideBelow: "md" | "lg" | undefined,
): string | undefined {
  return hideBelow ? DATA_TABLE_HIDE_BELOW_CLASS[hideBelow] : undefined;
}

export function DataTableView<TData>({
  table,
  testId,
  emptyMessage = "No results.",
  getRowProps,
}: {
  table: TanStackTable<TData>;
  testId?: string;
  emptyMessage?: string;
  getRowProps?: (row: Row<TData>) => DataTableViewRowProps | undefined;
}) {
  const columns = table.getVisibleLeafColumns();

  return (
    <div className="overflow-x-auto rounded-md border" data-testid={testId}>
      <Table>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id}>
              {headerGroup.headers.map((header) => {
                const canSort = header.column.getCanSort();
                const sorted = header.column.getIsSorted();
                const sortDirection = sorted === "desc" ? "desc" : "asc";
                return (
                  <TableHead
                    key={header.id}
                    align={
                      header.column.columnDef.meta?.align === "end"
                        ? "end"
                        : header.column.columnDef.meta?.align === "center"
                          ? "center"
                          : undefined
                    }
                    className={cn(
                      header.column.columnDef.meta?.severityGutter &&
                        DATA_TABLE_REGISTRY_SEVERITY_GUTTER_CLASS,
                      header.column.columnDef.meta?.headerClassName,
                      dataTableHideBelowClass(
                        header.column.columnDef.meta?.hideBelow,
                      ),
                    )}
                    aria-sort={
                      canSort
                        ? sortHeaderAriaSort(sorted !== false, sortDirection)
                        : undefined
                    }
                  >
                    {header.isPlaceholder
                      ? null
                      : flexRender(
                          header.column.columnDef.header,
                          header.getContext(),
                        )}
                  </TableHead>
                );
              })}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody>
          {table.getRowModel().rows.length ? (
            table.getRowModel().rows.map((row) => {
              const rowProps = getRowProps?.(row) ?? {};
              const { className, ...dataAttrs } = rowProps;
              return (
                <TableRow key={row.id} className={className} {...dataAttrs}>
                  {row.getVisibleCells().map((cell) => (
                    <TableCell
                      key={cell.id}
                      align={
                        cell.column.columnDef.meta?.align === "end"
                          ? "end"
                          : undefined
                      }
                      className={cn(
                        cell.column.columnDef.meta?.severityGutter &&
                          DATA_TABLE_REGISTRY_SEVERITY_GUTTER_CLASS,
                        cell.column.columnDef.meta?.className,
                        dataTableHideBelowClass(
                          cell.column.columnDef.meta?.hideBelow,
                        ),
                      )}
                    >
                      {flexRender(
                        cell.column.columnDef.cell,
                        cell.getContext(),
                      )}
                    </TableCell>
                  ))}
                </TableRow>
              );
            })
          ) : (
            <TableRow>
              <TableCell
                colSpan={columns.length}
                className="h-24 align-middle text-center text-muted-foreground"
              >
                {emptyMessage}
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  );
}

declare module "@tanstack/react-table" {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  interface ColumnMeta<TData, TValue> {
    align?: "start" | "center" | "end";
    /** Registry status-icon gutter — applies shared width/padding; cells stay baseline-aligned. */
    severityGutter?: boolean;
    /** `TableCell` classes — body alignment (e.g. `align-top`) stays off headers. */
    className?: string;
    /** `TableHead` classes — width/padding for gutter columns, etc. */
    headerClassName?: string;
    /** Hide this column below the given breakpoint (still in the DOM for a11y/search). */
    hideBelow?: "md" | "lg";
  }
}
