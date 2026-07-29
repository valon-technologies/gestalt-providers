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
import { ArrowDown, ArrowUp, ChevronsUpDown, Search } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
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

export function columnAriaSort(
  sorted: false | "asc" | "desc",
): "ascending" | "descending" | "none" {
  if (sorted === "asc") return "ascending";
  if (sorted === "desc") return "descending";
  return "none";
}

interface DataTableColumnHeaderProps<TData, TValue>
  extends React.HTMLAttributes<HTMLDivElement> {
  column: import("@tanstack/react-table").Column<TData, TValue>;
  title: string;
}

export function DataTableColumnHeader<TData, TValue>({
  column,
  title,
  className,
}: DataTableColumnHeaderProps<TData, TValue>) {
  if (!column.getCanSort()) {
    return <div className={cn(className)}>{title}</div>;
  }

  const sorted = column.getIsSorted();
  const SortIcon =
    sorted === "desc" ? ArrowDown : sorted === "asc" ? ArrowUp : ChevronsUpDown;
  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      onClick={() => column.toggleSorting(sorted === "asc")}
      className={cn(
        "-ml-3 h-8 hover:bg-neutral-hover active:bg-neutral-pressed hover:after:opacity-0 active:after:opacity-0",
        className,
      )}
    >
      <span>{title}</span>
      <SortIcon aria-hidden />
    </Button>
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
    <div className="overflow-hidden rounded-md border" data-testid={testId}>
      <Table>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id}>
              {headerGroup.headers.map((header) => {
                const canSort = header.column.getCanSort();
                return (
                  <TableHead
                    key={header.id}
                    align={
                      header.column.columnDef.meta?.align === "end"
                        ? "end"
                        : undefined
                    }
                    aria-sort={
                      canSort
                        ? columnAriaSort(header.column.getIsSorted())
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
                className="h-24 text-center text-muted-foreground"
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
    align?: "end";
  }
}
