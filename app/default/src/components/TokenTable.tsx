import type { APIToken } from "@/lib/api";
import { useRevokeTokenMutation } from "@/lib/queries";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { CopyableCode } from "@/components/ui/copyable-code";
import {
  DescriptionDetails,
  DescriptionItem,
  DescriptionList,
  DescriptionTerm,
} from "@/components/ui/description-list";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  DataTableColumnHeader,
  sortHeaderAriaSort,
} from "@/components/ui/data-table";
import { Link as UiLink } from "@/components/ui/link";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  SETTINGS_TOKENS_EMPTY_DESCRIPTION,
  SETTINGS_TOKENS_EMPTY_TITLE,
  SETTINGS_TOKENS_SCOPES_ALL_LABEL,
  SETTINGS_TOKENS_SCOPES_SHOW_LESS,
  settingsTokensScopesMoreLabel,
} from "@/features/settings/tokens-copy";
import {
  TOKEN_INVENTORY_DEFAULT_SORT,
  splitCollapsedTokenScopes,
  tokenCreatedAtMs,
  tokenCreatedLabel,
  tokenDisplayName,
  tokenExpiresAtMs,
  tokenExpiresLabel,
  tokenScopeEntries,
  tokenScopesSortKey,
  tokenStoredName,
  type TokenScopeEntry,
} from "@/features/settings/token-inventory";
import { cn } from "@/lib/cn";
import { disclosureCaretClassName } from "@/lib/disclosure-caret";
import { appIdFromTokenScope } from "@/lib/tokenScopes";
import { useMemo, useState } from "react";
import { Link } from "@tanstack/react-router";
import { ChevronDown } from "lucide-react";
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
} from "@tanstack/react-table";

interface TokenTableProps {
  tokens: APIToken[];
}

const columnHelper = createColumnHelper<APIToken>();

/** Pin every column except Scopes so expanding permissions cannot reflow the row. */
const TOKEN_INVENTORY_COLUMN_WIDTHS = {
  name: "14rem",
  id: "12rem",
  created: "14rem",
  expires: "14rem",
  actions: "6rem",
} as const;

function TokenInventoryColgroup() {
  return (
    <colgroup>
      <col style={{ width: TOKEN_INVENTORY_COLUMN_WIDTHS.name }} />
      <col style={{ width: TOKEN_INVENTORY_COLUMN_WIDTHS.id }} />
      <col />
      <col style={{ width: TOKEN_INVENTORY_COLUMN_WIDTHS.created }} />
      <col style={{ width: TOKEN_INVENTORY_COLUMN_WIDTHS.expires }} />
      <col style={{ width: TOKEN_INVENTORY_COLUMN_WIDTHS.actions }} />
    </colgroup>
  );
}

function TokenScopeChip({ entry }: { entry: TokenScopeEntry }) {
  const appId = appIdFromTokenScope(entry.scope);
  if (!appId) {
    return <span className="text-muted-foreground">{entry.label}</span>;
  }
  return (
    <UiLink asChild>
      <Link to="/apps/$app" params={{ app: appId }}>
        {entry.label}
      </Link>
    </UiLink>
  );
}

function TokenScopeChips({ entries }: { entries: TokenScopeEntry[] }) {
  return (
    <span className="flex flex-wrap gap-x-2 gap-y-1">
      {entries.map((entry, index) => (
        <TokenScopeChip key={`${entry.key}-${index}`} entry={entry} />
      ))}
    </span>
  );
}

function TokenScopesCell({ token }: { token: APIToken }) {
  const entries = tokenScopeEntries(token);
  const [expanded, setExpanded] = useState(false);

  if (!entries.length) {
    return (
      <span className="text-muted-foreground">
        {SETTINGS_TOKENS_SCOPES_ALL_LABEL}
      </span>
    );
  }

  const { preview, rest } = splitCollapsedTokenScopes(entries);
  if (rest.length === 0) {
    return <TokenScopeChips entries={preview} />;
  }

  return (
    <Collapsible open={expanded} onOpenChange={setExpanded}>
      <div className="min-w-0 space-y-1">
        <span className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
          {preview.map((entry, index) => (
            <TokenScopeChip key={`${entry.key}-${index}`} entry={entry} />
          ))}
          <Button
            asChild
            variant="ghost"
            size="xs"
            className="group -ml-1 gap-0.5 px-1.5 text-muted-foreground"
          >
            <CollapsibleTrigger
              type="button"
              className="w-auto max-w-none justify-start gap-0.5 px-1.5 font-normal text-muted-foreground"
            >
              {expanded
                ? SETTINGS_TOKENS_SCOPES_SHOW_LESS
                : settingsTokensScopesMoreLabel(rest.length)}
              <ChevronDown
                aria-hidden
                className={cn(
                  disclosureCaretClassName,
                  "stroke-inline-glyph text-current motion-reduce:transition-none",
                )}
              />
            </CollapsibleTrigger>
          </Button>
        </span>
        <CollapsibleContent className="flex flex-wrap gap-x-2 gap-y-1">
          {rest.map((entry, index) => (
            <TokenScopeChip key={`${entry.key}-${index}`} entry={entry} />
          ))}
        </CollapsibleContent>
      </div>
    </Collapsible>
  );
}

export default function TokenTable({ tokens }: TokenTableProps) {
  const revokeToken = useRevokeTokenMutation();
  const [pendingRevokeId, setPendingRevokeId] = useState<string | null>(null);
  const [revokingId, setRevokingId] = useState<string | null>(null);
  const [sorting, setSorting] = useState<SortingState>([
    {
      id: TOKEN_INVENTORY_DEFAULT_SORT.id,
      desc: TOKEN_INVENTORY_DEFAULT_SORT.desc,
    },
  ]);
  const error = revokeToken.error
    ? revokeToken.error instanceof Error
      ? revokeToken.error.message
      : "Failed to revoke token"
    : null;

  const pendingToken = tokens.find((token) => token.id === pendingRevokeId);

  const columns = useMemo(
    () => [
      columnHelper.accessor((row) => tokenDisplayName(row), {
        id: "name",
        meta: { className: "min-w-0", headerClassName: "min-w-0" },
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="Name" />
        ),
        cell: ({ row }) => {
          const stored = tokenStoredName(row.original);
          return stored ? (
            <span className="font-medium text-foreground break-words">
              {stored}
            </span>
          ) : (
            <span className="text-muted-foreground">
              {tokenDisplayName(row.original)}
            </span>
          );
        },
      }),
      columnHelper.accessor("id", {
        meta: { className: "min-w-0", headerClassName: "min-w-0" },
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="ID" />
        ),
        cell: ({ row }) => (
          <CopyableCode value={row.original.id} tooltip="Copy token ID" />
        ),
      }),
      columnHelper.accessor((row) => tokenScopesSortKey(row), {
        id: "scopes",
        meta: { className: "min-w-0", headerClassName: "min-w-0" },
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="Scopes" />
        ),
        cell: ({ row }) => <TokenScopesCell token={row.original} />,
      }),
      columnHelper.accessor((row) => tokenCreatedAtMs(row), {
        id: "createdAt",
        meta: { className: "whitespace-nowrap" },
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="Created" />
        ),
        cell: ({ row }) => (
          <time dateTime={row.original.createdAt} className="text-muted-foreground">
            {tokenCreatedLabel(row.original)}
          </time>
        ),
      }),
      columnHelper.accessor((row) => tokenExpiresAtMs(row), {
        id: "expiresAt",
        meta: { className: "whitespace-nowrap" },
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title="Expires" />
        ),
        cell: ({ row }) => {
          const iso = row.original.expiresAt;
          const label = tokenExpiresLabel(row.original);
          if (!iso || !Number.isFinite(Date.parse(iso))) {
            return <span className="text-muted-foreground">{label}</span>;
          }
          return (
            <time dateTime={iso} className="text-muted-foreground">
              {label}
            </time>
          );
        },
      }),
      columnHelper.display({
        id: "actions",
        enableSorting: false,
        meta: { align: "end" },
        header: () => <span className="sr-only">Actions</span>,
        cell: ({ row }) => (
          <Button
            type="button"
            variant="danger"
            size="xs"
            onClick={() => setPendingRevokeId(row.original.id)}
            disabled={revokingId === row.original.id}
          >
            Revoke
          </Button>
        ),
      }),
    ],
    [revokingId],
  );

  const table = useReactTable({
    data: tokens,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getRowId: (row) => row.id,
    enableSortingRemoval: false,
  });

  async function confirmRevoke() {
    if (!pendingRevokeId) return;
    setRevokingId(pendingRevokeId);
    try {
      await revokeToken.mutateAsync(pendingRevokeId);
      setPendingRevokeId(null);
    } catch {
      // surfaced via mutation error state
    } finally {
      setRevokingId(null);
    }
  }

  if (tokens.length === 0) {
    // Orientation only — primary create CTA lives on the page header.
    return (
      <div className="space-y-2 py-12 text-center">
        <p className="text-sm text-muted-foreground/70">
          {SETTINGS_TOKENS_EMPTY_TITLE}
        </p>
        <p className="text-sm text-muted-foreground">
          {SETTINGS_TOKENS_EMPTY_DESCRIPTION}
        </p>
      </div>
    );
  }

  return (
    <div>
      {error && <p className="mb-4 text-sm text-destructive">{error}</p>}
      <Table variant="line" className="table-fixed">
        <TokenInventoryColgroup />
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
                        : undefined
                    }
                    className={header.column.columnDef.meta?.headerClassName}
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
          {table.getRowModel().rows.map((row) => (
            <TableRow key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <TableCell
                  key={cell.id}
                  align={
                    cell.column.columnDef.meta?.align === "end"
                      ? "end"
                      : undefined
                  }
                  className={cell.column.columnDef.meta?.className}
                >
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>

      <AlertDialog
        open={pendingRevokeId !== null}
        onOpenChange={(open) => {
          if (!open && revokingId === null) {
            setPendingRevokeId(null);
          }
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Revoke token</AlertDialogTitle>
            <AlertDialogDescription>
              This token will be revoked immediately and can&apos;t be restored.
              Any apps or scripts using it will lose API access.
            </AlertDialogDescription>
          </AlertDialogHeader>
          {pendingToken ? (
            <DescriptionList density="condensed" termWidth="6.5rem">
              <DescriptionItem>
                <DescriptionTerm>Token</DescriptionTerm>
                <DescriptionDetails>
                  <span
                    className={
                      tokenStoredName(pendingToken)
                        ? "font-medium"
                        : "text-muted-foreground"
                    }
                  >
                    {tokenDisplayName(pendingToken)}
                  </span>
                </DescriptionDetails>
              </DescriptionItem>
              <DescriptionItem>
                <DescriptionTerm>Token ID</DescriptionTerm>
                <DescriptionDetails>
                  <CopyableCode
                    value={pendingToken.id}
                    tooltip="Copy token ID"
                  />
                </DescriptionDetails>
              </DescriptionItem>
            </DescriptionList>
          ) : null}
          <AlertDialogFooter>
            <AlertDialogCancel disabled={revokingId !== null}>
              Keep token
            </AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              disabled={revokingId !== null}
              onClick={(event) => {
                event.preventDefault();
                void confirmRevoke();
              }}
            >
              {revokingId !== null ? "Revoking..." : "Revoke token"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
