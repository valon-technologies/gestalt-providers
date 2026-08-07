import { appIdFromTokenScope } from "@/lib/tokenScopes";
import { useState } from "react";
import { Link } from "@tanstack/react-router";
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
} from "@/features/settings/tokens-copy";

interface TokenTableProps {
  tokens: APIToken[];
}

function TokenScopesCell({ scopes }: { scopes?: string[] }) {
  if (!scopes?.length) {
    return <span className="text-muted-foreground">all</span>;
  }

  return (
    <span className="flex flex-wrap gap-x-2 gap-y-1">
      {scopes.map((scope, index) => {
        const appId = appIdFromTokenScope(scope);
        if (!appId) {
          return (
            <span key={`${scope}-${index}`} className="text-muted-foreground">
              {scope}
            </span>
          );
        }
        return (
          <UiLink key={`${scope}-${index}`} asChild>
            <Link to="/apps/$app" params={{ app: appId }}>
              {scope}
            </Link>
          </UiLink>
        );
      })}
    </span>
  );
}

export default function TokenTable({ tokens }: TokenTableProps) {
  const revokeToken = useRevokeTokenMutation();
  const [pendingRevokeId, setPendingRevokeId] = useState<string | null>(null);
  const [revokingId, setRevokingId] = useState<string | null>(null);
  const error = revokeToken.error
    ? revokeToken.error instanceof Error
      ? revokeToken.error.message
      : "Failed to revoke token"
    : null;

  const pendingToken = tokens.find((token) => token.id === pendingRevokeId);

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
      <Table variant="line">
        <TableHeader>
          <TableRow>
            <TableHead>ID</TableHead>
            <TableHead>Scopes</TableHead>
            <TableHead>Created</TableHead>
            <TableHead>Expires</TableHead>
            <TableHead align="end">
              <span className="sr-only">Actions</span>
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {tokens.map((token) => (
            <TableRow key={token.id}>
              <TableCell>
                <CopyableCode value={token.id} tooltip="Copy token ID" />
              </TableCell>
              <TableCell>
                <TokenScopesCell scopes={token.scopes} />
              </TableCell>
              <TableCell className="text-muted-foreground">
                {new Date(token.createdAt).toLocaleDateString()}
              </TableCell>
              <TableCell className="text-muted-foreground">
                {token.expiresAt
                  ? new Date(token.expiresAt).toLocaleDateString()
                  : "Never"}
              </TableCell>
              <TableCell align="end">
                <Button
                  type="button"
                  variant="danger"
                  size="xs"
                  onClick={() => setPendingRevokeId(token.id)}
                  disabled={revokingId === token.id}
                >
                  Revoke
                </Button>
              </TableCell>
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
            {pendingToken ? (
              <p className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
                Token ID:{" "}
                <CopyableCode
                  value={pendingToken.id}
                  tooltip="Copy token ID"
                />
              </p>
            ) : null}
          </AlertDialogHeader>
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
