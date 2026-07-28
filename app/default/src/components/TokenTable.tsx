import { useState } from "react";
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
import { Code } from "@/components/ui/code";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

interface TokenTableProps {
  tokens: APIToken[];
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
    return (
      <p className="py-12 text-center text-sm text-muted-foreground/70">
        No API tokens yet.
      </p>
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
                <Code>{token.id}</Code>
              </TableCell>
              <TableCell className="text-muted-foreground">
                {token.scopes?.length ? token.scopes.join(" ") : "all"}
              </TableCell>
              <TableCell numeric className="text-muted-foreground">
                {new Date(token.createdAt).toLocaleDateString()}
              </TableCell>
              <TableCell numeric className="text-muted-foreground">
                {token.expiresAt
                  ? new Date(token.expiresAt).toLocaleDateString()
                  : "Never"}
              </TableCell>
              <TableCell align="end">
                <Button
                  type="button"
                  variant="danger"
                  size="sm"
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
              <p className="text-sm text-muted-foreground">
                Token ID: <Code>{pendingToken.id}</Code>
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
