
import { type APIToken } from "@/lib/api";
import { useRevokeManagedIdentityTokenMutation } from "@/lib/queries";
import Button from "./Button";

function formatPermissions(token: APIToken): string {
  if (token.permissions?.length) {
    return token.permissions
      .map((permission) => {
        const parts: string[] = [];
        if (permission.operations?.length) {
          parts.push(permission.operations.join(", "));
        }
        if (permission.actions?.length) {
          parts.push(`actions: ${permission.actions.join(", ")}`);
        }
        return parts.length > 0
          ? `${permission.plugin}: ${parts.join("; ")}`
          : `${permission.plugin}: all`;
      })
      .join(" · ");
  }
  return token.scopes?.length
    ? token.scopes.join(" ")
    : "All authorized access";
}

export default function IdentityTokenTable({
  identityID,
  tokens,
  canRevoke,
}: {
  identityID: string;
  tokens: APIToken[];
  canRevoke: boolean;
}) {
  const revokeToken = useRevokeManagedIdentityTokenMutation(identityID);
  const error = revokeToken.error
    ? revokeToken.error instanceof Error
      ? revokeToken.error.message
      : "Failed to revoke token"
    : null;

  async function handleRevoke(id: string) {
    try {
      await revokeToken.mutateAsync(id);
    } catch {
      // surfaced via mutation error state
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
    <div className="overflow-x-auto rounded-lg border border-border bg-card text-card-foreground">
      {error && <p className="mb-4 px-5 pt-4 text-sm text-destructive">{error}</p>}
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-left">
            <th className="px-5 py-3.5 label-text">Name</th>
            <th className="px-5 py-3.5 label-text">Permissions</th>
            <th className="px-5 py-3.5 label-text">Created</th>
            <th className="px-5 py-3.5 label-text">Expires</th>
            <th className="px-5 py-3.5 label-text"></th>
          </tr>
        </thead>
        <tbody>
          {tokens.map((token) => (
            <tr key={token.id} className="border-b border-border last:border-b-0">
              <td className="px-5 py-4 text-foreground font-medium">{token.name}</td>
              <td className="px-5 py-4 text-muted-foreground">{formatPermissions(token)}</td>
              <td className="px-5 py-4 text-muted-foreground font-mono text-xs">
                {new Date(token.createdAt).toLocaleDateString()}
              </td>
              <td className="px-5 py-4 text-muted-foreground font-mono text-xs">
                {token.expiresAt
                  ? new Date(token.expiresAt).toLocaleDateString()
                  : "Never"}
              </td>
              <td className="px-5 py-4">
                {canRevoke ? (
                  <Button
                    variant="danger"
                    onClick={() => void handleRevoke(token.id)}
                    disabled={revokeToken.isPending && revokeToken.variables === token.id}
                  >
                    {revokeToken.isPending && revokeToken.variables === token.id
                      ? "Revoking..."
                      : "Revoke"}
                  </Button>
                ) : null}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
