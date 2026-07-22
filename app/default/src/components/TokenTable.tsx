
import { APIToken, revokeToken } from "@/lib/api";
import Button from "./Button";
import { useState } from "react";
import { useInvalidateTokens } from "@/hooks/use-server-queries";

interface TokenTableProps {
  tokens: APIToken[];
  onRevoked: () => void | Promise<void>;
}

export default function TokenTable({ tokens, onRevoked }: TokenTableProps) {
  const invalidateTokens = useInvalidateTokens();
  const [revoking, setRevoking] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function handleRevoke(id: string) {
    setRevoking(id);
    setError(null);
    try {
      await revokeToken(id);
      await invalidateTokens();
      await onRevoked();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to revoke token");
    } finally {
      setRevoking(null);
    }
  }

  if (tokens.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-alpha p-8">
        <p className="text-center text-sm text-faint">No API tokens yet.</p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-alpha bg-base-white overflow-x-auto dark:bg-surface">
      {error && <p className="mb-4 px-5 pt-4 text-sm text-ember-500">{error}</p>}
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-alpha text-left">
            <th className="px-5 py-3.5 label-text">ID</th>
            <th className="px-5 py-3.5 label-text">Scopes</th>
            <th className="px-5 py-3.5 label-text">Created</th>
            <th className="px-5 py-3.5 label-text">Expires</th>
            <th className="px-5 py-3.5 label-text"></th>
          </tr>
        </thead>
        <tbody>
          {tokens.map((token) => (
            <tr key={token.id} className="border-b border-alpha last:border-b-0">
              <td className="px-5 py-4 text-foreground font-mono text-xs">{token.id}</td>
              <td className="px-5 py-4 text-muted-foreground">
                {token.scopes?.length ? token.scopes.join(" ") : "all"}
              </td>
              <td className="px-5 py-4 text-muted-foreground font-mono text-xs">
                {new Date(token.createdAt).toLocaleDateString()}
              </td>
              <td className="px-5 py-4 text-muted-foreground font-mono text-xs">
                {token.expiresAt
                  ? new Date(token.expiresAt).toLocaleDateString()
                  : "Never"}
              </td>
              <td className="px-5 py-4">
                <Button
                  variant="danger"
                  onClick={() => handleRevoke(token.id)}
                  disabled={revoking === token.id}
                >
                  {revoking === token.id ? "Revoking..." : "Revoke"}
                </Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
