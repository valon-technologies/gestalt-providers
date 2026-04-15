"use client";

import { useState } from "react";
import { revokeManagedIdentityToken, type APIToken } from "@/lib/api";
import Button from "./Button";

function formatPermissions(token: APIToken): string {
  if (token.permissions?.length) {
    return token.permissions
      .map((permission) =>
        permission.operations?.length
          ? `${permission.plugin}: ${permission.operations.join(", ")}`
          : `${permission.plugin}: all`,
      )
      .join(" · ");
  }
  return token.scopes || "No visible permissions";
}

export default function IdentityTokenTable({
  identityID,
  tokens,
  canRevoke,
  onRevoked,
}: {
  identityID: string;
  tokens: APIToken[];
  canRevoke: boolean;
  onRevoked: () => void | Promise<void>;
}) {
  const [revoking, setRevoking] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function handleRevoke(id: string) {
    setRevoking(id);
    setError(null);
    try {
      await revokeManagedIdentityToken(identityID, id);
      await onRevoked();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to revoke token");
    } finally {
      setRevoking(null);
    }
  }

  if (tokens.length === 0) {
    return (
      <p className="py-12 text-center text-sm text-faint">
        No API tokens yet.
      </p>
    );
  }

  return (
    <div className="rounded-lg border border-alpha bg-base-white overflow-x-auto dark:bg-surface">
      {error && <p className="mb-4 px-5 pt-4 text-sm text-ember-500">{error}</p>}
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-alpha text-left">
            <th className="px-5 py-3.5 label-text">Name</th>
            <th className="px-5 py-3.5 label-text">Permissions</th>
            <th className="px-5 py-3.5 label-text">Created</th>
            <th className="px-5 py-3.5 label-text">Expires</th>
            <th className="px-5 py-3.5 label-text"></th>
          </tr>
        </thead>
        <tbody>
          {tokens.map((token) => (
            <tr key={token.id} className="border-b border-alpha last:border-b-0">
              <td className="px-5 py-4 text-primary font-medium">{token.name}</td>
              <td className="px-5 py-4 text-muted">{formatPermissions(token)}</td>
              <td className="px-5 py-4 text-muted font-mono text-xs">
                {new Date(token.createdAt).toLocaleDateString()}
              </td>
              <td className="px-5 py-4 text-muted font-mono text-xs">
                {token.expiresAt
                  ? new Date(token.expiresAt).toLocaleDateString()
                  : "Never"}
              </td>
              <td className="px-5 py-4">
                {canRevoke ? (
                  <Button
                    variant="danger"
                    onClick={() => handleRevoke(token.id)}
                    disabled={revoking === token.id}
                  >
                    {revoking === token.id ? "Revoking..." : "Revoke"}
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
