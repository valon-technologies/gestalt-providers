"use client";

import { useState } from "react";
import {
  createManagedIdentityToken,
  type AccessPermission,
  type ManagedIdentityGrant,
} from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import Button from "./Button";

function uniqueOperations(operations?: string[]): string[] {
  if (!operations?.length) return [];
  return Array.from(new Set(operations)).sort();
}

function parseOperations(value: FormDataEntryValue | null): string[] {
  if (typeof value !== "string") return [];
  return uniqueOperations(
    value
      .split(",")
      .map((operation) => operation.trim())
      .filter(Boolean),
  );
}

export default function IdentityTokenCreateForm({
  identityID,
  grants,
  onCreated,
}: {
  identityID: string;
  grants: ManagedIdentityGrant[];
  onCreated: () => void | Promise<void>;
}) {
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [plaintext, setPlaintext] = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const fd = new FormData(form);
    const name = (fd.get("name") as string)?.trim();
    if (!name) return;

    const permissions: AccessPermission[] = [];
    for (const grant of grants) {
      const operations = uniqueOperations(grant.operations);
      if (fd.get(`plugin_${grant.plugin}`)) {
        permissions.push({ plugin: grant.plugin });
        continue;
      }
      if (operations.length === 0) {
        const narrowedOperations = parseOperations(fd.get(`plugin_ops_${grant.plugin}`));
        if (narrowedOperations.length > 0) {
          permissions.push({
            plugin: grant.plugin,
            operations: narrowedOperations,
          });
        } else if (fd.get(`plugin_${grant.plugin}`)) {
          permissions.push({ plugin: grant.plugin });
        }
        continue;
      }

      const selectedOperations = operations.filter((operation) =>
        fd.get(`op_${grant.plugin}_${operation}`),
      );
      if (selectedOperations.length > 0) {
        permissions.push({
          plugin: grant.plugin,
          operations: selectedOperations,
        });
      }
    }

    if (permissions.length === 0) {
      setError("Select at least one permission for the token.");
      return;
    }

    setCreating(true);
    setError(null);
    setPlaintext(null);

    try {
      const result = await createManagedIdentityToken(identityID, name, permissions);
      setPlaintext(result.token);
      form.reset();
      await onCreated();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create token");
    } finally {
      setCreating(false);
    }
  }

  return (
    <>
      <form onSubmit={handleSubmit} className="mt-6 rounded-lg border border-alpha bg-base-white p-5 dark:bg-surface">
        <div className="flex flex-col gap-4">
          <div>
            <label htmlFor="identity-token-name" className="label-text block">
              Token name
            </label>
            <input
              id="identity-token-name"
              name="name"
              type="text"
              required
              placeholder="e.g. release-bot"
              className={`mt-2 w-full ${INPUT_CLASSES}`}
            />
          </div>

          <div>
            <p className="label-text">Permissions</p>
            {grants.length === 0 ? (
              <p className="mt-2 text-sm text-faint">
                This identity has no visible grants yet.
              </p>
            ) : (
              <div className="mt-3 space-y-4">
                {grants.map((grant) => {
                  const operations = uniqueOperations(grant.operations);
                  return (
                    <div
                      key={grant.plugin}
                      className="rounded-md border border-alpha bg-base-50 p-4 dark:bg-surface-raised"
                    >
                      <p className="text-sm font-medium text-primary">{grant.plugin}</p>
                      <label className="mt-3 flex items-center gap-2 text-sm text-muted">
                        <input
                          type="checkbox"
                          name={`plugin_${grant.plugin}`}
                          className="h-4 w-4"
                        />
                        Grant full access to {grant.plugin}
                      </label>
                      {operations.length === 0 ? (
                        <div className="mt-3 space-y-3">
                          <div>
                            <label
                              htmlFor={`plugin-ops-${grant.plugin}`}
                              className="label-text block"
                            >
                              Operations for {grant.plugin}
                            </label>
                            <input
                              id={`plugin-ops-${grant.plugin}`}
                              name={`plugin_ops_${grant.plugin}`}
                              type="text"
                              placeholder="Optional, comma-separated"
                              className={`mt-2 w-full ${INPUT_CLASSES}`}
                            />
                            <p className="mt-2 text-xs text-faint">
                              Leave blank and check full access to mint a plugin-wide token.
                            </p>
                          </div>
                        </div>
                      ) : (
                        <>
                          <div className="mt-3 flex flex-wrap gap-3">
                            {operations.map((operation) => (
                              <label
                                key={operation}
                                className="flex items-center gap-2 text-sm text-muted"
                              >
                                <input
                                  type="checkbox"
                                  name={`op_${grant.plugin}_${operation}`}
                                  className="h-4 w-4"
                                />
                                {operation}
                              </label>
                            ))}
                          </div>
                          <p className="mt-2 text-xs text-faint">
                            Leave full access unchecked to mint an operation-scoped token.
                          </p>
                        </>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          <div className="flex items-center gap-3">
            <Button type="submit" disabled={creating || grants.length === 0}>
              {creating ? "Creating..." : "Create Token"}
            </Button>
          </div>
        </div>
      </form>

      {plaintext && (
        <div className="mt-6 rounded-lg border border-gold-300 bg-gold-50 p-5 dark:border-gold-700 dark:bg-gold-950/30">
          <p className="text-sm font-medium text-gold-800 dark:text-gold-300">
            Copy this token now. It will not be shown again.
          </p>
          <code className="mt-3 block break-all rounded-sm bg-base-white p-3 font-mono text-sm text-primary border border-alpha dark:bg-surface">
            {plaintext}
          </code>
        </div>
      )}

      {error && <p className="mt-4 text-sm text-ember-500">{error}</p>}
    </>
  );
}
