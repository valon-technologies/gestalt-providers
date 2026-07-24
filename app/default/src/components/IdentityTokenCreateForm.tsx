
import { useState } from "react";
import {
  createManagedIdentityToken,
  type AccessPermission,
  type ManagedIdentityGrant,
} from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import { CopyableCode } from "@/components/ui/copyable-code";
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

type ScopeMode = "all" | "restricted";

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
  const [scopeMode, setScopeMode] = useState<ScopeMode>("all");

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const fd = new FormData(form);
    const name = (fd.get("name") as string)?.trim();
    if (!name) return;

    let permissions: AccessPermission[] | undefined;
    if (scopeMode === "restricted") {
      permissions = [];
      for (const grant of grants) {
        if (fd.get(`plugin_${grant.plugin}`)) {
          permissions.push({ plugin: grant.plugin });
          continue;
        }
        const selectedOperations = parseOperations(fd.get(`plugin_ops_${grant.plugin}`));
        if (selectedOperations.length > 0) {
          permissions.push({
            plugin: grant.plugin,
            operations: selectedOperations,
          });
        }
      }

      if (permissions.length === 0) {
        setError("Choose at least one token limit, or use all authorized access.");
        return;
      }
    }

    setCreating(true);
    setError(null);
    setPlaintext(null);

    try {
      const result = await createManagedIdentityToken(identityID, name, permissions);
      setPlaintext(result.token);
      form.reset();
      setScopeMode("all");
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

          <fieldset>
            <legend className="label-text">Token access</legend>
            <p className="mt-2 text-sm text-faint">
              Token limits only narrow what this API key can use. App grants and connector credentials stay on the managed identity.
            </p>
            <div className="mt-3 space-y-3">
              <label className="flex items-start gap-3 rounded-md border border-alpha bg-base-50 p-4 text-sm text-muted dark:bg-surface-raised">
                <input
                  type="radio"
                  name="scope_mode"
                  value="all"
                  checked={scopeMode === "all"}
                  onChange={() => {
                    setScopeMode("all");
                    setError(null);
                  }}
                  className="mt-0.5 h-4 w-4"
                />
                <span>
                  <span className="block font-medium text-primary">All authorized access</span>
                  <span className="mt-1 block text-faint">
                    The token follows managed identity app authorization and connector credentials at use time.
                  </span>
                </span>
              </label>

              <label className="flex items-start gap-3 rounded-md border border-alpha bg-base-50 p-4 text-sm text-muted dark:bg-surface-raised">
                <input
                  type="radio"
                  name="scope_mode"
                  value="restricted"
                  checked={scopeMode === "restricted"}
                  onChange={() => {
                    setScopeMode("restricted");
                    setError(null);
                  }}
                  className="mt-0.5 h-4 w-4"
                />
                <span>
                  <span className="block font-medium text-primary">Restrict this token</span>
                  <span className="mt-1 block text-faint">
                    Limit this API key to selected apps or operations.
                  </span>
                </span>
              </label>
            </div>
          </fieldset>

          {scopeMode === "restricted" && (
            <div>
              <p className="label-text">Token limits</p>
              {grants.length === 0 ? (
                <p className="mt-2 text-sm text-faint">
                  This identity has no visible app grants to choose from. Use all authorized access or add an app grant first.
                </p>
              ) : (
                <div className="mt-3 space-y-4">
                  {grants.map((grant) => {
                    return (
                      <div
                        key={grant.plugin}
                        className="rounded-md border border-alpha bg-base-50 p-4 dark:bg-surface-raised"
                      >
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <p className="text-sm font-medium text-primary">{grant.plugin}</p>
                          <span className="rounded-full border border-alpha px-2.5 py-1 text-xs text-faint">
                            {grant.role}
                          </span>
                        </div>
                        <label className="mt-3 flex items-center gap-2 text-sm text-muted">
                          <input
                            type="checkbox"
                            name={`plugin_${grant.plugin}`}
                            className="h-4 w-4"
                          />
                          Limit this token to all authorized {grant.plugin} operations
                        </label>
                        <div className="mt-3">
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
                            Enter operation names for operation-level limits, or check all authorized operations for this app.
                          </p>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}

          <div className="flex items-center gap-3">
            <Button type="submit" disabled={creating}>
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
          <CopyableCode value={plaintext} className="mt-3 max-w-full" />
        </div>
      )}

      {error && <p className="mt-4 text-sm text-ember-500">{error}</p>}
    </>
  );
}
