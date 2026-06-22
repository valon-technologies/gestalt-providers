"use client";

import { useState } from "react";
import { createToken } from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import Button from "./Button";

interface TokenCreateFormProps {
  onCreated: () => void | Promise<void>;
}

const EXPIRY_OPTIONS = [
  { label: "7 days", value: 7 * 86400 },
  { label: "30 days", value: 30 * 86400 },
  { label: "90 days", value: 90 * 86400 },
  { label: "1 year", value: 365 * 86400 },
];

export default function TokenCreateForm({ onCreated }: TokenCreateFormProps) {
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [plaintext, setPlaintext] = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const formData = new FormData(form);
    const name = (formData.get("name") as string)?.trim();
    const scopes = (formData.get("scopes") as string)?.trim();
    const expiresInStr = (formData.get("expiresIn") as string)?.trim();
    if (!name) return;
    const expiresIn = expiresInStr ? parseInt(expiresInStr, 10) : undefined;

    setCreating(true);
    setError(null);
    setPlaintext(null);

    try {
      const result = await createToken(name, scopes, expiresIn);
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
      <form onSubmit={handleSubmit} className="flex flex-col gap-3 sm:flex-row sm:items-end">
        <div className="flex-1">
          <label
            htmlFor="token-name"
            className="label-text block"
          >
            Token name
          </label>
          <input
            id="token-name"
            name="name"
            type="text"
            required
            placeholder="e.g. ci-pipeline"
            className={`mt-2 w-full ${INPUT_CLASSES}`}
          />
        </div>
        <div className="flex-1">
          <label
            htmlFor="token-scopes"
            className="label-text block"
          >
            Scopes (optional)
          </label>
          <input
            id="token-scopes"
            name="scopes"
            type="text"
            placeholder="blank = full identity, or my-app / my-app:operation"
            className={`mt-2 w-full ${INPUT_CLASSES}`}
          />
        </div>
        <div className="sm:w-40">
          <label
            htmlFor="token-expiry"
            className="label-text block"
          >
            Expires in
          </label>
          <select
            id="token-expiry"
            name="expiresIn"
            defaultValue={EXPIRY_OPTIONS[1].value.toString()}
            className={`mt-2 w-full ${INPUT_CLASSES}`}
          >
            {EXPIRY_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </div>
        <Button type="submit" disabled={creating} className="sm:shrink-0">
          {creating ? "Creating..." : "Create Token"}
        </Button>
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
