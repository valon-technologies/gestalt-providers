import { useState } from "react";
import { INPUT_CLASSES } from "@/lib/constants";
import { useCreateTokenMutation } from "@/lib/queries";
import Button from "./Button";

const daySeconds = 24 * 60 * 60;
const tokenLifetimeSeconds: Record<string, number> = {
  "30d": 30 * daySeconds,
  "90d": 90 * daySeconds,
  "365d": 365 * daySeconds,
};
const defaultTokenLifetime = "30d";

export default function TokenCreateForm() {
  const createToken = useCreateTokenMutation();
  const [plaintext, setPlaintext] = useState<string | null>(null);
  const error = createToken.error
    ? createToken.error instanceof Error
      ? createToken.error.message
      : "Failed to create token"
    : null;

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const formData = new FormData(form);
    const name = (formData.get("name") as string)?.trim();
    const scopes = (formData.get("scopes") as string)?.trim();
    const lifetime = (formData.get("lifetime") as string) ?? defaultTokenLifetime;
    if (!name) return;

    setPlaintext(null);

    try {
      const result = await createToken.mutateAsync({
        name,
        scopes,
        expiresIn:
          tokenLifetimeSeconds[lifetime] ?? tokenLifetimeSeconds[defaultTokenLifetime],
      });
      setPlaintext(result.token);
      form.reset();
    } catch {
      // surfaced via mutation error state
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
            htmlFor="token-lifetime"
            className="label-text block"
          >
            Lifetime
          </label>
          <select
            id="token-lifetime"
            name="lifetime"
            defaultValue={defaultTokenLifetime}
            className={`mt-2 w-full ${INPUT_CLASSES}`}
          >
            <option value="30d">30 days</option>
            <option value="90d">90 days</option>
            <option value="365d">1 year</option>
          </select>
        </div>
        <Button type="submit" disabled={createToken.isPending} className="sm:shrink-0">
          {createToken.isPending ? "Creating..." : "Create Token"}
        </Button>
      </form>

      {plaintext && (
        <div className="mt-6 rounded-lg border border-warning-foreground/40 bg-warning p-5 text-warning-foreground">
          <p className="text-sm font-medium">
            Copy this token now. It will not be shown again.
          </p>
          <code className="mt-3 block break-all rounded-sm border border-border bg-background p-3 font-mono text-sm text-foreground">
            {plaintext}
          </code>
        </div>
      )}

      {error && <p className="mt-4 text-sm text-destructive">{error}</p>}
    </>
  );
}
