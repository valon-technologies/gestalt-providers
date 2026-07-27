
import { Link } from "@tanstack/react-router";
import type { ManagedIdentity } from "@/lib/api";

export default function IdentitySummaryCard({
  identity,
}: {
  identity: ManagedIdentity;
}) {
  return (
    <Link
      to="/identities"
      search={{ id: identity.subjectId }}
      className="group rounded-lg border border-border bg-muted p-6 transition-all duration-150 hover:border-input hover:shadow-card dark:bg-card"
    >
      <div className="flex items-start justify-between gap-4">
        <div>
          <span className="label-text">Managed Identity</span>
          <h2 className="mt-2 text-lg font-heading text-foreground">
            {identity.displayName}
          </h2>
          <p className="mt-2 font-mono text-xs text-muted-foreground">{identity.subjectId}</p>
        </div>
        <span className="rounded-full border border-border px-3 py-1 text-xs uppercase tracking-[0.16em] text-muted-foreground/70">
          {identity.kind}
        </span>
      </div>
      <p className="mt-4 text-sm text-muted-foreground group-hover:text-foreground transition-colors duration-150">
        Open identity
        <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
          &rarr;
        </span>
      </p>
    </Link>
  );
}
