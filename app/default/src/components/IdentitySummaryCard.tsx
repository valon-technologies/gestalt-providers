
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
      className="group rounded-lg border border-alpha bg-base-100 p-6 transition-all duration-150 hover:border-alpha-strong hover:shadow-card dark:bg-surface"
    >
      <div className="flex items-start justify-between gap-4">
        <div>
          <span className="label-text">Managed Identity</span>
          <h2 className="mt-2 text-lg font-heading text-primary">
            {identity.displayName}
          </h2>
          <p className="mt-2 font-mono text-xs text-muted">{identity.subjectId}</p>
        </div>
        <span className="rounded-full border border-alpha px-3 py-1 text-xs uppercase tracking-[0.16em] text-faint">
          {identity.kind}
        </span>
      </div>
      <p className="mt-4 text-sm text-muted group-hover:text-primary transition-colors duration-150">
        Open identity
        <span className="inline-block ml-1 transition-transform duration-150 group-hover:translate-x-0.5">
          &rarr;
        </span>
      </p>
    </Link>
  );
}
