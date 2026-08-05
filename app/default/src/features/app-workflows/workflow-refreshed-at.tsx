import { formatRegistryTime, formatRegistryTimeShort } from "@/features/registry/format";

/**
 * Same chrome as Versions (`Refreshed at 3:15 PM` + refresh control).
 * Uses the registry time formatters so the language stays identical.
 */
export function WorkflowRefreshedAt({
  dataUpdatedAt,
  refreshing,
  testId = "workflow-refreshed-at",
}: {
  dataUpdatedAt: number | null;
  refreshing: boolean;
  testId?: string;
}) {
  if (refreshing) {
    return <p className="text-sm text-muted-foreground">Refreshing…</p>;
  }

  if (!dataUpdatedAt) {
    return null;
  }

  const iso = new Date(dataUpdatedAt).toISOString();

  return (
    <p className="text-sm text-muted-foreground" data-testid={testId}>
      Refreshed at{" "}
      <time dateTime={iso} title={formatRegistryTime(iso)}>
        {formatRegistryTimeShort(dataUpdatedAt)}
      </time>
    </p>
  );
}
