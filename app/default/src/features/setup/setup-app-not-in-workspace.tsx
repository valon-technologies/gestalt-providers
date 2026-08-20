import { Badge } from "@/components/ui/badge";
import { SearchHighlight } from "@/components/ui/search-highlight";
import IntegrationIcon from "@/components/IntegrationIcon";
import { companionAppLabel } from "@/lib/buildPaths";

/** Catalog tile for a Setup app reference this workspace does not have. */
export function SetupAppNotInWorkspaceNotice({
  appId,
  query = "",
}: {
  appId: string;
  query?: string;
}) {
  const missingLabel = companionAppLabel(appId);
  return (
    <div className="h-full rounded-xl bg-neutral-hover p-3 text-foreground">
      <div className="flex items-start gap-3">
        <IntegrationIcon name={appId} displayName={missingLabel} size="md" />
        <div className="min-w-0 flex-1">
          <p className="text-sm font-heading text-foreground">
            <SearchHighlight text={missingLabel} query={query} variant="vivid" />
          </p>
          <Badge variant="warning" size="sm" className="mt-2">
            Not in workspace
          </Badge>
        </div>
      </div>
    </div>
  );
}
