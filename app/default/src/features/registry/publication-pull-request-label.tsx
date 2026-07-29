import type { AppAdminPublicationPullRequest } from "@/features/registry/types";
import { SearchHighlight } from "@/components/ui/search-highlight";

export const REGISTRY_TABLE_LINK_CLASS =
  "font-medium text-primary underline decoration-primary underline-offset-2 hover:text-primary";

export function PublicationPullRequestLabel({
  pullRequest,
  linkClassName = REGISTRY_TABLE_LINK_CLASS,
  titleClassName = "text-muted-foreground",
}: {
  pullRequest?: AppAdminPublicationPullRequest;
  linkClassName?: string;
  titleClassName?: string;
}) {
  if (!pullRequest?.number) return null;

  const title = pullRequest.title?.trim();
  const numberLabel = `PR #${pullRequest.number}`;

  return (
    <span className="inline-flex flex-wrap items-baseline gap-x-2">
      {pullRequest.url ? (
        <a
          href={pullRequest.url}
          target="_blank"
          rel="noreferrer"
          className={linkClassName}
        >
          <SearchHighlight text={numberLabel} />
        </a>
      ) : (
        <span className={linkClassName}>
          <SearchHighlight text={numberLabel} />
        </span>
      )}
      {title ? (
        <>
          <span className="text-muted-foreground" aria-hidden="true">
            ·
          </span>
          <span className={titleClassName}>
            <SearchHighlight text={title} />
          </span>
        </>
      ) : null}
    </span>
  );
}
