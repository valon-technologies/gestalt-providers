import type { AppAdminPublicationPullRequest } from "@/features/registry/types";

export const REGISTRY_TABLE_LINK_CLASS =
  "font-medium text-gold-700 underline decoration-gold-300 underline-offset-2 hover:text-gold-800 dark:text-gold-300";

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
    <span className="inline-flex flex-wrap items-baseline gap-x-1.5">
      {pullRequest.url ? (
        <a
          href={pullRequest.url}
          target="_blank"
          rel="noreferrer"
          className={linkClassName}
        >
          {numberLabel}
        </a>
      ) : (
        <span className={linkClassName}>{numberLabel}</span>
      )}
      {title ? (
        <>
          <span className="text-muted-foreground" aria-hidden="true">
            ·
          </span>
          <span className={titleClassName}>{title}</span>
        </>
      ) : null}
    </span>
  );
}
