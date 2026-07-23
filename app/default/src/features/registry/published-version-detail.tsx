import type { ReactNode } from "react";
import type { AppAdminPublishedVersion } from "@/features/registry/types";
import {
  formatRegistryTime,
  formatRegistryTimeAgo,
  shortenSourceRef,
} from "@/features/registry/format";

function ExternalLink({
  href,
  children,
}: {
  href: string;
  children: ReactNode;
}) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      className="text-gold-700 underline decoration-gold-300 underline-offset-2 hover:text-gold-800 dark:text-gold-300 dark:hover:text-gold-200"
    >
      {children}
    </a>
  );
}

function publishedCommitRef(version: AppAdminPublishedVersion): {
  href?: string;
  label: string;
} | null {
  const publication = version.publication;
  const sourceRef = shortenSourceRef(version.sourceRef);
  const triggerCommitRef = shortenSourceRef(publication?.triggerCommit?.sha);
  const label = sourceRef || triggerCommitRef;
  if (!label) return null;
  return {
    href: version.sourceUrl || publication?.triggerCommit?.url,
    label,
  };
}

export function PublishedVersionDetail({
  version,
}: {
  version: AppAdminPublishedVersion;
}) {
  const publication = version.publication;
  const publishedAgo = formatRegistryTimeAgo(version.publishedAt);
  const pullRequest = publication?.triggerPullRequest;
  const commit = publishedCommitRef(version);

  return (
    <div
      className="space-y-2 text-sm text-muted"
      data-testid="published-version-detail"
    >
      <p data-testid="published-version-summary">
        Published{" "}
        {publishedAgo ? (
          <>
            {publishedAgo}
            <span className="text-faint"> ({formatRegistryTime(version.publishedAt)})</span>
          </>
        ) : (
          formatRegistryTime(version.publishedAt)
        )}
        {version.platforms?.length ? ` · ${version.platforms.join(", ")}` : ""}
      </p>
      <p className="flex flex-wrap gap-x-3 gap-y-1">
        {pullRequest ? (
          <span>
            <ExternalLink href={pullRequest.url}>
              PR #{pullRequest.number}
              {pullRequest.title ? ` · ${pullRequest.title}` : ""}
            </ExternalLink>
          </span>
        ) : (
          <span data-testid="published-version-pr">PR: not recorded</span>
        )}
        {commit ? (
          <span>
            {commit.href ? (
              <ExternalLink href={commit.href}>Commit {commit.label}</ExternalLink>
            ) : (
              <>Commit {commit.label}</>
            )}
          </span>
        ) : null}
        {publication?.workflowRunUrl ? (
          <span>
            <ExternalLink href={publication.workflowRunUrl}>
              workflow run
            </ExternalLink>
          </span>
        ) : (
          <span data-testid="published-version-workflow">
            workflow: not recorded
          </span>
        )}
      </p>
    </div>
  );
}
