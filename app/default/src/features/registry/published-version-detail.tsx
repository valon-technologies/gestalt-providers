import type { ReactNode } from "react";
import type { AppAdminPublishedVersion } from "@/features/registry/types";
import {
  formatRegistryTime,
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

export function PublishedVersionDetail({
  version,
}: {
  version: AppAdminPublishedVersion;
}) {
  const shortRef = shortenSourceRef(version.sourceRef);
  const publication = version.publication;

  return (
    <div
      className="space-y-2 text-sm text-muted"
      data-testid="published-version-detail"
    >
      <p data-testid="published-version-summary">
        Published {formatRegistryTime(version.publishedAt)}
        {version.platforms?.length ? ` · ${version.platforms.join(", ")}` : ""}
      </p>
      <p className="flex flex-wrap gap-x-3 gap-y-1">
        {version.sourceUrl && shortRef ? (
          <span>
            <ExternalLink href={version.sourceUrl}>Commit {shortRef}</ExternalLink>
          </span>
        ) : shortRef ? (
          <span>Commit {shortRef}</span>
        ) : null}
        {publication?.triggerPullRequest ? (
          <span>
            <ExternalLink href={publication.triggerPullRequest.url}>
              PR #{publication.triggerPullRequest.number}
            </ExternalLink>
          </span>
        ) : publication?.triggerCommit ? (
          <span>
            <ExternalLink href={publication.triggerCommit.url}>
              commit {shortenSourceRef(publication.triggerCommit.sha)}
            </ExternalLink>
          </span>
        ) : (
          <span data-testid="published-version-pr">PR: not recorded</span>
        )}
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
