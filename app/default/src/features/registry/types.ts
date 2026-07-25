export type RegistryRollout = {
  version: string;
  state: string;
  createdAt?: string;
  enrollmentEndsAt?: string;
  deadline?: string;
  completedAt?: string;
  failedAt?: string;
};

export type RegistryAppSummary = {
  app: string;
  registry: string;
  desiredVersion?: string;
  rollout?: RegistryRollout;
  cohort?: {
    acknowledged: number;
    materialized: number;
    restarted: number;
    failed: number;
  };
};

export type RegistryAppDetail = RegistryAppSummary & {
  knownVersions: Array<{
    version: string;
    installedAt?: string;
    installedBy?: string;
  }>;
  latestPublished?: {
    version: string;
    publishedAt: string;
  };
};

export type AppAdminPublicationPullRequest = {
  number: number;
  url: string;
  title?: string;
};

export type AppAdminPublicationCommit = {
  sha: string;
  url: string;
};

export type AppAdminPublication = {
  workflowRunUrl?: string;
  triggerPullRequest?: AppAdminPublicationPullRequest;
  triggerCommit?: AppAdminPublicationCommit;
};

export type AppAdminPublishedVersion = {
  version: string;
  publishedAt: string;
  publishStartedAt?: string;
  publishDurationSeconds?: number;
  platforms?: string[];
  sourceRef?: string;
  sourceUrl?: string;
  publication?: AppAdminPublication;
};

export type AppAdminPendingVersion = {
  version: string;
  startedAt: string;
  updatedAt: string;
  phase: string;
  publishingForSeconds?: number;
  sourceRef?: string;
  sourceUrl?: string;
  publication?: AppAdminPublication;
};

export type AppAdminFailedVersion = {
  version: string;
  startedAt: string;
  failedAt: string;
  reason: string;
  publishDurationSeconds?: number;
  sourceRef?: string;
  sourceUrl?: string;
  publication?: AppAdminPublication;
};

export type AppAdminSnapshotRow =
  | {
      kind: "published";
      version: string;
      sortAt: string;
      published: AppAdminPublishedVersion;
    }
  | {
      kind: "pending";
      version: string;
      sortAt: string;
      pending: AppAdminPendingVersion;
    }
  | {
      kind: "failed";
      version: string;
      sortAt: string;
      failed: AppAdminFailedVersion;
    };

export type AppAdminRegistryResponse = RegistryAppSummary & {
  knownVersions: Array<{
    version: string;
    installedAt?: string;
    installedBy?: string;
  }>;
  publishedVersions: AppAdminPublishedVersion[];
  pendingVersions?: AppAdminPendingVersion[];
  failedVersions?: AppAdminFailedVersion[];
  selectionDisabled: boolean;
  disabledReason?: string;
};

export type AppAdminRegistryVersionResponse = {
  app: string;
  registry: string;
  fromVersion?: string;
  desiredVersion: string;
  rollout: RegistryRollout;
};
