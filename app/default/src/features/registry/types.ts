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
  platforms?: string[];
  sourceRef?: string;
  sourceUrl?: string;
  publication?: AppAdminPublication;
};

export type AppAdminRegistryResponse = RegistryAppSummary & {
  knownVersions: Array<{
    version: string;
    installedAt?: string;
    installedBy?: string;
  }>;
  publishedVersions: AppAdminPublishedVersion[];
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
