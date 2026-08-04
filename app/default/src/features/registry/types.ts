export type RegistryRollout = {
  version: string;
  state: string;
  targetSourceVersion?: string;
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

export type AppAdminAutoDeploy = {
  enabled: boolean;
  pendingVersion?: string;
  lastError?: string;
};

export type AppAdminFleetReplicaClass =
  | "on_desired"
  | "mismatched"
  | "error"
  | (string & {});

export type AppAdminFleetReplica = {
  instanceId: string;
  startedAt?: string;
  heartbeatAt: string;
  appState: string;
  runningVersion?: string;
  observedDesiredVersion?: string;
  lastError?: string;
  class: AppAdminFleetReplicaClass;
};

export type AppAdminFleetState = {
  state: string;
  sourceVersion?: string;
  desiredVersion?: string;
  minimumHealthyInstances: number;
  liveInstances: number;
  runningDesiredVersion: number;
  mismatched: number;
  errors: number;
  heartbeatTtlSeconds: number;
  evaluatedAt: string;
  /** Live replicas on the current source version (same pass as the aggregates). */
  replicas?: AppAdminFleetReplica[];
};

export type AppAdminRecovery = {
  recoveredAt: string;
  sourceVersion: string;
  liveInstances: number;
  minimumHealthyInstances: number;
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
  fleetState?: AppAdminFleetState;
  recovery?: AppAdminRecovery;
  autoDeploy: AppAdminAutoDeploy;
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

export type AppAdminRegistryRevision = {
  id: string;
  version: string;
  previousVersion?: string;
  deployedAt: string;
  deployedBy?: string;
  sourceRef?: string;
  sourceUrl?: string;
  publication?: AppAdminPublication;
  deploymentState?: string;
  deployableUntil?: string;
  current?: boolean;
  rolloutState?: string;
  rolloutForSeconds?: number;
  rolloutDurationSeconds?: number;
  rolloutCompletedAt?: string;
  rolloutFailedAt?: string;
  recovery?: AppAdminRecovery;
};

export type AppAdminRegistryHistoryResponse = {
  app: string;
  revisions: AppAdminRegistryRevision[];
  fleetState?: AppAdminFleetState;
  nextCursor?: string;
};
