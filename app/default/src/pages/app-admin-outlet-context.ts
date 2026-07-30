import type { AppAdminRegistryResponse } from "@/features/registry/types";

export type AppAdminOutletContext = {
  appName: string;
  registry: AppAdminRegistryResponse;
  appMountedPath?: string;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
  deployError: string | null;
  checkForNewVersions: () => void;
  isCheckingForNewVersions: boolean;
  registryUpdatedAt: number | null;
};
