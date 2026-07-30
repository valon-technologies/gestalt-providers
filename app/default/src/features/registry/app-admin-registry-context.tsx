import { createContext, useContext } from "react";
import type { AppAdminRegistryResponse } from "@/features/registry/types";

export type AppAdminRegistryContextValue = {
  appName: string;
  registry: AppAdminRegistryResponse;
  appMountedPath?: string;
  deployingVersion: string | null;
  onDeployVersion: (version: string) => void;
  deployError: string | null;
  registryError: string | null;
  checkForNewVersions: () => void;
  isCheckingForNewVersions: boolean;
  registryUpdatedAt: number | null;
};

const AppAdminRegistryContext = createContext<AppAdminRegistryContextValue | null>(null);

export function AppAdminRegistryProvider({
  value,
  children,
}: {
  value: AppAdminRegistryContextValue;
  children: React.ReactNode;
}) {
  return (
    <AppAdminRegistryContext.Provider value={value}>
      {children}
    </AppAdminRegistryContext.Provider>
  );
}

export function useAppAdminRegistryContext() {
  const context = useContext(AppAdminRegistryContext);
  if (!context) {
    throw new Error("useAppAdminRegistryContext requires AppAdminRegistryProvider");
  }
  return context;
}
